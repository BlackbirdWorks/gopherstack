package timestreamquery

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

// Catalog reports which Timestream Write databases and tables exist in a region.
type Catalog interface {
	DatabaseExists(region, database string) bool
	TableExists(region, database, table string) bool
}

// SetCatalog wires the Timestream Write catalog; unset, table references are not checked.
func (b *InMemoryBackend) SetCatalog(c Catalog) {
	b.mu.Lock("SetCatalog")
	defer b.mu.Unlock()
	b.catalog = c
}

func (b *InMemoryBackend) catalogLookup() Catalog {
	b.mu.RLock("catalogLookup")
	defer b.mu.RUnlock()

	return b.catalog
}

// checkTableRefs fails with ValidationException when a database.table reference in queryString is missing.
func (b *InMemoryBackend) checkTableRefs(ctx context.Context, queryString string) error {
	cat := b.catalogLookup()
	if cat == nil {
		return nil
	}

	region := getRegion(ctx, b.defaultRegion)

	for _, ref := range tableRefs(queryString) {
		line, col := lineCol(queryString, ref.offset)

		if !cat.DatabaseExists(region, ref.database) {
			return fmt.Errorf("%w: line %d:%d: Database '%s' does not exist", ErrValidation, line, col, ref.database)
		}

		if !cat.TableExists(region, ref.database, ref.table) {
			return fmt.Errorf(
				"%w: line %d:%d: Table '%s.%s' does not exist",
				ErrValidation, line, col, ref.database, ref.table,
			)
		}
	}

	return nil
}

const (
	qualifiedParts     = 2
	closingQuoteOffset = 2
)

type tableRef struct {
	database string
	table    string
	offset   int
}

func lineCol(s string, offset int) (int, int) {
	prefix := s[:offset]
	line := strings.Count(prefix, "\n") + 1
	col := offset - strings.LastIndex(prefix, "\n")

	return line, col
}

// tableRefs returns the database-qualified tables named after FROM or JOIN, skipping string literals.
func tableRefs(q string) []tableRef {
	var refs []tableRef

	upper := strings.ToUpper(q)
	inString := false

	for i := 0; i < len(q); i++ {
		if q[i] == '\'' {
			inString = !inString

			continue
		}

		if inString || !isWordStart(q, i) {
			continue
		}

		word := wordAt(upper, i)
		if word == "FROM" || word == "JOIN" {
			if ref, ok := parseRefAfter(q, i+len(word)); ok {
				refs = append(refs, ref)
			}
		}

		i += len(word) - 1
	}

	return refs
}

func isIdentByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
}

func isWordStart(s string, i int) bool {
	return isIdentByte(s[i]) && (i == 0 || !isIdentByte(s[i-1]))
}

func wordAt(s string, i int) string {
	j := i
	for j < len(s) && isIdentByte(s[j]) {
		j++
	}

	return s[i:j]
}

// parseRefAfter parses a two-part identifier starting after pos; one-part names (CTEs) and subqueries are skipped.
func parseRefAfter(q string, pos int) (tableRef, bool) {
	i := pos
	for i < len(q) && unicode.IsSpace(rune(q[i])) {
		i++
	}

	start := i

	var parts []string

	for {
		part, next, ok := parseIdent(q, i)
		if !ok {
			return tableRef{}, false
		}

		parts = append(parts, part)
		i = next

		if i < len(q) && q[i] == '.' {
			i++

			continue
		}

		break
	}

	if len(parts) != qualifiedParts {
		return tableRef{}, false
	}

	return tableRef{database: parts[0], table: parts[1], offset: start}, true
}

func parseIdent(q string, i int) (string, int, bool) {
	if i >= len(q) {
		return "", i, false
	}

	if q[i] == '"' {
		end := strings.IndexByte(q[i+1:], '"')
		if end < 0 {
			return "", i, false
		}

		return q[i+1 : i+1+end], i + end + closingQuoteOffset, true
	}

	j := i
	for j < len(q) && isIdentByte(q[j]) {
		j++
	}

	if j == i {
		return "", i, false
	}

	return q[i:j], j, true
}
