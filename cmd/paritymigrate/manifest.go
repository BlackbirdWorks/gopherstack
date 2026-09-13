package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const parityFileName = "PARITY.md"

// topLevelKeyRe matches a front-matter key at column 0. Mirrors
// cmd/gendocs/parser.go's topLevelKeyRe and cmd/staleclaims/manifest.go's own
// copy of it -- same file shape, each tool keeps its own copy deliberately
// (see cmd/parityfmtcheck's package doc for why: independently-maintained
// small copies don't drift the way a shared reserved-key list would).
var topLevelKeyRe = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*):(.*)$`)

// listItemRe matches a claim-list bullet, e.g. "  - some text". Mirrors
// cmd/gendocs/parser.go's listItemRe.
var listItemRe = regexp.MustCompile(`^\s*-\s+(.*)$`)

// sourceField is a front-matter list field this tool migrates FROM.
// deferred: is deliberately excluded -- per services/_PARITY_TEMPLATE.md and
// cmd/staleclaims/manifest.go's isClaimField, it records audit SCOPE
// ("consciously not audited this pass"), not a fix-status claim, so folding
// it into items_still_open would misrepresent an unaudited item as a known
// open gap.
func sourceFields() []string { return []string{"gaps", "residual_gaps"} }

const destField = "items_still_open"

// manifest is one services/<svc>/PARITY.md.
type manifest struct {
	dest       *block
	service    string
	path       string
	lines      []string
	sources    []block
	frontStart int
	frontEnd   int
}

// block is one top-level list field's extent plus its parsed items.
type block struct {
	field    string
	items    []item
	keyLine  int
	end      int
	isInline bool
}

// item is one bullet within a block, keeping its exact raw lines so a
// migration can relocate or delete it without re-serializing (and risking
// mis-escaping) its text.
type item struct {
	text     string
	rawLines []string
	start    int
	end      int
}

func discoverManifests(servicesDir string) ([]manifest, error) {
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", servicesDir, err)
	}

	var slugs []string
	for _, e := range entries {
		if e.IsDir() {
			slugs = append(slugs, e.Name())
		}
	}
	sort.Strings(slugs)

	manifests := make([]manifest, 0, len(slugs))
	for _, slug := range slugs {
		// filepath.Clean breaks gosec's G703 (path traversal via taint
		// analysis) dataflow chain, matching cmd/gendocs/readmetable.go's
		// established pattern for a locally-controlled (not user-supplied) path.
		path := filepath.Clean(filepath.Join(servicesDir, slug, parityFileName))

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				continue
			}

			return nil, fmt.Errorf("read %s: %w", path, readErr)
		}

		manifests = append(manifests, parseManifest(slug, path, string(data)))
	}

	return manifests, nil
}

func parseManifest(service, path, content string) manifest {
	lines := strings.Split(content, "\n")
	frontStart, frontEnd := extractFrontmatterRange(lines)

	m := manifest{service: service, path: path, lines: lines, frontStart: frontStart, frontEnd: frontEnd}

	for _, f := range sourceFields() {
		if b := findBlock(lines, frontStart, frontEnd, f); b != nil {
			m.sources = append(m.sources, *b)
		}
	}

	sort.Slice(m.sources, func(i, j int) bool { return m.sources[i].keyLine < m.sources[j].keyLine })

	m.dest = findBlock(lines, frontStart, frontEnd, destField)

	return m
}

// extractFrontmatterRange mirrors cmd/gendocs/parser.go's extractFrontmatter.
func extractFrontmatterRange(lines []string) (int, int) {
	start := 0
	if len(lines) > 0 && strings.TrimSpace(lines[0]) == "---" {
		start = 1
	}

	for i := start; i < len(lines); i++ {
		t := strings.TrimSpace(lines[i])
		if t == "---" || strings.HasPrefix(t, "## ") {
			return start, i
		}
	}

	return start, len(lines)
}

// findBlock locates field's block within lines[frontStart:frontEnd] and
// parses its items, folding continuation lines the same way
// cmd/gendocs/parser.go's parseListBlock does -- except a "#"-prefixed
// comment line is NEVER folded into an item (see item's doc comment): this
// tool must be able to delete an item's own lines without ever deleting a
// human-written standalone comment.
func findBlock(lines []string, frontStart, frontEnd int, field string) *block {
	for i := frontStart; i < frontEnd; i++ {
		m := topLevelKeyRe.FindStringSubmatch(lines[i])
		if m == nil || m[1] != field {
			continue
		}

		end := frontEnd
		for j := i + 1; j < frontEnd; j++ {
			if km := topLevelKeyRe.FindStringSubmatch(lines[j]); km != nil {
				end = j

				break
			}
		}

		b := block{field: field, keyLine: i, end: end}
		if cleanScalar(m[2]) == "[]" {
			b.isInline = true

			return &b
		}

		b.items = parseItems(lines, i+1, end)

		return &b
	}

	return nil
}

// parseItems walks lines[start:end) grouping each "- ..." bullet with any
// immediately-following non-blank, non-comment, non-bullet lines (a wrapped
// multi-line quoted string -- confirmed live in services/apigatewayv2/PARITY.md's
// gaps: block). Blank lines and "#"-prefixed comments are never part of an
// item and are left untouched by a later rewrite.
func parseItems(lines []string, start, end int) []item {
	var items []item

	i := start
	for i < end {
		m := listItemRe.FindStringSubmatch(lines[i])
		if m == nil {
			i++

			continue
		}

		itemStart := i
		texts := []string{m[1]}
		i++

		for i < end {
			t := strings.TrimSpace(lines[i])
			if t == "" || strings.HasPrefix(t, "#") || listItemRe.MatchString(lines[i]) {
				break
			}

			texts = append(texts, t)
			i++
		}

		items = append(items, item{
			start:    itemStart,
			end:      i,
			rawLines: append([]string(nil), lines[itemStart:i]...),
			text:     cleanListItem(strings.Join(texts, " ")),
		})
	}

	return items
}

// cleanListItem mirrors cmd/gendocs/parser.go's cleanListItem: strips a
// wrapping pair of double quotes and unescapes \" to ".
func cleanListItem(raw string) string {
	v := strings.TrimSpace(raw)
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		v = strings.ReplaceAll(v[1:len(v)-1], `\"`, `"`)
	}

	return v
}

// cleanScalar mirrors cmd/parityfmtcheck/check.go's cleanScalar.
func cleanScalar(raw string) string {
	v := strings.TrimSpace(raw)
	if strings.HasPrefix(v, "#") {
		return ""
	}
	if idx := strings.Index(v, " #"); idx >= 0 {
		v = strings.TrimSpace(v[:idx])
	}

	return strings.Trim(v, `"'`)
}

// otherText renders m's full file text with lines [excludeStart, excludeEnd)
// blanked out, so a token search over the result never matches the very
// claim being evaluated -- only genuinely elsewhere-in-the-file mentions.
func (m manifest) otherText(excludeStart, excludeEnd int) string {
	cp := make([]string, len(m.lines))
	copy(cp, m.lines)

	for i := excludeStart; i < excludeEnd && i < len(cp); i++ {
		cp[i] = ""
	}

	return strings.Join(cp, "\n")
}

func (m manifest) fullText() string {
	return strings.Join(m.lines, "\n")
}
