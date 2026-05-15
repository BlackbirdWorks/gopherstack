package lakeformation

import (
	"slices"
	"strings"
)

const (
	lfSplitInTwo  = 2  // SplitN limit for two-part key parsing
	lfDecimalBase = 10 // decimal base for token parsing
	lfItoaInitCap = 10 // initial capacity for itoa byte slice
)

// ---------------------------------------------------------------------------
// SearchTablesByLFTags — real implementation
// ---------------------------------------------------------------------------

// SearchTablesByLFTags returns tables whose LF-tags match all of the given expression tags.
// It scans b.resourceLFTags for resources of type "table:" and checks whether all
// expression tags are present in the resource's tag set.
func (b *InMemoryBackend) SearchTablesByLFTags(
	expression []LFTag,
	catalogID string,
	maxResults int,
	nextToken string,
) ([]TaggedTable, string) {
	b.mu.RLock("SearchTablesByLFTags")
	defer b.mu.RUnlock()

	const tablePrefix = "table:"

	results := make([]TaggedTable, 0, len(b.resourceLFTags))

	for key, pairs := range b.resourceLFTags {
		if !strings.HasPrefix(key, tablePrefix) {
			continue
		}

		// Check whether all expression tags match.
		if !lfTagsMatchExpression(pairs, expression) {
			continue
		}

		// Extract database + table name from key (format: "table:{db}.{table}").
		rest := strings.TrimPrefix(key, tablePrefix)
		parts := strings.SplitN(rest, ".", lfSplitInTwo)

		if len(parts) != lfSplitInTwo {
			continue
		}

		dbName := parts[0]
		tableName := parts[1]

		tagged := TaggedTable{
			Table: &TableResource{
				CatalogID:    catalogID,
				DatabaseName: dbName,
				Name:         tableName,
			},
			LFTagsOnTable: cloneLFTagPairs(pairs),
		}

		results = append(results, tagged)
	}

	return paginateTaggedTables(results, maxResults, nextToken)
}

// SearchDatabasesByLFTags returns databases whose LF-tags match all of the given expression tags.
func (b *InMemoryBackend) SearchDatabasesByLFTags(
	expression []LFTag,
	_ string, // catalogID: reserved for future multi-catalog support
	maxResults int,
	nextToken string,
) ([]TaggedDatabase, string) {
	b.mu.RLock("SearchDatabasesByLFTags")
	defer b.mu.RUnlock()

	const dbPrefix = "database:"

	results := make([]TaggedDatabase, 0, len(b.resourceLFTags))

	for key, pairs := range b.resourceLFTags {
		if !strings.HasPrefix(key, dbPrefix) {
			continue
		}

		if !lfTagsMatchExpression(pairs, expression) {
			continue
		}

		dbName := strings.TrimPrefix(key, dbPrefix)

		tagged := TaggedDatabase{
			Database: &DatabaseResource{Name: dbName},
			LFTags:   cloneLFTagPairs(pairs),
		}

		results = append(results, tagged)
	}

	return paginateTaggedDatabases(results, maxResults, nextToken)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// lfTagsMatchExpression returns true when all expression tags are satisfied by pairs.
// An expression tag is satisfied when the same TagKey appears in pairs with a TagValues
// that contains at least one of the expression tag's values.
func lfTagsMatchExpression(pairs []LFTagPair, expression []LFTag) bool {
	for _, exprTag := range expression {
		found := false

		for _, pair := range pairs {
			if pair.TagKey != exprTag.TagKey {
				continue
			}
			// Check if any value in pair.TagValues matches any value in exprTag.TagValues.
			for _, pv := range pair.TagValues {
				if slices.Contains(exprTag.TagValues, pv) {
					found = true

					break
				}
			}
			if found {
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func cloneLFTagPairs(pairs []LFTagPair) []LFTagPair {
	if pairs == nil {
		return nil
	}

	cp := make([]LFTagPair, len(pairs))
	for i, p := range pairs {
		cp[i] = LFTagPair{
			TagKey:    p.TagKey,
			TagValues: append([]string(nil), p.TagValues...),
		}
	}

	return cp
}

func paginateTaggedTables(list []TaggedTable, maxResults int, nextToken string) ([]TaggedTable, string) {
	const defaultMax = 100

	if maxResults <= 0 {
		maxResults = defaultMax
	}

	startIdx := 0

	if nextToken != "" {
		n := 0
		for _, b := range []byte(nextToken) {
			n = n*lfDecimalBase + int(b-'0')
		}
		startIdx = n
	}

	if startIdx >= len(list) {
		return []TaggedTable{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

func paginateTaggedDatabases(list []TaggedDatabase, maxResults int, nextToken string) ([]TaggedDatabase, string) {
	const defaultMax = 100

	if maxResults <= 0 {
		maxResults = defaultMax
	}

	startIdx := 0

	if nextToken != "" {
		n := 0
		for _, b := range []byte(nextToken) {
			n = n*lfDecimalBase + int(b-'0')
		}
		startIdx = n
	}

	if startIdx >= len(list) {
		return []TaggedDatabase{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	result := make([]byte, 0, lfItoaInitCap)

	for n > 0 {
		result = append([]byte{byte('0' + n%10)}, result...)
		n /= 10
	}

	return string(result)
}
