package kinesisvideo

import (
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// listByName applies an optional BEGINS_WITH name condition, sorts by name,
// and paginates.
func listByName[T any](
	all []*T, nameOf func(*T) string, cloneOf func(*T) *T,
	operator, value, nextToken string, maxResults, defaultLimit int,
) ([]*T, string) {
	matched := make([]*T, 0, len(all))

	for _, item := range all {
		if operator == comparisonOperatorBeginsWith && !strings.HasPrefix(nameOf(item), value) {
			continue
		}

		matched = append(matched, cloneOf(item))
	}

	sort.Slice(matched, func(i, j int) bool { return nameOf(matched[i]) < nameOf(matched[j]) })

	p := page.New(matched, nextToken, maxResults, defaultLimit)

	return p.Data, p.Next
}
