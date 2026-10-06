package accessanalyzer

import (
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// pageByQuery applies the maxResults/nextToken query members to a sorted slice.
func pageByQuery[T any](items []T, query string) (page.Page[T], error) {
	limit := 0

	if v := queryParamValue(query, "maxResults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return page.Page[T]{}, ErrValidation
		}

		limit = n
	}

	token := queryParamValue(query, "nextToken")
	if page.ValidateToken(token) != nil {
		return page.Page[T]{}, ErrValidation
	}

	return page.New(items, token, limit, max(len(items), 1)), nil
}
