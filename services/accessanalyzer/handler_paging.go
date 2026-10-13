package accessanalyzer

import (
	"fmt"
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

// pageStart resolves a continuation token (the key of the first item of the
// next page) to an index; an unknown token is a ValidationException.
func pageStart(n int, token string, keyAt func(i int) string) (int, error) {
	if token == "" {
		return 0, nil
	}

	for i := range n {
		if keyAt(i) == token {
			return i, nil
		}
	}

	return 0, fmt.Errorf("%w: invalid nextToken", ErrValidation)
}
