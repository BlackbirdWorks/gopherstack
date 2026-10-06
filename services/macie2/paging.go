package macie2

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// pagedResponse pages items under key; a token this backend did not issue is a ValidationException.
func pagedResponse[T any](b StorageBackend, key, token string, limit int, items []T) (any, int, error) {
	if token != "" && page.DecodeHMACToken(token, b.PaginationSecret()) == 0 {
		return nil, http.StatusBadRequest, ErrValidation
	}

	data, next := paginate(items, token, b.PaginationSecret(), limit)
	resp := map[string]any{key: data}
	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

// pagedQueryResponse is pagedResponse reading maxResults/nextToken from the query string.
func pagedQueryResponse[T any](b StorageBackend, key, query string, items []T) (any, int, error) {
	q, _ := url.ParseQuery(query)
	limit, _ := strconv.Atoi(q.Get("maxResults"))

	return pagedResponse(b, key, q.Get("nextToken"), limit, items)
}
