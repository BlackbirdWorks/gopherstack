// Package page provides a generic paginated list type used across service backends.
package page

import (
	"encoding/base64"
	"strconv"
)

// Page is a generic paginated list result.
// Data contains the items for this page.
// Next is an opaque continuation token; when empty there are no more pages.
type Page[T any] struct {
	Next string
	Data []T
}

// New creates a Page from a fully sorted slice, applying cursor-based pagination.
// token is an opaque continuation token (empty starts from the beginning).
// limit is the requested page size; when <= 0 defaultLimit is used.
func New[T any](all []T, token string, limit, defaultLimit int) Page[T] {
	if limit <= 0 {
		limit = defaultLimit
	}

	start := decode(token)
	if start >= len(all) {
		return Page[T]{Data: []T{}}
	}

	end := start + limit
	var next string

	if end < len(all) {
		next = encode(end)
	} else {
		end = len(all)
	}

	return Page[T]{Data: all[start:end], Next: next}
}

func encode(idx int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(idx)))
}

func decode(token string) int {
	if token == "" {
		return 0
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0
	}

	idx, err := strconv.Atoi(string(b))
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// EncodeToken encodes a page index as an opaque continuation token.
func EncodeToken(idx int) string {
	return encode(idx)
}

// DecodeToken decodes an opaque continuation token back into a page index.
// An empty string, malformed base64, or non-integer content all return 0 (start of list),
// which is safe to use directly as a slice offset.
func DecodeToken(token string) int {
	return decode(token)
}
