// Package page provides a generic paginated list type used across service backends.
package page

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
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
	if err != nil {
		return 0
	}

	return idx
}

// ValidateToken checks if a continuation token is syntactically valid.
// Returns an error if the token is not valid base64 or doesn't decode to an integer.
func ValidateToken(token string) error {
	if token == "" {
		return nil
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return err
	}

	_, err = strconv.Atoi(string(b))

	return err
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

// NewHMAC creates a Page using HMAC-signed tokens for tamper-evident pagination.
// token is an HMAC-signed continuation token (empty starts from the beginning).
// secret is the HMAC signing secret.
// limit is the requested page size; when <= 0 defaultLimit is used.
func NewHMAC[T any](all []T, token, secret string, limit, defaultLimit int) Page[T] {
	if limit <= 0 {
		limit = defaultLimit
	}

	start := DecodeHMACToken(token, secret)
	if start >= len(all) {
		return Page[T]{Data: []T{}}
	}

	end := start + limit
	var next string

	if end < len(all) {
		next = EncodeHMACToken(end, secret)
	} else {
		end = len(all)
	}

	return Page[T]{Data: all[start:end], Next: next}
}

// EncodeHMACToken encodes an index as an HMAC-signed token.
// Token format: base64url(strconv(idx) + "." + hmac-sha256-hex(secret, strconv(idx))).
func EncodeHMACToken(idx int, secret string) string {
	idxStr := strconv.Itoa(idx)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(idxStr))
	sig := hex.EncodeToString(mac.Sum(nil))
	payload := idxStr + "." + sig

	return base64.URLEncoding.EncodeToString([]byte(payload))
}

// DecodeHMACToken decodes and verifies an HMAC token, returning the index.
// Returns 0 on invalid or tampered tokens.
func DecodeHMACToken(token, secret string) int {
	const tokenParts = 2

	if token == "" {
		return 0
	}

	b, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}

	parts := strings.SplitN(string(b), ".", tokenParts)
	if len(parts) != tokenParts {
		return 0
	}

	idxStr, sig := parts[0], parts[1]
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(idxStr))
	expected := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(sig), []byte(expected)) {
		return 0
	}

	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}
