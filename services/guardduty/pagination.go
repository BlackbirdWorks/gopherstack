package guardduty

import (
	"encoding/base64"
	"strconv"
)

// decodeToken decodes a base64 pagination token into an integer offset. An
// empty token is treated as offset 0. Mirrors services/sns's decodeToken
// (this package can't import that unexported helper directly).
func decodeToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, err
	}

	offset, err := strconv.Atoi(string(decoded))
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// encodeToken encodes an integer offset as a base64 pagination token.
func encodeToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// paginate returns a page of items starting at offset (size items, or fewer
// once the slice is exhausted) plus the token to fetch the next page, or an
// empty token once there is nothing left.
func paginate[T any](items []T, offset, size int) ([]T, string) {
	if offset >= len(items) {
		return []T{}, ""
	}

	end := offset + size
	nextToken := ""

	if end < len(items) {
		nextToken = encodeToken(end)
	} else {
		end = len(items)
	}

	return items[offset:end], nextToken
}

// resolvePageSize returns the effective page size given a caller-requested
// size, a default, and a maximum. If requested is <= 0, defaultSize is used.
// If requested exceeds maxSize it is clamped.
func resolvePageSize(requested, defaultSize, maxSize int) int {
	if requested <= 0 {
		return defaultSize
	}

	if requested > maxSize {
		return maxSize
	}

	return requested
}
