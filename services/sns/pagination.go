package sns

import (
	"encoding/base64"
	"errors"
	"strconv"
)

var errNegativeToken = errors.New("sns: pagination token decodes to a negative offset")

// decodeToken decodes a base64 pagination token into an integer offset.
// An empty token is treated as offset 0. A token that decodes to a negative
// offset is rejected like any other malformed token, since paginate would
// otherwise slice items[offset:end] with a negative offset and panic.
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

	if offset < 0 {
		return 0, errNegativeToken
	}

	return offset, nil
}

// encodeToken encodes an integer offset as a base64 pagination token.
func encodeToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// appendBounded appends item to slice, evicting the oldest entries once the
// result exceeds maxLen. Used to bound the smsDeliveries/emailDeliveries/
// applicationDeliveries observability buffers (see maxRecordedDeliveries).
func appendBounded[T any](slice []T, item T, maxLen int) []T {
	slice = append(slice, item)
	if len(slice) > maxLen {
		slice = slice[len(slice)-maxLen:]
	}

	return slice
}

// paginate returns a page of items and the next token, or an empty token when exhausted.
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

// resolvePageSize returns the effective page size given a caller-requested size, a default,
// and a maximum. If requested is 0, defaultSize is used. If requested exceeds maxSize it is clamped.
func resolvePageSize(requested, defaultSize, maxSize int) int {
	if requested <= 0 {
		return defaultSize
	}

	if requested > maxSize {
		return maxSize
	}

	return requested
}
