package quicksight

import "errors"

var errBadPageToken = errors.New("invalid next-token")

// pageSliceStrict pages items by an offset token; a malformed token is errBadPageToken.
func pageSliceStrict[T any](items []T, maxResults int32, token string) ([]T, string, error) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0

	if token != "" {
		off, err := decodePageToken(token)
		if err != nil {
			return nil, "", errBadPageToken
		}

		start = min(off, len(items))
	}

	end := start + int(maxResults)
	next := ""

	if end < len(items) {
		next = encodePageToken(end)
	} else {
		end = len(items)
	}

	return items[start:end], next, nil
}
