package inspector2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const defaultPageSize = 100

// pageRequest holds the body maxResults/nextToken members every paged op binds.
type pageRequest struct {
	NextToken  string `json:"nextToken"`
	MaxResults int32  `json:"maxResults"`
}

func readPageRequest(c *echo.Context) (pageRequest, error) {
	var req pageRequest

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return req, fmt.Errorf("%w: invalid body", ErrValidation)
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return req, fmt.Errorf("%w: invalid JSON", ErrValidation)
		}
	}

	return req, nil
}

// pageItems returns one key-ordered page; the token is the next page's first key.
// An unresolvable token yields an empty page.
func pageItems[T any](items []T, key func(T) string, maxResults int32, nextToken string) ([]T, string) {
	size := int(maxResults)
	if size <= 0 {
		size = defaultPageSize
	}

	start := 0

	if nextToken != "" {
		start = len(items)

		for i, it := range items {
			if key(it) == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+size, len(items))

	next := ""
	if end < len(items) {
		next = key(items[end])
	}

	return items[start:end], next
}

func withPageToken(resp map[string]any, next string) map[string]any {
	if next != "" {
		resp["nextToken"] = next
	}

	return resp
}
