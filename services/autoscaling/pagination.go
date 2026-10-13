package autoscaling

import (
	"encoding/base64"
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// newPage pages all by the request's NextToken, rejecting tokens this service did not issue.
func newPage[T any](vals url.Values, all []T, maxRecords, defaultMax int) (page.Page[T], error) {
	token := vals.Get("NextToken")
	if token != "" {
		raw, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			return page.Page[T]{}, ErrInvalidNextToken
		}

		if n, convErr := strconv.Atoi(string(raw)); convErr != nil || n < 0 {
			return page.Page[T]{}, ErrInvalidNextToken
		}
	}

	return page.New(all, token, maxRecords, defaultMax), nil
}
