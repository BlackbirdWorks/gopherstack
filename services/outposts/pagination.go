package outposts

import (
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxPageLimit = 1000

// validatePage enforces the API reference's MaxResults range (1-1000) and a well-formed NextToken.
func validatePage(q url.Values) error {
	if raw := q.Get("MaxResults"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxPageLimit {
			return validationError("MaxResults must be between 1 and " + strconv.Itoa(maxPageLimit))
		}
	}

	if err := page.ValidateToken(q.Get("NextToken")); err != nil {
		return validationError("invalid NextToken")
	}

	return nil
}

// paginate pages an already stably-ordered slice by the request's MaxResults/NextToken.
func paginate[T any](items []T, q url.Values) (page.Page[T], error) {
	if err := validatePage(q); err != nil {
		return page.Page[T]{}, err
	}

	return page.New(items, q.Get("NextToken"), queryMaxResults(q), defaultPageLimit), nil
}
