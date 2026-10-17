package restroute

import (
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// MaxResultsQuery parses an optional integer query parameter; absent yields 0.
func MaxResultsQuery(q url.Values, name string) (int, error) {
	raw := q.Get(name)
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, Validation("%s must be an integer", name)
	}

	return n, nil
}

// Paginate validates maxResults (0 = default, otherwise 1..maxAllowed) and the token, then slices all.
func Paginate[T any](all []T, token string, maxResults, defaultLimit, maxAllowed int) (page.Page[T], error) {
	if maxResults < 0 || maxResults > maxAllowed {
		return page.Page[T]{}, Validation("maxResults must be between 1 and %d", maxAllowed)
	}

	if err := page.ValidateToken(token); err != nil {
		return page.Page[T]{}, Validation("nextToken is not valid")
	}

	return page.New(all, token, maxResults, defaultLimit), nil
}
