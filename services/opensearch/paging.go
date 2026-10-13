package opensearch

import (
	"maps"
	"net/http"
	"slices"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	errInvalidPaginationToken = "InvalidPaginationTokenException"
	errValidation             = "ValidationException"
	errResourceNotFound       = "ResourceNotFoundException"
)

// pageSpec names the response list key and request token member of a paged List/Describe op.
type pageSpec struct {
	extra     map[string]any
	key       string
	tokenQ    string
	tokenCode string
	// emptyToken emits NextToken "" on the last page for ops whose output marks it required (gopherstack-r80d).
	emptyToken bool
}

func listSpec(key string) pageSpec {
	return pageSpec{key: key, tokenQ: "nextToken", tokenCode: errValidation}
}

// writePagedList writes items honouring maxResults and the token query member. A non-nil
// sortKey fixes the order first; no maxResults means the whole list; a malformed token is rejected.
func writePagedList[T any](
	h *Handler, w http.ResponseWriter, r *http.Request, spec pageSpec, items []T, sortKey func(T) string,
) {
	q := r.URL.Query()

	token := q.Get(spec.tokenQ)
	if err := page.ValidateToken(token); err != nil {
		h.writeError(r, w, http.StatusBadRequest, spec.tokenCode, "Invalid pagination token")

		return
	}

	if sortKey != nil {
		items = slices.Clone(items)
		slices.SortStableFunc(items, func(a, b T) int {
			return compareStrings(sortKey(a), sortKey(b))
		})
	}

	limit := len(items)
	if n, err := strconv.Atoi(q.Get("maxResults")); err == nil && n > 0 {
		limit = n
	}

	p := page.New(items, token, limit, max(limit, 1))
	out := map[string]any{spec.key: p.Data}
	maps.Copy(out, spec.extra)
	if p.Next != "" || spec.emptyToken {
		out[jsonKeyNextToken] = p.Next
	}

	h.writeJSON(r, w, out)
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func withEmptyToken(s pageSpec) pageSpec {
	s.emptyToken = true

	return s
}
