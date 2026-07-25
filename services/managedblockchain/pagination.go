package managedblockchain

import (
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultListPageSize is the page size every List* operation falls back to
// when the caller omits maxResults (or supplies a non-positive value). Real
// AWS does not document a specific default for this service's List
// operations; 100 matches the convention already established elsewhere in
// gopherstack (see services/acmpca's defaultMaxItems).
const defaultListPageSize = 100

// paginationParams parses the maxResults/nextToken query parameters shared
// by every List* operation in this service. Both names match the real
// aws-sdk-go-v2 client's wire shape exactly (see serializers.go's
// SetQuery("maxResults") / SetQuery("nextToken") bindings, identical across
// ListNetworks/ListMembers/ListNodes/ListProposals/ListProposalVotes/
// ListAccessors/ListInvitations). A non-numeric or non-positive maxResults
// is treated the same as an absent one -- real AWS would reject it with a
// validation error, but silently falling back to the default page size is
// consistent with this service's existing no-hard-validation style for
// list filters.
func paginationParams(q url.Values) (int, string) {
	token := q.Get("nextToken")
	limit := 0

	if raw := q.Get("maxResults"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}

	return limit, token
}

// paginate applies cursor-based pagination (pkgs/page) to a fully
// materialized, already-sorted slice of List* results and returns the
// page's items plus a NextToken pointer (nil when there is no further
// page, matching every listXxxResponse's `NextToken *string
// json:"NextToken,omitempty"` field).
func paginate[T any](all []T, q url.Values) ([]T, *string) {
	limit, token := paginationParams(q)

	p := page.New(all, token, limit, defaultListPageSize)

	var next *string
	if p.Next != "" {
		next = &p.Next
	}

	return p.Data, next
}
