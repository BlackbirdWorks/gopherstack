package elasticsearch

import (
	"context"
	"net/http"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultCrossClusterPageSize is the "defaults to 100" page size documented
// on both DescribeInboundCrossClusterSearchConnectionsInput.MaxResults and
// its outbound sibling.
const defaultCrossClusterPageSize = 100

// nameValuesFilter is the wire shape of types.Filter, shared by
// DescribeInboundCrossClusterSearchConnections and
// DescribeOutboundCrossClusterSearchConnections (both restjson1 JSON-body
// fields, verified against their own serializeOpDocument functions).
type nameValuesFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// matchesFilters reports whether valueOf resolves and matches every filter
// in filters. A filter naming a field valueOf doesn't recognize excludes
// every connection (AWS rejects unknown Filternames at request time; this
// backend has no request-time validation for Filters, so an unrecognized
// name degrades to "matches nothing" rather than "silently ignored").
func matchesFilters(filters []nameValuesFilter, valueOf func(name string) (string, bool)) bool {
	for _, f := range filters {
		value, ok := valueOf(f.Name)
		if !ok || !slices.Contains(f.Values, value) {
			return false
		}
	}

	return true
}

// describeCrossClusterConnections is the shared filter+paginate+respond body
// for DescribeInboundCrossClusterSearchConnections and
// DescribeOutboundCrossClusterSearchConnections -- identical except for the
// connection type, its filter-field resolver, and its JSON conversion.
func describeCrossClusterConnections[T any](
	h *Handler, w http.ResponseWriter, r *http.Request,
	fetch func(context.Context) []T,
	filterValueOf func(T) func(string) (string, bool),
	toJSON func(T) any,
) {
	var req struct {
		NextToken  string             `json:"NextToken"`
		Filters    []nameValuesFilter `json:"Filters"`
		MaxResults int                `json:"MaxResults"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	connections := fetch(h.reqContext(r))
	matched := make([]T, 0, len(connections))
	for _, c := range connections {
		if matchesFilters(req.Filters, filterValueOf(c)) {
			matched = append(matched, c)
		}
	}

	pg := page.New(matched, req.NextToken, req.MaxResults, defaultCrossClusterPageSize)
	result := make([]any, 0, len(pg.Data))
	for _, c := range pg.Data {
		result = append(result, toJSON(c))
	}

	resp := map[string]any{"CrossClusterSearchConnections": result}
	if pg.Next != "" {
		resp["NextToken"] = pg.Next
	}

	h.writeJSON(r, w, resp)
}
