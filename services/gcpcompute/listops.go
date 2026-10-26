package gcpcompute

import (
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

func opResult(op *computeapi.Operation, err error) (any, error) {
	if err != nil {
		return nil, err
	}

	return op, nil
}

func collectionID(rt reqRoute) string {
	if rt.aggregated {
		return "projects/" + rt.project + "/aggregated/" + rt.collection
	}

	return relPath(rt.project, rt.scope, rt.loc, rt.collection, "")
}

func listBody(rq *request, kind string, entries []entry) (any, error) {
	lq, err := parseListQuery(rq.q)
	if err != nil {
		return nil, err
	}

	pg := applyList(entries, lq)

	items := make([]any, len(pg.Data))
	for i, e := range pg.Data {
		items[i] = e.item
	}

	id := strings.TrimSuffix(collectionID(rq.rt), "/")
	resp := &listResponse{Kind: kind, ID: id, SelfLink: apiBase + id, NextPageToken: pg.Next}

	if len(items) > 0 {
		resp.Items = items
	}

	return resp, nil
}

func aggBody(rq *request, kind, aggKey string, scopes []string, entries []entry) (any, error) {
	lq, err := parseListQuery(rq.q)
	if err != nil {
		return nil, err
	}

	pg := applyList(entries, lq)
	grouped := map[string][]any{}

	for _, e := range pg.Data {
		grouped[e.scope] = append(grouped[e.scope], e.item)
	}

	items := map[string]any{}

	for _, s := range scopes {
		if list, ok := grouped[s]; ok {
			items[s] = map[string]any{aggKey: list}

			continue
		}

		items[s] = map[string]any{"warning": scopeWarning{
			Code: "NO_RESULTS_ON_PAGE", Message: "There are no results for scope '" + s + "' on this page.",
		}}
	}

	id := collectionID(rq.rt)

	return &listResponse{Kind: kind, ID: id, SelfLink: apiBase + id, NextPageToken: pg.Next, Items: items}, nil
}

func zoneScopes() []string {
	zones := allZones()
	out := make([]string, len(zones))

	for i, z := range zones {
		out[i] = scopeZone + "/" + z
	}

	return out
}

func regionScopes() []string {
	out := make([]string, len(regionCatalog))
	for i, r := range regionCatalog {
		out[i] = scopeRegion + "/" + r.name
	}

	return out
}

// scopedList registers list and aggregatedList for a collection whose entries come from fn.
func (h *Handler) scopedList(
	collection, scope, kind, aggKind, aggKey string,
	scopes []string,
	fn func(b *InMemoryBackend, project, scope, loc string) []entry,
) {
	h.reg(collection, scope, verbList, func(h *Handler, rq *request) (any, error) {
		return listBody(rq, kind, fn(h.Backend, rq.rt.project, rq.rt.scope, rq.rt.loc))
	})

	if aggKind == "" {
		return
	}

	h.reg(collection, "agg", verbAggList, func(h *Handler, rq *request) (any, error) {
		return aggBody(rq, aggKind, aggKey, scopes, fn(h.Backend, rq.rt.project, "", ""))
	})
}

func mergeInto[T any](cur *T, raw map[string]any) (*T, *APIError) {
	m := toDoc(cur)
	if m == nil {
		return nil, errInvalid("Invalid JSON payload received.")
	}

	mergePatch(m, raw)

	out, err := fromDoc[T](m)
	if err != nil {
		return nil, errInvalid("Invalid JSON payload received: %v", err)
	}

	return out, nil
}
