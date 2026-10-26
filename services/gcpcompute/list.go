package gcpcompute

import (
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultPageSize = 500
	desc            = "desc"
	maxOrderTerms   = 2
)

type entry struct {
	item  any
	scope string
}

type listQuery struct {
	filter     filterNode
	orderField string
	pageToken  string
	maxResults int
	descending bool
}

func parseListQuery(q url.Values) (listQuery, *APIError) {
	lq := listQuery{pageToken: q.Get("pageToken"), maxResults: defaultPageSize}

	if raw := q.Get("maxResults"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return lq, errField("maxResults", raw, "")
		}

		lq.maxResults = min(n, defaultPageSize)
	}

	f, apiErr := parseFilter(q.Get("filter"))
	if apiErr != nil {
		return lq, apiErr
	}

	lq.filter = f

	if ob := strings.TrimSpace(q.Get("orderBy")); ob != "" {
		fields := strings.Fields(ob)
		if len(fields) > maxOrderTerms || (len(fields) == maxOrderTerms && fields[1] != desc && fields[1] != "asc") {
			return lq, errField("orderBy", ob, "")
		}

		lq.orderField = fields[0]
		lq.descending = len(fields) == maxOrderTerms && fields[1] == desc
	}

	return lq, nil
}

func sortEntries(entries []entry, lq listQuery) {
	docs := make(map[int]map[string]any, len(entries))
	if lq.orderField != "" {
		for i := range entries {
			docs[i] = toDoc(entries[i].item)
		}
	}

	idx := make([]int, len(entries))
	for i := range idx {
		idx[i] = i
	}

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.scope + "\x00" + nameOf(e.item)
	}

	sort.SliceStable(idx, func(a, b int) bool {
		ia, ib := idx[a], idx[b]
		if lq.orderField != "" {
			va := strings.Join(lookup(docs[ia], strings.Split(lq.orderField, ".")), ",")
			vb := strings.Join(lookup(docs[ib], strings.Split(lq.orderField, ".")), ",")

			if c := cmpValues(va, vb); c != 0 {
				if lq.descending {
					return c > 0
				}

				return c < 0
			}
		}

		return names[ia] < names[ib]
	})

	out := make([]entry, len(entries))
	for i, j := range idx {
		out[i] = entries[j]
	}

	copy(entries, out)
}

func nameOf(item any) string {
	if n, ok := item.(interface{ GetName() string }); ok {
		return n.GetName()
	}

	doc := toDoc(item)
	if s, ok := doc["name"].(string); ok {
		return s
	}

	return ""
}

func applyList(entries []entry, lq listQuery) page.Page[entry] {
	kept := entries[:0:0]

	for _, e := range entries {
		if lq.filter == nil || lq.filter.eval(toDoc(e.item)) {
			kept = append(kept, e)
		}
	}

	sortEntries(kept, lq)

	return page.New(kept, lq.pageToken, lq.maxResults, defaultPageSize)
}

type listResponse struct {
	Items         any    `json:"items,omitempty"`
	Kind          string `json:"kind"`
	ID            string `json:"id"`
	NextPageToken string `json:"nextPageToken,omitempty"`
	SelfLink      string `json:"selfLink"`
}

type scopeWarning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
