package azuresearch

import (
	"net/url"
	"strconv"
	"strings"
	"time"
)

func contextURL(base, index string) string {
	return base + "/indexes('" + index + "')/$metadata#docs(*)"
}

// highlightFields resolves the requested highlight field names.
func (ix *Index) highlightFields(names []string) ([]*fieldDef, error) {
	var out []*fieldDef

	for _, n := range names {
		fd, ok := ix.comp.byPath[n]
		if !ok || !fd.Searchable || !fd.isText() {
			return nil, badf("The field '%s' in the highlight parameter is not searchable.", n)
		}

		out = append(out, fd)
	}

	return out, nil
}

// searchResponse renders the search result page.
func (s *Service) searchResponse(
	ix *Index,
	req *searchRequest,
	res *searchResult,
	base string,
	post bool,
	now time.Time,
) (*ordered, error) {
	hf, err := ix.highlightFields(req.Highlight)
	if err != nil {
		return nil, err
	}

	pl, err := s.plan(ix, req, now)
	if err != nil {
		return nil, err
	}

	out := newOrdered()
	out.set(fieldContext, contextURL(base, ix.name()))

	if req.Count {
		out.set("@odata.count", res.Count)
	}

	if res.Facets != nil {
		out.set("@search.facets", res.Facets)
	}

	page := res.Hits
	if req.Skip >= len(page) {
		page = nil
	} else {
		page = page[req.Skip:]
	}

	more := false
	if len(page) > req.Top {
		page, more = page[:req.Top], true
	}

	match := pl.matcher()
	rows := make([]*ordered, 0, len(page))

	for _, h := range page {
		rows = append(rows, hitRow(pl, ix, req, h, hf, match))
	}

	if more && !req.TopSet {
		s.nextPage(out, req, base, ix.name(), post)
	}

	out.set(fieldValue, rows)

	return out, nil
}

func hitRow(
	pl *searchPlan,
	ix *Index,
	req *searchRequest,
	h hit,
	hf []*fieldDef,
	match func(*fieldDef, string) bool,
) *ordered {
	row := newOrdered()
	row.set("@search.score", scoreFloor(h.score))

	if len(hf) > 0 {
		hl := map[string][]string{}

		for _, fd := range hf {
			if frags := pl.highlights(h.doc, fd, req.PreTag, req.PostTag, match); len(frags) > 0 {
				hl[fd.Path] = frags
			}
		}

		if len(hl) > 0 {
			row.set("@search.highlights", hl)
		}
	}

	proj := project(ix.comp.fields, h.doc.Fields, req.Select, "")
	for _, k := range proj.keys {
		row.set(k, proj.vals[k])
	}

	return row
}

func (s *Service) nextPage(out *ordered, req *searchRequest, base, index string, post bool) {
	next := req.Skip + req.Top

	if post {
		np := newOrdered()
		np.set("search", req.Search)

		if req.Filter != "" {
			np.set("filter", req.Filter)
		}

		if req.Count {
			np.set(fieldCount, true)
		}

		np.set("skip", next)
		out.set("@search.nextPageParameters", np)

		return
	}

	q := url.Values{}
	q.Set("search", req.Search)
	q.Set("$skip", strconv.Itoa(next))

	if req.Filter != "" {
		q.Set("$filter", req.Filter)
	}

	out.set("@odata.nextLink", base+"/indexes('"+index+"')/docs?"+q.Encode())
}

func (ix *Index) name() string { return strOf(ix.Def, "name") }

func baseURL(host string) string {
	if host == "" {
		host = "localhost"
	}

	return "https://" + strings.TrimSuffix(host, "/")
}
