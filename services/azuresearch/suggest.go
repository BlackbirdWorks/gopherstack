package azuresearch

import (
	"net/url"
	"sort"
	"strings"
)

const (
	defaultSuggestTop = 5
	maxSuggestTop     = 100
)

type suggestRequest struct {
	Search       string
	Suggester    string
	Filter       string
	OrderBy      string
	PreTag       string
	PostTag      string
	Mode         string
	SearchFields []string
	Select       []string
	Top          int
	Fuzzy        bool
}

func suggestFromQuery(q url.Values, auto bool) (*suggestRequest, error) {
	r := &suggestRequest{
		Search:    q.Get("search"),
		Suggester: q.Get("suggesterName"),
		Filter:    q.Get("$filter"),
		OrderBy:   q.Get("$orderby"),
		PreTag:    q.Get("highlightPreTag"),
		PostTag:   q.Get("highlightPostTag"),
		Mode: q.Get(
			"autocompleteMode",
		),
		SearchFields: splitCSV(q.Get("searchFields")),
		Select:       splitCSV(q.Get("$select")),
		Fuzzy:        strings.EqualFold(q.Get("fuzzy"), "true"),
		Top:          defaultSuggestTop,
	}

	if v := q.Get("$top"); v != "" {
		n, err := intParam(v, "$top")
		if err != nil {
			return nil, err
		}

		r.Top = n
	}

	return r, r.validate(auto)
}

func suggestFromBody(body []byte, auto bool) (*suggestRequest, error) {
	raw := map[string]any{}
	if err := decodeJSON(body, &raw); err != nil {
		return nil, err
	}

	r := &suggestRequest{
		Search: strOf(raw, "search"), Suggester: strOf(raw, "suggesterName"), Filter: strOf(raw, "filter"),
		OrderBy: strOf(raw, "orderby"), PreTag: strOf(raw, "highlightPreTag"), PostTag: strOf(raw, "highlightPostTag"),
		Mode: strOf(raw, "autocompleteMode"), SearchFields: splitCSV(strOf(raw, "searchFields")),
		Select: splitCSV(strOf(raw, "select")), Fuzzy: boolOf(raw, "fuzzy", false), Top: defaultSuggestTop,
	}

	if n, ok := raw["top"].(interface{ String() string }); ok {
		t, err := intParam(n.String(), "top")
		if err != nil {
			return nil, err
		}

		r.Top = t
	}

	return r, r.validate(auto)
}

func (r *suggestRequest) validate(auto bool) error {
	if r.Search == "" {
		return badf("The request is invalid. Details: The parameter 'search' is required.")
	}

	if r.Suggester == "" {
		return badf("The request is invalid. Details: The parameter 'suggesterName' is required.")
	}

	if r.Top < 1 || r.Top > maxSuggestTop {
		return badf("Invalid value for $top: it must be between 1 and %d.", maxSuggestTop)
	}

	if auto {
		switch r.Mode {
		case "":
			r.Mode = "oneTerm"
		case "oneTerm", "twoTerms", "oneTermWithContext":
		default:
			return badf("Invalid value for autocompleteMode: '%s'.", r.Mode)
		}
	}

	return nil
}

type suggestCtx struct {
	ix     *Index
	pred   pred
	req    *suggestRequest
	fields []*fieldDef
	docs   []*Doc
}

func (s *Service) suggestSetup(ix *Index, req *suggestRequest) (*suggestCtx, error) {
	sg, ok := ix.comp.suggesters[req.Suggester]
	if !ok {
		return nil, badf("The suggester '%s' does not exist in index '%s'.", req.Suggester, ix.name())
	}

	paths := sg.SourceFields
	if len(req.SearchFields) > 0 {
		paths = req.SearchFields
	}

	sc := &suggestCtx{ix: ix, req: req, docs: ix.orderedDocs()}

	for _, p := range paths {
		fd, found := ix.comp.byPath[p]
		if !found || !containsStr(sg.SourceFields, p) {
			return nil, badf("The field '%s' is not a source field of suggester '%s'.", p, sg.Name)
		}

		sc.fields = append(sc.fields, fd)
	}

	if strings.TrimSpace(req.Filter) != "" {
		pr, err := compileFilter(
			req.Filter,
			&filterCtx{comp: ix.comp, isMatch: func(*fenv, string, string, string, string) bool { return false }},
		)
		if err != nil {
			return nil, err
		}

		sc.pred = pr
	}

	return sc, validateSelect(ix.comp, req.Select)
}

func (sc *suggestCtx) passes(d *Doc) bool {
	return sc.pred == nil || sc.pred(&fenv{d: d, doc: d.Fields, vars: map[string]any{}})
}

func queryTokens(text string) []string {
	toks := standardAnalyzer(nil, false, nil)(text)
	out := make([]string, len(toks))

	for i, t := range toks {
		out[i] = t.Term
	}

	return out
}

func termMatches(term, q string, prefix, fuzzy bool) bool {
	if prefix {
		if strings.HasPrefix(term, q) {
			return true
		}

		if !fuzzy || len(term) < len(q) {
			return false
		}

		return editDistance(term[:len(q)], q, 1) <= 1
	}

	return term == q || (fuzzy && editDistance(term, q, 1) <= 1)
}

// matchesField reports whether every query token matches a term of the field, the last as a prefix.
func (sc *suggestCtx) matchesField(d *Doc, fd *fieldDef, q []string) bool {
	ft := d.tok[fd.Path]
	if ft == nil {
		return false
	}

	for i, qt := range q {
		found := false

		for term := range ft.terms {
			if termMatches(term, qt, i == len(q)-1, sc.req.Fuzzy) {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func firstString(d *Doc, fd *fieldDef) string {
	for _, v := range valuesAt(d.Fields, strings.Split(fd.Path, "/")) {
		if s, ok := v.(string); ok {
			return s
		}
	}

	return ""
}

func (s *Service) suggest(ix *Index, req *suggestRequest) (*ordered, error) {
	sc, err := s.suggestSetup(ix, req)
	if err != nil {
		return nil, err
	}

	order, err := compileOrderBy(req.OrderBy, ix.comp)
	if err != nil {
		return nil, err
	}

	q := queryTokens(req.Search)

	var hits []hit

	for _, d := range sc.docs {
		if !sc.passes(d) {
			continue
		}

		for _, fd := range sc.fields {
			if sc.matchesField(d, fd, q) {
				hits = append(hits, hit{doc: d, key: keyOf(ix, d), score: 1})

				break
			}
		}
	}

	if order != nil {
		order.sortHits(hits)
	}

	if len(hits) > req.Top {
		hits = hits[:req.Top]
	}

	return sc.suggestResponse(hits, q), nil
}

func (sc *suggestCtx) suggestResponse(hits []hit, q []string) *ordered {
	rows := make([]*ordered, 0, len(hits))

	for _, h := range hits {
		row := newOrdered()

		for _, fd := range sc.fields {
			if sc.matchesField(h.doc, fd, q) {
				text := firstString(h.doc, fd)
				if sc.req.PreTag != "" || sc.req.PostTag != "" {
					text = sc.highlightText(text, fd, q)
				}

				row.set("@search.text", text)

				break
			}
		}

		sel := sc.req.Select
		proj := project(sc.ix.comp.fields, h.doc.Fields, append([]string{sc.ix.comp.key.Name}, sel...), "")

		for _, k := range proj.keys {
			row.set(k, proj.vals[k])
		}

		rows = append(rows, row)
	}

	out := newOrdered()
	out.set(fieldValue, rows)

	return out
}

func (sc *suggestCtx) highlightText(text string, fd *fieldDef, q []string) string {
	an := sc.ix.comp.indexAnalyzerFor(fd)

	var spans [][2]int

	for _, t := range an(text) {
		for i, qt := range q {
			if termMatches(t.Term, qt, i == len(q)-1, sc.req.Fuzzy) {
				spans = append(spans, [2]int{t.Start, t.End})

				break
			}
		}
	}

	return wrap(text, spans, 0, sc.req.PreTag, sc.req.PostTag)
}

func (s *Service) autocomplete(ix *Index, req *suggestRequest) (*ordered, error) {
	sc, err := s.suggestSetup(ix, req)
	if err != nil {
		return nil, err
	}

	q := queryTokens(req.Search)
	if len(q) == 0 {
		return nil, badf("The 'search' parameter does not contain any searchable terms.")
	}

	counts := map[string]int{}

	for _, d := range sc.docs {
		if !sc.passes(d) {
			continue
		}

		for _, fd := range sc.fields {
			sc.collectCompletions(d, fd, q, counts)
		}
	}

	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}

		return keys[i] < keys[j]
	})

	if len(keys) > req.Top {
		keys = keys[:req.Top]
	}

	rows := make([]map[string]any, 0, len(keys))
	prefix := strings.Join(q[:len(q)-1], " ")

	for _, k := range keys {
		plus := k
		if prefix != "" {
			plus = prefix + " " + k
		}

		text := k
		if req.Mode == "oneTermWithContext" {
			text = plus
		}

		rows = append(rows, map[string]any{"text": text, "queryPlusText": plus})
	}

	out := newOrdered()
	out.set(fieldValue, rows)

	return out, nil
}

func (sc *suggestCtx) collectCompletions(d *Doc, fd *fieldDef, q []string, counts map[string]int) {
	ft := d.tok[fd.Path]
	if ft == nil {
		return
	}

	last := q[len(q)-1]

	for _, ctxTok := range q[:len(q)-1] {
		if len(ft.terms[ctxTok]) == 0 {
			return
		}
	}

	for i, t := range ft.toks {
		if !termMatches(t.Term, last, true, sc.req.Fuzzy) {
			continue
		}

		cand := t.Term
		if sc.req.Mode == "twoTerms" && i+1 < len(ft.toks) {
			cand += " " + ft.toks[i+1].Term
		}

		counts[cand]++
	}
}
