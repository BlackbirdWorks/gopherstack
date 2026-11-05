package azuresearch

import (
	"math"
	"sort"
	"strings"
	"time"
)

const rrfK = 60

type hit struct {
	doc   *Doc
	key   string
	score float64
}

type searchResult struct {
	Facets    map[string][]map[string]any
	Hits      []hit
	Count     int
	Matchers  []matcherSet
	Truncated bool
}

type matcherSet struct {
	fn func(fd *fieldDef, term string) bool
}

type searchPlan struct {
	ix     *Index
	req    *searchRequest
	ctx    *searchCtx
	query  qNode
	filter pred
	fctx   *filterCtx
	now    time.Time
	params scoreParams
	prof   *scoringProfile
	text   bool
}

func (s *Service) synonymRules(ix *Index) map[string]*synRules {
	out := map[string]*synRules{}

	for _, f := range ix.comp.byPath {
		for _, name := range f.SynonymMaps {
			if obj, ok := s.Objects[kindSynonymMaps][name]; ok && out[name] == nil {
				out[name] = parseSolr(strOf(obj, "synonyms"))
			}
		}
	}

	return out
}

func (ix *Index) searchFields(req *searchRequest) ([]*fieldDef, error) {
	if len(req.SearchFields) == 0 {
		return ix.comp.searchable, nil
	}

	var out []*fieldDef

	for _, n := range req.SearchFields {
		fd, ok := ix.comp.byPath[n]
		if !ok || !fd.Searchable || !fd.isText() {
			return nil, badf("The field '%s' in the searchFields parameter is not searchable.", n)
		}

		out = append(out, fd)
	}

	return out, nil
}

func (s *Service) plan(ix *Index, req *searchRequest, now time.Time) (*searchPlan, error) {
	fields, err := ix.searchFields(req)
	if err != nil {
		return nil, err
	}

	pl := &searchPlan{ix: ix, req: req, now: now}

	pl.params, err = parseScoringParams(req.ScoringParams)
	if err != nil {
		return nil, err
	}

	profName := req.ScoringProfile
	if profName == "" {
		profName = ix.comp.defProfile
	}

	if profName != "" {
		p, ok := ix.comp.profiles[profName]
		if !ok {
			return nil, badf("The scoring profile '%s' does not exist.", profName)
		}

		pl.prof = p
	}

	pl.ctx = &searchCtx{ix: ix, fields: fields, syn: s.synonymRules(ix), anyMode: req.SearchMode == modeAny}
	if pl.prof != nil {
		pl.ctx.weights = pl.prof.Weights
	}

	pl.text = strings.TrimSpace(req.Search) != ""
	validate := func(f string) error {
		if fd, ok := ix.comp.byPath[f]; !ok || !fd.Searchable {
			return badf("The field '%s' in the query is not searchable.", f)
		}

		return nil
	}

	pl.query, err = parseQuery(req.Search, req.QueryType == queryFull, req.SearchMode == modeAll, validate)
	if err != nil {
		return nil, err
	}

	pl.fctx = &filterCtx{comp: ix.comp}
	pl.fctx.isMatch = pl.isMatch(s)

	if strings.TrimSpace(req.Filter) != "" {
		if pl.filter, err = compileFilter(req.Filter, pl.fctx); err != nil {
			return nil, err
		}
	}

	return pl, nil
}

// isMatch implements search.ismatch inside $filter.
func (pl *searchPlan) isMatch(s *Service) func(*fenv, string, string, string, string) bool {
	return func(e *fenv, text, fields, qtype, mode string) bool {
		sub := &searchRequest{Search: text, SearchFields: splitCSV(fields), QueryType: qtype, SearchMode: mode}
		if sub.finish() != nil {
			return false
		}

		sf, err := pl.ix.searchFields(sub)
		if err != nil {
			return false
		}

		q, err := parseQuery(text, qtype == queryFull, mode == modeAll, nil)
		if err != nil {
			return false
		}

		sc := &searchCtx{ix: pl.ix, fields: sf, syn: s.synonymRules(pl.ix), anyMode: mode == modeAny}
		ok, _ := sc.eval(q, e.d)

		return ok
	}
}

func (pl *searchPlan) passes(d *Doc) bool {
	return pl.filter == nil || pl.filter(&fenv{d: d, doc: d.Fields, vars: map[string]any{}})
}

func (ix *Index) orderedDocs() []*Doc {
	docs := make([]*Doc, 0, len(ix.Docs))
	for _, d := range ix.Docs {
		docs = append(docs, d)
	}

	sort.Slice(docs, func(i, j int) bool { return docs[i].Seq < docs[j].Seq })

	return docs
}

func (pl *searchPlan) textHits(docs []*Doc) []hit {
	var out []hit

	for _, d := range docs {
		if !pl.passes(d) {
			continue
		}

		ok, score := pl.ctx.eval(pl.query, d)
		if !ok {
			continue
		}

		if !pl.text {
			score = 1
		}

		if pl.prof != nil {
			score *= pl.prof.functionBoost(d, pl.params, pl.now)
		}

		out = append(out, hit{doc: d, key: keyOf(pl.ix, d), score: score})
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].score > out[j].score })

	return out
}

func keyOf(ix *Index, d *Doc) string {
	k, _ := d.Fields[ix.comp.key.Name].(string)

	return k
}

// search runs the full query pipeline over the index.
func (s *Service) search(ix *Index, req *searchRequest, version string, now time.Time) (*searchResult, error) {
	_ = version

	pl, err := s.plan(ix, req, now)
	if err != nil {
		return nil, err
	}

	order, err := compileOrderBy(req.OrderBy, ix.comp)
	if err != nil {
		return nil, err
	}

	if err = validateSelect(ix.comp, req.Select); err != nil {
		return nil, err
	}

	docs := ix.orderedDocs()

	var hits []hit

	switch {
	case len(req.Vectors) == 0:
		hits = pl.textHits(docs)
	default:
		hits, err = pl.vectorHits(docs)
		if err != nil {
			return nil, err
		}
	}

	if order != nil {
		order.sortHits(hits)
	}

	res := &searchResult{Hits: hits, Count: len(hits)}

	if res.Facets, err = computeFacets(ix.comp, req.Facets, hits); err != nil {
		return nil, err
	}

	res.Matchers = []matcherSet{{fn: pl.matcher()}}

	return res, nil
}

func scoreFloor(f float64) float64 {
	if math.IsNaN(f) {
		return 0
	}

	return f
}
