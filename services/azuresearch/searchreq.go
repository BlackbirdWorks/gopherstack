package azuresearch

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
)

const (
	defaultTop  = 50
	maxTop      = 1000
	maxSkip     = 100000
	modeAny     = "any"
	modeAll     = "all"
	queryFull   = "full"
	querySimple = "simple"
)

type vectorQuery struct {
	Kind       string
	Fields     string
	Vector     []float64
	K          int
	Weight     float64
	Exhaustive bool
}

type searchRequest struct {
	Search         string
	SearchMode     string
	QueryType      string
	Filter         string
	OrderBy        string
	ScoringProfile string
	PreTag         string
	PostTag        string
	VectorFilter   string
	SearchFields   []string
	Select         []string
	Facets         []string
	Highlight      []string
	ScoringParams  []string
	Vectors        []vectorQuery
	Top            int
	Skip           int
	Count          bool
	TopSet         bool
}

func splitCSV(s string) []string {
	var out []string

	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}

	return out
}

func intParam(s, name string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, badf("Invalid value for parameter '%s'.", name)
	}

	return n, nil
}

func (r *searchRequest) finish() error {
	if r.SearchMode == "" {
		r.SearchMode = modeAny
	}

	if r.QueryType == "" {
		r.QueryType = querySimple
	}

	if err := r.validateModes(); err != nil {
		return err
	}

	if !r.TopSet {
		r.Top = defaultTop
	}

	if r.Top < 0 || r.Top > maxTop && r.TopSet {
		return badf("Invalid value for $top: it must be between 0 and %d.", maxTop)
	}

	if r.Skip < 0 || r.Skip > maxSkip {
		return badf("Invalid value for $skip: it must be between 0 and %d.", maxSkip)
	}

	if r.PreTag == "" {
		r.PreTag = "<em>"
	}

	if r.PostTag == "" {
		r.PostTag = "</em>"
	}

	return nil
}

func (r *searchRequest) validateModes() error {
	if r.SearchMode != modeAny && r.SearchMode != modeAll {
		return badf("Invalid value for searchMode: '%s'.", r.SearchMode)
	}

	switch r.QueryType {
	case querySimple, queryFull:
		return nil
	case "semantic":
		return badf("Semantic ranking is not supported by this service.")
	}

	return badf("Invalid value for queryType: '%s'.", r.QueryType)
}

// searchFromQuery parses the GET form of a search request.
func searchFromQuery(raw url.Values) (*searchRequest, error) {
	q := make(url.Values, len(raw))
	for k, v := range raw {
		q[strings.ToLower(k)] = append(q[strings.ToLower(k)], v...)
	}

	r := &searchRequest{
		Search: q.Get("search"), SearchMode: q.Get("searchmode"), QueryType: q.Get("querytype"),
		Filter: q.Get("$filter"), OrderBy: q.Get("$orderby"), ScoringProfile: q.Get("scoringprofile"),
		SearchFields: splitCSV(q.Get("searchfields")), Select: splitCSV(q.Get("$select")),
		Highlight: splitCSV(q.Get("highlight")), PreTag: q.Get("highlightpretag"), PostTag: q.Get("highlightposttag"),
		Facets: q["facet"], ScoringParams: q["scoringparameter"], Count: strings.EqualFold(q.Get("$count"), "true"),
	}

	var err error

	if v := q.Get("$top"); v != "" {
		r.TopSet = true
		if r.Top, err = intParam(v, "$top"); err != nil {
			return nil, err
		}
	}

	if v := q.Get("$skip"); v != "" {
		if r.Skip, err = intParam(v, "$skip"); err != nil {
			return nil, err
		}
	}

	return r, r.finish()
}

// searchFromBody parses the POST form of a search request.
func searchFromBody(body []byte) (*searchRequest, error) {
	var raw map[string]any

	if err := decodeJSON(body, &raw); err != nil {
		return nil, err
	}

	r := &searchRequest{
		Search: strOf(raw, "search"), SearchMode: strOf(raw, "searchMode"), QueryType: strOf(raw, "queryType"),
		Filter: strOf(raw, "filter"), OrderBy: strOf(raw, "orderby"), ScoringProfile: strOf(raw, "scoringProfile"),
		SearchFields: splitCSV(strOf(raw, "searchFields")), Select: splitCSV(strOf(raw, "select")),
		Highlight: splitCSV(strOf(raw, "highlight")), PreTag: strOf(raw, "highlightPreTag"),
		PostTag: strOf(raw, "highlightPostTag"), VectorFilter: strOf(raw, "vectorFilterMode"),
		Count: boolOf(raw, fieldCount, false),
	}

	for _, f := range listOf(raw, "facets") {
		s, _ := f.(string)
		r.Facets = append(r.Facets, s)
	}

	for _, f := range listOf(raw, "scoringParameters") {
		s, _ := f.(string)
		r.ScoringParams = append(r.ScoringParams, s)
	}

	if n, ok := raw["top"].(json.Number); ok {
		r.TopSet = true
		r.Top, _ = intParam(n.String(), "top")
	}

	if n, ok := raw["skip"].(json.Number); ok {
		r.Skip, _ = intParam(n.String(), "skip")
	}

	vqs, err := parseVectorQueries(listOf(raw, "vectorQueries"))
	if err != nil {
		return nil, err
	}

	r.Vectors = vqs

	return r, r.finish()
}

func parseVectorQueries(list []any) ([]vectorQuery, error) {
	out := make([]vectorQuery, 0, len(list))

	for _, raw := range list {
		m := objOf(raw)
		vq := vectorQuery{Kind: strOf(m, "kind"), Fields: strOf(m, "fields"), K: defaultTop, Weight: 1}

		if vq.Kind != "vector" {
			return nil, badf("Vector query kind '%s' is not supported: the service has no vectorizer.", vq.Kind)
		}

		if n, ok := m["k"].(json.Number); ok {
			vq.K, _ = intParam(n.String(), "k")
		}

		if w, ok := m["weight"].(json.Number); ok {
			vq.Weight, _ = w.Float64()
		}

		vq.Exhaustive = boolOf(m, "exhaustive", false)

		for _, x := range listOf(m, "vector") {
			n, ok := x.(json.Number)
			if !ok {
				return nil, badf("The vector query must contain only numbers.")
			}

			f, _ := n.Float64()
			vq.Vector = append(vq.Vector, f)
		}

		if len(vq.Vector) == 0 || vq.Fields == "" || vq.K <= 0 {
			return nil, badf("A vector query requires vector, fields and a positive k.")
		}

		out = append(out, vq)
	}

	return out, nil
}

func decodeJSON(body []byte, out any) error {
	if len(strings.TrimSpace(string(body))) == 0 {
		return badf("The request body is empty.")
	}

	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()

	if err := dec.Decode(out); err != nil {
		return badf("The request is invalid. Details: the body is not valid JSON.")
	}

	return nil
}

// parseScoringParams turns "name-v1,v2" entries into a map.
func parseScoringParams(list []string) (scoreParams, error) {
	out := scoreParams{}

	for _, e := range list {
		name, vals, ok := strings.Cut(e, "-")
		if !ok || name == "" {
			return nil, badf("Invalid scoring parameter '%s': expected name-value.", e)
		}

		out[name] = splitUnescaped(vals, ',')
	}

	return out, nil
}

// decodeDefJSON decodes a resource definition (plain float64 numbers).
func decodeDefJSON(body []byte) (map[string]any, error) {
	var out map[string]any

	if len(strings.TrimSpace(string(body))) == 0 {
		return nil, badf("The request is invalid. Details: the request body is empty.")
	}

	if err := json.Unmarshal(body, &out); err != nil || out == nil {
		return nil, badf("The request is invalid. Details: the body is not a valid JSON object.")
	}

	return out, nil
}
