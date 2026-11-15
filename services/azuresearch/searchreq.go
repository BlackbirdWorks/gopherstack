package azuresearch

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	defaultTop  = 50
	maxTop      = 1000
	maxSkip     = 100000
	modeAny     = "any"
	modeAll     = "all"
	queryFull   = "full"
	querySimple = "simple"

	vecQueryVector      = "vector"
	vecQueryText        = "text"
	thresholdSimilarity = "vectorSimilarity"
	thresholdScore      = "searchScore"
	debugVector         = "vector"
	debugDisabled       = accessDisabled
	countAllResults     = "countAllResults"
	countRetrievable    = "countRetrievableResults"
	maxTextRecallLimit  = 10000
	defaultTextRecall   = 1000
)

type vectorThreshold struct {
	Kind  string
	Value float64
}

type vectorQuery struct {
	Threshold      *vectorThreshold
	Kind           string
	Fields         string
	Text           string
	FilterOverride string
	Vector         []float64
	K              int
	Weight         float64
	Oversampling   float64
	Exhaustive     bool
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
	Debug          string
	CountMode      string
	SearchFields   []string
	Select         []string
	Facets         []string
	Highlight      []string
	ScoringParams  []string
	Vectors        []vectorQuery
	Top            int
	Skip           int
	MaxTextRecall  int
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

	if err := r.validateVectorOptions(); err != nil {
		return err
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

	if n, ok := raw["top"].(jsonx.Number); ok {
		r.TopSet = true
		r.Top, _ = intParam(n.String(), "top")
	}

	if n, ok := raw["skip"].(jsonx.Number); ok {
		r.Skip, _ = intParam(n.String(), "skip")
	}

	vqs, err := parseVectorQueries(listOf(raw, "vectorQueries"))
	if err != nil {
		return nil, err
	}

	r.Vectors = vqs

	if err = r.parseVectorOptions(raw); err != nil {
		return nil, err
	}

	return r, r.finish()
}

func parseVectorQueries(list []any) ([]vectorQuery, error) {
	out := make([]vectorQuery, 0, len(list))

	for _, raw := range list {
		vq, err := parseVectorQuery(objOf(raw))
		if err != nil {
			return nil, err
		}

		out = append(out, vq)
	}

	return out, nil
}

func parseVectorQuery(m map[string]any) (vectorQuery, error) {
	vq := vectorQuery{Kind: strOf(m, "kind"), Fields: strOf(m, "fields"), K: defaultTop, Weight: 1}

	switch vq.Kind {
	case vecQueryVector, vecQueryText:
	default:
		return vq, badf("Vector query kind '%s' is not supported: only vector and text queries are accepted.", vq.Kind)
	}

	vq.Text = strOf(m, "text")
	vq.FilterOverride = strOf(m, "filterOverride")
	vq.Exhaustive = boolOf(m, "exhaustive", false)

	if n, ok := m["k"].(jsonx.Number); ok {
		vq.K, _ = intParam(n.String(), "k")
	}

	if w, ok := m["weight"].(jsonx.Number); ok {
		vq.Weight, _ = w.Float64()
	}

	if err := parseVectorExtras(&vq, m); err != nil {
		return vq, err
	}

	if vq.Fields == "" || vq.K <= 0 || vq.Weight <= 0 {
		return vq, badf("A vector query requires fields, a positive k and a positive weight.")
	}

	if vq.Kind == vecQueryVector && len(vq.Vector) == 0 || vq.Kind == vecQueryText && vq.Text == "" {
		return vq, badf("A vector query requires a vector, or text for a text query.")
	}

	return vq, nil
}

func parseVectorExtras(vq *vectorQuery, m map[string]any) error {
	if o, ok := m["oversampling"].(jsonx.Number); ok {
		vq.Oversampling, _ = o.Float64()
		if vq.Oversampling < minOversampling {
			return badf("The vector query oversampling must be at least 1.")
		}
	}

	if m["threshold"] != nil {
		t, err := parseThreshold(m["threshold"])
		if err != nil {
			return err
		}

		vq.Threshold = &t
	}

	for _, x := range listOf(m, "vector") {
		n, ok := x.(jsonx.Number)
		if !ok {
			return badf("The vector query must contain only numbers.")
		}

		f, _ := n.Float64()
		vq.Vector = append(vq.Vector, f)
	}

	return nil
}

func parseThreshold(raw any) (vectorThreshold, error) {
	m := objOf(raw)
	kind := strOf(m, "kind")

	if kind != thresholdSimilarity && kind != thresholdScore {
		return vectorThreshold{}, badf("The vector query threshold kind '%s' is not supported.", kind)
	}

	n, ok := m["value"].(jsonx.Number)
	if !ok {
		return vectorThreshold{}, badf("The vector query threshold requires a numeric value.")
	}

	v, _ := n.Float64()

	return vectorThreshold{Kind: kind, Value: v}, nil
}

func (r *searchRequest) parseVectorOptions(raw map[string]any) error {
	r.Debug = strOf(raw, "debug")

	hs := objOf(raw["hybridSearch"])
	if hs == nil {
		return nil
	}

	r.CountMode = strOf(hs, "countAndFacetMode")

	if n, ok := hs["maxTextRecallSize"].(jsonx.Number); ok {
		r.MaxTextRecall, _ = intParam(n.String(), "maxTextRecallSize")
		if r.MaxTextRecall < 1 || r.MaxTextRecall > maxTextRecallLimit {
			return badf("The value of maxTextRecallSize must be between 1 and %d.", maxTextRecallLimit)
		}
	}

	return nil
}

func (r *searchRequest) validateVectorOptions() error {
	switch r.Debug {
	case "", debugDisabled, debugVector:
	default:
		return badf("Invalid value for debug: '%s'.", r.Debug)
	}

	switch r.CountMode {
	case "", countAllResults, countRetrievable:
	default:
		return badf("Invalid value for countAndFacetMode: '%s'.", r.CountMode)
	}

	return nil
}

func decodeJSON(body []byte, out any) error {
	if len(strings.TrimSpace(string(body))) == 0 {
		return badf("The request body is empty.")
	}

	dec := jsontext.NewDecoder(strings.NewReader(string(body)))

	if err := json.UnmarshalDecode(dec, out, jsonx.V1(), jsonx.UseNumber()); err != nil {
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

	if err := json.Unmarshal(body, &out, jsonx.V1()); err != nil || out == nil {
		return nil, badf("The request is invalid. Details: the body is not a valid JSON object.")
	}

	return out, nil
}
