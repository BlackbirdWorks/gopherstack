package bqsql

import (
	"database/sql/driver"
	"encoding/json/v2"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/text/unicode/norm"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// SEARCH and TEXT_ANALYZE follow
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/search_functions and
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/text-analysis. No search index exists, so every
// call scans its data.
const (
	analyzerLog     = "LOG_ANALYZER"
	analyzerNoOp    = "NO_OP_ANALYZER"
	analyzerPattern = "PATTERN_ANALYZER"
	scopeValues     = "JSON_VALUES"
	scopeKeys       = "JSON_KEYS"
	scopeBoth       = "JSON_KEYS_AND_VALUES"
	defaultPattern  = `\b\w{2,}\b`
	searchReserved  = "[]<>(){}|!'\"*&?+/:=-\\~^"
	tagString       = "s"
	tagJSON         = "j"
	tagJSONType     = "J"
	tagNone         = "n"
	searchArgs      = 6
	searchMaxArgs   = 5
)

type analyzerSpec struct {
	name     string
	delims   []string
	patterns []*regexp.Regexp
	filters  []tokenFilter
}

type tokenFilter struct {
	stopWords map[string]bool
	mode      string
	icuNorm   string
	icuFold   bool
}

func defaultDelimiters() []string {
	d := []string{"\n", "\r", "\t", " "}
	for _, c := range `[]<>(){}|!;,'"*&?+/:=@.-$%\_` {
		d = append(d, string(c))
	}

	return append(d, "%21", "%26", "%2526", "%3B", "%3b", "%7C", "%7c", "%20", "%2B", "%2b", "%3D", "%3d", "%2520",
		"%5D", "%5d", "%5B", "%5b", "%3A", "%3a", "%0A", "%0a", "%2C", "%2c", "%28", "%29")
}

func newAnalyzer(name, options string) (*analyzerSpec, error) {
	if name == "" {
		name = analyzerLog
	}

	a := &analyzerSpec{name: strings.ToUpper(name)}

	var opts map[string]any

	if strings.TrimSpace(options) != "" {
		if err := json.Unmarshal([]byte(options), &opts, jsonx.V1()); err != nil {
			return nil, errBQ("analyzer_options is not valid JSON: %v", err)
		}
	}

	switch a.name {
	case analyzerLog:
		a.setDelimiters(opts)
	case analyzerPattern:
		if err := a.setPatterns(opts); err != nil {
			return nil, err
		}
	case analyzerNoOp:
		return a, nil
	default:
		return nil, errBQ("Unsupported analyzer %s", name)
	}

	return a, a.parseFilters(opts["token_filters"])
}

func (a *analyzerSpec) setDelimiters(opts map[string]any) {
	a.delims = defaultDelimiters()

	if raw, ok := opts["delimiters"].([]any); ok {
		a.delims = nil

		for _, d := range raw {
			if s, isStr := d.(string); isStr && s != "" {
				a.delims = append(a.delims, s)
			}
		}
	}

	sort.SliceStable(a.delims, func(i, j int) bool { return len(a.delims[i]) > len(a.delims[j]) })
}

func (a *analyzerSpec) setPatterns(opts map[string]any) error {
	pats := []string{defaultPattern}

	if raw, ok := opts["patterns"].([]any); ok && len(raw) > 0 {
		pats = make([]string, 0, len(raw))

		for _, p := range raw {
			pats = append(pats, strVal(p))
		}
	}

	for _, p := range pats {
		re, err := regexp.Compile(p)
		if err != nil {
			return errBQ("invalid pattern %q: %v", p, err)
		}

		a.patterns = append(a.patterns, re)
	}

	return nil
}

func (a *analyzerSpec) parseFilters(raw any) error {
	list, _ := raw.([]any)
	if len(list) == 0 {
		a.filters = []tokenFilter{{mode: modeLower}}

		return nil
	}

	for _, e := range list {
		m, _ := e.(map[string]any)

		switch {
		case m["stop_words"] != nil:
			words, _ := m["stop_words"].([]any)
			if len(words) == 0 {
				return errBQ("stop_words needs at least one element")
			}

			f := tokenFilter{mode: "STOP", stopWords: map[string]bool{}}
			for _, w := range words {
				s, _ := w.(string)
				f.stopWords[s] = true
			}

			a.filters = append(a.filters, f)
		case m["normalizer"] != nil:
			n, _ := m["normalizer"].(map[string]any)

			f := tokenFilter{mode: strings.ToUpper(strVal(n["mode"])), icuNorm: "NFKC", icuFold: true}
			if v := strVal(n["icu_normalize_mode"]); v != "" {
				f.icuNorm = strings.ToUpper(v)
			}

			if b, ok := n["icu_case_folding"].(bool); ok {
				f.icuFold = b
			}

			switch f.mode {
			case "NONE", modeLower, "UNICODE_LOWER", "ICU_NORMALIZE":
			default:
				return errBQ("Unsupported normalizer mode %q", f.mode)
			}

			a.filters = append(a.filters, f)
		default:
			return errBQ("Unsupported token filter")
		}
	}

	return nil
}

func strVal(v any) string {
	s, _ := v.(string)

	return s
}

func (f tokenFilter) apply(tok string) (string, bool) {
	switch f.mode {
	case "STOP":
		return tok, !f.stopWords[tok]
	case modeLower:
		return asciiLower(tok), true
	case "UNICODE_LOWER":
		return strings.ToLower(tok), true
	case "ICU_NORMALIZE":
		forms := map[string]norm.Form{"NFC": norm.NFC, "NFKC": norm.NFKC, "NFD": norm.NFD, "NFKD": norm.NFKD}
		tok = forms[f.icuNorm].String(tok)

		if f.icuFold {
			tok = strings.ToLower(tok)
		}

		return tok, true
	}

	return tok, true
}

func asciiLower(s string) string {
	b := []byte(s)

	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}

	return string(b)
}

// splitTokens splits text into raw tokens before filters run.
func (a *analyzerSpec) splitTokens(text string) []string {
	switch a.name {
	case analyzerNoOp:
		return []string{text}
	case analyzerPattern:
		return a.patternTokens(text)
	}

	var out []string

	start := 0

	for i := 0; i < len(text); {
		n := a.delimAt(text, i)
		if n == 0 {
			i++

			continue
		}

		if i > start {
			out = append(out, text[start:i])
		}

		i += n
		start = i
	}

	if start < len(text) {
		out = append(out, text[start:])
	}

	return out
}

// patternTokens takes the leftmost match of any pattern, drops the input up to it and repeats.
func (a *analyzerSpec) patternTokens(text string) []string {
	var out []string

	for rest := text; rest != ""; {
		var best []int

		for _, re := range a.patterns {
			if loc := re.FindStringIndex(rest); loc != nil && loc[1] > loc[0] && (best == nil || loc[0] < best[0]) {
				best = loc
			}
		}

		if best == nil {
			break
		}

		out = append(out, rest[best[0]:best[1]])
		rest = rest[best[1]:]
	}

	return out
}

func (a *analyzerSpec) delimAt(text string, i int) int {
	for _, d := range a.delims {
		if strings.HasPrefix(text[i:], d) {
			return len(d)
		}
	}

	return 0
}

func (a *analyzerSpec) tokens(text string) []string {
	raw := a.splitTokens(text)
	if a.name == analyzerNoOp {
		return raw
	}

	out := make([]string, 0, len(raw))

	for _, t := range raw {
		keep := true

		for _, f := range a.filters {
			if t, keep = f.apply(t); !keep {
				break
			}
		}

		if keep {
			out = append(out, t)
		}
	}

	return out
}

func containsPhrase(data, phrase []string) bool {
	if len(phrase) == 0 {
		return false
	}

	for i := 0; i+len(phrase) <= len(data); i++ {
		match := true

		for j, p := range phrase {
			if data[i+j] != p {
				match = false

				break
			}
		}

		if match {
			return true
		}
	}

	return false
}

type searchNode struct {
	left  *searchNode
	right *searchNode
	text  string
	kind  byte
}

type searchParser struct {
	src   []rune
	pos   int
	depth int
}

const searchMaxDepth = 16

func hasLogicalWord(s string) bool {
	for w := range strings.FieldsSeq(s) {
		if w == litAnd || w == litOr {
			return true
		}
	}

	return false
}

func parseSearchQuery(q string) (*searchNode, error) {
	return parseSearchQueryAt(q, 0)
}

func parseSearchQueryAt(q string, depth int) (*searchNode, error) {
	p := &searchParser{src: []rune(q), depth: depth}

	n, err := p.or()
	if err != nil {
		return nil, err
	}

	p.skip()

	if p.pos < len(p.src) {
		return nil, errBQ("Unexpected %q in the search query", string(p.src[p.pos]))
	}

	return n, nil
}

func (p *searchParser) skip() {
	for p.pos < len(p.src) && (p.src[p.pos] == ' ' || p.src[p.pos] == '\t' || p.src[p.pos] == '\n') {
		p.pos++
	}
}

func (p *searchParser) word() string {
	p.skip()

	start := p.pos
	for p.pos < len(p.src) && !strings.ContainsRune(" \t\n()\"`", p.src[p.pos]) {
		p.pos++
	}

	return string(p.src[start:p.pos])
}

func (p *searchParser) peekWord() string {
	save := p.pos
	w := p.word()
	p.pos = save

	return w
}

func (p *searchParser) or() (*searchNode, error) {
	left, err := p.and()
	if err != nil {
		return nil, err
	}

	for p.peekWord() == "OR" {
		p.word()

		right, rerr := p.and()
		if rerr != nil {
			return nil, rerr
		}

		left = &searchNode{kind: 'o', left: left, right: right}
	}

	return left, nil
}

func (p *searchParser) and() (*searchNode, error) {
	left, err := p.atom()
	if err != nil {
		return nil, err
	}

	for {
		p.skip()

		if p.pos >= len(p.src) || p.src[p.pos] == ')' || p.peekWord() == "OR" {
			return left, nil
		}

		if p.peekWord() == "AND" {
			p.word()
		}

		right, rerr := p.atom()
		if rerr != nil {
			return nil, rerr
		}

		left = &searchNode{kind: 'a', left: left, right: right}
	}
}

func (p *searchParser) atom() (*searchNode, error) {
	p.skip()

	if p.pos >= len(p.src) {
		return nil, errBQ("The search query ends unexpectedly")
	}

	switch c := p.src[p.pos]; c {
	case '(':
		p.pos++

		n, err := p.or()
		if err != nil {
			return nil, err
		}

		p.skip()

		if p.pos >= len(p.src) || p.src[p.pos] != ')' {
			return nil, errBQ("Missing ) in the search query")
		}

		p.pos++

		return n, nil
	case '"', '`':
		return p.quoted(c)
	}

	w := p.word()

	switch w {
	case "":
		return nil, errBQ("Unexpected input in the search query")
	case "NOT", "IN", "NEAR", "AND", "OR":
		return nil, errBQ("The reserved word %s must be wrapped in backticks to be searched", w)
	}

	return &searchNode{kind: 't', text: unescapeSearch(w)}, nil
}

func (p *searchParser) quoted(q rune) (*searchNode, error) {
	p.pos++

	var sb strings.Builder

	for p.pos < len(p.src) {
		c := p.src[p.pos]

		if c == '\\' && p.pos+1 < len(p.src) && (p.src[p.pos+1] == q || q == '"' && p.src[p.pos+1] == '\\') {
			sb.WriteRune(p.src[p.pos+1])

			p.pos += 2

			continue
		}

		if c == q {
			p.pos++

			kind := byte('p')
			if q == '`' {
				kind = 'b'
			}

			// search_functions example: "foo AND (bar OR baz)" matches 'foo bar', so operators in a phrase are logical.
			if kind == 'p' && p.depth < searchMaxDepth && hasLogicalWord(sb.String()) {
				if n, err := parseSearchQueryAt(sb.String(), p.depth+1); err == nil {
					return n, nil
				}
			}

			return &searchNode{kind: kind, text: sb.String()}, nil
		}

		sb.WriteRune(c)
		p.pos++
	}

	return nil, errBQ("Unterminated %c in the search query", q)
}

func unescapeSearch(w string) string {
	var sb strings.Builder

	rs := []rune(w)

	for i := 0; i < len(rs); i++ {
		if rs[i] == '\\' && i+1 < len(rs) && strings.ContainsRune(searchReserved, rs[i+1]) {
			i++
		}

		sb.WriteRune(rs[i])
	}

	return sb.String()
}

// eval reports whether the data string satisfies n.
func (a *analyzerSpec) eval(n *searchNode, data string, dataTokens []string) bool {
	switch n.kind {
	case 'a':
		return a.eval(n.left, data, dataTokens) && a.eval(n.right, data, dataTokens)
	case 'o':
		return a.eval(n.left, data, dataTokens) || a.eval(n.right, data, dataTokens)
	case 'b':
		return a.exactMatch(data, n.text)
	case 'p':
		return containsPhrase(dataTokens, a.tokens(n.text))
	}

	want := a.tokens(n.text)
	if len(want) == 0 {
		return false
	}

	have := make(map[string]bool, len(dataTokens))
	for _, t := range dataTokens {
		have[t] = true
	}

	for _, t := range want {
		if !have[t] {
			return false
		}
	}

	return true
}

// exactMatch finds term in data on delimiter boundaries, case-sensitively.
func (a *analyzerSpec) exactMatch(data, term string) bool {
	if term == "" {
		return false
	}

	for from := 0; ; {
		i := strings.Index(data[from:], term)
		if i < 0 {
			return false
		}

		i += from
		end := i + len(term)

		if (i == 0 || a.delimEndsAt(data, i)) && (end == len(data) || a.delimAt(data, end) > 0) {
			return true
		}

		from = i + 1
	}
}

func (a *analyzerSpec) delimEndsAt(data string, i int) bool {
	for _, d := range a.delims {
		if strings.HasSuffix(data[:i], d) {
			return true
		}
	}

	return false
}

func searchLeaves(v driver.Value, tag, scope string) ([]string, bool) {
	switch tag {
	case tagString:
		if v == nil {
			return nil, true
		}

		return []string{toStr(v)}, false
	case tagJSON, tagJSONType:
		if v == nil {
			return nil, true
		}

		var doc any
		if err := json.Unmarshal([]byte(toStr(v)), &doc, jsonx.V1()); err != nil {
			return nil, false
		}

		return walkSearchJSON(doc, tag == tagJSONType, scope, nil), false
	}

	return nil, false
}

func walkSearchJSON(v any, isJSON bool, scope string, out []string) []string {
	switch t := v.(type) {
	case string:
		if !isJSON || scope != scopeKeys {
			out = append(out, t)
		}
	case []any:
		for _, e := range t {
			out = walkSearchJSON(e, isJSON, scope, out)
		}
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			if isJSON && (scope == scopeKeys || scope == scopeBoth) {
				out = append(out, k)
			}

			out = walkSearchJSON(t[k], isJSON, scope, out)
		}
	}

	return out
}

// udfSearch takes (data, tag, query, json_scope, analyzer, analyzer_options).
func (a *analyzerSpec) matches(root *searchNode, leaf, query string) bool {
	if a.name == analyzerNoOp {
		return leaf == query
	}

	return a.eval(root, leaf, a.tokens(leaf))
}

func udfSearch(a []driver.Value) (driver.Value, error) {
	if a[2] == nil {
		return nil, errBQ("SEARCH: the search query must not be NULL")
	}

	scope := strings.ToUpper(toStr(a[3]))
	switch scope {
	case "", scopeValues, scopeKeys, scopeBoth:
	default:
		return nil, errBQ("SEARCH: invalid json_scope %q", scope)
	}

	an, err := newAnalyzer(toStr(a[4]), toStr(a[5]))
	if err != nil {
		return nil, err
	}

	query := toStr(a[2])

	var root *searchNode

	if an.name == analyzerNoOp {
		root = &searchNode{kind: 'n', text: query}
	} else {
		if root, err = parseSearchQuery(query); err != nil {
			return nil, err
		}

		if len(an.tokens(strings.NewReplacer("`", "", "\"", "").Replace(query))) == 0 {
			return nil, errBQ("SEARCH: the search query produced no tokens")
		}
	}

	leaves, hasNull := searchLeaves(a[0], toStr(a[1]), scope)

	for _, leaf := range leaves {
		if an.matches(root, leaf, query) {
			return boolVal(true), nil
		}
	}

	if hasNull {
		return nil, nil //nolint:nilnil // NULL when a field is NULL and nothing matched
	}

	return boolVal(false), nil
}

func udfTextAnalyze(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	an, err := newAnalyzer(toStr(a[1]), toStr(a[2]))
	if err != nil {
		return nil, err
	}

	toks := an.tokens(toStr(a[0]))
	out := make([]any, len(toks))

	for i, t := range toks {
		out[i] = t
	}

	return encodeJSON(out), nil
}

func addSearchUDFs(d map[string]udfDef) {
	d["search"] = udfDef{n: searchArgs, fn: udfSearch}
	d["text_analyze"] = udfDef{n: argc3, fn: udfTextAnalyze}
}

func addSearchFuncs(t map[string]*fnDef) {
	t["SEARCH"] = custom(arityTwo, searchMaxArgs, genSearch)
	t["TEXT_ANALYZE"] = custom(1, argc3, genNamed("text_analyze",
		[]string{"text", "analyzer", "analyzer_options"}, []string{"", "''", "''"}, arrayOf(scalar(KindString))))
}

func genSearch(c *compiler, n *node, sc *scope) (*gx, error) {
	ordered, err := namedArgs(n, []string{"data", "query", "json_scope", "analyzer", "analyzer_options"})
	if err != nil {
		return nil, err
	}

	if ordered[0] == nil || ordered[1] == nil {
		return nil, invalid("SEARCH requires the data to search and a search query")
	}

	data, err := c.expr(ordered[0], sc)
	if err != nil {
		return nil, err
	}

	sqls := []string{data.sql, quoteStr(tagNone)}

	switch data.typ.Kind {
	case KindString:
		sqls[1] = quoteStr(tagString)
	case KindStruct, KindArray:
		sqls[0], sqls[1] = "CAST(json("+data.sql+") AS TEXT)", quoteStr(tagJSON)
	case KindJSON:
		sqls[1] = quoteStr(tagJSONType)
	case KindUnknown, KindInt, KindFloat, KindBool, KindBytes, KindTimestamp, KindDate, KindTime, KindDatetime,
		KindNumeric, KindBigNumeric, KindGeography, KindInterval:
	}

	for i, def := range []string{"", "''", "''", "''"} {
		a := ordered[i+1]
		if a == nil {
			sqls = append(sqls, def)

			continue
		}

		g, gerr := c.expr(a, sc)
		if gerr != nil {
			return nil, gerr
		}

		sqls = append(sqls, g.sql)
	}

	return &gx{sql: "bq_search(" + strings.Join(sqls, ", ") + ")", typ: scalar(KindBool)}, nil
}
