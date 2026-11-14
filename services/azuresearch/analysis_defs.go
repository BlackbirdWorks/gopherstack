package azuresearch

import (
	"html"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	typePatternAnalyzer     = "#Microsoft.Azure.Search.PatternAnalyzer"
	typePatternTokenizer    = "#Microsoft.Azure.Search.PatternTokenizer"
	typeMappingCharFilter   = "#Microsoft.Azure.Search.MappingCharFilter"
	typePatternReplaceChar  = "#Microsoft.Azure.Search.PatternReplaceCharFilter"
	typePatternReplaceToken = "#Microsoft.Azure.Search.PatternReplaceTokenFilter"
	typeLengthFilter        = "#Microsoft.Azure.Search.LengthTokenFilter"
	typeElisionFilter       = "#Microsoft.Azure.Search.ElisionTokenFilter"
	typeLimitFilter         = "#Microsoft.Azure.Search.LimitTokenFilter"
	typeNGramFilter         = "#Microsoft.Azure.Search.NGramTokenFilterV2"
	typeEdgeNGramFilter     = "#Microsoft.Azure.Search.EdgeNGramTokenFilterV2"

	maxAnalysisLength = 300
	defaultMaxGram    = 2
	defaultPattern    = `\W+`
	mappingArrow      = "=>"
)

type tokenFilterFn = func([]Token) []Token

// analysisDefs holds the tokenizers, token filters and char filters an index definition declares by name.
type analysisDefs struct {
	tokenizers  map[string]Analyzer
	filters     map[string]tokenFilterFn
	charFilters map[string]func(string) string
}

func newAnalysisDefs() *analysisDefs {
	return &analysisDefs{
		tokenizers:  map[string]Analyzer{},
		filters:     map[string]tokenFilterFn{},
		charFilters: map[string]func(string) string{},
	}
}

func (d *analysisDefs) tokenizer(name string) (Analyzer, bool) {
	if d != nil {
		if fn, ok := d.tokenizers[name]; ok {
			return fn, true
		}
	}

	return customTokenizer(name)
}

func (d *analysisDefs) filter(name string, stops map[string]bool) (tokenFilterFn, bool) {
	if d != nil {
		if fn, ok := d.filters[name]; ok {
			return fn, true
		}
	}

	return customFilter(name, stops)
}

func (d *analysisDefs) charFilter(name string) (func(string) string, bool) {
	if d != nil {
		if fn, ok := d.charFilters[name]; ok {
			return fn, true
		}
	}

	if name == "html_strip" {
		return stripHTML, true
	}

	return nil, false
}

func buildAnalysisDefs(def map[string]any) (*analysisDefs, error) {
	d := newAnalysisDefs()

	for _, raw := range listOf(def, "tokenizers") {
		if err := d.addTokenizer(objOf(raw)); err != nil {
			return nil, err
		}
	}

	for _, raw := range listOf(def, "tokenFilters") {
		if err := d.addFilter(objOf(raw)); err != nil {
			return nil, err
		}
	}

	for _, raw := range listOf(def, "charFilters") {
		if err := d.addCharFilter(objOf(raw)); err != nil {
			return nil, err
		}
	}

	return d, nil
}

func (d *analysisDefs) addTokenizer(o map[string]any) error {
	name, kind := strOf(o, "name"), strOf(o, "@odata.type")
	if kind != typePatternTokenizer {
		return badf("The request is invalid. Details: tokenizer '%s' has unsupported type '%s'.", name, kind)
	}

	re, err := patternFrom(o, name)
	if err != nil {
		return err
	}

	group := -1
	if v, ok := o["group"].(float64); ok {
		group = int(v)
	}

	if group < -1 || group > re.NumSubexp() {
		return badf("The request is invalid. Details: tokenizer '%s': group must be between -1 and the number of "+
			"capture groups in the pattern.", name)
	}

	d.tokenizers[name] = func(text string) []Token { return splitTokens(re, text, group) }

	return nil
}

func (d *analysisDefs) addFilter(o map[string]any) error {
	name, kind := strOf(o, "name"), strOf(o, "@odata.type")

	fn, err := buildFilter(name, kind, o)
	if err != nil {
		return err
	}

	d.filters[name] = fn

	return nil
}

func buildFilter(name, kind string, o map[string]any) (tokenFilterFn, error) {
	switch kind {
	case typePatternReplaceToken:
		re, err := regexp.Compile(strOf(o, "pattern"))
		if err != nil {
			return nil, badf("The request is invalid. Details: token filter '%s' has an invalid pattern.", name)
		}

		repl := javaReplacement(strOf(o, "replacement"))

		return func(t []Token) []Token {
			return mapTerms(t, func(s string) string { return re.ReplaceAllString(s, repl) })
		}, nil
	case typeLengthFilter:
		return lengthFilter(name, o)
	case typeElisionFilter:
		return elisionFilter(o), nil
	case typeLimitFilter:
		maxCount := 1
		if v, ok := o["maxTokenCount"].(float64); ok {
			maxCount = int(v)
		}

		return func(t []Token) []Token { return t[:min(len(t), max(maxCount, 0))] }, nil
	case typeNGramFilter, typeEdgeNGramFilter:
		return gramFilter(name, kind, o)
	}

	return nil, badf("The request is invalid. Details: token filter '%s' has unsupported type '%s'.", name, kind)
}

func intProp(o map[string]any, key string, def int) int {
	if v, ok := o[key].(float64); ok {
		return int(v)
	}

	return def
}

func lengthFilter(name string, o map[string]any) (tokenFilterFn, error) {
	lo, hi := intProp(o, "min", 0), intProp(o, "max", maxAnalysisLength)
	if lo < 0 || hi > maxAnalysisLength || lo >= hi {
		return nil, badf("The request is invalid. Details: token filter '%s': min must be less than max, and max "+
			"cannot exceed %d.", name, maxAnalysisLength)
	}

	return func(t []Token) []Token {
		return slices.DeleteFunc(t, func(tok Token) bool {
			n := utf8.RuneCountInString(tok.Term)

			return n < lo || n > hi
		})
	}, nil
}

func elisionFilter(o map[string]any) tokenFilterFn {
	var articles []string

	for _, a := range listOf(o, "articles") {
		if s, ok := a.(string); ok {
			articles = append(articles, strings.ToLower(s))
		}
	}

	return func(t []Token) []Token {
		return mapTerms(t, func(s string) string {
			i := strings.IndexAny(s, "'’")
			if i <= 0 || !slices.Contains(articles, strings.ToLower(s[:i])) {
				return s
			}

			_, w := utf8.DecodeRuneInString(s[i:])

			return s[i+w:]
		})
	}
}

func gramFilter(name, kind string, o map[string]any) (tokenFilterFn, error) {
	lo, hi := intProp(o, "minGram", 1), intProp(o, "maxGram", defaultMaxGram)
	if lo < 1 || hi > maxAnalysisLength || lo >= hi {
		return nil, badf("The request is invalid. Details: token filter '%s': minGram must be less than maxGram, and "+
			"maxGram cannot exceed %d.", name, maxAnalysisLength)
	}

	back := strings.EqualFold(strOf(o, "side"), "back")
	edge := kind == typeEdgeNGramFilter

	return func(t []Token) []Token {
		var out []Token

		for _, tok := range t {
			for _, g := range grams([]rune(tok.Term), lo, hi, edge, back) {
				out = append(out, Token{Term: g, Pos: tok.Pos, Start: tok.Start, End: tok.End})
			}
		}

		return out
	}, nil
}

func grams(r []rune, lo, hi int, edge, back bool) []string {
	var out []string

	if edge {
		for n := lo; n <= min(hi, len(r)); n++ {
			if back {
				out = append(out, string(r[len(r)-n:]))
			} else {
				out = append(out, string(r[:n]))
			}
		}

		return out
	}

	for i := range r {
		for n := lo; n <= hi && i+n <= len(r); n++ {
			out = append(out, string(r[i:i+n]))
		}
	}

	return out
}

func (d *analysisDefs) addCharFilter(o map[string]any) error {
	name, kind := strOf(o, "name"), strOf(o, "@odata.type")

	switch kind {
	case typeMappingCharFilter:
		fn, err := mappingFilter(name, o)
		if err != nil {
			return err
		}

		d.charFilters[name] = fn
	case typePatternReplaceChar:
		re, err := regexp.Compile(strOf(o, "pattern"))
		if err != nil {
			return badf("The request is invalid. Details: char filter '%s' has an invalid pattern.", name)
		}

		repl := javaReplacement(strOf(o, "replacement"))
		d.charFilters[name] = func(s string) string { return re.ReplaceAllString(s, repl) }
	default:
		return badf("The request is invalid. Details: char filter '%s' has unsupported type '%s'.", name, kind)
	}

	return nil
}

func mappingFilter(name string, o map[string]any) (func(string) string, error) {
	rules := map[string]string{}

	for _, m := range listOf(o, "mappings") {
		s, _ := m.(string)

		from, to, ok := strings.Cut(s, mappingArrow)
		if !ok || from == "" {
			return nil, badf("The request is invalid. Details: char filter '%s': mapping '%s' must have the form "+
				"'a=>b'.", name, s)
		}

		rules[unescapeMapping(from)] = unescapeMapping(to)
	}

	keys := make([]string, 0, len(rules))
	for k := range rules {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, func(a, b string) int { return len(b) - len(a) })

	return func(text string) string {
		var sb strings.Builder

		for i := 0; i < len(text); {
			matched := false

			for _, k := range keys {
				if strings.HasPrefix(text[i:], k) {
					sb.WriteString(rules[k])

					i += len(k)
					matched = true

					break
				}
			}

			if !matched {
				sb.WriteByte(text[i])

				i++
			}
		}

		return sb.String()
	}, nil
}

func unescapeMapping(s string) string {
	return strings.NewReplacer(`\\`, `\`, `\n`, "\n", `\t`, "\t", `\r`, "\r", `\b`, "\b", `\f`, "\f").Replace(s)
}

var htmlTagRE = regexp.MustCompile(`<[^>]*>`)

func stripHTML(s string) string { return html.UnescapeString(htmlTagRE.ReplaceAllString(s, "")) }

// javaReplacement converts a Java Matcher replacement ($1, \$) to Go's Expand syntax.
func javaReplacement(s string) string {
	var sb strings.Builder

	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\' && i+1 < len(s):
			i++

			if s[i] == '$' {
				sb.WriteString("$$")
			} else {
				sb.WriteByte(s[i])
			}
		case c == '$' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
			j := i + 1
			for j < len(s) && s[j] >= '0' && s[j] <= '9' {
				j++
			}

			sb.WriteString("${" + s[i+1:j] + "}")

			i = j - 1
		default:
			sb.WriteByte(c)
		}
	}

	return sb.String()
}

// patternFrom compiles pattern with the RegexFlags (Java regex flag names, pipe separated).
func patternFrom(o map[string]any, name string) (*regexp.Regexp, error) {
	pattern := defaultPattern
	if p, ok := o["pattern"].(string); ok {
		pattern = p
	}

	var prefix strings.Builder

	if flags := strOf(o, "flags"); flags != "" {
		for f := range strings.SplitSeq(flags, "|") {
			switch strings.TrimSpace(f) {
			case "CASE_INSENSITIVE", "UNICODE_CASE":
				prefix.WriteString("(?i)")
			case "DOTALL":
				prefix.WriteString("(?s)")
			case "MULTILINE":
				prefix.WriteString("(?m)")
			case "LITERAL":
				pattern = regexp.QuoteMeta(pattern)
			case "UNIX_LINES", "CANON_EQ":
			case "COMMENTS":
				return nil, badf("The request is invalid. Details: '%s': the COMMENTS flag is not supported.", name)
			default:
				return nil, badf("The request is invalid. Details: '%s': unknown regex flag '%s'.", name, f)
			}
		}
	}

	re, err := regexp.Compile(prefix.String() + pattern)
	if err != nil {
		return nil, badf("The request is invalid. Details: '%s' has an invalid pattern.", name)
	}

	return re, nil
}

// splitTokens splits on matches of re (group -1) or emits capture `group` of each match; empty tokens are dropped.
func splitTokens(re *regexp.Regexp, text string, group int) []Token {
	var out []Token

	if group >= 0 {
		for _, m := range re.FindAllStringSubmatchIndex(text, -1) {
			if s, e := m[2*group], m[2*group+1]; s >= 0 && e > s {
				out = append(out, Token{Term: text[s:e], Start: s, End: e})
			}
		}

		return renumber(out)
	}

	last := 0

	for _, m := range re.FindAllStringIndex(text, -1) {
		if m[1] == m[0] {
			continue
		}

		if m[0] > last {
			out = append(out, Token{Term: text[last:m[0]], Start: last, End: m[0]})
		}

		last = m[1]
	}

	if last < len(text) {
		out = append(out, Token{Term: text[last:], Start: last, End: len(text)})
	}

	return renumber(out)
}

func patternAnalyzerFrom(o map[string]any) (Analyzer, error) {
	name := strOf(o, "name")

	re, err := patternFrom(o, name)
	if err != nil {
		return nil, err
	}

	lower := boolOf(o, "lowercase", true)
	stops := map[string]bool{}

	for _, w := range listOf(o, "stopwords") {
		s, _ := w.(string)
		stops[strings.ToLower(s)] = true
	}

	return func(text string) []Token {
		toks := splitTokens(re, text, -1)
		if lower {
			mapTerms(toks, strings.ToLower)
		}

		return dropStop(toks, stops)
	}, nil
}
