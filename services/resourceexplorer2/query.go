package resourceexplorer2

import (
	"path"
	"slices"
	"strings"
	"unicode"
)

const applicationTagKey = "awsApplication"

const (
	filterRegion       = "region"
	filterService      = "service"
	filterResourceType = "resourcetype"
	filterTag          = "tag"
	filterTagKey       = "tag.key"
	filterTagValue     = "tag.value"
	filterAccountID    = "accountid"
	filterID           = "id"
	filterApplication  = "application"
	filterKeyword      = ""
)

var filterNames = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	filterRegion: true, filterService: true, filterResourceType: true, filterTag: true,
	filterTagKey: true, filterTagValue: true, filterAccountID: true, filterID: true, filterApplication: true,
}

// clause is one parsed query term: a keyword (name "") or a filter whose values are OR-ed.
type clause struct {
	name    string
	values  []string
	phrase  bool
	negated bool
}

type piece struct {
	s   string
	lit bool
}

// parseQuery splits a Resource Explorer query string into keywords and filters.
func parseQuery(q string) ([]clause, error) {
	if len(q) > maxQueryLen {
		return nil, invalid("QueryString must be at most %d characters", maxQueryLen)
	}

	tokens, err := tokenize(q)
	if err != nil {
		return nil, err
	}

	out := make([]clause, 0, len(tokens))

	for _, tok := range tokens {
		c, perr := parseToken(tok)
		if perr != nil {
			return nil, perr
		}

		out = append(out, c)
	}

	return out, nil
}

func plain(tok []piece) string {
	var sb strings.Builder

	for _, p := range tok {
		sb.WriteString(p.s)
	}

	return sb.String()
}

func parseToken(tok []piece) (clause, error) {
	var c clause

	if len(tok) > 0 && !tok[0].lit && strings.HasPrefix(tok[0].s, "-") && len(plain(tok)) > 1 {
		c.negated = true
		tok = append([]piece{{s: tok[0].s[1:]}}, tok[1:]...)
	}

	if name, rest, ok := cutFilter(tok); ok {
		if len(plain(rest)) == 0 {
			return clause{}, invalid("filter %q needs a value", name)
		}

		c.name, c.values = name, splitValues(rest)

		return c, nil
	}

	for _, p := range tok {
		c.phrase = c.phrase || p.lit
	}

	c.values = []string{plain(tok)}

	return c, nil
}

func cutFilter(tok []piece) (string, []piece, bool) {
	var name strings.Builder

	for i, p := range tok {
		if p.lit {
			return "", nil, false
		}

		if idx := strings.IndexByte(p.s, ':'); idx >= 0 {
			name.WriteString(p.s[:idx])

			n := strings.ToLower(name.String())
			if !filterNames[n] {
				return "", nil, false
			}

			rest := append([]piece{{s: p.s[idx+1:]}}, tok[i+1:]...)

			return n, rest, true
		}

		name.WriteString(p.s)
	}

	return "", nil, false
}

func splitValues(tok []piece) []string {
	var (
		out []string
		cur strings.Builder
	)

	for _, p := range tok {
		if p.lit {
			cur.WriteString(p.s)

			continue
		}

		parts := strings.Split(p.s, ",")
		cur.WriteString(parts[0])

		for _, part := range parts[1:] {
			out = append(out, cur.String())
			cur.Reset()
			cur.WriteString(part)
		}
	}

	return append(out, cur.String())
}

// tokenize splits on whitespace; double quotes and backslash escapes mark literal pieces.
func tokenize(q string) ([][]piece, error) {
	var (
		out      [][]piece
		cur      []piece
		buf      strings.Builder
		inQuotes bool
		started  bool
	)

	flushBuf := func(lit bool) {
		if buf.Len() > 0 {
			cur = append(cur, piece{s: buf.String(), lit: lit})
			buf.Reset()
		}
	}

	flush := func() {
		flushBuf(inQuotes)

		if started {
			out = append(out, cur)
			cur, started = nil, false
		}
	}

	rs := []rune(q)

	for i := 0; i < len(rs); i++ {
		r := rs[i]

		switch {
		case r == '\\' && i+1 < len(rs):
			flushBuf(inQuotes)

			i++

			cur = append(cur, piece{s: string(rs[i]), lit: true})
			started = true
		case r == '"':
			flushBuf(inQuotes)

			inQuotes, started = !inQuotes, true
		case !inQuotes && (r == ' ' || r == '\t' || r == '\n'):
			flush()
		default:
			buf.WriteRune(r)

			started = true
		}
	}

	if inQuotes {
		return nil, invalid("QueryString has an unterminated quote")
	}

	flush()

	return out, nil
}

// candidate is the searchable projection of one resource.
type candidate struct {
	tags         map[string]string
	arn          string
	service      string
	resourceType string
	region       string
	account      string
	id           string
	words        []string
}

func words(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func globMatch(pattern, v string) bool {
	ok, err := path.Match(strings.ToLower(pattern), strings.ToLower(v))

	return err == nil && ok
}

func anyGlob(values []string, v string) bool {
	return slices.ContainsFunc(values, func(p string) bool { return globMatch(p, v) })
}

func (c *candidate) matches(cl clause) bool {
	switch cl.name {
	case filterRegion:
		return anyGlob(cl.values, c.region)
	case filterService:
		return anyGlob(cl.values, c.service)
	case filterResourceType:
		return anyGlob(cl.values, c.resourceType)
	case filterAccountID:
		return slices.Contains(cl.values, c.account)
	case filterID:
		return slices.ContainsFunc(cl.values, func(v string) bool {
			return strings.EqualFold(c.arn, v) || strings.EqualFold(c.id, v)
		})
	case filterTagKey:
		return slices.ContainsFunc(cl.values, func(v string) bool {
			_, ok := c.tags[v]

			return ok
		})
	case filterTagValue:
		return slices.ContainsFunc(cl.values, func(v string) bool {
			for _, tv := range c.tags {
				if tv == v {
					return true
				}
			}

			return false
		})
	case filterTag:
		return slices.ContainsFunc(cl.values, c.matchesTag)
	case filterApplication:
		return slices.ContainsFunc(cl.values, c.matchesApplication)
	default:
		return false
	}
}

func (c *candidate) matchesApplication(v string) bool {
	app, ok := c.tags[applicationTagKey]
	if !ok {
		return false
	}

	if app == v {
		return true
	}

	_, after, found := strings.Cut(app, ":group/")
	if !found {
		return false
	}

	name, _, _ := strings.Cut(after, "/")

	return name == v
}

func (c *candidate) matchesTag(v string) bool {
	switch strings.ToLower(v) {
	case "all":
		return len(c.tags) > 0
	case "none":
		return len(c.tags) == 0
	}

	k, val, hasValue := strings.Cut(v, "=")

	got, ok := c.tags[k]
	if !ok {
		return false
	}

	return !hasValue || got == val
}

func (c *candidate) hasWord(w string) bool {
	if pre, ok := strings.CutSuffix(w, "*"); ok {
		return slices.ContainsFunc(c.words, func(x string) bool { return strings.HasPrefix(x, pre) })
	}

	return slices.Contains(c.words, w)
}

func (c *candidate) hasPhrase(ws []string) bool {
	if len(ws) == 0 {
		return false
	}

	for i := 0; i+len(ws) <= len(c.words); i++ {
		if slices.Equal(c.words[i:i+len(ws)], ws) {
			return true
		}
	}

	return false
}

// keywordHits counts how many of the keyword's terms the resource matches.
func (c *candidate) keywordHits(cl clause) (int, int) {
	text := cl.values[0]

	hits, terms := 0, 0

	if cl.phrase {
		if c.hasPhrase(words(text)) {
			return 1, 1
		}

		return 0, 1
	}

	for _, w := range words(text) {
		terms++

		if c.hasWord(w) || (strings.HasSuffix(text, "*") && c.hasWord(w+"*")) {
			hits++
		}
	}

	return hits, terms
}

// evaluate applies the clauses and returns the match and its relevance.
// Filters are AND-ed; their values are OR-ed. Keywords are OR-ed; negations exclude.
func evaluate(c *candidate, clauses []clause) (bool, int) {
	var (
		score, terms int
		keywords     bool
	)

	for _, cl := range clauses {
		if cl.name == filterKeyword {
			hits, n := c.keywordHits(cl)

			switch {
			case cl.negated && hits > 0:
				return false, 0
			case !cl.negated:
				keywords, terms, score = true, terms+n, score+hits
			}

			continue
		}

		if c.matches(cl) == cl.negated {
			return false, 0
		}
	}

	if keywords && score == 0 && terms > 0 {
		return false, 0
	}

	return true, score
}

func usesTagFilter(clauses []clause) bool {
	return slices.ContainsFunc(clauses, func(cl clause) bool {
		return cl.name == filterTag || cl.name == filterTagKey || cl.name == filterTagValue ||
			cl.name == filterApplication
	})
}

func hasKeyword(clauses []clause) bool {
	return slices.ContainsFunc(clauses, func(cl clause) bool { return cl.name == filterKeyword })
}
