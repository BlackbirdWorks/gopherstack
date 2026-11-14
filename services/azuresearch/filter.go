package azuresearch

import (
	"fmt"
	"maps"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type tokKind int

const (
	tkEOF tokKind = iota
	tkIdent
	tkString
	tkNumber
	tkDate
	tkGeo
	tkSym
)

type ftoken struct {
	text string
	kind tokKind
}

var (
	dateLitRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(T[0-9:.]+(Z|[+-]\d{2}:\d{2})?)?`)
	numLitRE  = regexp.MustCompile(`^-?\d+(\.\d+)?([eE][+-]?\d+)?`)
)

func tokenizeFilter(s string) ([]ftoken, error) {
	var out []ftoken

	i := 0
	for i < len(s) {
		c := rune(s[i])

		switch {
		case unicode.IsSpace(c):
			i++
		case c == '\'':
			str, n, err := readQuoted(s[i:])
			if err != nil {
				return nil, err
			}

			out = append(out, ftoken{kind: tkString, text: str})
			i += n
		case strings.ContainsRune("(),:", c):
			out = append(out, ftoken{kind: tkSym, text: string(c)})
			i++
		case unicode.IsDigit(c) || (c == '-' && i+1 < len(s) && unicode.IsDigit(rune(s[i+1]))):
			tok, n := readNumberOrDate(s[i:])
			out = append(out, tok)
			i += n
		case unicode.IsLetter(c) || c == '_':
			tok, n, err := readIdent(s[i:])
			if err != nil {
				return nil, err
			}

			out = append(out, tok)
			i += n
		default:
			return nil, badf("Invalid expression: unexpected character '%c' at position %d.", c, i)
		}
	}

	return append(out, ftoken{kind: tkEOF}), nil
}

func readQuoted(s string) (string, int, error) {
	var sb strings.Builder

	for i := 1; i < len(s); i++ {
		if s[i] == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				sb.WriteByte('\'')

				i++

				continue
			}

			return sb.String(), i + 1, nil
		}

		sb.WriteByte(s[i])
	}

	return "", 0, badf("Invalid expression: unterminated string literal.")
}

func readNumberOrDate(s string) (ftoken, int) {
	if m := dateLitRE.FindString(s); m != "" {
		return ftoken{kind: tkDate, text: m}, len(m)
	}

	m := numLitRE.FindString(s)

	return ftoken{kind: tkNumber, text: m}, len(m)
}

func readIdent(s string) (ftoken, int, error) {
	i := 0
	for i < len(s) && (isWordRune(rune(s[i])) || s[i] == '.' || s[i] == '/') {
		i++
	}

	word := s[:i]
	if word == "geography" && i < len(s) && s[i] == '\'' {
		lit, n, err := readQuoted(s[i:])
		if err != nil {
			return ftoken{}, 0, err
		}

		return ftoken{kind: tkGeo, text: lit}, i + n, nil
	}

	return ftoken{kind: tkIdent, text: word}, i, nil
}

type fenv struct {
	d    *Doc
	doc  map[string]any
	vars map[string]any
}

type (
	pred    func(e *fenv) bool
	operand func(e *fenv) any
)

type filterCtx struct {
	comp    *compiled
	isMatch func(e *fenv, text, fields, qtype, mode string) bool
}

type fparser struct {
	ctx   *filterCtx
	scope map[string]*fieldDef
	toks  []ftoken
	pos   int
}

// compileFilter parses an OData $filter expression into a predicate.
func compileFilter(expr string, ctx *filterCtx) (pred, error) {
	toks, err := tokenizeFilter(expr)
	if err != nil {
		return nil, err
	}

	p := &fparser{ctx: ctx, toks: toks, scope: map[string]*fieldDef{}}

	pr, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tkEOF {
		return nil, badf("Invalid expression: unexpected token '%s'.", p.peek().text)
	}

	return pr, nil
}

func (p *fparser) peek() ftoken { return p.toks[p.pos] }

func (p *fparser) next() ftoken {
	t := p.toks[p.pos]
	if t.kind != tkEOF {
		p.pos++
	}

	return t
}

func (p *fparser) isWord(w string) bool {
	t := p.peek()

	return t.kind == tkIdent && t.text == w
}

func (p *fparser) isSym(s string) bool {
	t := p.peek()

	return t.kind == tkSym && t.text == s
}

func (p *fparser) expectSym(s string) error {
	if !p.isSym(s) {
		return badf("Invalid expression: expected '%s' but found '%s'.", s, p.peek().text)
	}

	p.pos++

	return nil
}

func (p *fparser) parseOr() (pred, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.isWord(opOr) {
		p.pos++

		right, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(e *fenv) bool { return l(e) || r(e) }
	}

	return left, nil
}

func (p *fparser) parseAnd() (pred, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.isWord(opAnd) {
		p.pos++

		right, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(e *fenv) bool { return l(e) && r(e) }
	}

	return left, nil
}

func (p *fparser) parseNot() (pred, error) {
	if p.isWord("not") {
		p.pos++

		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return func(e *fenv) bool { return !inner(e) }, nil
	}

	return p.parsePrimary()
}

func isCmpOp(s string) bool {
	switch s {
	case "eq", "ne", "gt", "lt", "ge", "le":
		return true
	}

	return false
}

func (p *fparser) parsePrimary() (pred, error) {
	if p.isSym("(") {
		p.pos++

		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		return inner, p.expectSym(")")
	}

	if p.isWord("true") || p.isWord("false") {
		v := p.next().text == "true"

		return func(*fenv) bool { return v }, nil
	}

	if pr, ok, err := p.tryBoolFunc(); ok || err != nil {
		return pr, err
	}

	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	op := p.peek()
	if op.kind != tkIdent || !isCmpOp(op.text) {
		return nil, badf("Invalid expression: expected a comparison operator but found '%s'.", op.text)
	}

	p.pos++

	right, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	return func(e *fenv) bool { return compareValues(op.text, left(e), right(e)) }, nil
}

func (p *fparser) tryBoolFunc() (pred, bool, error) {
	t := p.peek()
	if t.kind != tkIdent {
		return nil, false, nil
	}

	switch t.text {
	case "search.in":
		pr, err := p.parseSearchIn()

		return pr, true, err
	case "search.ismatch", "search.ismatchscoring":
		pr, err := p.parseIsMatch()

		return pr, true, err
	case "geo.intersects":
		pr, err := p.parseIntersects()

		return pr, true, err
	case "not":
		return nil, false, nil
	}

	if strings.Contains(t.text, "/") && p.toks[p.pos+1].kind == tkSym && p.toks[p.pos+1].text == "(" {
		pr, err := p.parseLambda()

		return pr, true, err
	}

	if p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == tkSym && p.toks[p.pos+1].text == ":" {
		return nil, false, nil
	}

	return p.boolField()
}

// boolField handles a bare Edm.Boolean field used as a predicate.
func (p *fparser) boolField() (pred, bool, error) {
	nxt := p.toks[p.pos+1]
	if (nxt.kind == tkIdent && isCmpOp(nxt.text)) || (nxt.kind == tkSym && nxt.text == "(") {
		return nil, false, nil
	}

	op, err := p.parseOperand()
	if err != nil {
		return nil, true, err
	}

	return func(e *fenv) bool {
		b, _ := op(e).(bool)

		return b
	}, true, nil
}

func (p *fparser) parseOperand() (operand, error) {
	t := p.next()

	switch t.kind {
	case tkString:
		s := t.text

		return func(*fenv) any { return s }, nil
	case tkNumber:
		f, _ := strconv.ParseFloat(t.text, 64)

		return func(*fenv) any { return f }, nil
	case tkDate:
		d, ok := parseDate(t.text)
		if !ok {
			return nil, badf("Invalid expression: bad date literal '%s'.", t.text)
		}

		return func(*fenv) any { return d }, nil
	case tkGeo:
		return nil, badf("Invalid expression: geography literal is only valid as a geo function argument.")
	case tkEOF, tkSym:
	case tkIdent:
		return p.identOperand(t)
	}

	return nil, badf("Invalid expression: unexpected '%s'.", t.text)
}

func (p *fparser) identOperand(t ftoken) (operand, error) {
	switch t.text {
	case "null":
		return func(*fenv) any { return nil }, nil
	case "true":
		return func(*fenv) any { return true }, nil
	case "false":
		return func(*fenv) any { return false }, nil
	case litNaN:
		return func(*fenv) any { return math.NaN() }, nil
	case litINF:
		return func(*fenv) any { return math.Inf(1) }, nil
	case "geo.distance":
		return p.parseGeoDistance()
	}

	return p.pathOperand(t.text)
}

// pathOperand resolves a field path, validating it is filterable when it is rooted at the document.
func (p *fparser) pathOperand(path string) (operand, error) {
	segs := strings.Split(path, "/")

	if fd, ok := p.scope[segs[0]]; ok {
		if len(segs) > 1 {
			if _, err := childField(fd, segs[1:]); err != nil {
				return nil, err
			}
		}

		name := segs[0]
		rest := segs[1:]

		return func(e *fenv) any { return drill(e.vars[name], rest) }, nil
	}

	fd, ok := p.ctx.comp.byPath[path]
	if !ok {
		return nil, badf("Invalid expression: The unknown field '%s' is not present.", path)
	}

	if fd.Base != typeComplex && !fd.Filterable {
		return nil, badf("Invalid expression: The field '%s' is not filterable.", path)
	}

	return func(e *fenv) any { return drill(e.doc, segs) }, nil
}

func childField(fd *fieldDef, segs []string) (*fieldDef, error) {
	cur := fd

	for _, s := range segs {
		var found *fieldDef

		for _, c := range cur.Children {
			if c.Name == s {
				found = c
			}
		}

		if found == nil {
			return nil, badf("Invalid expression: unknown field '%s'.", s)
		}

		cur = found
	}

	return cur, nil
}

func drill(v any, segs []string) any {
	for _, s := range segs {
		switch t := v.(type) {
		case map[string]any:
			v = t[s]
		case []any:
			out := make([]any, 0, len(t))
			for _, el := range t {
				out = append(out, drill(el, []string{s}))
			}

			v = out
		default:
			return nil
		}
	}

	return v
}

func (p *fparser) parseLambda() (pred, error) {
	path := p.next().text
	cut := strings.LastIndex(path, "/")
	collPath, kind := path[:cut], path[cut+1:]

	if kind != modeAny && kind != modeAll {
		return nil, badf("Invalid expression: unsupported function '%s'.", path)
	}

	coll, err := p.lambdaCollection(collPath)
	if err != nil {
		return nil, err
	}

	p.pos++ // (

	if p.isSym(")") {
		p.pos++

		if kind == "all" {
			return func(*fenv) bool { return true }, nil
		}

		return func(e *fenv) bool {
			arr, _ := coll(e).([]any)

			return len(arr) > 0
		}, nil
	}

	v := p.next().text
	if err = p.expectSym(":"); err != nil {
		return nil, err
	}

	fd := p.lambdaField(collPath)
	saved, had := p.scope[v]
	p.scope[v] = fd

	body, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if had {
		p.scope[v] = saved
	} else {
		delete(p.scope, v)
	}

	if err = p.expectSym(")"); err != nil {
		return nil, err
	}

	return lambdaPred(kind == "all", coll, v, body), nil
}

func lambdaPred(all bool, coll operand, v string, body pred) pred {
	return func(e *fenv) bool {
		arr, _ := coll(e).([]any)
		for _, el := range arr {
			vars := make(map[string]any, len(e.vars)+1)
			maps.Copy(vars, e.vars)

			vars[v] = el
			ok := body(&fenv{d: e.d, doc: e.doc, vars: vars})

			if all && !ok {
				return false
			}

			if !all && ok {
				return true
			}
		}

		return all
	}
}

func (p *fparser) lambdaCollection(path string) (operand, error) {
	segs := strings.Split(path, "/")
	if fd, ok := p.scope[segs[0]]; ok {
		_ = fd
		name, rest := segs[0], segs[1:]

		return func(e *fenv) any { return drill(e.vars[name], rest) }, nil
	}

	fd, ok := p.ctx.comp.byPath[path]
	if !ok || !fd.Collection {
		return nil, badf("Invalid expression: '%s' is not a collection field.", path)
	}

	if fd.Base != typeComplex && !fd.Filterable {
		return nil, badf("Invalid expression: The field '%s' is not filterable.", path)
	}

	return func(e *fenv) any { return drill(e.doc, segs) }, nil
}

func (p *fparser) lambdaField(path string) *fieldDef {
	segs := strings.Split(path, "/")
	if fd, ok := p.scope[segs[0]]; ok {
		child, err := childField(fd, segs[1:])
		if err == nil {
			return child
		}

		return fd
	}

	return p.ctx.comp.byPath[path]
}

func (p *fparser) args() ([]ftoken, error) {
	p.pos++ // function name

	if err := p.expectSym("("); err != nil {
		return nil, err
	}

	var out []ftoken

	for !p.isSym(")") {
		if p.peek().kind == tkEOF {
			return nil, badf("Invalid expression: missing ')'.")
		}

		if p.isSym(",") {
			p.pos++

			continue
		}

		out = append(out, p.next())
	}

	p.pos++

	return out, nil
}

func (p *fparser) parseSearchIn() (pred, error) {
	args, err := p.args()
	if err != nil || len(args) < 2 || len(args) > 3 {
		return nil, badf("Invalid expression: search.in expects (field, 'values'[, 'delimiters']).")
	}

	val, err := p.pathOperand(args[0].text)
	if err != nil {
		return nil, err
	}

	delims := " ,"
	if len(args) == searchInArgsMax {
		delims = args[2].text
	}

	set := map[string]bool{}
	for _, w := range strings.FieldsFunc(args[1].text, func(r rune) bool { return strings.ContainsRune(delims, r) }) {
		set[w] = true
	}

	return func(e *fenv) bool {
		switch t := val(e).(type) {
		case string:
			return set[t]
		case []any:
			for _, el := range t {
				if s, ok := el.(string); ok && set[s] {
					return true
				}
			}
		}

		return false
	}, nil
}

func (p *fparser) parseIsMatch() (pred, error) {
	args, err := p.args()
	if err != nil || len(args) < 1 || len(args) > 4 {
		return nil, badf("Invalid expression: search.ismatch expects (search[, fields, queryType, searchMode]).")
	}

	texts := [4]string{args[0].text, "", "simple", "any"}
	for i := 1; i < len(args); i++ {
		texts[i] = args[i].text
	}

	for _, name := range splitCSV(texts[1]) {
		if fd, ok := p.ctx.comp.byPath[name]; !ok || !fd.Searchable {
			return nil, badf("Invalid expression: the field '%s' in search.ismatch is not searchable.", name)
		}
	}

	return func(e *fenv) bool { return p.ctx.isMatch(e, texts[0], texts[1], texts[2], texts[3]) }, nil
}

func (p *fparser) parseGeoDistance() (operand, error) {
	p.pos--

	args, err := p.args()
	if err != nil || len(args) != 2 {
		return nil, badf("Invalid expression: geo.distance expects (field, geography'POINT(lon lat)').")
	}

	pt, err := p.geoArg(args)
	if err != nil {
		return nil, err
	}

	field, err := p.pathOperand(args[0].text)
	if err != nil {
		return nil, err
	}

	return func(e *fenv) any {
		g, ok := parseGeoPoint(field(e))
		if !ok {
			return nil
		}

		return haversineKM(g, pt)
	}, nil
}

func (p *fparser) geoArg(args []ftoken) (geoPoint, error) {
	if args[1].kind != tkGeo {
		args[0], args[1] = args[1], args[0]
	}

	if args[1].kind != tkGeo {
		return geoPoint{}, badf("Invalid expression: a geography literal is required.")
	}

	return parsePointLiteral(args[1].text)
}

var pointRE = regexp.MustCompile(`(?i)^\s*POINT\s*\(\s*(-?[\d.]+)\s+(-?[\d.]+)\s*\)\s*$`)

func parsePointLiteral(s string) (geoPoint, error) {
	m := pointRE.FindStringSubmatch(s)
	if m == nil {
		return geoPoint{}, badf("Invalid expression: bad geography point '%s'.", s)
	}

	lon, _ := strconv.ParseFloat(m[1], 64)
	lat, _ := strconv.ParseFloat(m[2], 64)

	return geoPoint{Lon: lon, Lat: lat}, nil
}

func (p *fparser) parseIntersects() (pred, error) {
	args, err := p.args()
	if err != nil || len(args) != 2 {
		return nil, badf("Invalid expression: geo.intersects expects (field, geography'POLYGON(...)').")
	}

	if args[1].kind != tkGeo {
		return nil, badf("Invalid expression: a geography polygon literal is required.")
	}

	ring, err := parsePolygon(args[1].text)
	if err != nil {
		return nil, err
	}

	field, err := p.pathOperand(args[0].text)
	if err != nil {
		return nil, err
	}

	return func(e *fenv) bool {
		g, ok := parseGeoPoint(field(e))

		return ok && pointInRing(g, ring)
	}, nil
}

var polyRE = regexp.MustCompile(`(?i)^\s*POLYGON\s*\(\s*\((.*)\)\s*\)\s*$`)

func parsePolygon(s string) ([]geoPoint, error) {
	m := polyRE.FindStringSubmatch(s)
	if m == nil {
		return nil, badf("Invalid expression: bad geography polygon '%s'.", s)
	}

	var ring []geoPoint

	for pair := range strings.SplitSeq(m[1], ",") {
		var lon, lat float64
		if _, err := fmt.Sscanf(strings.TrimSpace(pair), "%f %f", &lon, &lat); err != nil {
			return nil, badf("Invalid expression: bad polygon coordinate '%s'.", pair)
		}

		ring = append(ring, geoPoint{Lon: lon, Lat: lat})
	}

	return ring, nil
}

func pointInRing(pt geoPoint, ring []geoPoint) bool {
	inside := false

	for i, j := 0, len(ring)-1; i < len(ring); j, i = i, i+1 {
		a, b := ring[i], ring[j]
		if (a.Lat > pt.Lat) != (b.Lat > pt.Lat) &&
			pt.Lon < (b.Lon-a.Lon)*(pt.Lat-a.Lat)/(b.Lat-a.Lat)+a.Lon {
			inside = !inside
		}
	}

	return inside
}

// compareValues applies an OData comparison to two decoded values.
func compareValues(op string, l, r any) bool {
	if l == nil || r == nil {
		switch op {
		case "eq":
			return l == nil && r == nil
		case "ne":
			return (l == nil) != (r == nil)
		}

		return false
	}

	c, ok := orderValues(l, r)
	if !ok {
		return op == "ne"
	}

	switch op {
	case "eq":
		return c == 0
	case "ne":
		return c != 0
	case "gt":
		return c > 0
	case "lt":
		return c < 0
	case "ge":
		return c >= 0
	case "le":
		return c <= 0
	}

	return false
}

func orderValues(l, r any) (int, bool) {
	if lt, rt, ok := datePair(l, r); ok {
		return lt.Compare(rt), true
	}

	if lf, ok := asFloat(l); ok {
		if rf, rok := asFloat(r); rok {
			if math.IsNaN(lf) || math.IsNaN(rf) {
				return 0, false
			}

			return cmpFloat(lf, rf), true
		}
	}

	switch lv := l.(type) {
	case string:
		if rv, ok := r.(string); ok {
			return strings.Compare(lv, rv), true
		}
	case bool:
		if rv, ok := r.(bool); ok {
			if lv == rv {
				return 0, true
			}

			return 1, true
		}
	}

	return 0, false
}

// datePair converts both operands to times when at least one is a date literal.
func datePair(l, r any) (time.Time, time.Time, bool) {
	lt, lIs := l.(time.Time)
	rt, rIs := r.(time.Time)

	if !lIs && !rIs {
		return lt, rt, false
	}

	if !lIs {
		lt, lIs = parseDate(l)
	}

	if !rIs {
		rt, rIs = parseDate(r)
	}

	return lt, rt, lIs && rIs
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}
