package gcpspanner

import (
	"encoding/json/v2"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	maxOpFilterBytes = 4096
	maxOpFilterToks  = 256
	maxOpFilterDepth = 32
	defaultOpsPage   = 100
)

type opPred func(doc map[string]any) bool

type opTok struct {
	text   string
	quoted bool
}

func nextOpToken(rs []rune, i int) (opTok, int, error) {
	switch rs[i] {
	case '(', ')':
		return opTok{text: string(rs[i])}, i + 1, nil
	case '"':
		j := i + 1
		for j < len(rs) && rs[j] != '"' {
			j++
		}

		if j >= len(rs) {
			return opTok{}, 0, errInvalid("Invalid filter: unterminated quoted string.")
		}

		return opTok{text: string(rs[i+1 : j]), quoted: true}, j + 1, nil
	default:
		j := i
		for j < len(rs) && !unicode.IsSpace(rs[j]) && rs[j] != '(' && rs[j] != ')' && rs[j] != '"' {
			j++
		}

		return opTok{text: string(rs[i:j])}, j, nil
	}
}

func tokenizeOpFilter(s string) ([]opTok, error) {
	if len(s) > maxOpFilterBytes {
		return nil, errInvalid("Invalid filter: longer than %d bytes.", maxOpFilterBytes)
	}

	var toks []opTok

	rs := []rune(s)

	for i := 0; i < len(rs); {
		if unicode.IsSpace(rs[i]) {
			i++

			continue
		}

		tok, next, err := nextOpToken(rs, i)
		if err != nil {
			return nil, err
		}

		toks, i = append(toks, tok), next

		if len(toks) > maxOpFilterToks {
			return nil, errInvalid("Invalid filter: more than %d tokens.", maxOpFilterToks)
		}
	}

	return toks, nil
}

type opParser struct {
	toks  []opTok
	pos   int
	depth int
}

func (p *opParser) is(text string) bool {
	return p.pos < len(p.toks) && !p.toks[p.pos].quoted && strings.EqualFold(p.toks[p.pos].text, text)
}

func (p *opParser) keyword(kw string) bool {
	if p.is(kw) {
		p.pos++

		return true
	}

	return false
}

func (p *opParser) more() bool { return p.pos < len(p.toks) && !p.is(")") }

// parseOpFilter accepts the filter grammar of the List*Operations requests: comparisons joined by AND, OR and
// NOT with parentheses, implicitly ANDed, OR binding tighter than AND.
func parseOpFilter(s string) (opPred, error) {
	toks, err := tokenizeOpFilter(s)
	if err != nil {
		return nil, err
	}

	p := &opParser{toks: toks}

	pred, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	if p.pos < len(p.toks) {
		return nil, errInvalid("Invalid filter: unexpected %q.", p.toks[p.pos].text)
	}

	return pred, nil
}

func (p *opParser) parseAnd() (opPred, error) {
	var preds []opPred

	for {
		next, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		preds = append(preds, next)

		if p.keyword("AND") && !p.more() {
			return nil, errInvalid("Invalid filter: expected an expression after AND.")
		}

		if !p.more() {
			break
		}
	}

	return func(d map[string]any) bool {
		return !slices.ContainsFunc(preds, func(f opPred) bool { return !f(d) })
	}, nil
}

func (p *opParser) parseOr() (opPred, error) {
	first, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	preds := []opPred{first}

	for p.keyword("OR") {
		next, nerr := p.parseTerm()
		if nerr != nil {
			return nil, nerr
		}

		preds = append(preds, next)
	}

	return func(d map[string]any) bool { return slices.ContainsFunc(preds, func(f opPred) bool { return f(d) }) }, nil
}

func (p *opParser) parseTerm() (opPred, error) {
	if p.pos >= len(p.toks) {
		return nil, errInvalid("Invalid filter: unexpected end of expression.")
	}

	p.depth++
	defer func() { p.depth-- }()

	if p.depth > maxOpFilterDepth {
		return nil, errInvalid("Invalid filter: nested deeper than %d levels.", maxOpFilterDepth)
	}

	switch {
	case p.keyword("NOT"), p.keyword("-"):
		inner, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		return func(d map[string]any) bool { return !inner(d) }, nil
	case p.keyword("("):
		inner, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		if !p.keyword(")") {
			return nil, errInvalid("Invalid filter: missing closing parenthesis.")
		}

		return inner, nil
	}

	return p.parseComparison()
}

var opFilterOps = []string{"<=", ">=", "!=", "<", ">", "=", ":"} //nolint:gochecknoglobals // immutable catalog

func splitOp(w string) (string, string, string, bool) {
	best, op := -1, ""

	for _, o := range opFilterOps {
		if i := strings.Index(w, o); i > 0 && (best < 0 || i < best) {
			best, op = i, o
		}
	}

	if best < 0 {
		return "", "", "", false
	}

	if best+1 < len(w) && slices.Contains(opFilterOps, w[best:best+2]) {
		op = w[best : best+2]
	}

	return w[:best], op, w[best+len(op):], true
}

func (p *opParser) parseComparison() (opPred, error) {
	t := p.toks[p.pos]
	p.pos++

	if t.quoted || t.text == ")" {
		return nil, errInvalid("Invalid filter: expected field, operator and value.")
	}

	field, op, rest, ok := splitOp(t.text)
	if !ok {
		if p.pos >= len(p.toks) {
			return nil, errInvalid("Invalid filter: expected field, operator and value.")
		}

		field, op, rest, ok = t.text, p.toks[p.pos].text, "", slices.Contains(opFilterOps, p.toks[p.pos].text)
		p.pos++
	}

	if !ok {
		return nil, errInvalid("Invalid filter: expected field, operator and value.")
	}

	val := opTok{text: rest}

	if rest == "" {
		if p.pos >= len(p.toks) {
			return nil, errInvalid("Invalid filter: missing value for %q.", field)
		}

		val = p.toks[p.pos]
		p.pos++
	}

	path := strings.Split(strings.ToLower(field), ".")

	return func(d map[string]any) bool { return compareDoc(d, path, op, val) }, nil
}

func walk(v any, path []string) []any {
	if len(path) == 0 {
		return []any{v}
	}

	switch t := v.(type) {
	case map[string]any:
		for k, c := range t {
			if strings.EqualFold(k, path[0]) {
				return walk(c, path[1:])
			}
		}
	case []any:
		var out []any
		for _, e := range t {
			out = append(out, walk(e, path)...)
		}

		return out
	}

	return nil
}

func scalar(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bool:
		return strconv.FormatBool(t), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	default:
		return "", false
	}
}

func compareDoc(doc map[string]any, path []string, op string, val opTok) bool {
	found := walk(doc, path)

	if op == "!=" {
		return !slices.ContainsFunc(found, func(v any) bool { return compareOne(v, "=", val) })
	}

	return slices.ContainsFunc(found, func(v any) bool { return compareOne(v, op, val) })
}

func compareOne(v any, op string, val opTok) bool {
	if !val.quoted && val.text == "*" && op == ":" {
		return true
	}

	l, ok := scalar(v)
	if !ok {
		return false
	}

	l, r := strings.ToLower(l), strings.ToLower(val.text)

	switch op {
	case ":":
		return strings.Contains(l, r)
	case "=":
		return l == r
	}

	return orderedCompare(l, r, op)
}

func orderedCompare(l, r, op string) bool {
	c := strings.Compare(l, r)

	if lt, err := time.Parse(time.RFC3339Nano, strings.ToUpper(l)); err == nil {
		if rt, rerr := time.Parse(time.RFC3339Nano, strings.ToUpper(r)); rerr == nil {
			c = lt.Compare(rt)
		}
	} else if lf, lerr := strconv.ParseFloat(l, 64); lerr == nil {
		if rf, rerr := strconv.ParseFloat(r, 64); rerr == nil {
			switch {
			case lf < rf:
				c = -1
			case lf > rf:
				c = 1
			default:
				c = 0
			}
		}
	}

	switch op {
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	default:
		return c >= 0
	}
}

func opDoc(op *longrunningpb.Operation) map[string]any {
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(op)
	if err != nil {
		return nil
	}

	doc := map[string]any{}
	if err = json.Unmarshal(raw, &doc); err != nil {
		return nil
	}

	doc["done"] = op.GetDone()

	return doc
}

func opStart(doc map[string]any) string {
	for _, p := range [][]string{{"metadata", "start_time"}, {"metadata", "progress", "start_time"}} {
		for _, v := range walk(doc, p) {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}

	return ""
}

// filterOps keeps the operations matching filter, newest start_time first.
func filterOps(ops []*longrunningpb.Operation, filter string) ([]*longrunningpb.Operation, error) {
	keep := opPred(func(map[string]any) bool { return true })

	if strings.TrimSpace(filter) != "" {
		var err error
		if keep, err = parseOpFilter(filter); err != nil {
			return nil, err
		}
	}

	type row struct {
		op    *longrunningpb.Operation
		start string
	}

	rows := make([]row, 0, len(ops))

	for _, o := range ops {
		doc := opDoc(o)
		if doc != nil && keep(doc) {
			rows = append(rows, row{o, opStart(doc)})
		}
	}

	slices.SortStableFunc(rows, func(a, b row) int {
		if c := strings.Compare(b.start, a.start); c != 0 {
			return c
		}

		return strings.Compare(a.op.GetName(), b.op.GetName())
	})

	out := make([]*longrunningpb.Operation, len(rows))
	for i, r := range rows {
		out[i] = r.op
	}

	return out, nil
}
