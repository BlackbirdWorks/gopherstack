package connect

import (
	"encoding/json/v2"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	maxExprDepth    = 32
	maxStatElements = 10
	stringMatchAny  = "MATCHES_ANY"
	stringMatchNone = "MATCHES_NONE"
	statAvg         = "AVG"
	statMax         = "MAX"
)

type exprNode struct {
	name string
	kids []*exprNode
	num  float64
	op   byte
}

type exprParser struct {
	src   string
	pos   int
	depth int
}

func (p *exprParser) skip() {
	for p.pos < len(p.src) && p.src[p.pos] == ' ' {
		p.pos++
	}
}

func (p *exprParser) peek() byte {
	p.skip()

	if p.pos >= len(p.src) {
		return 0
	}

	return p.src[p.pos]
}

func parseFormula(src string) (*exprNode, error) {
	p := &exprParser{src: strings.ReplaceAll(strings.ReplaceAll(src, "\t", " "), "\n", " ")}

	n, err := p.expr()
	if err != nil {
		return nil, err
	}

	if p.peek() != 0 {
		return nil, invalidParamErr("Calculation has unexpected %q", string(p.src[p.pos]))
	}

	return n, nil
}

func (p *exprParser) expr() (*exprNode, error) {
	left, err := p.term()
	if err != nil {
		return nil, err
	}

	for c := p.peek(); c == '+' || c == '-'; c = p.peek() {
		p.pos++

		right, rerr := p.term()
		if rerr != nil {
			return nil, rerr
		}

		left = &exprNode{op: c, kids: []*exprNode{left, right}}
	}

	return left, nil
}

func (p *exprParser) term() (*exprNode, error) {
	left, err := p.unary()
	if err != nil {
		return nil, err
	}

	for c := p.peek(); c == '*' || c == '/'; c = p.peek() {
		p.pos++

		right, rerr := p.unary()
		if rerr != nil {
			return nil, rerr
		}

		left = &exprNode{op: c, kids: []*exprNode{left, right}}
	}

	return left, nil
}

func (p *exprParser) unary() (*exprNode, error) {
	if p.peek() == '-' {
		p.pos++

		p.depth++
		defer func() { p.depth-- }()

		if p.depth > maxExprDepth {
			return nil, invalidParamErr("Calculation is nested too deeply")
		}

		k, err := p.unary()
		if err != nil {
			return nil, err
		}

		return &exprNode{op: 'n', kids: []*exprNode{k}}, nil
	}

	return p.primary()
}

func (p *exprParser) primary() (*exprNode, error) {
	c := p.peek()

	switch {
	case c == '(':
		return p.group()
	case c >= '0' && c <= '9' || c == '.':
		return p.number()
	case c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		return p.ident()
	default:
		return nil, invalidParamErr("Calculation is incomplete or has an unexpected character")
	}
}

func (p *exprParser) number() (*exprNode, error) {
	start := p.pos
	for p.pos < len(p.src) && (p.src[p.pos] == '.' || p.src[p.pos] >= '0' && p.src[p.pos] <= '9') {
		p.pos++
	}

	v, err := strconv.ParseFloat(p.src[start:p.pos], 64)
	if err != nil {
		return nil, invalidParamErr("Calculation has an invalid number %q", p.src[start:p.pos])
	}

	return &exprNode{op: 'c', num: v}, nil
}

func (p *exprParser) group() (*exprNode, error) {
	p.pos++
	p.depth++
	defer func() { p.depth-- }()

	if p.depth > maxExprDepth {
		return nil, invalidParamErr("Calculation is nested too deeply")
	}

	n, err := p.expr()
	if err != nil {
		return nil, err
	}

	if p.peek() != ')' {
		return nil, invalidParamErr("Calculation has unbalanced parentheses")
	}

	p.pos++

	return n, nil
}

func (p *exprParser) ident() (*exprNode, error) {
	start := p.pos
	for p.pos < len(p.src) && (p.src[p.pos] == '_' || p.src[p.pos] >= '0' && p.src[p.pos] <= '9' ||
		p.src[p.pos] >= 'A' && p.src[p.pos] <= 'Z' || p.src[p.pos] >= 'a' && p.src[p.pos] <= 'z') {
		p.pos++
	}

	name := p.src[start:p.pos]

	if p.peek() != '(' {
		return &exprNode{op: 'a', name: name}, nil
	}

	if !slices.Contains([]string{"SUM", statAvg, "MIN", statMax}, name) {
		return nil, invalidParamErr("Calculation function %q is not supported; use SUM, AVG, MIN or MAX", name)
	}

	arg, err := p.group()
	if err != nil {
		return nil, err
	}

	if countElements(arg) > maxStatElements {
		return nil, invalidParamErr("%s can hold at most %d elements", name, maxStatElements)
	}

	return &exprNode{op: 's', name: name, kids: []*exprNode{arg}}, nil
}

func countElements(n *exprNode) int {
	if n.op == 'a' || n.op == 'c' {
		return 1
	}

	total := 0
	for _, k := range n.kids {
		total += countElements(k)
	}

	return total
}

func aliasesOf(n *exprNode, out map[string]bool) {
	if n.op == 'a' {
		out[n.name] = true
	}

	for _, k := range n.kids {
		aliasesOf(k, out)
	}
}

type customComponent struct {
	filters []MetricFilterV2
	spec    v2Spec
}

func decodeComponentFilters(raw []byte) ([]MetricFilterV2, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var in []struct {
		String *struct {
			Comparison string   `json:"Comparison"`
			Values     []string `json:"Values"`
		} `json:"StringCondition"`
		Number  *struct{} `json:"NumberCondition"`
		Boolean *struct{} `json:"BooleanCondition"`
		Key     string    `json:"MetricFilterKey"`
		Negate  bool      `json:"Negate"`
	}

	if err := json.Unmarshal(raw, &in, jsonx.V1()); err != nil {
		return nil, invalidParamErr("MetricFilters is not valid")
	}

	out := make([]MetricFilterV2, 0, len(in))

	for _, f := range in {
		if f.String == nil || f.Number != nil || f.Boolean != nil || !slices.Contains(v2Dims(), f.Key) {
			return nil, invalidParamErr("component filter %q is not supported: only string conditions on %s",
				f.Key, strings.Join(v2Dims(), ", "))
		}

		if err := oneOf(
			"StringCondition.Comparison",
			f.String.Comparison,
			stringMatchAny,
			stringMatchNone,
		); err != nil {
			return nil, err
		}

		out = append(out, MetricFilterV2{
			MetricFilterKey: f.Key, MetricFilterValues: f.String.Values,
			Negate: f.Negate != (f.String.Comparison == stringMatchNone),
		})
	}

	return out, nil
}

type compiledMetric struct {
	root       *exprNode
	components map[string]customComponent
}

func compileCustomMetric(calc *MetricCalculation) (*compiledMetric, error) {
	if calc == nil {
		return nil, invalidParamErr("the metric has no calculation")
	}

	root, err := parseFormula(calc.Calculation)
	if err != nil {
		return nil, err
	}

	specs, comps := v2Specs(), map[string]customComponent{}

	for _, c := range calc.CalculationComponents {
		name := c.MetricName
		if name == "" {
			name = strings.ToUpper(idTail(c.MetricID))
		}

		spec, ok := specs[name]
		if !ok || spec.sample == nil {
			return nil, invalidParamErr("component %q uses metric %q, which has no per-contact value", c.Alias, name)
		}

		filters, ferr := decodeComponentFilters(c.MetricFilters)
		if ferr != nil {
			return nil, ferr
		}

		for _, f := range filters {
			if !slices.Contains(spec.metricFilters, f.MetricFilterKey) {
				return nil, invalidParamErr("metric %q does not accept the metric filter %q", name, f.MetricFilterKey)
			}
		}

		comps[c.Alias] = customComponent{spec: spec, filters: filters}
	}

	used := map[string]bool{}
	aliasesOf(root, used)

	for a := range used {
		if _, ok := comps[a]; !ok {
			return nil, invalidParamErr("Calculation refers to %q, which is not a component alias", a)
		}
	}

	if err = checkStatisticsOnly(root, false); err != nil {
		return nil, err
	}

	return &compiledMetric{root: root, components: comps}, nil
}

func checkStatisticsOnly(n *exprNode, inStat bool) error {
	if n.op == 'a' && !inStat {
		return invalidParamErr("alias %q must be inside SUM, AVG, MIN or MAX", n.name)
	}

	if n.op == 's' && inStat {
		return invalidParamErr("statistics cannot be nested")
	}

	for _, k := range n.kids {
		if err := checkStatisticsOnly(k, inStat || n.op == 's'); err != nil {
			return err
		}
	}

	return nil
}

type customEval struct {
	b        *Backend
	m        *compiledMetric
	w        window
	inst     string
	contacts []*contactRec
}

func (e *customEval) sample(alias string, c *contactRec) (float64, bool) {
	comp := e.m.components[alias]
	if !e.b.matchesMetric(e.inst, c, comp.filters) {
		return 0, false
	}

	return comp.spec.sample(c, e.w)
}

func (e *customEval) value(n *exprNode, c *contactRec) (float64, bool) {
	switch n.op {
	case 'c':
		return n.num, true
	case 'a':
		return e.sample(n.name, c)
	case 'n':
		v, ok := e.value(n.kids[0], c)

		return -v, ok
	case 's':
		return e.statistic(n), true
	}

	l, lok := e.value(n.kids[0], c)
	r, rok := e.value(n.kids[1], c)

	if !lok || !rok {
		return 0, false
	}

	switch n.op {
	case '+':
		return l + r, true
	case '-':
		return l - r, true
	case '*':
		return l * r, true
	default:
		if r == 0 {
			return 0, true
		}

		return l / r, true
	}
}

func (e *customEval) statistic(n *exprNode) float64 {
	var vals []float64

	for _, c := range e.contacts {
		if v, ok := e.value(n.kids[0], c); ok {
			vals = append(vals, v)
		}
	}

	if len(vals) == 0 {
		return 0
	}

	switch n.name {
	case "SUM":
		return sumOf(vals)
	case statAvg:
		return sumOf(vals) / float64(len(vals))
	case "MIN":
		return slices.Min(vals)
	default:
		return slices.Max(vals)
	}
}

func sumOf(vs []float64) float64 {
	var t float64
	for _, v := range vs {
		t += v
	}

	return t
}

func (b *Backend) evalCustomMetric(
	inst string, m *compiledMetric, w window, contacts []*contactRec,
) float64 {
	e := &customEval{b: b, m: m, w: w, inst: inst, contacts: contacts}
	v, _ := e.value(m.root, nil)

	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}

	return v
}
