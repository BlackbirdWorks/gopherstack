package ocilogging

import (
	"math"
	"slices"
	"strconv"
	"strings"
)

const (
	kindSelect    = "select"
	kindExtend    = "extend"
	kindDedup     = "dedup"
	kindSummarize = "summarize"
)

var projectOps = []string{kindSelect, kindExtend, kindDedup, kindSummarize} //nolint:gochecknoglobals // keyword list

// projCol is a select, extend or group-by expression with its output name; star stands for select *.
type projCol struct {
	e    vexpr
	name string
	star bool
}

// aggSpec is one aggregate of a summarize.
type aggSpec struct {
	arg  vexpr
	fn   string
	name string
}

var aggFuncs = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"sum": true, "avg": true, "min": true, "max": true, "count": true, "first": true, "last": true,
}

func (p *parser) parseProject(kind string) (qop, *APIError) {
	p.next()

	switch kind {
	case kindSummarize:
		return p.parseSummarize()
	case kindDedup:
		cols, err := p.parseCols(false)

		return qop{kind: kind, cols: cols}, err
	case kindExtend:
		cols, err := p.parseCols(false)
		if err == nil && len(cols) != 1 {
			err = errParam("extend takes one expression")
		}

		return qop{kind: kind, cols: cols}, err
	}

	cols, err := p.parseCols(true)

	return qop{kind: kind, cols: cols}, err
}

// parseCols reads a comma separated list of expressions with optional "as name" aliases.
func (p *parser) parseCols(star bool) ([]projCol, *APIError) {
	var cols []projCol

	for {
		c, err := p.parseCol(star)
		if err != nil {
			return nil, err
		}

		cols = append(cols, c)

		if !p.isPunct(",") {
			return cols, nil
		}

		p.next()
	}
}

func (p *parser) parseCol(star bool) (projCol, *APIError) {
	if star && p.isPunct("*") {
		p.next()

		return projCol{star: true}, nil
	}

	e, err := p.parseAdd()
	if err != nil {
		return projCol{}, err
	}

	c := projCol{e: e, name: e.String()}

	if p.isWord("as") {
		p.next()

		t := p.next()
		if t.kind != tkPath || len(t.path) != 1 {
			return projCol{}, errParam("an identifier is required after as")
		}

		c.name = t.path[0]
	}

	return c, nil
}

func (p *parser) parseSummarize() (qop, *APIError) {
	o := qop{kind: kindSummarize}

	for {
		a, err := p.parseAgg()
		if err != nil {
			return qop{}, err
		}

		o.aggs = append(o.aggs, a)

		if !p.isPunct(",") {
			break
		}

		p.next()
	}

	if p.isWord("by") {
		p.next()

		cols, err := p.parseCols(false)
		if err != nil {
			return qop{}, err
		}

		o.cols = cols
	}

	return o, nil
}

func (p *parser) parseAgg() (aggSpec, *APIError) {
	t := p.next()
	if t.kind != tkPath || len(t.path) != 1 || !aggFuncs[strings.ToLower(t.path[0])] || !p.isPunct("(") {
		return aggSpec{}, errParam("summarize needs aggregate functions: sum, avg, min, max, count, first or last")
	}

	a := aggSpec{fn: strings.ToLower(t.path[0])}

	p.next()

	if !p.isPunct(")") {
		arg, err := p.parseAdd()
		if err != nil {
			return aggSpec{}, err
		}

		a.arg = arg
	}

	if !p.isPunct(")") || (a.arg == nil && a.fn != kindCount) {
		return aggSpec{}, errParam("malformed aggregate " + a.fn)
	}

	p.next()

	a.name = a.fn + "("
	if a.arg != nil {
		a.name += a.arg.String()
	}

	a.name += ")"

	if p.isWord("as") {
		p.next()

		n := p.next()
		if n.kind != tkPath || len(n.path) != 1 {
			return aggSpec{}, errParam("an identifier is required after as")
		}

		a.name = n.path[0]
	}

	return a, nil
}

// Execution.

func (o qop) runSelect(rows []*row, cols []string) ([]*row, []string) {
	var names []string

	for _, c := range o.cols {
		if c.star {
			names = append(names, cols...)
		} else {
			names = append(names, c.name)
		}
	}

	for _, r := range rows {
		r.cols, r.projected = o.project(r, cols), true
	}

	return rows, slices.Compact(names)
}

func (o qop) project(r *row, std []string) map[string]any {
	next := map[string]any{}

	for _, c := range o.cols {
		if c.star {
			for _, n := range std {
				if v, ok := r.lookup([]string{n}); ok {
					next[n] = v
				}
			}

			continue
		}

		if v, ok := c.e.value(r); ok {
			next[c.name] = v
		}
	}

	return next
}

func (o qop) runExtend(rows []*row, cols []string) ([]*row, []string) {
	c := o.cols[0]

	for _, r := range rows {
		v, ok := c.e.value(r)
		if r.cols == nil {
			r.cols = map[string]any{}
		}

		if ok {
			r.cols[c.name] = v
		} else {
			delete(r.cols, c.name)
		}
	}

	if !slices.Contains(cols, c.name) {
		cols = append(slices.Clone(cols), c.name)
	}

	return rows, cols
}

func (o qop) runDedup(rows []*row) []*row {
	seen := map[string]bool{}

	return slices.DeleteFunc(rows, func(r *row) bool {
		vals := make([]any, len(o.cols))
		oks := make([]bool, len(o.cols))

		for i, c := range o.cols {
			vals[i], oks[i] = c.e.value(r)
		}

		k := groupKey(vals, oks)
		if seen[k] {
			return true
		}

		seen[k] = true

		return false
	})
}

type aggState struct {
	first, last any
	count       int
	n           int
	sum         float64
	min, max    float64
	hasFirst    bool
}

func (s *aggState) add(v any, ok bool) {
	if !ok || v == nil {
		return
	}

	s.count++

	if !s.hasFirst {
		s.first, s.hasFirst = v, true
	}

	s.last = v

	if f, isNum := toFloat(v); isNum {
		if s.n == 0 {
			s.min, s.max = f, f
		}

		s.n++
		s.sum += f
		s.min, s.max = min(s.min, f), max(s.max, f)
	}
}

func (s *aggState) result(fn string) (any, bool) {
	switch fn {
	case kindCount:
		return float64(s.count), true
	case "first":
		return s.first, s.hasFirst
	case "last":
		return s.last, s.hasFirst
	}

	if s.n == 0 {
		return nil, false
	}

	switch fn {
	case "sum":
		return finiteOrNone(s.sum)
	case "avg":
		return finiteOrNone(s.sum / float64(s.n))
	case "min":
		return s.min, true
	}

	return s.max, true
}

func finiteOrNone(f float64) (any, bool) {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return nil, false
	}

	return f, true
}

type group struct {
	first  *row
	by     []any
	byOK   []bool
	states []aggState
}

func groupKey(vals []any, oks []bool) string {
	var sb strings.Builder

	for i, v := range vals {
		if oks[i] {
			str := scalarString(v)
			sb.WriteString("1")
			sb.WriteString(strconv.Itoa(len(str)))
			sb.WriteByte(':')
			sb.WriteString(str)
		} else {
			sb.WriteString("0")
		}
	}

	return sb.String()
}

func (o qop) runSummarize(rows []*row) ([]*row, []string, *APIError) {
	groups := map[string]*group{}
	order := []*group{}

	for _, r := range rows {
		vals := make([]any, len(o.cols))
		oks := make([]bool, len(o.cols))

		for i, c := range o.cols {
			vals[i], oks[i] = c.e.value(r)
		}

		k := groupKey(vals, oks)

		g := groups[k]
		if g == nil {
			if len(groups) == maxSummaryGroup {
				return nil, nil, errParam("summarize produces too many groups")
			}

			g = &group{first: r, by: vals, byOK: oks, states: make([]aggState, len(o.aggs))}
			groups[k], order = g, append(order, g)
		}

		o.accumulate(g, r)
	}

	if len(o.cols) == 0 && len(order) == 0 {
		order = append(order, &group{states: make([]aggState, len(o.aggs))})
	}

	out := make([]*row, 0, len(order))
	for _, g := range order {
		out = append(out, o.groupRow(g))
	}

	return out, o.columnNames(), nil
}

func (o qop) accumulate(g *group, r *row) {
	for i, a := range o.aggs {
		if a.arg == nil {
			g.states[i].count++

			continue
		}

		v, ok := a.arg.value(r)
		g.states[i].add(v, ok)
	}
}

func (o qop) columnNames() []string {
	names := make([]string, 0, len(o.cols)+len(o.aggs))
	for _, c := range o.cols {
		names = append(names, c.name)
	}

	for _, a := range o.aggs {
		names = append(names, a.name)
	}

	return names
}

func (o qop) groupRow(g *group) *row {
	vals := map[string]any{}

	for i, c := range o.cols {
		if g.byOK[i] {
			vals[c.name] = g.by[i]
		}
	}

	for i, a := range o.aggs {
		if v, ok := g.states[i].result(a.fn); ok {
			vals[a.name] = v
		}
	}

	nr := &row{cols: vals, projected: true}
	if g.first != nil {
		nr.ms, nr.seq = g.first.ms, g.first.seq
	}

	return nr
}
