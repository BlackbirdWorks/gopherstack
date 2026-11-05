package sparql

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"
)

type dataset struct {
	s      *Store
	def    []Term
	named  []Term
	defR   bool
	namedR bool
}

type evaluator struct {
	ctx     context.Context
	ds      *dataset
	started time.Time
	funcs   map[string]builtin
	labels  map[string]Term
	baseIRI string
	steps   int
	bn      int
}

func newEvaluator(ctx context.Context, ds *dataset, base string) *evaluator {
	return &evaluator{
		ctx: ctx, ds: ds, started: time.Now().UTC(), funcs: newFuncTable(),
		labels: map[string]Term{}, baseIRI: base,
	}
}

func (ev *evaluator) tick() error {
	ev.steps++
	if ev.steps&1023 == 0 {
		return ev.ctx.Err()
	}

	return nil
}

func (ev *evaluator) freshBlank() Term {
	ev.bn++

	return Blank("q" + strconv.Itoa(ev.bn))
}

func (ev *evaluator) labeledBlank(label string) Term {
	if t, ok := ev.labels[label]; ok {
		return t
	}

	t := ev.freshBlank()
	ev.labels[label] = t

	return t
}

func (ev *evaluator) base(ref string) string {
	p := &parser{base: ev.baseIRI}

	return p.resolve(ref)
}

func (ev *evaluator) namedAllowed(g Term) bool {
	if !ev.ds.namedR {
		return true
	}

	for _, n := range ev.ds.named {
		if n == g {
			return true
		}
	}

	return false
}

func (ev *evaluator) namedGraphs() []Term {
	if ev.ds.namedR {
		return ev.ds.named
	}

	return ev.ds.s.graphsLocked()
}

// scan visits matching quads of graph gc (the dataset's default graph when gc is unbound), de-duplicating triples
// that appear in several graphs of the default graph.
func (ev *evaluator) scan(gc, s, p, o Term, fn func(Quad) bool) {
	st := ev.ds.s

	if gc.Bound() {
		if ev.namedAllowed(gc) {
			st.match(s, p, o, gc, fn)
		}

		return
	}

	if ev.ds.defR {
		ev.scanGraphs(ev.ds.def, s, p, o, fn)

		return
	}

	if len(st.byG) <= 1 {
		st.match(s, p, o, Term{}, fn)

		return
	}

	seen := map[Triple]struct{}{}

	st.match(s, p, o, Term{}, func(q Quad) bool {
		if _, dup := seen[q.Triple]; dup {
			return true
		}

		seen[q.Triple] = struct{}{}

		return fn(q)
	})
}

func (ev *evaluator) scanGraphs(graphs []Term, s, p, o Term, fn func(Quad) bool) {
	seen := map[Triple]struct{}{}
	cont := true

	for _, g := range graphs {
		if !cont {
			return
		}

		ev.ds.s.match(s, p, o, g, func(q Quad) bool {
			if len(graphs) > 1 {
				if _, dup := seen[q.Triple]; dup {
					return true
				}

				seen[q.Triple] = struct{}{}
			}

			cont = fn(q)

			return cont
		})
	}
}

func identity() []Binding { return []Binding{{}} }

func (ev *evaluator) evalGroup(g *group, gc Term) ([]Binding, error) {
	return ev.evalGroupSeeded(g, gc, identity())
}

func (ev *evaluator) evalGroupSeeded(g *group, gc Term, seed []Binding) ([]Binding, error) {
	sols, filters, err := ev.evalParts(g, gc, seed)
	if err != nil {
		return nil, err
	}

	return ev.applyFilters(sols, filters, gc)
}

func (ev *evaluator) evalParts(g *group, gc Term, seed []Binding) ([]Binding, []Expr, error) {
	sols := seed

	var filters []Expr

	for _, el := range g.Elems {
		if f, ok := el.(*filterEl); ok {
			filters = append(filters, f.E)

			continue
		}

		var err error
		if sols, err = ev.evalElement(el, gc, sols); err != nil {
			return nil, nil, err
		}
	}

	return sols, filters, nil
}

func (ev *evaluator) applyFilters(sols []Binding, filters []Expr, gc Term) ([]Binding, error) {
	if len(filters) == 0 {
		return sols, nil
	}

	out := sols[:0:0]

	for _, b := range sols {
		if err := ev.tick(); err != nil {
			return nil, err
		}

		if ev.passes(filters, b, gc) {
			out = append(out, b)
		}
	}

	return out, nil
}

func (ev *evaluator) passes(filters []Expr, b Binding, gc Term) bool {
	for _, f := range filters {
		t, err := ev.eval(f, &env{b: b, gc: gc})
		if err != nil {
			return false
		}

		if ok, berr := ebv(t); berr != nil || !ok {
			return false
		}
	}

	return true
}

func (ev *evaluator) evalElement(el element, gc Term, sols []Binding) ([]Binding, error) {
	switch x := el.(type) {
	case *bgpEl:
		return ev.evalBGP(x.Triples, sols, gc)
	case *optionalEl:
		r, filters, err := ev.evalParts(x.G, gc, identity())
		if err != nil {
			return nil, err
		}

		return ev.join(sols, r, filters, true, gc)
	case *unionEl:
		var all []Binding

		for _, alt := range x.Alts {
			r, err := ev.evalGroup(alt, gc)
			if err != nil {
				return nil, err
			}

			all = append(all, r...)
		}

		return ev.join(sols, all, nil, false, gc)
	case *minusEl:
		r, err := ev.evalGroup(x.G, gc)
		if err != nil {
			return nil, err
		}

		return minus(sols, r), nil
	case *bindEl:
		return ev.evalBind(x, sols, gc), nil
	case *valuesEl:
		return ev.join(sols, valuesRows(x.V), nil, false, gc)
	case *graphEl:
		return ev.evalGraph(x, sols)
	case *subSelectEl:
		rows, _, err := ev.selectRows(x.Q, gc)
		if err != nil {
			return nil, err
		}

		return ev.join(sols, rows, nil, false, gc)
	case *groupEl:
		r, err := ev.evalGroup(x.G, gc)
		if err != nil {
			return nil, err
		}

		return ev.join(sols, r, nil, false, gc)
	default:
		return nil, syntaxf("unsupported pattern element")
	}
}

func (ev *evaluator) evalBind(x *bindEl, sols []Binding, gc Term) []Binding {
	out := make([]Binding, 0, len(sols))

	for _, b := range sols {
		if _, exists := b[x.Var]; exists {
			out = append(out, b)

			continue
		}

		t, err := ev.eval(x.E, &env{b: b, gc: gc})
		if err != nil {
			out = append(out, b)

			continue
		}

		nb := b.clone()
		nb[x.Var] = t
		out = append(out, nb)
	}

	return out
}

func valuesRows(v *valuesData) []Binding {
	out := make([]Binding, 0, len(v.Rows))

	for _, row := range v.Rows {
		b := Binding{}

		for i, t := range row {
			if t.Bound() {
				b[v.Vars[i]] = t
			}
		}

		out = append(out, b)
	}

	return out
}

func (ev *evaluator) evalGraph(x *graphEl, sols []Binding) ([]Binding, error) {
	if !x.Name.isVar() {
		r, err := ev.evalGroup(x.G, x.Name.Term)
		if err != nil {
			return nil, err
		}

		return ev.join(sols, r, nil, false, Term{})
	}

	var all []Binding

	for _, g := range ev.namedGraphs() {
		r, err := ev.evalGroup(x.G, g)
		if err != nil {
			return nil, err
		}

		for _, b := range r {
			if cur, ok := b[x.Name.Var]; ok && cur != g {
				continue
			}

			nb := b.clone()
			nb[x.Name.Var] = g
			all = append(all, nb)
		}
	}

	return ev.join(sols, all, nil, false, Term{})
}

func compatible(a, b Binding) bool {
	if len(b) < len(a) {
		a, b = b, a
	}

	for k, v := range a {
		if o, ok := b[k]; ok && o != v {
			return false
		}
	}

	return true
}

func merge(a, b Binding) Binding {
	out := make(Binding, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}

	for k, v := range b {
		out[k] = v
	}

	return out
}

func commonVars(a, b []Binding) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	var common []string

	for k := range a[0] {
		if allBound(a, k) && allBound(b, k) {
			common = append(common, k)
		}
	}

	sort.Strings(common)

	return common
}

func allBound(rows []Binding, k string) bool {
	for _, r := range rows {
		if _, ok := r[k]; !ok {
			return false
		}
	}

	return true
}

// join combines a and b; cond (the filters of an OPTIONAL group) is tested on each merged row, and unmatched
// rows of a are kept when keep is set.
func (ev *evaluator) join(a, b []Binding, cond []Expr, keep bool, gc Term) ([]Binding, error) {
	if len(a) == 1 && len(a[0]) == 0 && len(cond) == 0 && !keep {
		return b, nil
	}

	var index map[string][]Binding

	common := commonVars(a, b)
	if len(common) > 0 && len(b) > 8 {
		index = make(map[string][]Binding, len(b))
		for _, r := range b {
			k := bindingKey(r, common)
			index[k] = append(index[k], r)
		}
	}

	out := make([]Binding, 0, len(a))

	for _, l := range a {
		if err := ev.tick(); err != nil {
			return nil, err
		}

		cands := b
		if index != nil {
			cands = index[bindingKey(l, common)]
		}

		matched := false

		for _, r := range cands {
			if !compatible(l, r) {
				continue
			}

			m := merge(l, r)
			if len(cond) > 0 && !ev.passes(cond, m, gc) {
				continue
			}

			matched = true

			out = append(out, m)
		}

		if keep && !matched {
			out = append(out, l)
		}
	}

	return out, nil
}

func minus(a, b []Binding) []Binding {
	out := make([]Binding, 0, len(a))

	for _, l := range a {
		removed := false

		for _, r := range b {
			if sharesVar(l, r) && compatible(l, r) {
				removed = true

				break
			}
		}

		if !removed {
			out = append(out, l)
		}
	}

	return out
}

func sharesVar(a, b Binding) bool {
	for k := range a {
		if _, ok := b[k]; ok {
			return true
		}
	}

	return false
}

func sortedKeys(b Binding) []string {
	keys := make([]string, 0, len(b))
	for k := range b {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func collectVars(g *group, seen map[string]bool, out *[]string) {
	add := func(n node) {
		if n.isVar() && !strings.HasPrefix(n.Var, "_:") && !seen[n.Var] {
			seen[n.Var] = true
			*out = append(*out, n.Var)
		}
	}

	for _, el := range g.Elems {
		switch x := el.(type) {
		case *bgpEl:
			for _, tp := range x.Triples {
				add(tp.S)
				add(tp.P)
				add(tp.O)
			}
		case *optionalEl:
			collectVars(x.G, seen, out)
		case *unionEl:
			for _, a := range x.Alts {
				collectVars(a, seen, out)
			}
		case *bindEl:
			add(varNode(x.Var))
		case *valuesEl:
			for _, v := range x.V.Vars {
				add(varNode(v))
			}
		case *graphEl:
			add(x.Name)
			collectVars(x.G, seen, out)
		case *subSelectEl:
			for _, v := range projectedVars(x.Q) {
				add(varNode(v))
			}
		case *groupEl:
			collectVars(x.G, seen, out)
		}
	}
}

func projectedVars(q *Query) []string {
	if !q.Star {
		vars := make([]string, 0, len(q.Proj))
		for _, p := range q.Proj {
			vars = append(vars, p.Var)
		}

		return vars
	}

	var out []string

	collectVars(q.Where, map[string]bool{}, &out)

	return out
}
