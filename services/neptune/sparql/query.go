package sparql

import (
	"context"
	"sort"
	"strings"
)

// ResultKind says which payload of a Result is populated.
type ResultKind uint8

// Result kinds.
const (
	ResultSelect ResultKind = iota
	ResultAsk
	ResultGraph
)

// Result is the outcome of a query.
type Result struct {
	Vars    []string
	Rows    []Binding
	Triples []Triple
	Kind    ResultKind
	Bool    bool
}

// Query parses and runs a SPARQL query against the store.
func (s *Store) Query(ctx context.Context, src string) (*Result, error) {
	q, err := ParseQuery(src)
	if err != nil {
		return nil, err
	}

	return s.Run(ctx, q)
}

// Run evaluates a parsed query under a read lock.
func (s *Store) Run(ctx context.Context, q *Query) (*Result, error) {
	s.mu.RLock("Query")
	defer s.mu.RUnlock()

	ev := newEvaluator(ctx, s.datasetFor(q.From, q.FromNamed), q.Base)

	return ev.run(q)
}

func (s *Store) datasetFor(from, named []Term) *dataset {
	r := len(from)+len(named) > 0

	return &dataset{s: s, def: from, named: named, defR: r, namedR: r}
}

type item struct {
	b    Binding
	rows []Binding
	keys []Term
}

func (ev *evaluator) run(q *Query) (*Result, error) {
	rows, vars, err := ev.selectRows(q, Term{})
	if err != nil {
		return nil, err
	}

	switch q.Form {
	case formAsk:
		return &Result{Kind: ResultAsk, Bool: len(rows) > 0}, nil
	case formConstruct:
		return &Result{Kind: ResultGraph, Triples: ev.construct(q.Template, rows)}, nil
	case formDescribe:
		return &Result{Kind: ResultGraph, Triples: ev.describe(q, rows, vars)}, nil
	default:
		return &Result{Kind: ResultSelect, Vars: vars, Rows: rows}, nil
	}
}

func queryAggregates(q *Query) bool {
	if q.grouped || len(q.Having) > 0 {
		return true
	}

	for _, p := range q.Proj {
		if p.Expr != nil && hasAggregate(p.Expr) {
			return true
		}
	}

	for _, o := range q.Order {
		if hasAggregate(o.Expr) {
			return true
		}
	}

	return false
}

// selectRows evaluates the pattern and solution modifiers, returning the projected rows.
func (ev *evaluator) selectRows(q *Query, gc Term) ([]Binding, []string, error) {
	sols, err := ev.evalGroup(q.Where, gc)
	if err != nil {
		return nil, nil, err
	}

	if q.Values != nil {
		if sols, err = ev.join(sols, valuesRows(q.Values), nil, false, gc); err != nil {
			return nil, nil, err
		}
	}

	agg := queryAggregates(q)

	var items []*item

	if agg {
		items, err = ev.groupItems(q, sols, gc)
	} else {
		items = make([]*item, len(sols))
		for i, b := range sols {
			items[i] = &item{b: b}
		}
	}

	if err != nil {
		return nil, nil, err
	}

	ev.project(q, items, gc, agg)

	vars := projectedVars(q)
	if q.Form != formSelect && !q.Star && q.Form != formAsk {
		vars = patternVars(q.Where)
	}

	ev.orderItems(q, items, gc, agg)

	rows := make([]Binding, 0, len(items))
	for _, it := range items {
		rows = append(rows, it.b)
	}

	if q.Form == formSelect || q.Form == formDescribe {
		rows = projectRows(rows, vars)
		if q.Distinct {
			rows = distinctRows(rows, vars)
		}
	}

	return slice(rows, q.Offset, q.Limit), vars, nil
}

func slice(rows []Binding, offset, limit int) []Binding {
	if offset > 0 {
		if offset >= len(rows) {
			return nil
		}

		rows = rows[offset:]
	}

	if limit >= 0 && limit < len(rows) {
		rows = rows[:limit]
	}

	return rows
}

func projectRows(rows []Binding, vars []string) []Binding {
	out := make([]Binding, len(rows))

	for i, b := range rows {
		nb := make(Binding, len(vars))

		for _, v := range vars {
			if t, ok := b[v]; ok {
				nb[v] = t
			}
		}

		out[i] = nb
	}

	return out
}

func distinctRows(rows []Binding, vars []string) []Binding {
	seen := map[string]struct{}{}
	out := rows[:0:0]

	for _, b := range rows {
		k := bindingKey(b, vars)
		if _, dup := seen[k]; dup {
			continue
		}

		seen[k] = struct{}{}

		out = append(out, b)
	}

	return out
}

func (ev *evaluator) groupItems(q *Query, sols []Binding, gc Term) ([]*item, error) {
	type grp struct {
		it  *item
		key string
	}

	var order []*grp

	index := map[string]*grp{}

	for _, b := range sols {
		if err := ev.tick(); err != nil {
			return nil, err
		}

		key := ""
		gb := Binding{}

		var keySb223 strings.Builder
		for i, g := range q.GroupBy {
			t, err := ev.eval(g.Expr, &env{b: b, gc: gc})
			if err != nil {
				keySb223.WriteString("\x01\x00")

				continue
			}

			keySb223.WriteString(t.String() + "\x00")

			if g.Var != "" {
				gb[g.Var] = t
			}

			_ = i
		}
		key += keySb223.String()

		g, ok := index[key]
		if !ok {
			g = &grp{key: key, it: &item{b: gb}}
			index[key] = g
			order = append(order, g)
		}

		g.it.rows = append(g.it.rows, b)
	}

	if len(order) == 0 && len(q.GroupBy) == 0 {
		order = append(order, &grp{it: &item{b: Binding{}}})
	}

	items := make([]*item, 0, len(order))

	for _, g := range order {
		if ev.passesHaving(q, g.it, gc) {
			items = append(items, g.it)
		}
	}

	return items, nil
}

func (ev *evaluator) passesHaving(q *Query, it *item, gc Term) bool {
	for _, h := range q.Having {
		t, err := ev.eval(h, &env{b: it.b, rows: it.rows, agg: true, gc: gc})
		if err != nil {
			return false
		}

		if ok, berr := ebv(t); berr != nil || !ok {
			return false
		}
	}

	return true
}

// project evaluates SELECT expressions into each item's binding, in order so later ones can use earlier aliases.
func (ev *evaluator) project(q *Query, items []*item, gc Term, agg bool) {
	for _, it := range items {
		for _, p := range q.Proj {
			if p.Expr == nil {
				continue
			}

			t, err := ev.eval(p.Expr, &env{b: it.b, rows: it.rows, agg: agg, gc: gc})
			if err != nil {
				continue
			}

			nb := it.b.clone()
			nb[p.Var] = t
			it.b = nb
		}
	}
}

func (ev *evaluator) orderItems(q *Query, items []*item, gc Term, agg bool) {
	if len(q.Order) == 0 {
		return
	}

	for _, it := range items {
		it.keys = make([]Term, len(q.Order))

		for i, o := range q.Order {
			if t, err := ev.eval(o.Expr, &env{b: it.b, rows: it.rows, agg: agg, gc: gc}); err == nil {
				it.keys[i] = t
			}
		}
	}

	sort.SliceStable(items, func(i, j int) bool {
		for k, o := range q.Order {
			c := orderCompare(items[i].keys[k], items[j].keys[k])
			if c == 0 {
				continue
			}

			if o.Desc {
				return c > 0
			}

			return c < 0
		}

		return false
	})
}

func (ev *evaluator) construct(tmpl []triplePat, rows []Binding) []Triple {
	seen := map[Triple]struct{}{}

	var out []Triple

	for _, b := range rows {
		blanks := map[string]Term{}

		resolve := func(n node) Term {
			if !n.isVar() {
				return n.Term
			}

			if strings.HasPrefix(n.Var, "_:") {
				t, ok := blanks[n.Var]
				if !ok {
					t = ev.freshBlank()
					blanks[n.Var] = t
				}

				return t
			}

			return b[n.Var]
		}

		for _, tp := range tmpl {
			t := Triple{S: resolve(tp.S), P: resolve(tp.P), O: resolve(tp.O)}
			if !validTriple(t) {
				continue
			}

			if _, dup := seen[t]; dup {
				continue
			}

			seen[t] = struct{}{}

			out = append(out, t)
		}
	}

	return out
}

func validTriple(t Triple) bool {
	return t.S.Bound() && t.P.Bound() && t.O.Bound() && t.S.Kind != KindLiteral && t.P.Kind == KindIRI
}

func (ev *evaluator) describe(q *Query, rows []Binding, vars []string) []Triple {
	var targets []Term

	add := func(t Term) {
		if t.Kind == KindIRI || t.Kind == KindBlank {
			targets = append(targets, t)
		}
	}

	for _, n := range q.Describe {
		if !n.isVar() {
			add(n.Term)

			continue
		}

		for _, b := range rows {
			add(b[n.Var])
		}
	}

	if q.Star {
		for _, b := range rows {
			for _, v := range vars {
				add(b[v])
			}
		}
	}

	return ev.describeAll(targets)
}

// describeAll returns each resource's concise bounded description: its outgoing triples, following blank nodes.
func (ev *evaluator) describeAll(targets []Term) []Triple {
	seen := map[Triple]struct{}{}
	done := map[Term]bool{}

	var out []Triple

	for len(targets) > 0 {
		r := targets[0]
		targets = targets[1:]

		if done[r] {
			continue
		}

		done[r] = true

		ev.scan(Term{}, r, Term{}, Term{}, func(qd Quad) bool {
			if _, dup := seen[qd.Triple]; !dup {
				seen[qd.Triple] = struct{}{}

				out = append(out, qd.Triple)
			}

			if qd.O.Kind == KindBlank {
				targets = append(targets, qd.O)
			}

			return true
		})
	}

	return out
}
