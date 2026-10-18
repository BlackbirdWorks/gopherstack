package opencypher

import "slices"

const maxVarHops = 64

func (x *exec) execMatch(c matchClause, rows []row) ([]row, error) {
	var out []row

	for _, r := range rows {
		found := false

		err := x.walkPatterns(c.pats, 0, r, map[string]bool{}, func(m row) error {
			if c.where != nil {
				v, err := x.eval(c.where, m, nil)
				if err != nil {
					return err
				}

				if b, _ := v.(bool); !b {
					return nil
				}
			}

			found = true
			out = append(out, m)

			return nil
		})
		if err != nil {
			return nil, err
		}

		if !found && c.optional {
			out = append(out, nullBindings(c.pats, r))
		}
	}

	return out, nil
}

func nullBindings(pats []pattern, r row) row {
	out := r.with("", nil)

	bind := func(name string) {
		if _, has := out[name]; name != "" && !has {
			out[name] = nil
		}
	}

	for _, p := range pats {
		bind(p.pathVar)

		for _, n := range p.nodes {
			bind(n.v)
		}

		for _, rel := range p.rels {
			bind(rel.v)
		}
	}

	return out
}

func (x *exec) walkPatterns(pats []pattern, i int, r row, used map[string]bool, emit func(row) error) error {
	if i == len(pats) {
		return emit(r)
	}

	return x.walkPattern(pats[i], r, used, func(m row) error {
		return x.walkPatterns(pats, i+1, m, used, emit)
	})
}

func (x *exec) walkPattern(p pattern, r row, used map[string]bool, emit func(row) error) error {
	first := p.nodes[0]

	cands, err := x.nodeCandidates(first, r)
	if err != nil {
		return err
	}

	for _, n := range cands {
		r2, ok, nerr := x.bindNode(first, n, r)
		if nerr != nil {
			return nerr
		}

		if !ok {
			continue
		}

		path := &Path{Nodes: []*Node{n}}

		if err = x.expand(p, 0, n, r2, used, path, emit); err != nil {
			return err
		}
	}

	return nil
}

func (x *exec) nodeCandidates(np nodePat, r row) ([]*Node, error) {
	v, bound := r[np.v]
	if np.v == "" || !bound {
		return x.g.allNodes(), nil
	}

	if v == nil {
		return nil, nil
	}

	n, ok := v.(*Node)
	if !ok {
		return nil, malformed("Type mismatch: expected Node but was %s", typeName(v))
	}

	return []*Node{n}, nil
}

func (x *exec) bindNode(np nodePat, n *Node, r row) (row, bool, error) {
	if !hasLabels(n, np.labels) {
		return nil, false, nil
	}

	ok, err := x.propsMatch(np.props, n.Props, r)
	if err != nil || !ok {
		return nil, false, err
	}

	if np.v == "" {
		return r, true, nil
	}

	if cur, bound := r[np.v]; bound {
		cn, isNode := cur.(*Node)

		return r, isNode && cn.ID == n.ID, nil
	}

	return r.with(np.v, n), true, nil
}

func (x *exec) propsMatch(m *mapExpr, props map[string]any, r row) (bool, error) {
	if m == nil {
		return true, nil
	}

	for i, k := range m.keys {
		want, err := x.eval(m.vals[i], r, nil)
		if err != nil {
			return false, err
		}

		if eq, known := equals(props[k], want); !known || !eq {
			return false, nil
		}
	}

	return true, nil
}

func (x *exec) expand(
	p pattern,
	i int,
	cur *Node,
	r row,
	used map[string]bool,
	path *Path,
	emit func(row) error,
) error {
	if i == len(p.rels) {
		out := r
		if p.pathVar != "" {
			out = r.with(
				p.pathVar,
				&Path{Nodes: append([]*Node{}, path.Nodes...), Rels: append([]*Rel{}, path.Rels...)},
			)
		}

		return emit(out)
	}

	rp, np := p.rels[i], p.nodes[i+1]

	if rp.varLen {
		return x.expandVar(p, i, cur, r, used, path, emit)
	}

	return x.eachEdge(rp, cur, used, func(e *Rel, other *Node) error {
		r2, ok, err := x.bindRel(rp, e, r)
		if err != nil || !ok {
			return err
		}

		r3, ok, err := x.bindNode(np, other, r2)
		if err != nil || !ok {
			return err
		}

		used[e.ID] = true

		path.Nodes, path.Rels = append(path.Nodes, other), append(path.Rels, e)
		err = x.expand(p, i+1, other, r3, used, path, emit)
		path.Nodes, path.Rels = path.Nodes[:len(path.Nodes)-1], path.Rels[:len(path.Rels)-1]

		delete(used, e.ID)

		return err
	})
}

func (x *exec) bindRel(rp relPat, e *Rel, r row) (row, bool, error) {
	ok, err := x.propsMatch(rp.props, e.Props, r)
	if err != nil || !ok {
		return nil, false, err
	}

	if rp.v == "" {
		return r, true, nil
	}

	if cur, bound := r[rp.v]; bound {
		ce, isRel := cur.(*Rel)

		return r, isRel && ce.ID == e.ID, nil
	}

	return r.with(rp.v, e), true, nil
}

func typeOK(rp relPat, e *Rel) bool {
	if len(rp.types) == 0 {
		return true
	}

	return slices.Contains(rp.types, e.Type)
}

func usable(rp relPat, e *Rel, used map[string]bool) bool { return !used[e.ID] && typeOK(rp, e) }

func (x *exec) eachEdge(rp relPat, cur *Node, used map[string]bool, f func(*Rel, *Node) error) error {
	if rp.dir >= 0 {
		for _, e := range slices.Clone(x.g.out[cur.ID]) {
			if !usable(rp, e, used) {
				continue
			}

			if err := f(e, x.g.nodes[e.End]); err != nil {
				return err
			}
		}
	}

	if rp.dir > 0 {
		return nil
	}

	for _, e := range slices.Clone(x.g.in[cur.ID]) {
		if !usable(rp, e, used) || (rp.dir == 0 && e.Start == e.End) {
			continue
		}

		if err := f(e, x.g.nodes[e.Start]); err != nil {
			return err
		}
	}

	return nil
}

type varWalk struct {
	x     *exec
	r     row
	used  map[string]bool
	path  *Path
	emit  func(row) error
	p     pattern
	np    nodePat
	rp    relPat
	i     int
	limit int
}

func (x *exec) expandVar(
	p pattern, i int, cur *Node, r row, used map[string]bool, path *Path, emit func(row) error,
) error {
	rp := p.rels[i]
	limit := rp.maxHops

	if (rp.unbounded && limit == 0) || limit > maxVarHops {
		limit = maxVarHops
	}

	w := &varWalk{x: x, p: p, r: r, used: used, path: path, emit: emit, rp: rp, np: p.nodes[i+1], i: i, limit: limit}

	return w.walk(cur, 0, nil)
}

func (w *varWalk) walk(at *Node, depth int, hops []any) error {
	if depth >= w.rp.minHops {
		if err := w.emitAt(at, hops); err != nil {
			return err
		}
	}

	if depth >= w.limit {
		return nil
	}

	return w.x.eachEdge(w.rp, at, w.used, func(e *Rel, other *Node) error {
		if ok, err := w.x.propsMatch(w.rp.props, e.Props, w.r); err != nil || !ok {
			return err
		}

		w.used[e.ID] = true
		w.path.Nodes, w.path.Rels = append(w.path.Nodes, other), append(w.path.Rels, e)
		err := w.walk(other, depth+1, append(hops, e))
		w.path.Nodes, w.path.Rels = w.path.Nodes[:len(w.path.Nodes)-1], w.path.Rels[:len(w.path.Rels)-1]

		delete(w.used, e.ID)

		return err
	})
}

func (w *varWalk) emitAt(at *Node, hops []any) error {
	r2 := w.r
	if w.rp.v != "" {
		r2 = w.r.with(w.rp.v, append([]any{}, hops...))
	}

	r3, ok, err := w.x.bindNode(w.np, at, r2)
	if err != nil || !ok {
		return err
	}

	return w.x.expand(w.p, w.i+1, at, r3, w.used, w.path, w.emit)
}
