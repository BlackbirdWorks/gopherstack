package opencypher

func (x *exec) execCreate(c createClause, rows []row) ([]row, error) {
	out := make([]row, 0, len(rows))

	for _, r := range rows {
		cur := r

		for _, p := range c.pats {
			var err error
			if cur, err = x.createPattern(p, cur); err != nil {
				return nil, err
			}
		}

		out = append(out, cur)
	}

	return out, nil
}

func (x *exec) createPattern(p pattern, r row) (row, error) {
	cur := r
	nodes := make([]*Node, len(p.nodes))

	for i, np := range p.nodes {
		n, next, err := x.createNode(np, cur)
		if err != nil {
			return nil, err
		}

		nodes[i], cur = n, next
	}

	path := &Path{Nodes: nodes}

	for i, rp := range p.rels {
		if rp.varLen {
			return nil, malformed("Variable length relationships cannot be used in CREATE")
		}

		if len(rp.types) != 1 {
			return nil, malformed("A single relationship type must be specified for CREATE")
		}

		if rp.dir == 0 {
			return nil, malformed("Only directed relationships are supported in CREATE")
		}

		props, err := x.evalProps(rp.props, cur)
		if err != nil {
			return nil, err
		}

		from, to := nodes[i], nodes[i+1]
		if rp.dir < 0 {
			from, to = to, from
		}

		e := x.g.addRel(rp.types[0], from, to, props)
		path.Rels = append(path.Rels, e)

		if rp.v != "" {
			cur = cur.with(rp.v, e)
		}
	}

	if p.pathVar != "" {
		cur = cur.with(p.pathVar, path)
	}

	return cur, nil
}

func (x *exec) createNode(np nodePat, r row) (*Node, row, error) {
	if np.v != "" {
		if v, bound := r[np.v]; bound {
			n, ok := v.(*Node)
			if !ok {
				return nil, nil, malformed("Variable `%s` already declared as %s", np.v, typeName(v))
			}

			return n, r, nil
		}
	}

	props, err := x.evalProps(np.props, r)
	if err != nil {
		return nil, nil, err
	}

	id := ""

	if raw, has := props[IDProperty]; has {
		s, ok := raw.(string)
		if !ok {
			return nil, nil, malformed("~id must be a String")
		}

		id = s

		delete(props, IDProperty)
	}

	n, err := x.g.addNode(id, np.labels, props)
	if err != nil {
		return nil, nil, err
	}

	return n, r.with(np.v, n), nil
}

func (x *exec) evalProps(m *mapExpr, r row) (map[string]any, error) {
	props := map[string]any{}
	if m == nil {
		return props, nil
	}

	for i, k := range m.keys {
		v, err := x.eval(m.vals[i], r, nil)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		if k != IDProperty && !validProperty(v) {
			return nil, &Error{
				Code:   CodeConstraintViolation,
				Detail: "Property values must be scalars or lists of scalars: " + k,
			}
		}

		props[k] = v
	}

	return props, nil
}

func (x *exec) execMerge(c mergeClause, rows []row) ([]row, error) {
	var out []row

	for _, r := range rows {
		var matches []row

		err := x.walkPattern(c.pat, r, map[string]bool{}, func(m row) error {
			matches = append(matches, m)

			return nil
		})
		if err != nil {
			return nil, err
		}

		items, produced := c.onMatch, matches

		if len(matches) == 0 {
			created, cerr := x.createPattern(c.pat, r)
			if cerr != nil {
				return nil, cerr
			}

			items, produced = c.onCreate, []row{created}
		}

		if err = x.applyItems(items, produced); err != nil {
			return nil, err
		}

		out = append(out, produced...)
	}

	return out, nil
}

func (x *exec) applyItems(items []setItem, rows []row) error {
	for _, r := range rows {
		for _, it := range items {
			if err := x.applyItem(it, r); err != nil {
				return err
			}
		}
	}

	return nil
}

func (x *exec) applyItem(it setItem, r row) error {
	switch it.op {
	case ":":
		return x.setLabels(it, r)
	case "remove":
		return x.removeProp(it, r)
	}

	val, err := x.eval(it.val, r, nil)
	if err != nil {
		return err
	}

	if pe, ok := it.target.(propExpr); ok {
		base, berr := x.eval(pe.e, r, nil)
		if berr != nil {
			return berr
		}

		return setProp(base, pe.key, val)
	}

	id, ok := it.target.(identExpr)
	if !ok {
		return malformed("Invalid SET target")
	}

	base, err := x.eval(id, r, nil)
	if err != nil || base == nil {
		return err
	}

	return replaceProps(base, val, it.op == "+=")
}

func setProp(base any, key string, val any) error {
	var props map[string]any

	switch t := base.(type) {
	case nil:
		return nil
	case *Node:
		props = t.Props
	case *Rel:
		props = t.Props
	default:
		return malformed("Type mismatch: expected Node or Relationship but was %s", typeName(base))
	}

	if key == IDProperty {
		return constraint("The ~id property cannot be modified")
	}

	if val == nil {
		delete(props, key)

		return nil
	}

	if !validProperty(val) {
		return constraint("Property values must be scalars or lists of scalars: %s", key)
	}

	props[key] = val

	return nil
}

func replaceProps(base, val any, merge bool) error {
	props, ok := propsOf(base)
	if !ok {
		return malformed("Type mismatch: expected Node or Relationship but was %s", typeName(base))
	}

	src, ok := propsOf(val)
	if !ok {
		return malformed("Type mismatch: expected Map, Node or Relationship but was %s", typeName(val))
	}

	if !merge {
		for k := range props {
			delete(props, k)
		}
	}

	for k, v := range src {
		if err := setProp(base, k, v); err != nil {
			return err
		}
	}

	return nil
}

func (x *exec) setLabels(it setItem, r row) error {
	v, ok := r[it.v]
	if !ok {
		return malformed("Variable `%s` not defined", it.v)
	}

	n, isNode := v.(*Node)
	if !isNode {
		if v == nil {
			return nil
		}

		return malformed("Type mismatch: expected Node but was %s", typeName(v))
	}

	n.Labels = dedupe(append(n.Labels, it.labels...))

	return nil
}

func (x *exec) execRemove(c removeClause, rows []row) error {
	for _, r := range rows {
		for _, it := range c.items {
			if it.op == ":" {
				x.removeLabels(it, r)

				continue
			}

			if err := x.applyItem(it, r); err != nil {
				return err
			}
		}
	}

	return nil
}

func (x *exec) removeLabels(it setItem, r row) {
	n, ok := r[it.v].(*Node)
	if !ok {
		return
	}

	kept := n.Labels[:0:0]

	for _, l := range n.Labels {
		drop := false

		for _, d := range it.labels {
			drop = drop || d == l
		}

		if !drop {
			kept = append(kept, l)
		}
	}

	n.Labels = kept
}

func (x *exec) removeProp(it setItem, r row) error {
	pe, ok := it.target.(propExpr)
	if !ok {
		return malformed("REMOVE expects a property or label")
	}

	base, err := x.eval(pe.e, r, nil)
	if err != nil {
		return err
	}

	return setProp(base, pe.key, nil)
}

func (x *exec) execDelete(c deleteClause, rows []row) error {
	for _, r := range rows {
		for _, e := range c.exprs {
			v, err := x.eval(e, r, nil)
			if err != nil {
				return err
			}

			if err = x.deleteValue(v, c.detach); err != nil {
				return err
			}
		}
	}

	return nil
}

func (x *exec) deleteValue(v any, detach bool) error {
	switch t := v.(type) {
	case nil:
		return nil
	case *Rel:
		x.g.deleteRel(t)
	case *Node:
		edges := append(append([]*Rel(nil), x.g.out[t.ID]...), x.g.in[t.ID]...)
		if len(edges) > 0 && !detach {
			return constraint("Cannot delete node, because it still has relationships. " +
				"To delete this node, you must first delete its relationships.")
		}

		for _, e := range edges {
			x.g.deleteRel(e)
		}

		x.g.deleteNode(t)
	case *Path:
		for _, e := range t.Rels {
			x.g.deleteRel(e)
		}

		for _, n := range t.Nodes {
			if err := x.deleteValue(n, detach); err != nil {
				return err
			}
		}
	default:
		return malformed("Type mismatch: expected Node, Relationship or Path but was %s", typeName(v))
	}

	return nil
}
