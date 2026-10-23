package gremlin

import (
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func regProject(m map[string]stepFn) {
	m["values"] = flatStep(stepValues)
	m["properties"] = flatStep(stepProperties)
	m["valueMap"] = mapStep(stepValueMap)
	m["elementMap"] = mapStep(stepElementMap)
	m[tokID] = mapStep(func(_ *exec, s *Step, t *trav) (any, error) { return idOf(s, t.v) })
	m[tokLabel] = mapStep(stepLabel)
	m[tokKey] = mapStep(propPart(tokKey, func(p *Prop) any { return p.Key }))
	m[tokValue] = mapStep(propPart(tokValue, func(p *Prop) any { return p.Val }))
	m["constant"] = mapStep(func(_ *exec, s *Step, _ *trav) (any, error) {
		if len(s.Args) != 1 {
			return nil, invalidArg("constant() takes one argument")
		}

		return normalizeValue(s.Args[0]), nil
	})
	m["identity"] = func(_ *exec, _ *Step, in []*trav) ([]*trav, error) { return in, nil }
	m["barrier"] = m["identity"]
	m["as"] = perTrav(func(_ *exec, s *Step, t *trav) ([]*trav, error) {
		return []*trav{t.labeled(stringArgs(s.Args))}, nil
	})
	m["select"] = perTrav(stepSelect)
	m["path"] = mapStep(stepPath)
	m["project"] = mapStep(stepProject)
	m["unfold"] = flatStep(stepUnfold)
	m["loops"] = mapStep(func(_ *exec, _ *Step, t *trav) (any, error) { return int64(t.loops), nil })
	m["map"] = perTrav(subStep(true))
	m["flatMap"] = perTrav(subStep(false))
}

func idOf(s *Step, v any) (any, error) {
	if id := elementIDOrValue(v); id != nil {
		return id, nil
	}

	return nil, malformed("The %s() step can only be applied to graph elements", s.Name)
}

func stepLabel(_ *exec, s *Step, t *trav) (any, error) {
	if _, ok := elemProps(t.v); ok {
		return elemLabel(t.v), nil
	}

	if p, ok := t.v.(*Prop); ok {
		return p.Key, nil
	}

	return nil, malformed("The %s() step can only be applied to graph elements", s.Name)
}

func propPart(name string, part func(*Prop) any) func(*exec, *Step, *trav) (any, error) {
	return func(_ *exec, _ *Step, t *trav) (any, error) {
		p, ok := t.v.(*Prop)
		if !ok {
			return nil, malformed("The %s() step can only be applied to properties", name)
		}

		return part(p), nil
	}
}

func subStep(single bool) func(*exec, *Step, *trav) ([]*trav, error) {
	return func(x *exec, s *Step, t *trav) ([]*trav, error) {
		tr, err := argTraversal(s, 0)
		if err != nil {
			return nil, err
		}

		out, err := x.sub(tr, t)
		if err != nil {
			return nil, err
		}

		if single && len(out) > 1 {
			out = out[:1]
		}

		res := make([]*trav, len(out))
		for i, o := range out {
			res[i] = t.next(o.v)
		}

		return res, nil
	}
}

func keyFilter(keys []string) func(string) bool {
	if len(keys) == 0 {
		return func(string) bool { return true }
	}

	set := make(map[string]bool, len(keys))
	for _, k := range keys {
		set[k] = true
	}

	return func(k string) bool { return set[k] }
}

func stepValues(_ *exec, s *Step, t *trav) ([]any, error) {
	if p, ok := t.v.(*Prop); ok {
		return nil, malformed("The values() step cannot be applied to property %q", p.Key)
	}

	props, ok := elemProps(t.v)
	if !ok {
		return nil, malformed("The values() step can only be applied to graph elements")
	}

	keep := keyFilter(stringArgs(s.Args))

	var out []any

	for _, k := range sortedKeys(props) {
		if keep(k) {
			out = append(out, propValues(t.v, k)...)
		}
	}

	return out, nil
}

func stepProperties(_ *exec, s *Step, t *trav) ([]any, error) {
	props, ok := elemProps(t.v)
	if !ok {
		return nil, malformed("The properties() step can only be applied to graph elements")
	}

	_, isVertex := t.v.(*opencypher.Node)
	keep := keyFilter(stringArgs(s.Args))

	var out []any

	for _, k := range sortedKeys(props) {
		if !keep(k) {
			continue
		}

		for _, v := range propValues(t.v, k) {
			out = append(out, &Prop{Owner: t.v, Key: k, Val: v, VP: isVertex})
		}
	}

	return out, nil
}

func wantsTokens(s *Step) bool {
	for _, a := range s.Args {
		if b, ok := a.(bool); ok && b {
			return true
		}
	}

	for _, w := range modsNamed(s, "with") {
		for _, a := range w.Args {
			if t, ok := a.(Token); ok && t.Kind == kindWithOptions && (t.Name == "tokens" || t.Name == "ids") {
				return true
			}
		}
	}

	return false
}

func stepValueMap(_ *exec, s *Step, t *trav) (any, error) {
	props, ok := elemProps(t.v)
	if !ok {
		return nil, malformed("The valueMap() step can only be applied to graph elements")
	}

	out := NewMap()

	if wantsTokens(s) {
		out.Put(Token{Kind: kindT, Name: tokID}, elementID(t.v))
		out.Put(Token{Kind: kindT, Name: tokLabel}, elemLabel(t.v))
	}

	keep := keyFilter(stringArgs(s.Args))
	_, isVertex := t.v.(*opencypher.Node)

	for _, k := range sortedKeys(props) {
		if !keep(k) {
			continue
		}

		vals := propValues(t.v, k)
		if isVertex {
			out.Put(k, vals)
		} else if len(vals) > 0 {
			out.Put(k, vals[0])
		}
	}

	return out, nil
}

func stepElementMap(x *exec, s *Step, t *trav) (any, error) {
	props, ok := elemProps(t.v)
	if !ok {
		return nil, malformed("The elementMap() step can only be applied to graph elements")
	}

	out := NewMap()
	out.Put(Token{Kind: kindT, Name: tokID}, elementID(t.v))
	out.Put(Token{Kind: kindT, Name: tokLabel}, elemLabel(t.v))

	if r, isRel := t.v.(*opencypher.Rel); isRel {
		out.Put(Token{Kind: kindDirection, Name: dirIn}, endpointMap(x, r.End))
		out.Put(Token{Kind: kindDirection, Name: dirOut}, endpointMap(x, r.Start))
	}

	keep := keyFilter(stringArgs(s.Args))

	for _, k := range sortedKeys(props) {
		if vals := propValues(t.v, k); keep(k) && len(vals) > 0 {
			out.Put(k, vals[len(vals)-1])
		}
	}

	return out, nil
}

func endpointMap(x *exec, id string) *Map {
	m := NewMap()
	m.Put(Token{Kind: kindT, Name: tokID}, id)

	if n := x.st.Node(id); n != nil {
		m.Put(Token{Kind: kindT, Name: tokLabel}, vertexLabel(n))
	}

	return m
}

func stepSelect(x *exec, s *Step, t *trav) ([]*trav, error) {
	if len(s.Args) == 1 {
		if c, ok := s.Args[0].(Token); ok && c.Kind == kindColumn {
			return selectColumn(c, t)
		}
	}

	pop := "last"
	args := s.Args

	if len(args) > 0 {
		if p, ok := args[0].(Token); ok && p.Kind == kindPop {
			pop, args = p.Name, args[1:]
		}
	}

	labels := stringArgs(args)
	if len(labels) == 0 {
		return nil, invalidArg("select() requires at least one label")
	}

	by := byMods(s)
	vals := make([]any, len(labels))

	for i, l := range labels {
		v, ok, err := x.selectLabel(t, l, pop, by, i)
		if err != nil || !ok {
			return nil, err
		}

		vals[i] = v
	}

	if len(labels) == 1 {
		return []*trav{t.next(vals[0])}, nil
	}

	out := NewMap()
	for i, l := range labels {
		out.Put(l, vals[i])
	}

	return []*trav{t.next(out)}, nil
}

func (x *exec) selectLabel(t *trav, label, pop string, by []*Step, i int) (any, bool, error) {
	v, ok := popLabel(t, label, pop)
	if !ok {
		return nil, false, nil
	}

	return x.applyBy(by, i, t.keep(v))
}

func popLabel(t *trav, label, pop string) (any, bool) {
	switch pop {
	case "all":
		all := t.lookupAll(label)

		return all, len(all) > 0
	case "first":
		all := t.lookupAll(label)
		if len(all) == 0 {
			return nil, false
		}

		return all[0], true
	}

	return t.lookup(label)
}

func selectColumn(c Token, t *trav) ([]*trav, error) {
	m, ok := t.v.(*Map)
	if !ok {
		return nil, malformed("select(%s) can only be applied to maps", c.Name)
	}

	out := make([]any, 0, m.Len())

	for i := range m.Len() {
		k, v := m.Entry(i)
		if c.Name == "keys" {
			out = append(out, k)
		} else {
			out = append(out, v)
		}
	}

	return []*trav{t.next(out)}, nil
}

func stepPath(x *exec, s *Step, t *trav) (any, error) {
	p := t.toPath()
	by := byMods(s)

	if len(by) == 0 {
		return p, nil
	}

	for i, o := range p.Objects {
		v, ok, err := x.applyBy(by, i, t.keep(o))
		if err != nil {
			return nil, err
		}

		if ok {
			p.Objects[i] = v
		}
	}

	return p, nil
}

func stepProject(x *exec, s *Step, t *trav) (any, error) {
	keys := stringArgs(s.Args)
	by := byMods(s)
	out := NewMap()

	for i, k := range keys {
		v, ok, err := x.applyBy(by, i, t)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, malformed("The provided traverser does not map to a value: %s", k)
		}

		out.Put(k, v)
	}

	return out, nil
}

func stepUnfold(_ *exec, _ *Step, t *trav) ([]any, error) {
	switch v := t.v.(type) {
	case []any:
		return v, nil
	case Set:
		return v, nil
	case *Map:
		out := make([]any, v.Len())

		for i := range out {
			k, val := v.Entry(i)
			e := NewMap()
			e.Put(k, val)
			out[i] = e
		}

		return out, nil
	}

	return []any{t.v}, nil
}
