package gremlin

func byMods(s *Step) []*Step {
	var out []*Step

	for i := range s.Mods {
		if s.Mods[i].Name == "by" {
			out = append(out, &s.Mods[i])
		}
	}

	return out
}

func modsNamed(s *Step, name string) []*Step {
	var out []*Step

	for i := range s.Mods {
		if s.Mods[i].Name == name {
			out = append(out, &s.Mods[i])
		}
	}

	return out
}

// byValue evaluates a by() modulator against a traverser; ok is false when it yields nothing.
func (x *exec) byValue(by *Step, t *trav) (any, bool, error) {
	if len(by.Args) == 0 {
		return t.v, true, nil
	}

	switch a := by.Args[0].(type) {
	case string:
		return mapOrProp(t.v, a)
	case Token:
		return x.byToken(a, t)
	case *Traversal:
		return x.firstValue(a, t)
	}

	return t.v, true, nil
}

func (x *exec) byToken(a Token, t *trav) (any, bool, error) {
	switch a.Kind {
	case kindT:
		v, ok := tokenValue(a, t.v)

		return v, ok, nil
	case kindOrder:
		return t.v, true, nil
	}

	return t.v, true, nil
}

func mapOrProp(v any, key string) (any, bool, error) {
	if m, ok := v.(*Map); ok {
		got, found := m.Get(key)
		if l, isList := got.([]any); isList && len(l) == 1 {
			got = l[0]
		}

		return got, found, nil
	}

	vals := propValues(v, key)
	if len(vals) == 0 {
		return nil, false, nil
	}

	return vals[0], true, nil
}

// orderOf returns the Order token on a by() modulator, defaulting to asc.
func orderOf(by *Step) string {
	for _, a := range by.Args {
		if t, ok := a.(Token); ok && t.Kind == kindOrder {
			return t.Name
		}
	}

	return "asc"
}

// byRound cycles through the by() modulators, one per position.
func byRound(by []*Step, i int) *Step {
	if len(by) == 0 {
		return nil
	}

	return by[i%len(by)]
}

func (x *exec) applyBy(by []*Step, i int, t *trav) (any, bool, error) {
	b := byRound(by, i)
	if b == nil {
		return t.v, true, nil
	}

	return x.byValue(b, t)
}
