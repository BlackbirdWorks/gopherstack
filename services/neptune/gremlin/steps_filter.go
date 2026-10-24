package gremlin

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func regFilter(m map[string]stepFn) {
	m["has"] = filterStep(stepHas)
	m["hasNot"] = filterStep(func(_ *exec, s *Step, t *trav) (bool, error) {
		keys := stringArgs(s.Args)

		return len(keys) == 1 && len(propValues(t.v, keys[0])) == 0, nil
	})
	m["hasLabel"] = filterStep(stepHasLabel)
	m["hasId"] = filterStep(stepHasID)
	m["hasKey"] = filterStep(propPartStep(func(p *Prop) any { return p.Key }))
	m["hasValue"] = filterStep(propPartStep(func(p *Prop) any { return p.Val }))
	m["is"] = filterStep(stepIs)
	m["where"] = filterStep(stepWhere)
	m["not"] = filterStep(stepNot)
	m[opAnd] = filterStep(stepAnd)
	m[opOr] = filterStep(stepOr)
	m["filter"] = filterStep(stepFilter)
	m["simplePath"] = filterStep(func(_ *exec, _ *Step, t *trav) (bool, error) { return isSimple(t), nil })
	m["cyclicPath"] = filterStep(func(_ *exec, _ *Step, t *trav) (bool, error) { return !isSimple(t), nil })
	m["limit"] = stepLimit
	m["skip"] = stepSkip
	m["range"] = stepRange
	m["tail"] = stepTail
	m["dedup"] = stepDedup
	m["none"] = func(_ *exec, _ *Step, _ []*trav) ([]*trav, error) { return nil, nil }
	m["discard"] = m["none"]
	m["coin"] = filterStep(func(_ *exec, _ *Step, _ *trav) (bool, error) { return true, nil })
}

func isSimple(t *trav) bool {
	seen := map[string]bool{}

	for n := t.path; n != nil; n = n.parent {
		k := keyOf(n.obj)
		if seen[k] {
			return false
		}

		seen[k] = true
	}

	return true
}

func stepHas(x *exec, s *Step, t *trav) (bool, error) {
	switch len(s.Args) {
	case 1:
		key, ok := s.Args[0].(string)

		return ok && len(propValues(t.v, key)) > 0, nil
	case 2: //nolint:mnd // key and value
		return hasKeyValue(x, t, s.Args[0], s.Args[1])
	case 3: //nolint:mnd // label, key and value
		label, ok := s.Args[0].(string)
		if !ok || !labelMatches(t.v, label) {
			return false, nil
		}

		return hasKeyValue(x, t, s.Args[1], s.Args[2])
	}

	return false, invalidArg("has() takes one to three arguments")
}

func hasKeyValue(x *exec, t *trav, key, want any) (bool, error) {
	if tr, ok := want.(*Traversal); ok {
		vals, err := x.sub(tr, t)
		if err != nil || len(vals) == 0 {
			return false, err
		}

		want = vals[0].v
	}

	switch k := key.(type) {
	case string:
		return matchAny(propValues(t.v, k), want), nil
	case Token:
		return hasToken(k, t.v, want), nil
	}

	return false, invalidArg("has() key must be a string or T token")
}

func hasToken(k Token, v, want any) bool {
	if k.Name == tokLabel {
		return matchLabel(v, want)
	}

	if k.Name == tokID {
		return matchID(v, want)
	}

	actual, ok := tokenValue(k, v)

	return ok && matchOne(actual, want)
}

func matchLabel(v, want any) bool {
	switch w := want.(type) {
	case Pred:
		return w.test(elemLabel(v)) || anyLabel(v, w)
	case string:
		return labelMatches(v, w)
	}

	return false
}

func anyLabel(v any, p Pred) bool {
	if n, ok := v.(*opencypher.Node); ok {
		return slices.ContainsFunc(n.Labels, func(l string) bool { return p.test(l) })
	}

	return false
}

func matchID(v, want any) bool {
	id := elementID(v)
	if id == "" {
		return false
	}

	if p, ok := want.(Pred); ok {
		return p.test(id)
	}

	return id == idString(want)
}

func stepHasLabel(_ *exec, s *Step, t *trav) (bool, error) {
	for _, a := range flattenArgs(s.Args) {
		if matchLabel(t.v, a) {
			return true, nil
		}
	}

	return false, nil
}

func stepHasID(_ *exec, s *Step, t *trav) (bool, error) {
	for _, a := range flattenArgs(s.Args) {
		if matchID(t.v, a) {
			return true, nil
		}
	}

	return false, nil
}

func propPartStep(part func(*Prop) any) func(*exec, *Step, *trav) (bool, error) {
	return func(_ *exec, s *Step, t *trav) (bool, error) {
		p, ok := t.v.(*Prop)
		if !ok {
			return false, nil
		}

		for _, a := range flattenArgs(s.Args) {
			if matchOne(part(p), a) {
				return true, nil
			}
		}

		return false, nil
	}
}

func stepIs(_ *exec, s *Step, t *trav) (bool, error) {
	if len(s.Args) != 1 {
		return false, invalidArg("is() takes one argument")
	}

	return matchOne(t.v, s.Args[0]), nil
}

func stepNot(x *exec, s *Step, t *trav) (bool, error) {
	tr, err := argTraversal(s, 0)
	if err != nil {
		return false, err
	}

	ok, err := x.exists(tr, t)

	return !ok, err
}

func stepFilter(x *exec, s *Step, t *trav) (bool, error) {
	if len(s.Args) == 1 {
		if p, ok := s.Args[0].(Pred); ok {
			return p.test(t.v), nil
		}
	}

	tr, err := argTraversal(s, 0)
	if err != nil {
		return false, err
	}

	return x.exists(tr, t)
}

func stepAnd(x *exec, s *Step, t *trav) (bool, error) {
	for i := range s.Args {
		tr, err := argTraversal(s, i)
		if err != nil {
			return false, err
		}

		ok, err := x.exists(tr, t)
		if err != nil || !ok {
			return false, err
		}
	}

	return true, nil
}

func stepOr(x *exec, s *Step, t *trav) (bool, error) {
	for i := range s.Args {
		tr, err := argTraversal(s, i)
		if err != nil {
			return false, err
		}

		ok, err := x.exists(tr, t)
		if err != nil {
			return false, err
		}

		if ok {
			return true, nil
		}
	}

	return false, nil
}

func stepWhere(x *exec, s *Step, t *trav) (bool, error) {
	switch len(s.Args) {
	case 1:
		switch a := s.Args[0].(type) {
		case *Traversal:
			return x.exists(a, t)
		case Pred:
			return x.wherePred(s, t, t.v, a)
		}
	case 2: //nolint:mnd // label and predicate
		label, _ := s.Args[0].(string)
		p, ok := s.Args[1].(Pred)

		left, found := t.lookup(label)
		if !ok || !found {
			return false, nil
		}

		return x.wherePred(s, t, left, p)
	}

	return false, invalidArg("where() takes a traversal or predicate")
}

func (x *exec) wherePred(s *Step, t *trav, left any, p Pred) (bool, error) {
	by := byMods(s)
	rp, ok, err := x.resolveLabels(p, t, by)
	if err != nil || !ok {
		return false, err
	}

	if len(by) > 0 {
		left, ok, err = x.byValue(by[0], t.keep(left))
		if err != nil || !ok {
			return false, err
		}
	}

	return rp.test(left), nil
}

// resolveLabels replaces string arguments with the path objects those step labels name.
func (x *exec) resolveLabels(p Pred, t *trav, by []*Step) (Pred, bool, error) {
	out := Pred{Op: p.Op}

	for _, c := range p.Children {
		rc, ok, err := x.resolveLabels(c, t, by)
		if err != nil || !ok {
			return out, false, err
		}

		out.Children = append(out.Children, rc)
	}

	for _, a := range flattenArgs(p.Args) {
		label, isStr := a.(string)
		if !isStr {
			out.Args = append(out.Args, a)

			continue
		}

		obj, found := t.lookup(label)
		if !found {
			return out, false, nil
		}

		if len(by) > 1 {
			v, ok, err := x.byValue(by[1], t.keep(obj))
			if err != nil || !ok {
				return out, false, err
			}

			obj = v
		}

		out.Args = append(out.Args, obj)
	}

	return out, true, nil
}

func stepLimit(_ *exec, s *Step, in []*trav) ([]*trav, error) {
	if scope(s) == nameLocal {
		return localSlice(s, in, func(l []any, n, _ int) []any { return l[:min(n, len(l))] })
	}

	n, err := argInt(s, len(s.Args)-1)
	if err != nil {
		return nil, err
	}

	return in[:max(0, min(n, len(in)))], nil
}

func stepSkip(_ *exec, s *Step, in []*trav) ([]*trav, error) {
	n, err := argInt(s, len(s.Args)-1)
	if err != nil {
		return nil, err
	}

	return in[min(max(n, 0), len(in)):], nil
}

func stepRange(_ *exec, s *Step, in []*trav) ([]*trav, error) {
	lo, err := argInt(s, len(s.Args)-2) //nolint:mnd // low is second to last
	if err != nil {
		return nil, err
	}

	hi, err := argInt(s, len(s.Args)-1)
	if err != nil {
		return nil, err
	}

	lo = min(max(lo, 0), len(in))
	if hi < 0 {
		hi = len(in)
	}

	return in[lo:min(max(hi, lo), len(in))], nil
}

func stepTail(_ *exec, s *Step, in []*trav) ([]*trav, error) {
	n := 1
	if len(s.Args) > 0 {
		var err error
		if n, err = argInt(s, len(s.Args)-1); err != nil {
			return nil, err
		}
	}

	return in[max(0, len(in)-max(n, 0)):], nil
}

func scope(s *Step) string {
	for _, a := range s.Args {
		if t, ok := a.(Token); ok && t.Kind == kindScope {
			return t.Name
		}
	}

	return "global"
}

func localSlice(s *Step, in []*trav, f func(l []any, n, hi int) []any) ([]*trav, error) {
	n, err := argInt(s, len(s.Args)-1)
	if err != nil {
		return nil, err
	}

	out := make([]*trav, 0, len(in))

	for _, t := range in {
		if l, ok := t.v.([]any); ok {
			out = append(out, t.next(f(l, n, 0)))
		} else {
			out = append(out, t)
		}
	}

	return out, nil
}

func stepDedup(x *exec, s *Step, in []*trav) ([]*trav, error) {
	by := byMods(s)
	seen := map[string]bool{}
	out := make([]*trav, 0, len(in))

	for _, t := range in {
		v := t.v

		if len(by) > 0 {
			bv, ok, err := x.byValue(by[0], t)
			if err != nil {
				return nil, err
			}

			if !ok {
				continue
			}

			v = bv
		}

		k := keyOf(v)
		if !seen[k] {
			seen[k] = true

			out = append(out, t)
		}
	}

	return out, nil
}
