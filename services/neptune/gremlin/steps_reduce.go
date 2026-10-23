package gremlin

import (
	"slices"
	"sort"
)

func regReduce(m map[string]stepFn) {
	m["count"] = stepCount
	m["fold"] = func(_ *exec, _ *Step, in []*trav) ([]*trav, error) {
		return []*trav{rootTrav()[0].next(values(in))}, nil
	}
	m[stepSum] = reduceNumbers(sumValues)
	m["mean"] = reduceNumbers(meanValues)
	m["min"] = reduceOrdered(-1)
	m["max"] = reduceOrdered(1)
	m["order"] = stepOrder
	m["group"] = stepGroup
	m["groupCount"] = stepGroupCount
	m["aggregate"] = stepAggregate
	m["store"] = stepAggregate
	m["cap"] = stepCap
	m["sideEffect"] = perTrav(func(x *exec, s *Step, t *trav) ([]*trav, error) {
		tr, err := argTraversal(s, 0)
		if err != nil {
			return nil, err
		}

		_, err = x.sub(tr, t)

		return []*trav{t}, err
	})
}

func stepCount(_ *exec, s *Step, in []*trav) ([]*trav, error) {
	if scope(s) == nameLocal {
		return mapLocal(in, func(v any) (any, bool) { return int64(collectionLen(v)), true }), nil
	}

	return []*trav{rootTrav()[0].next(int64(len(in)))}, nil
}

func collectionLen(v any) int {
	switch t := v.(type) {
	case []any:
		return len(t)
	case Set:
		return len(t)
	case *Map:
		return t.Len()
	}

	return 1
}

func mapLocal(in []*trav, f func(any) (any, bool)) []*trav {
	out := make([]*trav, 0, len(in))

	for _, t := range in {
		if v, ok := f(t.v); ok {
			out = append(out, t.next(v))
		}
	}

	return out
}

// numbersOf returns the numeric values, erroring on anything else.
func numbersOf(vs []any) ([]any, error) {
	for _, v := range vs {
		if !isNumber(v) {
			return nil, malformed("Can't use a non-numeric value in a numeric reduction")
		}
	}

	return vs, nil
}

func reduceNumbers(f func([]any) any) stepFn {
	return func(_ *exec, s *Step, in []*trav) ([]*trav, error) {
		if scope(s) == nameLocal {
			return mapLocal(in, func(v any) (any, bool) {
				l, _ := v.([]any)
				if _, err := numbersOf(l); err != nil || len(l) == 0 {
					return nil, false
				}

				return f(l), true
			}), nil
		}

		vs, err := numbersOf(values(in))
		if err != nil {
			return nil, err
		}

		if len(vs) == 0 {
			if s.Name == stepSum {
				return []*trav{rootTrav()[0].next(int64(0))}, nil
			}

			return nil, nil
		}

		return []*trav{rootTrav()[0].next(f(vs))}, nil
	}
}

func sumValues(vs []any) any {
	allInt := true

	var si int64

	var sf float64

	for _, v := range vs {
		allInt = allInt && isInt(v)
		si += toInt(v)
		sf += toFloat(v)
	}

	if allInt {
		return si
	}

	return sf
}

func meanValues(vs []any) any {
	var sf float64
	for _, v := range vs {
		sf += toFloat(v)
	}

	return sf / float64(len(vs))
}

func reduceOrdered(want int) stepFn {
	return func(_ *exec, s *Step, in []*trav) ([]*trav, error) {
		if scope(s) == nameLocal {
			return mapLocal(in, func(v any) (any, bool) {
				l, _ := v.([]any)

				return extreme(l, want)
			}), nil
		}

		v, ok := extreme(values(in), want)
		if !ok {
			return nil, nil
		}

		return []*trav{rootTrav()[0].next(v)}, nil
	}
}

func extreme(vs []any, want int) (any, bool) {
	if len(vs) == 0 {
		return nil, false
	}

	best := vs[0]

	for _, v := range vs[1:] {
		if c, ok := compareValues(v, best); ok && c == want {
			best = v
		}
	}

	return best, true
}

func stepOrder(x *exec, s *Step, in []*trav) ([]*trav, error) {
	by := byMods(s)

	if scope(s) == nameLocal {
		return mapLocal(in, func(v any) (any, bool) { return orderLocal(v, by), true }), nil
	}

	type keyed struct {
		t    *trav
		keys []any
	}

	items := make([]keyed, len(in))

	for i, t := range in {
		ks, err := x.sortKeys(by, t)
		if err != nil {
			return nil, err
		}

		items[i] = keyed{t: t, keys: ks}
	}

	sort.SliceStable(items, func(a, b int) bool {
		return compareKeys(by, items[a].keys, items[b].keys) < 0
	})

	out := make([]*trav, len(items))
	for i, it := range items {
		out[i] = it.t
	}

	return out, nil
}

func (x *exec) sortKeys(by []*Step, t *trav) ([]any, error) {
	if len(by) == 0 {
		return []any{t.v}, nil
	}

	keys := make([]any, len(by))

	for i, b := range by {
		v, _, err := x.byValue(b, t)
		if err != nil {
			return nil, err
		}

		keys[i] = v
	}

	return keys, nil
}

func compareKeys(by []*Step, a, b []any) int {
	for i := range a {
		c, ok := compareValues(a[i], b[i])
		if !ok {
			c = compareFallback(a[i], b[i])
		}

		if len(by) > i && orderOf(by[i]) == nameDesc {
			c = -c
		}

		if c != 0 {
			return c
		}
	}

	return 0
}

func compareFallback(a, b any) int {
	ak, bk := keyOf(a), keyOf(b)

	switch {
	case ak < bk:
		return -1
	case ak > bk:
		return 1
	}

	return 0
}

func orderLocal(v any, by []*Step) any {
	desc := len(by) > 0 && orderOf(by[0]) == nameDesc
	sortCol := func(c int) int {
		if desc {
			return -c
		}

		return c
	}

	switch t := v.(type) {
	case []any:
		out := slices.Clone(t)
		sort.SliceStable(out, func(i, j int) bool { return sortCol(compareFallbackOrd(out[i], out[j])) < 0 })

		return out
	case *Map:
		return orderMap(t, by, sortCol)
	}

	return v
}

func compareFallbackOrd(a, b any) int {
	if c, ok := compareValues(a, b); ok {
		return c
	}

	return compareFallback(a, b)
}

func orderMap(m *Map, by []*Step, sortCol func(int) int) *Map {
	byValues := false

	for _, b := range by {
		for _, a := range b.Args {
			if t, ok := a.(Token); ok && t.Kind == kindColumn && t.Name == "values" {
				byValues = true
			}
		}
	}

	idx := make([]int, m.Len())
	for i := range idx {
		idx[i] = i
	}

	sort.SliceStable(idx, func(a, b int) bool {
		ka, va := m.Entry(idx[a])
		kb, vb := m.Entry(idx[b])

		if byValues {
			return sortCol(compareFallbackOrd(va, vb)) < 0
		}

		return sortCol(compareFallbackOrd(ka, kb)) < 0
	})

	out := NewMap()

	for _, i := range idx {
		k, v := m.Entry(i)
		out.Put(k, v)
	}

	return out
}

func stepGroup(x *exec, s *Step, in []*trav) ([]*trav, error) {
	by := byMods(s)
	groups := NewMap()
	members := map[string][]*trav{}

	var order []any

	for _, t := range in {
		k, ok, err := x.applyBy(by[:min(1, len(by))], 0, t)
		if err != nil {
			return nil, err
		}

		if !ok {
			continue
		}

		ks := keyOf(k)
		if _, seen := members[ks]; !seen {
			order = append(order, k)
		}

		members[ks] = append(members[ks], t)
	}

	for _, k := range order {
		v, err := x.groupValue(by, members[keyOf(k)])
		if err != nil {
			return nil, err
		}

		groups.Put(k, v)
	}

	return []*trav{rootTrav()[0].next(groups)}, nil
}

func isReducing(name string) bool {
	switch name {
	case "count", stepSum, "min", "max", "mean", "fold", "groupCount", "group":
		return true
	}

	return false
}

func (x *exec) groupValue(by []*Step, ts []*trav) (any, error) {
	if len(by) < 2 { //nolint:mnd // key and value selectors
		return values(ts), nil
	}

	vsel := by[1]
	if len(vsel.Args) == 0 {
		return values(ts), nil
	}

	switch a := vsel.Args[0].(type) {
	case *Traversal:
		out, err := x.run(a, ts)
		if err != nil {
			return nil, err
		}

		if n := len(a.Steps); n > 0 && isReducing(a.Steps[n-1].Name) && len(out) == 1 {
			return out[0].v, nil
		}

		return values(out), nil
	default:
		res := make([]any, 0, len(ts))

		for _, t := range ts {
			v, ok, err := x.byValue(vsel, t)
			if err != nil {
				return nil, err
			}

			if ok {
				res = append(res, v)
			}
		}

		return res, nil
	}
}

func stepGroupCount(x *exec, s *Step, in []*trav) ([]*trav, error) {
	by := byMods(s)
	counts := NewMap()
	keys := map[string]any{}
	n := map[string]int64{}

	var order []string

	for _, t := range in {
		k, ok, err := x.applyBy(by, 0, t)
		if err != nil {
			return nil, err
		}

		if !ok {
			continue
		}

		ks := keyOf(k)
		if _, seen := n[ks]; !seen {
			order = append(order, ks)
			keys[ks] = k
		}

		n[ks]++
	}

	for _, ks := range order {
		counts.Put(keys[ks], n[ks])
	}

	return []*trav{rootTrav()[0].next(counts)}, nil
}

func stepAggregate(x *exec, s *Step, in []*trav) ([]*trav, error) {
	labels := stringArgs(s.Args)
	if len(labels) != 1 {
		return nil, invalidArg("%s() takes a side-effect key", s.Name)
	}

	existing, _ := x.side[labels[0]].([]any)
	x.side[labels[0]] = append(existing, values(in)...)

	return in, nil
}

func stepCap(x *exec, s *Step, _ []*trav) ([]*trav, error) {
	labels := stringArgs(s.Args)
	if len(labels) != 1 {
		return nil, invalidArg("cap() takes a side-effect key")
	}

	v, ok := x.side[labels[0]]
	if !ok {
		v = []any{}
	}

	return []*trav{rootTrav()[0].next(v)}, nil
}
