package gremlin

import (
	"math"
	"slices"
)

func regSample(m map[string]stepFn) {
	m["sample"] = stepSample
	m["coin"] = filterStep(stepCoin)
}

func stepCoin(x *exec, s *Step, _ *trav) (bool, error) {
	if len(s.Args) != 1 || !isNumber(s.Args[0]) {
		return false, invalidArg("coin() takes a probability")
	}

	return x.rnd.Float64() < toFloat(s.Args[0]), nil
}

func stepSample(x *exec, s *Step, in []*trav) ([]*trav, error) {
	if len(s.Args) == 0 {
		return nil, invalidArg("sample() takes a size")
	}

	n, err := argInt(s, len(s.Args)-1)
	if err != nil {
		return nil, err
	}

	if n < 0 {
		return nil, invalidArg("sample() size must not be negative")
	}

	if scope(s) == nameLocal {
		return mapLocal(in, func(v any) (any, bool) { return x.sampleLocal(v, n), true }), nil
	}

	by := byMods(s)
	if len(by) > 1 {
		return nil, invalidArg("sample() accepts at most one by() modulator")
	}

	if len(in) <= n {
		return in, nil
	}

	weights := make([]float64, len(in))

	for i, t := range in {
		w, werr := x.sampleWeight(by, t)
		if werr != nil {
			return nil, werr
		}

		weights[i] = w
	}

	picked := x.weightedPick(weights, n)
	out := make([]*trav, len(picked))

	for i, idx := range picked {
		out[i] = in[idx]
	}

	return out, nil
}

func (x *exec) sampleWeight(by []*Step, t *trav) (float64, error) {
	if len(by) == 0 {
		return 1, nil
	}

	v, ok, err := x.byValue(by[0], t)
	if err != nil {
		return 0, err
	}

	if !ok || !isNumber(v) {
		return 0, malformed("sample() by() must produce a numeric weight")
	}

	return toFloat(v), nil
}

// weightedPick draws n distinct indexes with probability proportional to weight (Efraimidis-Spirakis) and
// returns them in ascending order.
func (x *exec) weightedPick(weights []float64, n int) []int {
	type scored struct {
		key float64
		idx int
	}

	all := make([]scored, len(weights))

	for i, w := range weights {
		key := math.Inf(-1)
		if w > 0 {
			key = math.Log(1-x.rnd.Float64()) / w
		}

		all[i] = scored{key: key, idx: i}
	}

	slices.SortStableFunc(all, func(a, b scored) int {
		switch {
		case a.key > b.key:
			return -1
		case a.key < b.key:
			return 1
		}

		return 0
	})

	idx := make([]int, 0, n)
	for _, sc := range all[:n] {
		idx = append(idx, sc.idx)
	}

	slices.Sort(idx)

	return idx
}

func (x *exec) sampleLocal(v any, n int) any {
	switch t := v.(type) {
	case []any:
		return pickIndexes(t, x.uniformPick(len(t), n))
	case Set:
		return Set(pickIndexes(t, x.uniformPick(len(t), n)))
	case *Map:
		out := NewMap()

		for _, i := range x.uniformPick(t.Len(), n) {
			k, val := t.Entry(i)
			out.Put(k, val)
		}

		return out
	}

	return v
}

func pickIndexes(l []any, idx []int) []any {
	out := make([]any, len(idx))
	for i, j := range idx {
		out[i] = l[j]
	}

	return out
}

func (x *exec) uniformPick(size, n int) []int {
	if size <= n {
		idx := make([]int, size)
		for i := range idx {
			idx[i] = i
		}

		return idx
	}

	idx := x.rnd.Perm(size)[:n]
	slices.Sort(idx)

	return idx
}
