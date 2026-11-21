package gremlin

import (
	"slices"
	"strconv"
	"strings"
)

const (
	kindSackBarrier = "Barrier"
	normSack        = "normSack"
)

func (t *trav) weight() int64 { return max(t.bulk, 1) }

// bulkAware steps consume bulked traversers directly; every other step sees them expanded.
func bulkAware(name string) bool {
	switch name {
	case "barrier", "count", "fold", "dedup", "groupCount":
		return true
	}

	return false
}

// expandBulk replaces each bulked traverser with bulk copies.
func expandBulk(in []*trav) ([]*trav, error) {
	var total int64

	bulked := false

	for _, t := range in {
		total += t.weight()
		bulked = bulked || t.bulk > 1
	}

	if !bulked {
		return in, nil
	}

	if total > maxTraversers {
		return nil, constraint("Traverser limit of %d exceeded", maxTraversers)
	}

	out := make([]*trav, 0, total)

	for _, t := range in {
		c := *t
		c.bulk = 0

		for range t.weight() {
			cp := c
			out = append(out, &cp)
		}
	}

	return out, nil
}

func totalBulk(in []*trav) int64 {
	var n int64

	for _, t := range in {
		n += t.weight()
	}

	return n
}

func stepBarrier(x *exec, s *Step, in []*trav) ([]*trav, error) {
	norm, size, err := barrierArgs(s)
	if err != nil {
		return nil, err
	}

	if norm && !x.sack.on {
		return nil, malformed("The provided traverser does not support a sack; use withSack() to enable one")
	}

	var out []*trav

	for len(in) > 0 {
		n := len(in)
		if size > 0 {
			n = min(n, size)
		}

		merged, merr := x.mergeTravs(in[:n])
		if merr != nil {
			return nil, merr
		}

		out = append(out, merged...)
		in = in[n:]
	}

	if norm {
		return normalizeSacks(out)
	}

	return out, nil
}

func barrierArgs(s *Step) (bool, int, error) {
	if len(s.Args) == 0 {
		return false, 0, nil
	}

	if len(s.Args) > 1 {
		return false, 0, invalidArg("barrier() takes at most one argument")
	}

	if tok, ok := s.Args[0].(Token); ok && tok.Kind == kindSackBarrier && tok.Name == normSack {
		return true, 0, nil
	}

	if isInt(s.Args[0]) && toInt(s.Args[0]) > 0 {
		return false, int(min(toInt(s.Args[0]), maxTraversers)), nil
	}

	return false, 0, invalidArg("barrier() takes normSack or a positive integer")
}

func (x *exec) mergeTravs(in []*trav) ([]*trav, error) {
	idx := map[string]int{}
	out := make([]*trav, 0, len(in))

	for _, t := range in {
		k := x.mergeKey(t)

		i, seen := idx[k]
		if !seen {
			c := *t
			c.bulk = t.weight()
			idx[k] = len(out)
			out = append(out, &c)

			continue
		}

		m := out[i]
		m.bulk += t.weight()

		if x.sack.on && x.sack.merge != "" {
			ns, err := applyOperator(x.sack.merge, m.sack, t.sack)
			if err != nil {
				return nil, err
			}

			m.sack = ns
		}
	}

	return out, nil
}

func (x *exec) mergeKey(t *trav) string {
	var b strings.Builder

	b.WriteString(lenKey(keyOf(t.v)))
	b.WriteString(lenKey(strconv.Itoa(t.loops)))

	if x.sack.on && x.sack.merge == "" {
		b.WriteString(lenKey(keyOf(t.sack)))
	}

	if x.needsPath {
		for n := t.path; n != nil; n = n.parent {
			b.WriteString(lenKey(keyOf(n.obj)))

			for _, l := range n.labels {
				b.WriteString(lenKey(l))
			}

			b.WriteString("|")
		}
	}

	return b.String()
}

func normalizeSacks(in []*trav) ([]*trav, error) {
	var sum float64

	for _, t := range in {
		if !isNumber(t.sack) {
			return nil, malformed("barrier(normSack) requires numeric sacks")
		}

		sum += toFloat(t.sack) * float64(t.weight())
	}

	out := make([]*trav, len(in))

	for i, t := range in {
		c := *t
		c.sack = toFloat(t.sack) / sum
		out[i] = &c
	}

	return out, nil
}

func pathSensitive(name string) bool {
	switch name {
	case "as", "path", "select", "simplePath", "cyclicPath", "tree", "subgraph", "match", "from", "to":
		return true
	}

	return false
}

func traversalNeedsPath(tr *Traversal) bool {
	for i := range tr.Steps {
		if stepNeedsPath(&tr.Steps[i]) {
			return true
		}
	}

	return false
}

func stepNeedsPath(s *Step) bool {
	if pathSensitive(s.Name) {
		return true
	}

	if slices.ContainsFunc(s.Args, argNeedsPath) {
		return true
	}

	for i := range s.Mods {
		if stepNeedsPath(&s.Mods[i]) {
			return true
		}
	}

	return false
}

func argNeedsPath(a any) bool {
	switch t := a.(type) {
	case *Traversal:
		return traversalNeedsPath(t)
	case []any:
		if slices.ContainsFunc(t, argNeedsPath) {
			return true
		}
	}

	return false
}
