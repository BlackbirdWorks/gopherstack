package sparql

func resolveNode(n node, b Binding) Term {
	if n.isVar() {
		return b[n.Var]
	}

	return n.Term
}

func (ev *evaluator) evalBGP(tps []triplePat, input []Binding, gc Term) ([]Binding, error) {
	var out []Binding

	for _, in := range input {
		if err := ev.matchPats(tps, in, gc, &out); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (ev *evaluator) matchPats(pending []triplePat, b Binding, gc Term, out *[]Binding) error {
	if len(pending) == 0 {
		*out = append(*out, b)

		return nil
	}

	i := pickPattern(pending, b)
	rest := make([]triplePat, 0, len(pending)-1)
	rest = append(rest, pending[:i]...)
	rest = append(rest, pending[i+1:]...)

	return ev.matchOne(pending[i], b, gc, func(nb Binding) error { return ev.matchPats(rest, nb, gc, out) })
}

func pickPattern(pending []triplePat, b Binding) int {
	best, bestScore := 0, -1<<30

	for i, tp := range pending {
		score := 0

		for _, n := range []node{tp.S, tp.P, tp.O} {
			if !n.isVar() || b[n.Var].Bound() {
				score++
			}
		}

		if tp.Path != nil {
			score -= 2
		}

		if score > bestScore {
			best, bestScore = i, score
		}
	}

	return best
}

func (ev *evaluator) matchOne(tp triplePat, b Binding, gc Term, next func(Binding) error) error {
	if tp.Path != nil {
		return ev.matchPath(tp, b, gc, next)
	}

	var ferr error

	ev.scan(gc, resolveNode(tp.S, b), resolveNode(tp.P, b), resolveNode(tp.O, b), func(q Quad) bool {
		if ferr = ev.tick(); ferr != nil {
			return false
		}

		nb, ok := bindTriple(tp, q.Triple, b)
		if !ok {
			return true
		}

		ferr = next(nb)

		return ferr == nil
	})

	return ferr
}

func bindTriple(tp triplePat, t Triple, b Binding) (Binding, bool) {
	nb := b
	cloned := false

	set := func(n node, v Term) bool {
		if !n.isVar() {
			return true
		}

		if cur, ok := nb[n.Var]; ok {
			return cur == v
		}

		if !cloned {
			nb, cloned = b.clone(), true
		}

		nb[n.Var] = v

		return true
	}

	if !set(tp.S, t.S) || !set(tp.P, t.P) || !set(tp.O, t.O) {
		return nil, false
	}

	return nb, true
}

func (ev *evaluator) matchPath(tp triplePat, b Binding, gc Term, next func(Binding) error) error {
	s, o := resolveNode(tp.S, b), resolveNode(tp.O, b)

	return ev.pathPairs(tp.Path, s, o, gc, func(ss, oo Term) error {
		nb, ok := bindTriple(triplePat{S: tp.S, P: node{}, O: tp.O}, Triple{S: ss, O: oo}, b)
		if !ok {
			return nil
		}

		return next(nb)
	})
}

func (ev *evaluator) pathPairs(pa *path, s, o, gc Term, fn func(s, o Term) error) error {
	switch {
	case s.Bound():
		ends, err := ev.step(pa, s, gc, true)
		if err != nil {
			return err
		}

		for _, e := range ends {
			if o.Bound() && e != o {
				continue
			}

			if err = fn(s, e); err != nil {
				return err
			}
		}
	case o.Bound():
		starts, err := ev.step(pa, o, gc, false)
		if err != nil {
			return err
		}

		for _, st := range starts {
			if err = fn(st, o); err != nil {
				return err
			}
		}
	default:
		for _, n := range ev.allNodes(gc) {
			ends, err := ev.step(pa, n, gc, true)
			if err != nil {
				return err
			}

			for _, e := range ends {
				if err = fn(n, e); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (ev *evaluator) allNodes(gc Term) []Term {
	seen := map[Term]struct{}{}

	var out []Term

	add := func(t Term) {
		if _, ok := seen[t]; !ok {
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}

	ev.scan(gc, Term{}, Term{}, Term{}, func(q Quad) bool {
		add(q.S)
		add(q.O)

		return true
	})

	return out
}

type termSet struct {
	seen map[Term]struct{}
	list []Term
}

func newTermSet() *termSet { return &termSet{seen: map[Term]struct{}{}} }

func (s *termSet) add(t Term) bool {
	if _, ok := s.seen[t]; ok {
		return false
	}

	s.seen[t] = struct{}{}
	s.list = append(s.list, t)

	return true
}

// step returns the distinct nodes reached from start along pa (forward) or reaching start (reverse).
func (ev *evaluator) step(pa *path, start, gc Term, fwd bool) ([]Term, error) {
	if err := ev.tick(); err != nil {
		return nil, err
	}

	switch pa.kind {
	case pLink:
		return ev.linkStep(pa.iri, start, gc, fwd), nil
	case pInv:
		return ev.step(pa.subs[0], start, gc, !fwd)
	case pSeq:
		return ev.seqStep(pa.subs, start, gc, fwd)
	case pAlt:
		out := newTermSet()

		for _, sub := range pa.subs {
			r, err := ev.step(sub, start, gc, fwd)
			if err != nil {
				return nil, err
			}

			for _, t := range r {
				out.add(t)
			}
		}

		return out.list, nil
	case pZeroOrOne:
		out := newTermSet()
		out.add(start)

		r, err := ev.step(pa.subs[0], start, gc, fwd)
		if err != nil {
			return nil, err
		}

		for _, t := range r {
			out.add(t)
		}

		return out.list, nil
	case pZeroOrMore, pOneOrMore:
		return ev.closure(pa, start, gc, fwd)
	default:
		return ev.negStep(pa, start, gc, fwd), nil
	}
}

func (ev *evaluator) linkStep(iri, start, gc Term, fwd bool) []Term {
	out := newTermSet()

	if fwd {
		ev.scan(gc, start, iri, Term{}, func(q Quad) bool { out.add(q.O); return true })
	} else {
		ev.scan(gc, Term{}, iri, start, func(q Quad) bool { out.add(q.S); return true })
	}

	return out.list
}

func (ev *evaluator) seqStep(subs []*path, start, gc Term, fwd bool) ([]Term, error) {
	cur := []Term{start}

	for i := range subs {
		sub := subs[i]
		if !fwd {
			sub = subs[len(subs)-1-i]
		}

		next := newTermSet()

		for _, n := range cur {
			r, err := ev.step(sub, n, gc, fwd)
			if err != nil {
				return nil, err
			}

			for _, t := range r {
				next.add(t)
			}
		}

		cur = next.list
	}

	return cur, nil
}

func (ev *evaluator) closure(pa *path, start, gc Term, fwd bool) ([]Term, error) {
	out := newTermSet()
	frontier := []Term{start}

	if pa.kind == pZeroOrMore {
		out.add(start)
	}

	visited := map[Term]struct{}{start: {}}

	for len(frontier) > 0 {
		var nextFrontier []Term

		for _, n := range frontier {
			r, err := ev.step(pa.subs[0], n, gc, fwd)
			if err != nil {
				return nil, err
			}

			for _, t := range r {
				out.add(t)

				if _, seen := visited[t]; !seen {
					visited[t] = struct{}{}

					nextFrontier = append(nextFrontier, t)
				}
			}
		}

		frontier = nextFrontier
	}

	return out.list, nil
}

func (ev *evaluator) negStep(pa *path, start, gc Term, fwd bool) []Term {
	fwdSet, invSet := map[Term]bool{}, map[Term]bool{}

	for _, n := range pa.neg {
		if n.inv {
			invSet[n.iri] = true
		} else {
			fwdSet[n.iri] = true
		}
	}

	out := newTermSet()

	collect := func(excl map[Term]bool, forward bool) {
		if forward {
			ev.scan(gc, start, Term{}, Term{}, func(q Quad) bool {
				if !excl[q.P] {
					out.add(q.O)
				}

				return true
			})

			return
		}

		ev.scan(gc, Term{}, Term{}, start, func(q Quad) bool {
			if !excl[q.P] {
				out.add(q.S)
			}

			return true
		})
	}

	if len(fwdSet) > 0 || len(invSet) == 0 {
		collect(fwdSet, fwd)
	}

	if len(invSet) > 0 {
		collect(invSet, !fwd)
	}

	return out.list
}
