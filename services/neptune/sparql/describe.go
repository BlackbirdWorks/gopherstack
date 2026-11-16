package sparql

// describeAll builds the DESCRIBE response for targets under the hinted mode
// (sparql-query-hints-for-describe.html); SymmetricOneStep is the default.
func (ev *evaluator) describeAll(targets []Term, h Hints) []Triple {
	d := &describer{ev: ev, seen: map[Triple]struct{}{}, limit: h.statementLimit()}

	mode := h.DescribeMode
	if mode == "" {
		mode = DescribeSymmetricOneStep
	}

	for _, t := range targets {
		switch mode {
		case DescribeForwardOneStep:
			d.outgoing(t, nil)
		case DescribeSymmetricOneStep:
			d.outgoing(t, nil)
			d.incoming(t)
		case DescribeCBD:
			d.bounded(t, h.iterationLimit(), false)
		case DescribeSCBD:
			d.bounded(t, h.iterationLimit(), true)
		}
	}

	return d.out
}

type describer struct {
	ev    *evaluator
	seen  map[Triple]struct{}
	out   []Triple
	limit int
}

func (d *describer) full() bool { return d.limit > 0 && len(d.out) >= d.limit }

// add records tr once and reports whether it was new.
func (d *describer) add(tr Triple) bool {
	if _, dup := d.seen[tr]; dup || d.full() {
		return false
	}

	d.seen[tr] = struct{}{}
	d.out = append(d.out, tr)

	return true
}

func (d *describer) outgoing(s Term, fresh *[]Triple) {
	d.ev.scan(Term{}, s, Term{}, Term{}, func(qd Quad) bool {
		if d.add(qd.Triple) && fresh != nil {
			*fresh = append(*fresh, qd.Triple)
		}

		return true
	})
}

func (d *describer) incoming(o Term) {
	d.ev.scan(Term{}, Term{}, Term{}, o, func(qd Quad) bool {
		d.add(qd.Triple)

		return true
	})
}

func (d *describer) objects(p, o Term) []Term {
	var out []Term

	d.ev.scan(Term{}, Term{}, p, o, func(qd Quad) bool {
		out = append(out, qd.S)

		return true
	})

	return out
}

// bounded computes the (symmetric) concise bounded description of root; iterations of 0 mean no limit.
func (d *describer) bounded(root Term, iterations int, symmetric bool) {
	frontier := []Term{root}
	expanded := map[Term]bool{}

	for round := 0; len(frontier) > 0 && !d.full(); round++ {
		var added []Triple

		for _, r := range frontier {
			if expanded[r] {
				continue
			}

			expanded[r] = true

			d.outgoing(r, &added)

			if symmetric && r == root {
				d.incoming(r)
			}
		}

		if iterations > 0 && round+1 >= iterations {
			break
		}

		frontier = d.nextFrontier(added)
	}
}

// nextFrontier lists the blank nodes the new statements point to and the reification nodes describing them.
func (d *describer) nextFrontier(added []Triple) []Term {
	var next []Term

	for _, tr := range added {
		if tr.O.Kind == KindBlank {
			next = append(next, tr.O)
		}

		next = append(next, d.reifications(tr)...)
	}

	return next
}

func (d *describer) reifications(tr Triple) []Term {
	var out []Term

	for _, r := range d.objects(IRI(RDFSubject), tr.S) {
		if len(d.objectsOf(r, RDFPredicate, tr.P)) == 0 || len(d.objectsOf(r, RDFObject, tr.O)) == 0 {
			continue
		}

		if r.Kind == KindBlank {
			out = append(out, r)

			continue
		}

		d.add(Triple{S: r, P: IRI(RDFSubject), O: tr.S})
	}

	return out
}

func (d *describer) objectsOf(s Term, pred string, o Term) []Term {
	var out []Term

	d.ev.scan(Term{}, s, IRI(pred), o, func(qd Quad) bool {
		out = append(out, qd.O)

		return true
	})

	return out
}
