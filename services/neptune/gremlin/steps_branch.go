package gremlin

func regBranch(m map[string]stepFn) {
	m["union"] = perTrav(stepUnion)
	m["coalesce"] = perTrav(stepCoalesce)
	m["optional"] = perTrav(stepOptional)
	m[nameLocal] = perTrav(func(x *exec, s *Step, t *trav) ([]*trav, error) {
		tr, err := argTraversal(s, 0)
		if err != nil {
			return nil, err
		}

		return x.sub(tr, t)
	})
	m["choose"] = perTrav(stepChoose)
	m["repeat"] = perTrav(stepRepeat)
}

func travArgs(s *Step) ([]*Traversal, error) {
	out := make([]*Traversal, 0, len(s.Args))

	for i := range s.Args {
		tr, err := argTraversal(s, i)
		if err != nil {
			return nil, err
		}

		out = append(out, tr)
	}

	return out, nil
}

func stepUnion(x *exec, s *Step, t *trav) ([]*trav, error) {
	trs, err := travArgs(s)
	if err != nil {
		return nil, err
	}

	var out []*trav

	for _, tr := range trs {
		r, rerr := x.sub(tr, t)
		if rerr != nil {
			return nil, rerr
		}

		out = append(out, r...)
	}

	return out, nil
}

func stepCoalesce(x *exec, s *Step, t *trav) ([]*trav, error) {
	trs, err := travArgs(s)
	if err != nil {
		return nil, err
	}

	for _, tr := range trs {
		r, rerr := x.sub(tr, t)
		if rerr != nil {
			return nil, rerr
		}

		if len(r) > 0 {
			return r, nil
		}
	}

	return nil, nil
}

func stepOptional(x *exec, s *Step, t *trav) ([]*trav, error) {
	tr, err := argTraversal(s, 0)
	if err != nil {
		return nil, err
	}

	r, err := x.sub(tr, t)
	if err != nil || len(r) > 0 {
		return r, err
	}

	return []*trav{t}, nil
}

func stepChoose(x *exec, s *Step, t *trav) ([]*trav, error) {
	if len(s.Args) == 0 {
		return nil, invalidArg("choose() requires a predicate or traversal")
	}

	if len(s.Args) == 1 {
		return x.chooseOption(s, t)
	}

	ok, err := x.chooseCond(s.Args[0], t)
	if err != nil {
		return nil, err
	}

	branch := 1
	if !ok {
		branch = 2
	}

	if branch >= len(s.Args) {
		return []*trav{t}, nil
	}

	tr, ok2 := s.Args[branch].(*Traversal)
	if !ok2 {
		return nil, invalidArg("choose() branches must be traversals")
	}

	return x.sub(tr, t)
}

func (x *exec) chooseCond(cond any, t *trav) (bool, error) {
	switch c := cond.(type) {
	case Pred:
		return c.test(t.v), nil
	case *Traversal:
		return x.exists(c, t)
	}

	return false, invalidArg("choose() condition must be a predicate or traversal")
}

// chooseOption implements choose(traversal).option(value, branch).
func (x *exec) chooseOption(s *Step, t *trav) ([]*trav, error) {
	sel, ok := s.Args[0].(*Traversal)
	if !ok {
		return nil, invalidArg("choose() requires a traversal")
	}

	val, found, err := x.firstValue(sel, t)
	if err != nil {
		return nil, err
	}

	var none *Traversal

	for _, o := range modsNamed(s, "option") {
		tr, key, hasKey := optionParts(o)
		if tr == nil {
			continue
		}

		if !hasKey {
			none = tr

			continue
		}

		if found && equalValues(val, key) {
			return x.sub(tr, t)
		}
	}

	if none != nil {
		return x.sub(none, t)
	}

	return nil, nil
}

func optionParts(o *Step) (*Traversal, any, bool) {
	switch len(o.Args) {
	case 1:
		tr, _ := o.Args[0].(*Traversal)

		return tr, nil, false
	case 2: //nolint:mnd // key and branch
		tr, _ := o.Args[1].(*Traversal)
		if k, ok := o.Args[0].(Token); ok && k.Name == "none" {
			return tr, nil, false
		}

		return tr, o.Args[0], true
	}

	return nil, nil, false
}

type loopSpec struct {
	body      *Traversal
	until     *Traversal
	emit      *Traversal
	times     int
	hasTimes  bool
	preUntil  bool
	postUntil bool
	preEmit   bool
	postEmit  bool
	emitAll   bool
}

func loopSpecOf(s *Step) (*loopSpec, error) {
	body, err := argTraversal(s, 0)
	if err != nil {
		return nil, err
	}

	spec := &loopSpec{body: body}

	for i := range s.Mods {
		m := &s.Mods[i]

		switch m.Name {
		case modTimes:
			n, terr := argInt(m, 0)
			if terr != nil {
				return nil, terr
			}

			spec.times, spec.hasTimes = n, true
		case modUntil:
			spec.until, _ = m.Args[0].(*Traversal)
			spec.preUntil, spec.postUntil = m.Pre, !m.Pre
		case modEmit:
			spec.emitAll = len(m.Args) == 0
			if !spec.emitAll {
				spec.emit, _ = m.Args[0].(*Traversal)
			}

			spec.preEmit, spec.postEmit = m.Pre, !m.Pre
		}
	}

	if !spec.hasTimes && spec.until == nil {
		return nil, invalidArg("repeat() requires times() or until()")
	}

	return spec, nil
}

func (x *exec) loopDone(spec *loopSpec, t *trav, pre bool) (bool, error) {
	if spec.hasTimes && !pre && t.loops >= spec.times {
		return true, nil
	}

	if spec.until != nil && spec.preUntil == pre {
		return x.exists(spec.until, t)
	}

	return false, nil
}

func (x *exec) loopEmit(spec *loopSpec, t *trav, pre bool) (bool, error) {
	if pre != spec.preEmit || (!spec.preEmit && !spec.postEmit) {
		return false, nil
	}

	if spec.emitAll {
		return true, nil
	}

	return x.exists(spec.emit, t)
}

func stepRepeat(x *exec, s *Step, t *trav) ([]*trav, error) {
	spec, err := loopSpecOf(s)
	if err != nil {
		return nil, err
	}

	var out []*trav

	frontier := []*trav{t}

	for round := 0; len(frontier) > 0; round++ {
		if round > maxLoops {
			return nil, constraint("repeat() exceeded the loop limit of %d", maxLoops)
		}

		cont, exited, serr := x.splitFrontier(spec, frontier)
		if serr != nil {
			return nil, serr
		}

		out = append(out, exited...)

		if frontier, serr = x.advance(spec, cont, &out); serr != nil {
			return nil, serr
		}
	}

	return out, nil
}

// splitFrontier applies the until/emit checks written before repeat().
func (x *exec) splitFrontier(spec *loopSpec, frontier []*trav) ([]*trav, []*trav, error) {
	var cont, exited []*trav

	for _, f := range frontier {
		emit, eerr := x.loopEmit(spec, f, true)
		if eerr != nil {
			return nil, nil, eerr
		}

		if emit {
			exited = append(exited, f)
		}

		done, derr := x.loopDone(spec, f, true)
		if derr != nil {
			return nil, nil, derr
		}

		if done {
			if !emit {
				exited = append(exited, f)
			}

			continue
		}

		cont = append(cont, f)
	}

	return cont, exited, nil
}

// advance runs the body once over the frontier and returns the traversers that loop again.
func (x *exec) advance(spec *loopSpec, cont []*trav, out *[]*trav) ([]*trav, error) {
	var next []*trav

	for _, c := range cont {
		res, err := x.sub(spec.body, c)
		if err != nil {
			return nil, err
		}

		for _, r := range res {
			r = &trav{v: r.v, path: r.path, loops: c.loops + 1}

			done, derr := x.loopDone(spec, r, false)
			if derr != nil {
				return nil, derr
			}

			if done {
				*out = append(*out, r)

				continue
			}

			emit, eerr := x.loopEmit(spec, r, false)
			if eerr != nil {
				return nil, eerr
			}

			if emit {
				*out = append(*out, r)
			}

			next = append(next, r)
		}
	}

	if len(next) > maxTraversers {
		return nil, constraint("Traverser limit of %d exceeded", maxTraversers)
	}

	return next, nil
}
