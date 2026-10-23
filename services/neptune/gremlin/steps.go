package gremlin

func buildSteps() map[string]stepFn {
	m := map[string]stepFn{}

	regSource(m)
	regNav(m)
	regFilter(m)
	regProject(m)
	regReduce(m)
	regBranch(m)
	regMutate(m)

	return m
}

// perTrav builds a step that maps each traverser to zero or more output traversers.
func perTrav(f func(x *exec, s *Step, t *trav) ([]*trav, error)) stepFn {
	return func(x *exec, s *Step, in []*trav) ([]*trav, error) {
		var out []*trav

		for _, t := range in {
			r, err := f(x, s, t)
			if err != nil {
				return nil, err
			}

			out = append(out, r...)

			if len(out) > maxTraversers {
				return nil, constraint("Traverser limit of %d exceeded", maxTraversers)
			}
		}

		return out, nil
	}
}

// filterStep keeps the traversers for which f is true.
func filterStep(f func(x *exec, s *Step, t *trav) (bool, error)) stepFn {
	return perTrav(func(x *exec, s *Step, t *trav) ([]*trav, error) {
		ok, err := f(x, s, t)
		if err != nil || !ok {
			return nil, err
		}

		return []*trav{t}, nil
	})
}

// mapStep replaces each traverser's object with f's result.
func mapStep(f func(x *exec, s *Step, t *trav) (any, error)) stepFn {
	return perTrav(func(x *exec, s *Step, t *trav) ([]*trav, error) {
		v, err := f(x, s, t)
		if err != nil {
			return nil, err
		}

		return []*trav{t.next(v)}, nil
	})
}

// flatStep emits one traverser per value f returns.
func flatStep(f func(x *exec, s *Step, t *trav) ([]any, error)) stepFn {
	return perTrav(func(x *exec, s *Step, t *trav) ([]*trav, error) {
		vs, err := f(x, s, t)
		if err != nil {
			return nil, err
		}

		out := make([]*trav, len(vs))
		for i, v := range vs {
			out[i] = t.next(v)
		}

		return out, nil
	})
}
