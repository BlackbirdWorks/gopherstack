package wfinterp

import "strings"

type vctx struct {
	targets    []*block
	inLoop     bool
	inParallel bool
}

func (c *compiler) validateRoutine(p *Program, r *routine) error {
	seen := map[string]bool{}
	for _, pr := range r.params {
		if seen[pr.name] {
			return &SyntaxError{Routine: r.name, Msg: "duplicate parameter " + pr.name}
		}

		seen[pr.name] = true
	}

	if r.name == mainRoutine && len(r.params) > 1 {
		return &SyntaxError{Routine: r.name, Msg: "main takes at most one parameter"}
	}

	return c.validateBlock(p, r.body, vctx{})
}

func (c *compiler) validateBlock(p *Program, b *block, v vctx) error {
	v.targets = append(append([]*block{}, v.targets...), b)

	for _, s := range b.steps {
		c.step = s.name

		if err := c.validateStep(p, s, v); err != nil {
			return err
		}
	}

	return nil
}

func (c *compiler) validateStep(p *Program, s *step, v vctx) error {
	if err := c.validateNext(s, v); err != nil {
		return err
	}

	if s.hasRet && v.inParallel {
		return &SyntaxError{Routine: c.routine, Step: s.name, Line: s.line,
			Msg: "return is not allowed inside a parallel branch or iteration"}
	}

	if s.call != nil {
		if err := c.validateCall(p, s); err != nil {
			return err
		}
	}

	return c.validateChildren(p, s, v)
}

func (c *compiler) validateNext(s *step, v vctx) error {
	if s.next == "" || s.next == nextEnd {
		return nil
	}

	if s.next == nextBreak || s.next == nextContinue {
		if !v.inLoop {
			return &SyntaxError{Routine: c.routine, Step: s.name, Line: s.line,
				Msg: "next: " + s.next + " is only valid inside a for loop"}
		}

		return nil
	}

	for _, b := range v.targets {
		if _, ok := b.index[s.next]; ok {
			return nil
		}
	}

	return &SyntaxError{Routine: c.routine, Step: s.name, Line: s.line,
		Msg: "next refers to step " + quote(s.next) + ", which does not exist in an enclosing block"}
}

func quote(s string) string { return "'" + s + "'" }

func (c *compiler) validateChildren(p *Program, s *step, v vctx) error {
	if s.nested != nil {
		if err := c.validateBlock(p, s.nested, v); err != nil {
			return err
		}
	}

	for _, cd := range s.sw {
		cd.body.name = s.name
		if err := c.validateStep(p, cd.body, v); err != nil {
			return err
		}
	}

	if s.loop != nil {
		lv := v
		lv.inLoop = true

		if err := c.validateBlock(p, s.loop.body, lv); err != nil {
			return err
		}
	}

	if s.par != nil {
		if err := c.validateParallel(p, s.par, v); err != nil {
			return err
		}
	}

	if s.try != nil {
		if err := c.validateTry(p, s.try, v); err != nil {
			return err
		}
	}

	return nil
}

func (c *compiler) validateParallel(p *Program, ps *parSpec, v vctx) error {
	v.inParallel, v.inLoop = true, false
	v.targets = nil

	if ps.loop != nil {
		lv := v
		lv.inLoop = true

		return c.validateBlock(p, ps.loop.body, lv)
	}

	for _, br := range ps.branches {
		if err := c.validateBlock(p, br.body, v); err != nil {
			return err
		}
	}

	return nil
}

func (c *compiler) validateTry(p *Program, t *trySpec, v vctx) error {
	if err := c.validateBlock(p, t.body, v); err != nil {
		return err
	}

	if t.except != nil {
		return c.validateBlock(p, t.except, v)
	}

	return nil
}

func (c *compiler) validateCall(p *Program, s *step) error {
	cs := s.call
	fail := func(msg string) error {
		return &SyntaxError{Routine: c.routine, Step: s.name, Line: s.line, Msg: msg}
	}

	if sub, ok := p.subs[cs.fn]; ok {
		return validateSubArgs(sub, cs, fail)
	}

	if spec, ok := c.eng.funcs[cs.fn]; ok {
		if !cs.hasArgs && spec.min > 0 {
			return fail("call " + cs.fn + " needs args")
		}

		names := map[string]any{}
		for k := range cs.args {
			names[k] = nil
		}

		if _, err := spec.bind(names); err != nil {
			return fail(strings.TrimPrefix(err.Error(), ""))
		}

		return nil
	}

	if c.eng.isConnector(cs.fn) {
		return nil
	}

	return fail("function or subworkflow " + quote(cs.fn) + " not found")
}

func validateSubArgs(sub *routine, cs *callSpec, fail func(string) error) error {
	known := map[string]bool{}
	for _, pr := range sub.params {
		known[pr.name] = true

		if _, given := cs.args[pr.name]; !given && !pr.hasDef {
			return fail("call to " + sub.name + " is missing the argument " + quote(pr.name))
		}
	}

	for k := range cs.args {
		if !known[k] {
			return fail("call to " + sub.name + " has the unknown argument " + quote(k))
		}
	}

	return nil
}
