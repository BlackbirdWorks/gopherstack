package wfinterp

import (
	"fmt"
	"strings"
)

// scopeInfo mirrors the runtime scope chain: defs is every variable the scope can hold, seen only those assigned
// by steps already checked.
type scopeInfo struct {
	defs   map[string]bool
	seen   map[string]bool
	shared map[string]bool
	parent *scopeInfo
	branch bool
}

func newScopeInfo(parent *scopeInfo) *scopeInfo {
	return &scopeInfo{defs: map[string]bool{}, seen: map[string]bool{}, parent: parent}
}

func (s *scopeInfo) has(name string) bool {
	for cur := s; cur != nil; cur = cur.parent {
		if cur.defs[name] {
			return true
		}
	}

	return false
}

func (s *scopeInfo) assigned(name string) bool {
	for cur := s; cur != nil; cur = cur.parent {
		if cur.seen[name] {
			return true
		}
	}

	return false
}

func (s *scopeInfo) define(name string) {
	s.defs[name] = true
	s.seen[name] = true
}

// blockedWrite reports whether a write would reach a parent variable across an unshared parallel boundary.
func (s *scopeInfo) blockedWrite(name string) bool {
	blocked := false

	for cur := s; cur != nil; cur = cur.parent {
		if cur.seen[name] {
			return blocked
		}

		if cur.branch && !cur.shared[name] {
			blocked = true
		}
	}

	return false
}

type scopeChecker struct {
	c *compiler
	p *Program
}

// checkScopes rejects undefined variables and writes across an unshared parallel boundary.
func (c *compiler) checkScopes(p *Program, r *routine) error {
	sc := &scopeChecker{c: c, p: p}
	root := newScopeInfo(nil)

	for _, pr := range r.params {
		root.define(pr.name)
	}

	collectDefs(r.body, root)

	return sc.block(r.body, root)
}

func collectDefs(b *block, si *scopeInfo) {
	for _, s := range b.steps {
		collectStepDefs(s, si)
	}
}

func collectStepDefs(s *step, si *scopeInfo) {
	for _, a := range s.assigns {
		si.defs[a.target.root] = true
	}

	if s.call != nil && s.call.result != nil {
		si.defs[s.call.result.root] = true
	}

	if s.nested != nil {
		collectDefs(s.nested, si)
	}

	for _, cd := range s.sw {
		collectStepDefs(cd.body, si)
	}

	if s.try != nil {
		collectDefs(s.try.body, si)
	}
}

func (sc *scopeChecker) block(b *block, si *scopeInfo) error {
	for _, s := range b.steps {
		if err := sc.step(s, si); err != nil {
			return err
		}
	}

	return nil
}

func (sc *scopeChecker) fail(s *step, format string, a ...any) error {
	return &SyntaxError{Routine: sc.c.routine, Step: s.name, Line: s.line, Msg: fmt.Sprintf(format, a...)}
}

func (sc *scopeChecker) step(s *step, si *scopeInfo) error {
	sc.c.step = s.name

	if err := sc.stepExprs(s, si); err != nil {
		return err
	}

	for _, a := range s.assigns {
		if err := sc.write(s, a.target, si); err != nil {
			return err
		}
	}

	if s.call != nil && s.call.result != nil {
		if err := sc.write(s, s.call.result, si); err != nil {
			return err
		}
	}

	return sc.children(s, si)
}

func (sc *scopeChecker) write(s *step, lv *lvalue, si *scopeInfo) error {
	for _, a := range lv.path {
		if a.idx != nil {
			if err := sc.node(s, a.idx, si); err != nil {
				return err
			}
		}
	}

	if si.blockedWrite(lv.root) {
		return sc.fail(s, "variable %s belongs to a parent scope and is not shared with this parallel step",
			quote(lv.root))
	}

	if len(lv.path) > 0 && !si.assigned(lv.root) {
		return sc.fail(s, "variable %s is not defined", quote(lv.root))
	}

	si.seen[lv.root] = true

	return nil
}

func (sc *scopeChecker) stepExprs(s *step, si *scopeInfo) error {
	raws := []any{}

	for _, a := range s.assigns {
		raws = append(raws, a.val)
	}

	if s.call != nil && s.call.hasArgs {
		raws = append(raws, s.call.args)
	}

	if s.hasRaise {
		raws = append(raws, s.raise)
	}

	if s.hasRet {
		raws = append(raws, s.ret)
	}

	for _, cd := range s.sw {
		raws = append(raws, cd.test)
	}

	if s.loop != nil {
		raws = append(raws, s.loop.in, s.loop.rng)
	}

	if s.par != nil {
		raws = append(raws, s.par.limit)

		if s.par.loop != nil {
			raws = append(raws, s.par.loop.in, s.par.loop.rng)
		}
	}

	if s.try != nil {
		raws = append(raws, s.try.retry)
	}

	for _, raw := range raws {
		if err := sc.raw(s, raw, si); err != nil {
			return err
		}
	}

	return nil
}

func (sc *scopeChecker) children(s *step, si *scopeInfo) error {
	if s.nested != nil {
		if err := sc.block(s.nested, si); err != nil {
			return err
		}
	}

	for _, cd := range s.sw {
		if err := sc.step(cd.body, si); err != nil {
			return err
		}

		sc.c.step = s.name
	}

	if s.loop != nil {
		if err := sc.loop(s.loop, si, false); err != nil {
			return err
		}
	}

	if s.par != nil {
		if err := sc.parallel(s.par, si); err != nil {
			return err
		}
	}

	if s.try != nil {
		return sc.try(s.try, si)
	}

	return nil
}

func (sc *scopeChecker) loop(f *forSpec, parent *scopeInfo, branch bool) error {
	ls := newScopeInfo(parent)
	ls.branch = branch

	return sc.loopBody(f, ls, parent)
}

func (sc *scopeChecker) loopBody(f *forSpec, ls, _ *scopeInfo) error {
	ls.define(f.value)

	if f.index != "" {
		ls.define(f.index)
	}

	collectDefs(f.body, ls)

	return sc.block(f.body, ls)
}

func (sc *scopeChecker) parallel(ps *parSpec, si *scopeInfo) error {
	shared := map[string]bool{}
	for _, n := range ps.shared {
		shared[n] = true
	}

	mk := func() *scopeInfo {
		bs := newScopeInfo(si)
		bs.branch, bs.shared = true, shared

		return bs
	}

	if ps.loop != nil {
		return sc.loopBody(ps.loop, mk(), si)
	}

	for _, br := range ps.branches {
		bs := mk()
		collectDefs(br.body, bs)

		if err := sc.block(br.body, bs); err != nil {
			return err
		}
	}

	return nil
}

func (sc *scopeChecker) try(t *trySpec, si *scopeInfo) error {
	if err := sc.block(t.body, si); err != nil {
		return err
	}

	if t.except == nil {
		return nil
	}

	es := newScopeInfo(si)
	if t.exceptAs != "" {
		es.define(t.exceptAs)
	}

	collectDefs(t.except, es)

	return sc.block(t.except, es)
}

func (sc *scopeChecker) raw(s *step, v any, si *scopeInfo) error {
	switch x := v.(type) {
	case *Expr:
		return sc.node(s, x.root, si)
	case map[string]any:
		for _, e := range x {
			if err := sc.raw(s, e, si); err != nil {
				return err
			}
		}
	case []any:
		for _, e := range x {
			if err := sc.raw(s, e, si); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sc *scopeChecker) node(s *step, n node, si *scopeInfo) error {
	switch x := n.(type) {
	case *nameNode:
		return sc.resolve(s, x.name, x.name, si)
	case *memberNode:
		if d, ok := dotted(x); ok {
			return sc.resolve(s, d, strings.Split(d, ".")[0], si)
		}

		return sc.node(s, x.x, si)
	case *indexNode:
		return sc.nodes(s, si, x.x, x.i)
	case *callNode:
		if _, named := dotted(x.fn); !named {
			if err := sc.node(s, x.fn, si); err != nil {
				return err
			}
		}

		return sc.nodes(s, si, x.args...)
	case *unaryNode:
		return sc.node(s, x.x, si)
	case *binNode:
		return sc.nodes(s, si, x.l, x.r)
	case *listNode:
		return sc.nodes(s, si, x.items...)
	case *mapNode:
		for i := range x.keys {
			if err := sc.nodes(s, si, x.keys[i], x.vals[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sc *scopeChecker) nodes(s *step, si *scopeInfo, ns ...node) error {
	for _, n := range ns {
		if err := sc.node(s, n, si); err != nil {
			return err
		}
	}

	return nil
}

func (sc *scopeChecker) resolve(s *step, dottedName, root string, si *scopeInfo) error {
	if si.has(root) {
		return nil
	}

	if _, ok := sc.p.subs[root]; ok {
		return nil
	}

	parts := strings.Split(dottedName, ".")
	for i := len(parts); i > 0; i-- {
		if _, ok := sc.c.eng.lookupConst(strings.Join(parts[:i], ".")); ok {
			return nil
		}
	}

	return sc.fail(s, "variable %s is not defined", quote(root))
}
