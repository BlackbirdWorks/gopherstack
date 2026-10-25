package wfinterp

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxSteps    = 100000
	maxCallDeep = 20
	maxSleep    = 31536000
)

type outKind int

const (
	outNormal outKind = iota
	outJump
	outEnd
	outReturn
	outBreak
	outContinue
)

type outcome struct {
	value  any
	target string
	kind   outKind
}

type scope struct {
	vars   map[string]any
	parent *scope
	shared map[string]bool
	branch bool
}

func newScope(parent *scope) *scope { return &scope{vars: map[string]any{}, parent: parent} }

func (s *scope) get(name string) (any, bool) {
	for cur := s; cur != nil; cur = cur.parent {
		if v, ok := cur.vars[name]; ok {
			return v, true
		}
	}

	return nil, false
}

// owner finds the scope that holds name and fails when a parallel branch boundary that does not share it lies between.
func (s *scope) owner(name string) (*scope, error) {
	blocked := false

	for cur := s; cur != nil; cur = cur.parent {
		if _, ok := cur.vars[name]; ok {
			if blocked {
				return nil, typeErr(
					"variable %q belongs to a parent scope and is not shared with this parallel step",
					name,
				)
			}

			return cur, nil
		}

		if cur.branch && !cur.shared[name] {
			blocked = true
		}
	}

	return nil, nil //nolint:nilnil // not defined anywhere
}

func (s *scope) set(name string, v any) error {
	o, err := s.owner(name)
	if err != nil {
		return err
	}

	if o == nil {
		o = s
	}

	o.vars[name] = v

	return nil
}

type exec struct {
	eng    *Engine
	prog   *Program
	host   *Host
	frames map[int]Frame
	steps  atomic.Int64
	tids   atomic.Int64
	mu     sync.Mutex
	fmu    sync.Mutex
}

type rctx struct {
	x       *exec
	ctx     context.Context
	routine *routine
	tid     int
	depth   int
}

// Run executes the main subworkflow with arg as its argument and returns the workflow result.
func (e *Engine) Run(ctx context.Context, p *Program, arg any, host *Host) (any, error) {
	if host == nil {
		host = &Host{}
	}

	x := &exec{eng: e, prog: p, host: host, frames: map[int]Frame{}}
	r := &rctx{x: x, ctx: ctx, routine: p.subs[mainRoutine]}

	args := map[string]any{}
	if ps := r.routine.params; len(ps) > 0 && arg != nil {
		args[ps[0].name] = arg
	}

	return r.runRoutine(r.routine, args)
}

func (r *rctx) runRoutine(sub *routine, args map[string]any) (any, error) {
	sc := newScope(nil)

	for _, pr := range sub.params {
		switch v, given := args[pr.name]; {
		case given:
			sc.vars[pr.name] = deepCopy(v)
		case pr.hasDef:
			dv, err := r.evalValue(sc, pr.def)
			if err != nil {
				return nil, err
			}

			sc.vars[pr.name] = deepCopy(dv)
		default:
			return nil, r.stamp(typeErr("missing required argument %q of %s", pr.name, sub.name), "")
		}
	}

	out, err := r.runBlock(sc, sub.body)
	if err != nil {
		return nil, err
	}

	if out.kind == outReturn {
		return out.value, nil
	}

	return nil, nil //nolint:nilnil // a routine without return yields null
}

func (r *rctx) stamp(err error, step string) error { return r.stampErr(err, step) }

func (r *rctx) stampErr(err error, step string) *Error {
	var e *Error
	if !errors.As(err, &e) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return &Error{Value: errMap("the execution was cancelled", TagSystemError), cancel: true}
		}

		e = &Error{Value: errMap(err.Error(), TagSystemError)}
	}

	if len(e.Stack) == 0 {
		e.Stack = []Frame{{Routine: r.routine.name, Step: step}}
	}

	return e
}

func (r *rctx) runBlock(sc *scope, b *block) (outcome, error) {
	for i := 0; i < len(b.steps); {
		out, err := r.runStep(sc, b.steps[i])
		if err != nil {
			return out, err
		}

		switch out.kind {
		case outNormal:
			i++
		case outJump:
			idx, ok := b.index[out.target]
			if !ok {
				return out, nil
			}

			i = idx
		default:
			return out, nil
		}
	}

	return outcome{}, nil
}

func (x *exec) setFrame(tid int, f *Frame) {
	x.fmu.Lock()
	defer x.fmu.Unlock()

	if f == nil {
		delete(x.frames, tid)
	} else {
		x.frames[tid] = *f
	}

	if x.host.Progress == nil {
		return
	}

	ids := make([]int, 0, len(x.frames))
	for id := range x.frames {
		ids = append(ids, id)
	}

	sort.Ints(ids)

	out := make([]Frame, len(ids))
	for i, id := range ids {
		out[i] = x.frames[id]
	}

	x.host.Progress(out)
}

func (r *rctx) runStep(sc *scope, s *step) (outcome, error) {
	if err := r.ctx.Err(); err != nil {
		return outcome{}, r.stamp(err, s.name)
	}

	if r.x.steps.Add(1) > maxSteps {
		e := rtErr(TagResourceLimitError, "the execution exceeded the limit of %d steps", maxSteps)
		e.fatal = true

		return outcome{}, r.stamp(e, s.name)
	}

	r.x.setFrame(r.tid, &Frame{Routine: r.routine.name, Step: s.name, Line: s.line})

	out, err := r.dispatch(sc, s)
	if err != nil {
		var e *Error
		if errors.As(err, &e) && len(e.Stack) == 0 {
			e.Stack = []Frame{{Routine: r.routine.name, Step: s.name, Line: s.line}}
		}

		return out, r.stamp(err, s.name)
	}

	if out.kind == outNormal && s.next != "" {
		return nextOutcome(s.next), nil
	}

	return out, nil
}

func nextOutcome(next string) outcome {
	switch next {
	case nextEnd:
		return outcome{kind: outEnd}
	case nextBreak:
		return outcome{kind: outBreak}
	case nextContinue:
		return outcome{kind: outContinue}
	default:
		return outcome{kind: outJump, target: next}
	}
}

func (r *rctx) dispatch(sc *scope, s *step) (outcome, error) {
	switch {
	case len(s.assigns) > 0:
		return outcome{}, r.doAssign(sc, s)
	case s.call != nil:
		return outcome{}, r.doCall(sc, s.call)
	case s.hasSwitch:
		return r.doSwitch(sc, s)
	case s.loop != nil:
		return r.doFor(sc, s.loop)
	case s.par != nil:
		return outcome{}, r.doParallel(sc, s.par)
	case s.try != nil:
		return r.doTry(sc, s.try)
	case s.hasRaise:
		v, err := r.evalValue(sc, s.raise)
		if err != nil {
			return outcome{}, err
		}

		return outcome{}, &Error{Value: deepCopy(v)}
	case s.hasRet:
		v, err := r.evalValue(sc, s.ret)
		if err != nil {
			return outcome{}, err
		}

		return outcome{kind: outReturn, value: deepCopy(v)}, nil
	case s.nested != nil:
		return r.runBlock(sc, s.nested)
	default:
		return outcome{}, nil
	}
}

func (r *rctx) evaluator(sc *scope) *evaluator {
	return &evaluator{eng: r.x.eng, vars: sc.get, subs: r.x.prog.subs, cc: &callCtx{ctx: r.ctx, x: r.x}}
}

func (r *rctx) evalRaw(sc *scope, raw any) (any, error) {
	switch x := raw.(type) {
	case *Expr:
		return r.evaluator(sc).eval(x.root)
	case map[string]any:
		out := make(map[string]any, len(x))

		for k, v := range x {
			ev, err := r.evalRaw(sc, v)
			if err != nil {
				return nil, err
			}

			out[k] = ev
		}

		return out, nil
	case []any:
		out := make([]any, len(x))

		for i, v := range x {
			ev, err := r.evalRaw(sc, v)
			if err != nil {
				return nil, err
			}

			out[i] = ev
		}

		return out, nil
	default:
		return raw, nil
	}
}

func (r *rctx) evalValue(sc *scope, raw any) (any, error) {
	r.x.mu.Lock()
	defer r.x.mu.Unlock()

	return r.evalRaw(sc, raw)
}

func (r *rctx) doAssign(sc *scope, s *step) error {
	r.x.mu.Lock()
	defer r.x.mu.Unlock()

	for _, a := range s.assigns {
		v, err := r.evalRaw(sc, a.val)
		if err != nil {
			return err
		}

		if err = r.setLvalue(sc, a.target, deepCopy(v)); err != nil {
			return err
		}
	}

	return nil
}

func (r *rctx) setLvalue(sc *scope, lv *lvalue, val any) error {
	if len(lv.path) == 0 {
		return sc.set(lv.root, val)
	}

	if _, err := sc.owner(lv.root); err != nil {
		return err
	}

	cur, ok := sc.get(lv.root)
	if !ok {
		return rtErr(TagKeyError, "variable %q is not defined", lv.root)
	}

	keys := make([]any, len(lv.path))
	ev := r.evaluator(sc)

	for i, a := range lv.path {
		if a.idx == nil {
			keys[i] = a.name

			continue
		}

		k, err := ev.eval(a.idx)
		if err != nil {
			return err
		}

		keys[i] = k
	}

	for _, k := range keys[:len(keys)-1] {
		next, err := getField(cur, k)
		if err != nil {
			return err
		}

		cur = next
	}

	return putField(cur, keys[len(keys)-1], val)
}

func putField(container, key, val any) error {
	switch c := container.(type) {
	case map[string]any:
		if _, isStr := key.(string); !isStr && !isNumber(key) {
			return typeErr("map keys must be strings, not %s", typeName(key))
		}

		c[formatValue(key)] = val

		return nil
	case []any:
		i, ok := key.(int64)
		if !ok {
			return typeErr("list index must be an integer, not %s", typeName(key))
		}

		if i < 0 || i >= int64(len(c)) {
			return rtErr(TagIndexError, "list index out of range: %d", i)
		}

		c[i] = val

		return nil
	default:
		return typeErr("cannot assign into a %s", typeName(container))
	}
}

func (r *rctx) doCall(sc *scope, cs *callSpec) error {
	args := map[string]any{}

	if cs.hasArgs {
		v, err := r.evalValue(sc, cs.args)
		if err != nil {
			return err
		}

		args, _ = deepCopy(v).(map[string]any)
	}

	res, err := r.invoke(cs.fn, args)
	if err != nil {
		return err
	}

	if cs.result == nil {
		return nil
	}

	r.x.mu.Lock()
	defer r.x.mu.Unlock()

	return r.setLvalue(sc, cs.result, deepCopy(res))
}

func (r *rctx) invoke(fn string, args map[string]any) (any, error) {
	if sub, ok := r.x.prog.subs[fn]; ok {
		return r.callSub(sub, args)
	}

	if spec, ok := r.x.eng.funcs[fn]; ok {
		a, err := spec.bind(args)
		if err != nil {
			return nil, err
		}

		return spec.fn(&callCtx{ctx: r.ctx, x: r.x}, a)
	}

	if r.x.eng.isConnector(fn) && r.x.host.Connector != nil {
		res, handled, err := r.x.host.Connector(r.ctx, fn, args)
		if err != nil {
			return nil, err
		}

		if handled {
			return res, nil
		}
	}

	return nil, rtErr(TagKeyError, "function not found: %s", fn)
}

func (r *rctx) callSub(sub *routine, args map[string]any) (any, error) {
	if r.depth+1 > maxCallDeep {
		e := rtErr(TagRecursionError, "the call stack is deeper than %d subworkflow calls", maxCallDeep)
		e.fatal = true

		return nil, e
	}

	for k := range args {
		known := false

		for _, p := range sub.params {
			known = known || p.name == k
		}

		if !known {
			return nil, typeErr("%s has no parameter %q", sub.name, k)
		}
	}

	child := &rctx{x: r.x, ctx: r.ctx, routine: sub, tid: r.tid, depth: r.depth + 1}

	return child.runRoutine(sub, args)
}

// callRef calls a function reference with positional arguments, as a retry predicate does.
func (r *rctx) callRef(ref *FuncRef, args ...any) (any, error) {
	if sub, ok := r.x.prog.subs[ref.Name]; ok {
		named := map[string]any{}

		for i, a := range args {
			if i < len(sub.params) {
				named[sub.params[i].name] = a
			}
		}

		return r.callSub(sub, named)
	}

	spec, ok := r.x.eng.funcs[ref.Name]
	if !ok {
		return nil, rtErr(TagKeyError, "function not found: %s", ref.Name)
	}

	a := make([]any, len(spec.params))
	copy(a, args)

	return spec.fn(&callCtx{ctx: r.ctx, x: r.x}, a)
}

func (r *rctx) doSwitch(sc *scope, s *step) (outcome, error) {
	for _, cd := range s.sw {
		v, err := r.evalValue(sc, cd.test)
		if err != nil {
			return outcome{}, err
		}

		b, ok := v.(bool)
		if !ok {
			return outcome{}, typeErr("switch condition must be a boolean, not %s", typeName(v))
		}

		if b {
			return r.runStep(sc, cd.body)
		}
	}

	return outcome{}, nil
}

func (r *rctx) loopItems(sc *scope, f *forSpec) ([]any, error) {
	v, err := r.evalValue(sc, pick(f.hasIn, f.in, f.rng))
	if err != nil {
		return nil, err
	}

	l, ok := v.([]any)
	if !ok {
		return nil, typeErr("for needs a list, not %s", typeName(v))
	}

	if f.hasIn {
		return l, nil
	}

	return expandRange(l)
}

func pick(first bool, a, b any) any {
	if first {
		return a
	}

	return b
}

const (
	maxRange    = 1000000
	rangeBounds = 2
)

func expandRange(l []any) ([]any, error) {
	if len(l) != rangeBounds {
		return nil, typeErr("range needs exactly two numbers")
	}

	bi, bInt := l[0].(int64)
	ei, eInt := l[1].(int64)

	if bInt && eInt {
		if ei-bi >= maxRange {
			return nil, rtErr(TagResourceLimitError, "range is longer than %d iterations", maxRange)
		}

		var out []any
		for i := bi; i <= ei; i++ {
			out = append(out, i)
		}

		return out, nil
	}

	bf, ok1 := toFloat(l[0])
	ef, ok2 := toFloat(l[1])

	if !ok1 || !ok2 {
		return nil, typeErr("range bounds must be numbers")
	}

	var out []any

	for f := bf; f <= ef && len(out) < maxRange; f++ {
		out = append(out, f)
	}

	return out, nil
}

func (r *rctx) doFor(sc *scope, f *forSpec) (outcome, error) {
	items, err := r.loopItems(sc, f)
	if err != nil {
		return outcome{}, err
	}

	for i, it := range items {
		ls := newScope(sc)
		r.bindLoop(ls, f, i, it)

		out, berr := r.runBlock(ls, f.body)
		if berr != nil {
			return out, berr
		}

		switch out.kind {
		case outBreak:
			return outcome{}, nil
		case outNormal, outContinue:
		default:
			return out, nil
		}
	}

	return outcome{}, nil
}

func (r *rctx) bindLoop(ls *scope, f *forSpec, i int, item any) {
	r.x.mu.Lock()
	defer r.x.mu.Unlock()

	ls.vars[f.value] = deepCopy(item)

	if f.index != "" {
		ls.vars[f.index] = int64(i)
	}
}

type branchErr struct {
	err *Error
	id  string
}

func (r *rctx) doParallel(sc *scope, ps *parSpec) error {
	shared := map[string]bool{}
	for _, s := range ps.shared {
		shared[s] = true
	}

	type job struct {
		run func(*rctx) error
		id  string
	}

	var jobs []job

	if ps.loop != nil {
		items, err := r.loopItems(sc, ps.loop)
		if err != nil {
			return err
		}

		for i, it := range items {
			jobs = append(jobs, job{id: strconv.Itoa(i), run: func(br *rctx) error {
				bs := &scope{vars: map[string]any{}, parent: sc, shared: shared, branch: true}
				br.bindLoop(bs, ps.loop, i, it)

				_, berr := br.runBlock(bs, ps.loop.body)

				return berr
			}})
		}
	} else {
		for _, b := range ps.branches {
			jobs = append(jobs, job{id: b.name, run: func(br *rctx) error {
				bs := &scope{vars: map[string]any{}, parent: sc, shared: shared, branch: true}
				_, berr := br.runBlock(bs, b.body)

				return berr
			}})
		}
	}

	limit := len(jobs)

	if ps.hasLimit {
		v, err := r.evalValue(sc, ps.limit)
		if err != nil {
			return err
		}

		n, ok := v.(int64)
		if !ok || n < 1 {
			return typeErr("concurrency_limit must be a positive integer")
		}

		limit = int(min(n, int64(len(jobs))))
	}

	var (
		wg    sync.WaitGroup
		emu   sync.Mutex
		fails []branchErr
		sem   = make(chan struct{}, max(limit, 1))
	)

	for _, j := range jobs {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			br := &rctx{x: r.x, ctx: r.ctx, routine: r.routine, tid: int(r.x.tids.Add(1)), depth: r.depth}
			defer r.x.setFrame(br.tid, nil)

			if err := j.run(br); err != nil {
				emu.Lock()
				fails = append(fails, branchErr{id: j.id, err: br.stampErr(err, "")})
				emu.Unlock()
			}
		})
	}

	wg.Wait()

	return r.branchFailure(fails)
}

func (r *rctx) branchFailure(fails []branchErr) error {
	if err := r.ctx.Err(); err != nil {
		return r.stamp(err, "")
	}

	if len(fails) == 0 {
		return nil
	}

	for _, f := range fails {
		if f.err.fatal || f.err.cancel {
			return f.err
		}
	}

	list := make([]any, len(fails))
	for i, f := range fails {
		list[i] = map[string]any{
			"id":    f.id,
			"error": map[string]any{"context": f.err.Context(), "payload": f.err.Value},
		}
	}

	m := errMap("UnhandledBranchError: One or more branches or iterations encountered an unhandled runtime error",
		TagUnhandledBranch, TagRuntimeError)
	m["branches"] = list
	m["truncated"] = false

	return &Error{Value: m}
}

func (r *rctx) doTry(sc *scope, t *trySpec) (outcome, error) {
	for attempt := int64(0); ; attempt++ {
		out, err := r.runBlock(sc, t.body)
		if err == nil {
			return out, nil
		}

		var e *Error
		if !errors.As(err, &e) || e.fatal || e.cancel {
			return out, err
		}

		retry, delay, rerr := r.retryDecision(sc, t, e, attempt)
		if rerr != nil {
			return outcome{}, rerr
		}

		if retry {
			if serr := r.x.host.clock().Sleep(r.ctx, delay); serr != nil {
				return outcome{}, r.stamp(serr, "")
			}

			continue
		}

		if !t.hasExcept {
			return outcome{}, e
		}

		es := newScope(sc)
		if t.exceptAs != "" {
			r.x.mu.Lock()
			es.vars[t.exceptAs] = deepCopy(e.Value)
			r.x.mu.Unlock()
		}

		return r.runBlock(es, t.except)
	}
}

func (r *rctx) retryDecision(sc *scope, t *trySpec, e *Error, attempt int64) (bool, time.Duration, error) {
	if !t.hasRetry {
		return false, 0, nil
	}

	v, err := r.evalValue(sc, t.retry)
	if err != nil {
		return false, 0, err
	}

	pol, ok := v.(map[string]any)
	if !ok {
		return false, 0, typeErr("retry must be a policy map, not %s", typeName(v))
	}

	ref, ok := pol["predicate"].(*FuncRef)
	if !ok {
		return false, 0, typeErr("retry.predicate must be a function reference")
	}

	maxRetries := int64(defaultMaxRetries)
	if m, isInt := pol["max_retries"].(int64); isInt {
		maxRetries = m
	}

	if attempt >= maxRetries {
		return false, 0, nil
	}

	res, err := r.callRef(ref, deepCopy(e.Value))
	if err != nil {
		return false, 0, err
	}

	if b, isBool := res.(bool); !isBool || !b {
		return false, 0, nil
	}

	return true, backoffDelay(pol["backoff"], attempt), nil
}

func backoffDelay(b any, attempt int64) time.Duration {
	initial, maxDelay, mult := 1.0, float64(defaultMaxDelay), defaultMultiplier

	if m, ok := b.(map[string]any); ok {
		if f, isNum := toFloat(m["initial_delay"]); isNum {
			initial = f
		}

		if f, isNum := toFloat(m["max_delay"]); isNum {
			maxDelay = f
		}

		if f, isNum := toFloat(m["multiplier"]); isNum {
			mult = f
		}
	}

	d := initial
	for i := int64(0); i < attempt && d < maxDelay; i++ {
		d *= mult
	}

	return time.Duration(min(d, maxDelay) * float64(time.Second))
}
