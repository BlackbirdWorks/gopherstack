package bqsql

import (
	"context"
	"slices"
	"strings"
)

// ChildEvent describes one statement a script executed, for child job records.
type ChildEvent struct {
	Result    *Result
	Err       error
	Text      string
	Statement string
}

// ScriptHost applies a script's side effects to live state.
type ScriptHost interface {
	// Apply commits catalog and table changes so later statements see them.
	Apply(ctx context.Context, effects []Effect) error
	BeginTx() error
	CommitTx() error
	RollbackTx() error
	Child(ev ChildEvent)
}

// ScriptResult is the outcome of running a script: the last statement's result and the number of child statements.
type ScriptResult struct {
	Last     *Result
	Children int
}

const (
	maxScriptSteps  = 1_000_000
	maxCallDepth    = 50
	errorParamName  = "error"
	scriptStmtLabel = "SCRIPT"
)

type ctrlKind uint8

const (
	ctrlBreak ctrlKind = iota
	ctrlContinue
	ctrlReturn
)

type ctrl struct {
	label string
	kind  ctrlKind
}

func (c *ctrl) Error() string { return "script control flow escaped its block" }

type runner struct {
	ctx    context.Context
	host   ScriptHost
	base   *Input
	temps  map[string]*RoutineData
	last   *Result
	scopes []map[string]*Param
	steps  int
	calls  int
	kids   int
	inTx   bool
}

// Lookup implements VarScope over the active block scopes.
func (r *runner) Lookup(name string) (*Param, bool) {
	key := strings.ToLower(name)

	for _, scope := range slices.Backward(r.scopes) {
		if p, ok := scope[key]; ok {
			return p, true
		}
	}

	return nil, false
}

// RunScript executes sc against host. A transaction left open when the script ends is rolled back.
func RunScript(ctx context.Context, sc *Script, in *Input, host ScriptHost) (*ScriptResult, error) {
	r := &runner{ctx: ctx, host: host, base: in, temps: map[string]*RoutineData{}}
	r.scopes = []map[string]*Param{{}}

	err := r.block(sc.body)

	if r.inTx {
		if rerr := host.RollbackTx(); err == nil && rerr != nil {
			err = rerr
		}

		r.inTx = false
	}

	var c *ctrl
	if asCtrl(err, &c) {
		if c.kind == ctrlReturn {
			err = nil
		} else {
			err = invalid("BREAK or CONTINUE outside of a loop")
		}
	}

	if err != nil {
		return nil, err
	}

	return &ScriptResult{Last: r.last, Children: r.kids}, nil
}

func asCtrl(err error, out **ctrl) bool {
	c, ok := err.(*ctrl) //nolint:errorlint // ctrl is never wrapped

	if ok {
		*out = c
	}

	return ok
}

func (r *runner) input() *Input {
	in := *r.base
	in.Vars = r
	in.Temp = r.temps

	return &in
}

func (r *runner) tick() error {
	r.steps++

	if r.steps > maxScriptSteps {
		return &Error{Kind: ErrRuntime, Message: "Script exceeded the maximum number of executed statements"}
	}

	return r.ctx.Err()
}

func (r *runner) block(stmts []*scriptStmt) error {
	for _, s := range stmts {
		if err := r.tick(); err != nil {
			return err
		}

		if err := r.stmt(s); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) scoped(stmts []*scriptStmt) error {
	r.scopes = append(r.scopes, map[string]*Param{})

	defer func() { r.scopes = r.scopes[:len(r.scopes)-1] }()

	return r.block(stmts)
}

func (r *runner) stmt(s *scriptStmt) error {
	switch s.kind {
	case skSQL:
		return r.runSQL(s.stmt)
	case skDeclare:
		return r.declare(s)
	case skSet:
		return r.set(s)
	case skIf:
		return r.ifStmt(s)
	case skWhile, skLoop, skRepeat:
		return r.loop(s)
	case skFor:
		return r.forStmt(s)
	case skBlock:
		return r.blockStmt(s)
	case skBreak, skContinue, skReturn:
		return jump(s)
	case skRaise:
		return r.raise(s)
	case skExecute:
		return r.executeImmediate(s)
	case skCall:
		return r.call(s)
	case skAssert:
		return r.assert(s)
	case skBegin, skCommit, skRollback:
		return r.transaction(s.kind)
	}

	return unsupported("script statement")
}

func jump(s *scriptStmt) error {
	switch s.kind { //nolint:exhaustive // only the jump statements reach here
	case skBreak:
		return &ctrl{kind: ctrlBreak, label: firstOr(s.names)}
	case skContinue:
		return &ctrl{kind: ctrlContinue, label: firstOr(s.names)}
	}

	return &ctrl{kind: ctrlReturn}
}

func firstOr(s []string) string {
	if len(s) > 0 {
		return s[0]
	}

	return ""
}

func (r *runner) runSQL(st *Statement) error {
	res, err := Run(r.ctx, st, r.input(), false)
	if err != nil {
		r.host.Child(ChildEvent{Text: st.Text, Err: err})

		return err
	}

	kept := res.Effects[:0:0]

	for _, e := range res.Effects {
		if e.Routine != nil && e.Routine.Temp {
			r.registerTemp(e)

			continue
		}

		kept = append(kept, e)
	}

	if err = r.host.Apply(r.ctx, kept); err != nil {
		r.host.Child(ChildEvent{Text: st.Text, Err: err})

		return err
	}

	r.kids++
	r.last = res
	r.host.Child(ChildEvent{Text: st.Text, Statement: res.Statement, Result: res})

	return nil
}

func (r *runner) registerTemp(e Effect) {
	key := strings.ToLower(e.Ref.Table)

	if e.Op == OpDropRoutine {
		delete(r.temps, key)

		return
	}

	r.temps[key] = e.Routine
}

func (r *runner) evalRaw(n *node) (*Result, error) {
	st := &Statement{
		Kind:  StmtQuery,
		query: &Query{body: &setExpr{sel: &selectStmt{items: []selectItem{{expr: n}}}}},
	}

	return Run(r.ctx, st, r.input(), false)
}

// eval evaluates one expression to its stored value and schema field.
func (r *runner) eval(n *node) (any, Field, error) {
	res, err := r.evalRaw(n)
	if err != nil {
		return nil, Field{}, err
	}

	if len(res.Rows) != 1 || len(res.Fields) != 1 {
		return nil, Field{}, invalid("Expression must return exactly one value")
	}

	return res.Rows[0][0], res.Fields[0], nil
}

func (r *runner) evalBool(n *node) (bool, error) {
	v, f, err := r.eval(n)
	if err != nil {
		return false, err
	}

	if f.Type != "BOOLEAN" && f.Type != litBool {
		return false, invalid("Condition must be of type BOOL, got %s", f.Type)
	}

	b, _ := v.(string)

	return b == litTrueX, nil
}

func (r *runner) declare(s *scriptStmt) error {
	top := r.scopes[len(r.scopes)-1]

	var (
		val any
		fld Field
	)

	switch {
	case s.expr != nil && s.typ != nil:
		cast := &node{kind: nCast, castTo: s.typ, args: []*node{s.expr}, pos: s.pos}

		v, f, err := r.eval(cast)
		if err != nil {
			return err
		}

		val, fld = v, f
	case s.expr != nil:
		v, f, err := r.eval(s.expr)
		if err != nil {
			return err
		}

		val, fld = v, f
	default:
		fld = TypeField("", *s.typ)
	}

	for _, name := range s.names {
		key := strings.ToLower(name)
		if _, dup := top[key]; dup {
			return invalid("Variable %s is already declared in this block", name)
		}

		f := fld
		f.Name = name
		top[key] = &Param{Name: name, Value: val, Field: f}
	}

	return nil
}

func (r *runner) assign(name string, n *node) error {
	p, ok := r.Lookup(name)
	if !ok {
		return invalid("Variable %s has not been declared", name)
	}

	return r.assignValue(p, n)
}

func (r *runner) assignValue(p *Param, n *node) error {
	typ := p.Field.SQLType()
	cast := &node{kind: nCast, castTo: &typ, args: []*node{n}, pos: n.pos}

	v, _, err := r.eval(cast)
	if err != nil {
		return err
	}

	p.Value = v

	return nil
}

func (r *runner) set(s *scriptStmt) error {
	if !s.tuple {
		return r.assign(s.names[0], s.expr)
	}

	if s.exprs != nil {
		for i, name := range s.names {
			if err := r.assign(name, s.exprs[i]); err != nil {
				return err
			}
		}

		return nil
	}

	v, f, err := r.eval(s.expr)
	if err != nil {
		return err
	}

	rec, ok := v.([]any)
	if f.Type != litRecord || !ok || len(rec) != len(s.names) {
		return invalid("SET (...) requires a STRUCT with %d fields", len(s.names))
	}

	for i, name := range s.names {
		p, found := r.Lookup(name)
		if !found {
			return invalid("Variable %s has not been declared", name)
		}

		sub := f.Fields[i]
		sub.Name = name
		lit := &Param{Name: setVarName, Value: rec[i], Field: sub}

		if err = r.assignParam(p, lit); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) assignParam(p, src *Param) error {
	holder := &runner{ctx: r.ctx, host: r.host, base: r.base, temps: r.temps}
	holder.scopes = []map[string]*Param{{setVarName: src}}

	return holder.assignValue(p, &node{kind: nPath, parts: []string{setVarName}})
}

func (r *runner) ifStmt(s *scriptStmt) error {
	for i, cond := range s.conds {
		ok, err := r.evalBool(cond)
		if err != nil {
			return err
		}

		if ok {
			return r.scoped(s.blocks[i])
		}
	}

	return r.scoped(s.body)
}

func (r *runner) loop(s *scriptStmt) error {
	for {
		if err := r.tick(); err != nil {
			return err
		}

		if s.kind == skWhile {
			if stop, err := r.stopLoop(s.expr, false); stop || err != nil {
				return err
			}
		}

		done, err := r.iteration(s, s.body)
		if err != nil || done {
			return err
		}

		if s.kind == skRepeat {
			if stop, cerr := r.stopLoop(s.expr, true); stop || cerr != nil {
				return cerr
			}
		}
	}
}

// stopLoop evaluates a loop condition; a WHILE stops when it is false, a REPEAT when its UNTIL is true.
func (r *runner) stopLoop(cond *node, stopWhen bool) (bool, error) {
	ok, err := r.evalBool(cond)

	return ok == stopWhen, err
}

// iteration runs one pass of a loop body; done is true when BREAK or LEAVE ends the loop.
func (r *runner) iteration(s *scriptStmt, body []*scriptStmt) (bool, error) {
	err := r.scoped(body)

	var c *ctrl
	if !asCtrl(err, &c) {
		return false, err
	}

	if c.kind == ctrlReturn || c.label != "" && !strings.EqualFold(c.label, s.label) {
		return false, err
	}

	return c.kind == ctrlBreak, nil
}

func (r *runner) forStmt(s *scriptStmt) error {
	st := &Statement{Kind: StmtQuery, query: s.query}

	res, err := Run(r.ctx, st, r.input(), false)
	if err != nil {
		return err
	}

	rec := Field{Name: s.names[0], Type: litRecord, Mode: ModeNullable, Fields: res.Fields}

	for _, row := range res.Rows {
		if err = r.tick(); err != nil {
			return err
		}

		r.scopes = append(
			r.scopes,
			map[string]*Param{strings.ToLower(s.names[0]): {Name: s.names[0], Value: row, Field: rec}},
		)
		done, ierr := r.iteration(s, s.body)
		r.scopes = r.scopes[:len(r.scopes)-1]

		if ierr != nil || done {
			return ierr
		}
	}

	return nil
}

func (r *runner) blockStmt(s *scriptStmt) error {
	err := r.scoped(s.body)

	var c *ctrl

	if err == nil || asCtrl(err, &c) {
		if c != nil && c.kind != ctrlReturn && c.label != "" && strings.EqualFold(c.label, s.label) {
			return nil
		}

		return err
	}

	if !s.hasHandler {
		return err
	}

	e := AsError(err)
	r.scopes = append(r.scopes, map[string]*Param{})

	defer func() { r.scopes = r.scopes[:len(r.scopes)-1] }()

	saved := r.base.Params
	r.base = withErrorParam(r.base, e.Message, s)

	defer func() { r.base.Params = saved }()

	return r.block(s.handler)
}

func withErrorParam(in *Input, msg string, s *scriptStmt) *Input {
	cp := *in

	fields := []Field{
		{Name: "message", Type: litString, Mode: ModeNullable},
		{Name: "statement_text", Type: litString, Mode: ModeNullable},
		{Name: "formatted_stack_trace", Type: litString, Mode: ModeNullable},
		{Name: "stack_trace", Type: litRecord, Mode: ModeRepeated, Fields: []Field{
			{Name: "line", Type: litInteger, Mode: ModeNullable},
			{Name: "column", Type: litInteger, Mode: ModeNullable},
			{Name: "filename", Type: litString, Mode: ModeNullable},
			{Name: "location", Type: litString, Mode: ModeNullable},
		}},
	}

	cp.Params = append(append([]Param{}, in.Params...), Param{
		Name: errorParamName, Value: []any{msg, "", "", []any{}},
		Field: Field{Name: errorParamName, Type: litRecord, Mode: ModeNullable, Fields: fields},
	})

	_ = s

	return &cp
}

func (r *runner) raise(s *scriptStmt) error {
	if s.expr == nil {
		if p, ok := r.errorParam(); ok {
			return &Error{Kind: ErrRuntime, Message: p}
		}

		return invalid("RAISE without a message is only allowed in an exception handler")
	}

	v, _, err := r.eval(s.expr)
	if err != nil {
		return err
	}

	msg, _ := v.(string)

	return &Error{Kind: ErrRuntime, Message: msg}
}

func (r *runner) errorParam() (string, bool) {
	for i := range r.base.Params {
		if r.base.Params[i].Name == errorParamName {
			if rec, ok := r.base.Params[i].Value.([]any); ok && len(rec) > 0 {
				msg, _ := rec[0].(string)

				return msg, true
			}
		}
	}

	return "", false
}

func (r *runner) assert(s *scriptStmt) error {
	ok, err := r.evalBool(s.expr)
	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	msg := "Assertion failed"

	if len(s.exprs) > 0 {
		v, _, derr := r.eval(s.exprs[0])
		if derr != nil {
			return derr
		}

		if d, isStr := v.(string); isStr {
			msg += ": " + d
		}
	}

	return &Error{Kind: ErrRuntime, Message: msg}
}

func (r *runner) transaction(kind scriptKind) error {
	if kind == skBegin {
		if r.inTx {
			return invalid("Transactions may not be nested")
		}

		if err := r.host.BeginTx(); err != nil {
			return err
		}

		r.inTx = true

		return nil
	}

	if !r.inTx {
		return invalid("COMMIT or ROLLBACK requires an active transaction")
	}

	r.inTx = false

	if kind == skCommit {
		return r.host.CommitTx()
	}

	return r.host.RollbackTx()
}

func (r *runner) executeImmediate(s *scriptStmt) error {
	v, f, err := r.eval(s.expr)
	if err != nil {
		return err
	}

	text, ok := v.(string)
	if !ok || f.Type != litString {
		return invalid("EXECUTE IMMEDIATE requires a STRING expression")
	}

	st, err := ParseStatement(text)
	if err != nil {
		return err
	}

	params := make([]Param, 0, len(s.exprs))

	for i, e := range s.exprs {
		val, fld, eerr := r.eval(e)
		if eerr != nil {
			return eerr
		}

		fld.Name = s.usingNames[i]
		params = append(params, Param{Name: s.usingNames[i], Value: val, Field: fld})
	}

	in := *r.base
	in.Params = params
	in.Temp = r.temps

	res, err := Run(r.ctx, st, &in, false)
	if err != nil {
		r.host.Child(ChildEvent{Text: text, Err: err})

		return err
	}

	if err = r.host.Apply(r.ctx, res.Effects); err != nil {
		return err
	}

	r.kids++
	r.last = res
	r.host.Child(ChildEvent{Text: text, Statement: res.Statement, Result: res})

	return r.intoVars(s, res)
}

func (r *runner) intoVars(s *scriptStmt, res *Result) error {
	if len(s.names) == 0 {
		return nil
	}

	if len(res.Rows) != 1 || len(res.Fields) != len(s.names) {
		return invalid("EXECUTE IMMEDIATE INTO requires a single row with %d columns", len(s.names))
	}

	for i, name := range s.names {
		p, ok := r.Lookup(name)
		if !ok {
			return invalid("Variable %s has not been declared", name)
		}

		if err := r.assignParam(p, &Param{Name: setVarName, Value: res.Rows[0][i], Field: res.Fields[i]}); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) call(s *scriptStmt) error {
	if isRefreshCall(s.target) {
		return r.refreshView(s)
	}

	if r.calls >= maxCallDepth {
		return invalid("Procedure calls are nested too deeply")
	}

	c := newCompiler(r.input())

	rt, ok, err := c.findRoutine(s.target)
	if err != nil {
		return err
	}

	if !ok || rt.Kind != RoutineProcedure {
		return notFound("Not found: Procedure %s", strings.Join(s.target, "."))
	}

	if len(s.exprs) != len(rt.Args) {
		return invalid("Procedure %s expects %d arguments, got %d", rt.Ref.Table, len(rt.Args), len(s.exprs))
	}

	body, err := ParseScript(procedureScript(rt.Body))
	if err != nil {
		return err
	}

	frame, outs, err := r.procedureFrame(rt, s)
	if err != nil {
		return err
	}

	sub := &runner{
		ctx:   r.ctx,
		host:  r.host,
		base:  r.procedureInput(rt),
		temps: r.temps,
		steps: r.steps,
		calls: r.calls + 1,
	}
	sub.scopes = []map[string]*Param{frame}
	sub.inTx = r.inTx

	err = sub.block(body.body)

	var cc *ctrl
	if asCtrl(err, &cc) && cc.kind == ctrlReturn {
		err = nil
	}

	r.steps, r.kids, r.inTx = sub.steps, r.kids+sub.kids, sub.inTx

	if sub.last != nil {
		r.last = sub.last
	}

	if err != nil {
		return err
	}

	for name, target := range outs {
		if err = r.assignParam(target, frame[name]); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) procedureInput(rt *RoutineData) *Input {
	in := *r.base
	if !rt.Temp {
		in.Project, in.Dataset = rt.Ref.Project, rt.Ref.Dataset
	}

	return &in
}

// procedureFrame binds a CALL's arguments; OUT and INOUT arguments must be variables and are copied back afterwards.
func (r *runner) procedureFrame(rt *RoutineData, s *scriptStmt) (map[string]*Param, map[string]*Param, error) {
	frame := map[string]*Param{}
	outs := map[string]*Param{}

	for i, a := range rt.Args {
		key := strings.ToLower(a.Name)
		p := &Param{Name: a.Name, Field: TypeField(a.Name, a.Type)}

		if a.Mode == ModeOut || a.Mode == ModeInOut {
			arg := s.exprs[i]
			if arg.kind != nPath || len(arg.parts) != 1 {
				return nil, nil, invalid("Argument %s of procedure %s must be a variable", a.Name, rt.Ref.Table)
			}

			target, ok := r.Lookup(arg.parts[0])
			if !ok {
				return nil, nil, invalid("Variable %s has not been declared", arg.parts[0])
			}

			outs[key] = target

			if a.Mode == ModeInOut {
				p.Value = target.Value
			}
		}

		if a.Mode == "" || a.Mode == ModeIn {
			if err := r.assignValue(p, s.exprs[i]); err != nil {
				return nil, nil, err
			}
		}

		frame[key] = p
	}

	return frame, outs, nil
}

func isRefreshCall(target []string) bool {
	return len(target) == 2 && strings.EqualFold(target[0], "BQ") &&
		strings.EqualFold(target[1], "REFRESH_MATERIALIZED_VIEW")
}

// refreshView implements CALL BQ.REFRESH_MATERIALIZED_VIEW('project.dataset.view').
func (r *runner) refreshView(s *scriptStmt) error {
	if len(s.exprs) != 1 {
		return invalid("BQ.REFRESH_MATERIALIZED_VIEW expects one STRING argument")
	}

	v, f, err := r.eval(s.exprs[0])
	if err != nil {
		return err
	}

	name, ok := v.(string)
	if !ok || f.Type != litString {
		return invalid("BQ.REFRESH_MATERIALIZED_VIEW expects a STRING argument")
	}

	c := newCompiler(r.input())

	ref, err := c.resolveRef(strings.Split(strings.Trim(name, "`"), "."))
	if err != nil {
		return err
	}

	td, err := c.lookupTable(ref)
	if err != nil {
		return err
	}

	if !td.Materialized {
		return invalid("%s is not a materialized view", ref)
	}

	r.kids++
	r.host.Child(ChildEvent{
		Text:      "CALL BQ.REFRESH_MATERIALIZED_VIEW('" + name + "')",
		Statement: "CALL",
		Result:    &Result{Statement: "CALL"},
	})

	return r.host.Apply(r.ctx, []Effect{{Op: OpRefreshView, Ref: ref}})
}
