package azurearm

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	logicCodeActionFailed   = "ActionFailed"
	logicMsgActionFailed    = "An action failed. No dependent actions succeeded."
	logicCodeSkipped        = "ActionSkipped"
	logicCodeBranchSkipped  = "ActionBranchingConditionNotSatisfied"
	logicCodeConditionFail  = "ActionConditionFailed"
	logicCodeInvalidTmpl    = "InvalidTemplate"
	logicCodeNotImplemented = "NotImplemented"
)

type logicResponse struct {
	headers map[string]string
	body    any
	status  int
}

type logicLive struct {
	cancel    context.CancelFunc
	done      chan struct{}
	resp      chan logicResponse
	cancelled atomic.Bool
	sent      atomic.Bool
}

type logicLiveSet []*logicLive

func (s logicLiveSet) cancelAll() {
	for _, l := range s {
		l.cancel()
	}
}

type logicTermination struct {
	err    any
	status string
}

type logicVar struct {
	value any
	typ   string
}

type logicFrame struct {
	item  any
	name  string
	index int
	until bool
}

// logicEnv is one lexical level of a run: loop iterations get their own so repeated actions do not collide.
type logicEnv struct {
	parent *logicEnv
	local  map[string]*logicActionRec
	frame  *logicFrame
	inLoop bool
}

func (e *logicEnv) child(frame *logicFrame, inLoop bool) *logicEnv {
	return &logicEnv{parent: e, local: map[string]*logicActionRec{}, frame: frame, inLoop: inLoop || e.inLoop}
}

// logicRunState is the engine's view of one run.
type logicRunState struct {
	ctx     context.Context
	params  map[string]any
	live    *logicLive
	run     *logicRunRec
	stop    context.CancelFunc
	vars    map[string]*logicVar
	p       *LogicProvider
	trigger map[string]any
	info    map[string]any
	term    *logicTermination
	clock   func() time.Time
	def     map[string]any
	iters   map[string][]*logicEnv
	wfKey   string
	wfName  string
}

type logicEvalCtx struct {
	rs  *logicRunState
	env *logicEnv
	now func() time.Time
}

func (rs *logicRunState) evalCtx(env *logicEnv) *logicEvalCtx {
	return &logicEvalCtx{rs: rs, env: env, now: rs.clock}
}

func (rs *logicRunState) eval(env *logicEnv, v any) (any, error) {
	return evalLogicValue(v, rs.evalCtx(env))
}

type logicOutcome struct {
	err     any
	inputs  any
	outputs any
	status  string
	code    string
	hasIn   bool
	hasOut  bool
}

func failOutcome(code, msg string) logicOutcome {
	return logicOutcome{status: logicStFailed, code: code, err: map[string]any{logicKCode: code, fieldMessage: msg}}
}

func exprFailure(name string, err error) logicOutcome {
	return failOutcome(
		logicCodeInvalidTmpl,
		"Unable to process template language expressions for action '"+name+"': "+strings.TrimPrefix(
			err.Error(),
			"logic expression error: ",
		),
	)
}

func okOutcome() logicOutcome { return logicOutcome{status: logicStSucceeded} }

func (rs *logicRunState) begin(env *logicEnv, name, typ string) *logicActionRec {
	rec := &logicActionRec{
		Name: name, Type: typ, Status: logicStRunning, Start: rs.clock().UTC(), Tracking: uuid.NewString(),
	}

	rs.p.mu.Lock("begin")
	env.local[name] = rec

	if !env.inLoop {
		rs.run.Actions[name] = rec
		rs.run.Order = append(rs.run.Order, name)
	}

	rs.p.mu.Unlock()

	return rec
}

func (rs *logicRunState) finish(rec *logicActionRec, out logicOutcome) {
	end := rs.clock().UTC()

	rs.p.mu.Lock("finish")
	rec.Status, rec.Code, rec.Error, rec.End = out.status, out.code, out.err, end

	if out.hasIn {
		rec.Inputs = logicContent{Value: out.inputs, Set: true}
	}

	if out.hasOut {
		rec.Outputs = logicContent{Value: out.outputs, Set: true}
	}

	rs.p.mu.Unlock()
}

func (rs *logicRunState) addAggregate(env *logicEnv, rec *logicActionRec) {
	rs.p.mu.Lock("addAggregate")
	env.local[rec.Name] = rec

	if !env.inLoop {
		rs.run.Actions[rec.Name] = rec
		rs.run.Order = append(rs.run.Order, rec.Name)
	}

	rs.p.mu.Unlock()
}

// execActions runs a scope's actions honoring runAfter; independent actions run in parallel.
func (rs *logicRunState) execActions(env *logicEnv, defs map[string]any) map[string]string {
	done := make(map[string]chan struct{}, len(defs))
	for name := range defs {
		done[name] = make(chan struct{})
	}

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		statuses = make(map[string]string, len(defs))
	)

	setStatus := func(name, st string) {
		mu.Lock()
		statuses[name] = st
		mu.Unlock()
	}

	getStatus := func(name string) string {
		mu.Lock()
		defer mu.Unlock()

		return statuses[name]
	}

	for name, a := range defs {
		def, _ := objectOf(a)

		wg.Go(func() {
			defer close(done[name])

			ra := logicRunAfterOf(def)
			for dep := range ra {
				<-done[dep]
			}

			setStatus(name, rs.runOne(env, name, def, ra, getStatus))
		})
	}

	wg.Wait()

	return statuses
}

func (rs *logicRunState) runOne(
	env *logicEnv, name string, def map[string]any, ra map[string][]string, status func(string) string,
) string {
	typ := stringOf(def[fieldType])

	if unmet := unmetRunAfter(ra, status); unmet != "" {
		rec := rs.skipped(env, name, typ, logicCodeConditionFail, unmet)
		rs.skipChildren(env, typ, def, logicCodeConditionFail, unmet)

		return rec.Status
	}

	if rs.ctx.Err() != nil {
		rec := rs.skipped(env, name, typ, logicCodeSkipped, "")
		rs.skipChildren(env, typ, def, logicCodeSkipped, "")

		return rec.Status
	}

	rec := rs.begin(env, name, typ)
	out := rs.dispatch(env, name, strings.ToLower(typ), def)
	rs.finish(rec, out)

	return out.status
}

func unmetRunAfter(ra map[string][]string, status func(string) string) string {
	for dep, allowed := range ra {
		got := status(dep)
		ok := false

		for _, a := range allowed {
			if strings.EqualFold(a, got) {
				ok = true
			}
		}

		if !ok {
			return "The execution of template action is skipped: the 'runAfter' condition for action '" + dep +
				"' is not satisfied. Expected status values '" + strings.Join(allowed, ",") +
				"' and actual value '" + got + "'."
		}
	}

	return ""
}

func (rs *logicRunState) skipped(env *logicEnv, name, typ, code, msg string) *logicActionRec {
	now := rs.clock().UTC()
	rec := &logicActionRec{
		Name: name, Type: typ, Status: logicStSkipped, Code: code, Start: now, End: now, Tracking: uuid.NewString(),
	}

	if msg != "" {
		rec.Error = map[string]any{logicKCode: code, fieldMessage: msg}
	}

	rs.addAggregate(env, rec)

	return rec
}

func (rs *logicRunState) skipChildren(env *logicEnv, typ string, def map[string]any, code, msg string) {
	for _, child := range logicChildScopes(strings.ToLower(typ), def) {
		rs.skipAll(env, child, code, msg)
	}
}

func (rs *logicRunState) skipAll(env *logicEnv, defs map[string]any, code, msg string) {
	for name, a := range defs {
		def, _ := objectOf(a)
		typ := stringOf(def[fieldType])

		rs.skipped(env, name, typ, code, msg)
		rs.skipChildren(env, typ, def, code, msg)
	}
}

// scopeStatus derives a container's status: an action that failed (or timed out) with no executed handler (an
// action whose runAfter lists that status) fails the scope.
func scopeStatus(defs map[string]any, statuses map[string]string) (string, any) {
	failed, timedOut, cancelled := false, false, false

	for name, st := range statuses {
		switch st {
		case logicStFailed:
			failed = failed || !handled(defs, statuses, name, st)
		case logicStTimedOut:
			timedOut = timedOut || !handled(defs, statuses, name, st)
		case logicStCancelled:
			cancelled = true
		}
	}

	switch {
	case failed:
		return logicStFailed, map[string]any{logicKCode: logicCodeActionFailed, fieldMessage: logicMsgActionFailed}
	case timedOut:
		return logicStTimedOut, nil
	case cancelled:
		return logicStCancelled, nil
	}

	return logicStSucceeded, nil
}

func handled(defs map[string]any, statuses map[string]string, name, status string) bool {
	for other, a := range defs {
		if statuses[other] == logicStSkipped || statuses[other] == "" {
			continue
		}

		def, _ := objectOf(a)
		for _, allowed := range logicRunAfterOf(def)[name] {
			if strings.EqualFold(allowed, status) {
				return true
			}
		}
	}

	return false
}

func containerOutcome(defs map[string]any, statuses map[string]string) logicOutcome {
	st, err := scopeStatus(defs, statuses)
	out := logicOutcome{status: st, err: err}

	if st == logicStFailed {
		out.code = logicCodeActionFailed
	}

	return out
}

func (rs *logicRunState) dispatch(env *logicEnv, name, typ string, def map[string]any) logicOutcome {
	switch typ {
	case "compose":
		return rs.runCompose(env, name, def)
	case logicSchemeHTTP:
		return rs.runHTTP(env, name, def)
	case logicActResponse:
		return rs.runResponse(env, name, def)
	case logicActTerminate:
		return rs.runTerminate(env, name, def)
	case "wait":
		return rs.runWait(env, name, def)
	case logicActScope:
		return rs.runScope(env, def)
	case logicActIf:
		return rs.runIf(env, name, def)
	case logicActSwitch:
		return rs.runSwitch(env, name, def)
	case logicActForeach:
		return rs.runForeach(env, name, def)
	case logicActUntil:
		return rs.runUntil(env, name, def)
	case "parsejson", "select", "query", "join", logicTypeTable:
		return rs.runDataOp(env, name, typ, def)
	}

	if strings.HasSuffix(typ, "variable") {
		return rs.runVariable(env, name, typ, def)
	}

	return failOutcome(logicCodeNotImplemented, "The action type '"+stringOf(def[fieldType])+
		"' is not supported by this emulator; the action did not run.")
}

func (rs *logicRunState) runScope(env *logicEnv, def map[string]any) logicOutcome {
	actions, _ := objectOf(def[logicKActions])

	return containerOutcome(actions, rs.execActions(env, actions))
}

// sleep waits d on the injected clock; it reports false if the run was stopped first.
func (rs *logicRunState) sleep(d time.Duration) bool {
	if d <= 0 {
		return rs.ctx.Err() == nil
	}

	t := rs.p.currentClock().NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C():
		return true
	case <-rs.ctx.Done():
		return false
	}
}
