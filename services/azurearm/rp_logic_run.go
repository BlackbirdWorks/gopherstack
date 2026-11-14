package azurearm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
)

type logicFire struct {
	scheduled time.Time
	inputs    any
	outputs   any
	trigger   string
	client    string
	hasInputs bool
	respond   bool
}

type logicStarted struct {
	live    *logicLive
	runName string
}

func (p *LogicProvider) currentClock() gcpscale.Clock {
	p.mu.RLock("clock")
	defer p.mu.RUnlock()

	return p.clock
}

// startRunLocked records a run and its trigger history and launches the engine; caller holds p.mu.
func (p *LogicProvider) startRunLocked(w *logicWorkflow, f logicFire) (*logicStarted, error) {
	ts, ok := w.Triggers[f.trigger]
	if !ok {
		return nil, ErrLogicTriggerNotFound
	}

	if w.State != logicStateEnabled || ts.State != logicStateEnabled {
		return nil, logicBadRequest(
			"WorkflowDisabled",
			"The workflow '%s' or trigger '%s' is disabled.",
			w.Name,
			f.trigger,
		)
	}

	now := p.clock.Now().UTC()
	name := p.newRunName(now)
	tracking := uuid.NewString()
	trig := logicRunTrigger{
		Name: f.trigger, Start: now, End: now, Scheduled: f.scheduled, Status: logicStSucceeded, Tracking: tracking,
		Outputs: logicContent{Value: f.outputs, Set: true},
	}

	if f.hasInputs {
		trig.Inputs = logicContent{Value: f.inputs, Set: true}
	}

	run := &logicRunRec{
		Name: name, Status: logicStRunning, Start: now, Version: w.Version, Correlation: uuid.NewString(),
		ClientID: f.client, Actions: map[string]*logicActionRec{}, Trigger: trig,
	}

	w.Runs = append(w.Runs, run)
	w.Histories = append(w.Histories, &logicHistory{
		Name: name, Trigger: f.trigger, Start: now, End: now, Scheduled: f.scheduled, Status: logicStSucceeded,
		Fired: true, Run: name, Tracking: tracking, Outputs: trig.Outputs, Inputs: trig.Inputs,
	})
	trimLogicHistory(w)

	last := now
	ts.Last = &last

	ctx, cancel := context.WithCancel(p.rootCtx)
	live := &logicLive{cancel: cancel, done: make(chan struct{})}

	if f.respond {
		live.resp = make(chan logicResponse, 1)
	}

	runCtx, stop := context.WithCancel(ctx)
	rs := &logicRunState{
		p: p, run: run, live: live, ctx: runCtx, stop: stop, vars: map[string]*logicVar{},
		params: effectiveLogicParams(w), wfKey: w.key(), def: w.Definition, clock: p.clock.Now, wfName: w.Name,
		trigger: map[string]any{
			fieldName: f.trigger, "outputs": f.outputs, logicKStartTime: logicTime(now), logicKEndTime: logicTime(now),
			logicKScheduledTime: logicTime(f.scheduled), logicKStatus: logicStSucceeded, logicKTrackingID: tracking,
		},
		info: map[string]any{
			"id":          w.armID(),
			fieldName:     w.Name,
			fieldType:     namespaceMicrosoftLogic + "/" + logicWorkflowsType,
			fieldLocation: w.Location,
			"tags":        tagsAny(w.Tags),
			"run": map[string]any{
				"id":      w.armID() + "/runs/" + name,
				fieldName: name,
				fieldType: "Microsoft.Logic/workflows/runs",
			},
		},
	}

	if f.hasInputs {
		rs.trigger["inputs"] = f.inputs
	}

	p.live[w.key()+"|"+name] = live

	p.wg.Go(func() {
		defer cancel()

		rs.execute()
	})

	return &logicStarted{runName: name, live: live}, nil
}

func tagsAny(t map[string]string) map[string]any {
	out := make(map[string]any, len(t))
	for k, v := range t {
		out[k] = v
	}

	return out
}

func trimLogicHistory(w *logicWorkflow) {
	for len(w.Runs) > logicMaxRuns && w.Runs[0].Status != logicStRunning {
		w.Runs = w.Runs[1:]
	}

	if len(w.Histories) > logicMaxHistories {
		w.Histories = w.Histories[len(w.Histories)-logicMaxHistories:]
	}
}

// effectiveLogicParams merges definition defaults with values supplied in properties.parameters.
func effectiveLogicParams(w *logicWorkflow) map[string]any {
	out := map[string]any{}
	defs, _ := objectOf(w.Definition["parameters"])

	for name, d := range defs {
		dm, _ := objectOf(d)
		if v, has := dm["defaultValue"]; has {
			out[name] = normLogic(v)
		}
	}

	for name, pv := range w.Parameters {
		pm, _ := objectOf(pv)
		if v, has := pm["value"]; has {
			out[name] = normLogic(v)
		}
	}

	return out
}

// execute runs the definition's actions and records the final run, trigger and history state.
func (rs *logicRunState) execute() {
	defer close(rs.live.done)
	defer rs.stop()

	actions, _ := objectOf(rs.def[logicKActions])
	env := &logicEnv{local: map[string]*logicActionRec{}}
	statuses := rs.execActions(env, actions)

	st, errObj := scopeStatus(actions, statuses)
	code := codeFor(st)

	rs.p.mu.Lock("finalizeRun")
	defer rs.p.mu.Unlock()

	switch {
	case rs.term != nil:
		st, errObj = rs.term.status, rs.term.err
		code = codeFor(st)
	case rs.live.cancelled.Load():
		st, errObj, code = logicStCancelled, nil, ""
	}

	rs.run.Status, rs.run.Error, rs.run.Code = st, errObj, code
	rs.run.End = rs.clock().UTC()

	delete(rs.p.live, rs.wfKey+"|"+rs.run.Name)
}

func (p *LogicProvider) fireRecurrence(wfKey, trigger string, scheduled time.Time) {
	p.mu.Lock("fireRecurrence")
	defer p.mu.Unlock()

	w, ok := p.wf[wfKey]
	if !ok {
		return
	}

	ts, ok := w.Triggers[trigger]
	if !ok || ts.Next == nil || !ts.Next.Equal(scheduled) {
		return
	}

	_, _ = p.startRunLocked(w, logicFire{trigger: trigger, scheduled: scheduled, outputs: map[string]any{}})
	p.armTriggerLocked(w, trigger, scheduled)
}

func (p *LogicProvider) armWorkflowLocked(w *logicWorkflow) {
	defs, _ := objectOf(w.Definition[logicSegTriggers])
	for name, t := range defs {
		if typeOf(t) == logicTrigRecurrence {
			p.armTriggerLocked(w, name, p.clock.Now().Add(-time.Nanosecond))
		}
	}
}

// armTriggerLocked schedules the first occurrence strictly after the given instant.
func (p *LogicProvider) armTriggerLocked(w *logicWorkflow, name string, after time.Time) {
	key := w.key() + "|" + name

	if old, ok := p.timers[key]; ok {
		old.Stop()
		delete(p.timers, key)
	}

	ts := w.Triggers[name]
	ts.Next = nil

	if w.State != logicStateEnabled || ts.State != logicStateEnabled {
		return
	}

	defs, _ := objectOf(w.Definition[logicSegTriggers])
	def, _ := objectOf(defs[name])

	next, ok := parseLogicRecurrence(def, ts.Anchor).next(after)
	if !ok {
		return
	}

	ts.Next = &next
	wfKey := w.key()

	p.timers[key] = p.clock.AfterFunc(max(next.Sub(p.clock.Now()), 0), func() { p.fireRecurrence(wfKey, name, next) })
}

// ResetTrigger re-anchors a recurrence trigger at the current time.
func (p *LogicProvider) resetTriggerLocked(w *logicWorkflow, name string) error {
	if w.triggerType(name) != logicTrigRecurrence {
		return logicBadRequest(
			"InvalidTriggerType",
			"The trigger '%s' is not a polling or recurrence trigger and cannot be reset.",
			name,
		)
	}

	ts := w.Triggers[name]
	ts.Anchor, ts.Last = p.clock.Now().UTC(), nil
	p.armTriggerLocked(w, name, p.clock.Now().Add(-time.Nanosecond))

	return nil
}

// runTriggerLocked fires a recurrence trigger on demand.
func (p *LogicProvider) runTriggerLocked(w *logicWorkflow, name string) error {
	if w.triggerType(name) != logicTrigRecurrence {
		return logicBadRequest(
			"InvalidTriggerType",
			"The trigger '%s' cannot be run manually; only recurrence triggers can.",
			name,
		)
	}

	_, err := p.startRunLocked(w, logicFire{trigger: name, scheduled: p.clock.Now().UTC(), outputs: map[string]any{}})

	return err
}

// CancelRun cancels a running workflow run.
func (p *LogicProvider) cancelRunLocked(w *logicWorkflow, run string) (*logicLive, error) {
	for _, r := range w.Runs {
		if !strings.EqualFold(r.Name, run) {
			continue
		}

		l, ok := p.live[w.key()+"|"+r.Name]
		if !ok || r.Status != logicStRunning {
			return nil, logicBadRequest(
				"RunNotRunning",
				"The workflow run '%s' is not running (status %s).",
				run,
				r.Status,
			)
		}

		return l, nil
	}

	return nil, ErrLogicRunNotFound
}

func logicCancelRun(l *logicLive) {
	l.cancelled.Store(true)
	l.cancel()
}

func (p *LogicProvider) findWorkflow(sub, rg, name string) (*logicWorkflow, error) {
	w, ok := p.wf[logicKey(sub, rg, name)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLogicWorkflowNotFound, name)
	}

	return w, nil
}
