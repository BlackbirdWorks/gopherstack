package azurearm

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	logicTrigHTTP      = logicSchemeHTTP
	logicMaxRetryAfter = 24 * time.Hour
)

func isPollingTrigger(typ string) bool { return typ == logicTrigRecurrence || typ == logicTrigHTTP }

func validateLogicHTTPTrigger(name string, m map[string]any) error {
	in, _ := objectOf(m["inputs"])

	switch strings.ToUpper(stringOf(in["method"])) {
	case http.MethodGet, http.MethodPut, http.MethodPost, http.MethodPatch, http.MethodDelete, http.MethodHead:
	default:
		return invalidTemplate("the trigger '%s' of type 'Http' requires a valid 'method' in its inputs", name)
	}

	if stringOf(in["uri"]) == "" {
		return invalidTemplate("the trigger '%s' of type 'Http' requires 'uri' in its inputs", name)
	}

	return validateLogicRecurrence(name, m)
}

type logicPollPlan struct {
	def     map[string]any
	params  map[string]any
	wfDef   map[string]any
	pollURL string
	wfName  string
}

func (p *LogicProvider) pollPlanLocked(wfKey, trigger string, scheduled time.Time, manual bool) (logicPollPlan, bool) {
	w, ok := p.wf[wfKey]
	if !ok {
		return logicPollPlan{}, false
	}

	ts, ok := w.Triggers[trigger]
	if !ok || w.triggerType(trigger) != logicTrigHTTP {
		return logicPollPlan{}, false
	}

	if !manual && (ts.Next == nil || !ts.Next.Equal(scheduled)) {
		return logicPollPlan{}, false
	}

	return logicPollPlan{
		def: w.triggerDef(trigger), params: effectiveLogicParams(w), pollURL: ts.PollURL, wfName: w.Name,
		wfDef: w.Definition,
	}, true
}

// pollState is a run state for the trigger's outbound call: it has its own lock so the call never holds the
// provider's.
func (p *LogicProvider) pollState(plan logicPollPlan, clock func() time.Time) *logicRunState {
	scratch := &LogicProvider{mu: lockmetrics.New("azurearm.logicpoll"), clock: p.currentClock()}
	scratch.http.Store(p.http.Load())

	return &logicRunState{
		p: scratch, ctx: p.rootCtx, run: &logicRunRec{}, params: plan.params, vars: map[string]*logicVar{},
		def: plan.wfDef, clock: clock, wfName: plan.wfName, trigger: map[string]any{},
	}
}

type logicPollResult struct {
	err      error
	inputs   map[string]any
	resp     *logicHTTPResponse
	location string
}

func (p *LogicProvider) pollOnce(plan logicPollPlan, trigger string) logicPollResult {
	rs := p.pollState(plan, p.currentClock().Now)
	env := &logicEnv{local: map[string]*logicActionRec{}}

	in, err := rs.evalInputs(env, trigger, plan.def)
	if err != nil {
		return logicPollResult{err: err}
	}

	pol, err := parseRetryPolicy(in)
	if err != nil {
		return logicPollResult{err: err}
	}

	req, err := rs.buildHTTPRequest(in, plan.def)
	if err != nil {
		return logicPollResult{err: err}
	}

	req.async = false

	if plan.pollURL != "" {
		req = followPollURL(req, plan.pollURL)
	}

	resp, _, err := rs.doHTTP(req, pol, logicHTTPTimeout)
	res := logicPollResult{err: err, inputs: httpInputs(in), resp: resp}

	if resp != nil {
		res.location = asyncLocation(req.url, resp.headerValue("Location"))
	}

	return res
}

func (r *logicHTTPResponse) headerValue(name string) any {
	for k, v := range r.headers {
		if strings.EqualFold(k, name) {
			return v
		}
	}

	return nil
}

// followPollURL points req at the Location of the previous poll. Another origin gets a bare GET with only the
// workflow headers and no client certificate, so the credentials of the configured endpoint never leave it.
func followPollURL(req *logicHTTPRequest, next string) *logicHTTPRequest {
	if sameOrigin(req.url, next) {
		req.url = next

		return req
	}

	return &logicHTTPRequest{
		method: http.MethodGet, url: next, header: pollHeaders(req, next), async: false,
	}
}

// retryAfter reads Retry-After as delay-seconds or an HTTP-date, clamped to logicMaxRetryAfter.
func retryAfter(r *logicHTTPResponse, now time.Time) time.Duration {
	raw, _ := r.headerValue("Retry-After").(string)
	raw = strings.TrimSpace(raw)

	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if n < 0 {
			return 0
		}

		return min(time.Duration(min(n, int64(logicMaxRetryAfter/time.Second)))*time.Second, logicMaxRetryAfter)
	}

	at, err := http.ParseTime(raw)
	if err != nil {
		return 0
	}

	return min(max(at.Sub(now), 0), logicMaxRetryAfter)
}

// firePoll runs one poll of an Http trigger outside the provider lock, then records its result under it.
func (p *LogicProvider) firePoll(wfKey, trigger string, scheduled time.Time, manual bool) {
	p.mu.RLock("firePoll")
	plan, ok := p.pollPlanLocked(wfKey, trigger, scheduled, manual)
	p.mu.RUnlock()

	if !ok {
		return
	}

	res := p.pollOnce(plan, trigger)

	p.mu.Lock("firePollResult")
	defer p.mu.Unlock()

	w, ok := p.wf[wfKey]
	if !ok {
		return
	}

	ts, ok := w.Triggers[trigger]
	if !ok || !manual && (ts.Next == nil || !ts.Next.Equal(scheduled)) || p.rootCtx.Err() != nil {
		return
	}

	next := p.applyPollResultLocked(w, ts, trigger, scheduled, res)

	if !manual {
		p.armTriggerAtLocked(w, trigger, scheduled, next)
	}
}

// applyPollResultLocked records the poll and returns the earliest instant of the next one (zero: by recurrence).
func (p *LogicProvider) applyPollResultLocked(
	w *logicWorkflow, ts *logicTriggerState, trigger string, scheduled time.Time, res logicPollResult,
) time.Time {
	now := p.clock.Now().UTC()
	f := logicFire{trigger: trigger, scheduled: scheduled, inputs: res.inputs, hasInputs: true}

	if res.err != nil {
		p.recordPollFailure(w, f, now, res.err)

		return time.Time{}
	}

	ts.PollURL = res.location
	f.outputs = res.resp.outputs()

	if res.resp.status == http.StatusOK {
		_, _ = p.startRunLocked(w, f)

		return time.Time{}
	}

	p.recordSkippedTrigger(w, f, now, nil)

	if d := retryAfter(res.resp, now); d > 0 {
		return now.Add(d)
	}

	return time.Time{}
}

func (p *LogicProvider) recordPollFailure(w *logicWorkflow, f logicFire, now time.Time, cause error) {
	code := "TriggerRequestFailed"
	if errors.Is(cause, context.DeadlineExceeded) {
		code = "TriggerTimedOut"
	}

	h := &logicHistory{
		Name: p.newRunName(now), Trigger: f.trigger, Start: now, End: now, Scheduled: f.scheduled,
		Status: logicStFailed, Code: code, Tracking: uuid.NewString(),
		Error:  map[string]any{logicKCode: code, fieldMessage: cause.Error()},
		Inputs: logicContent{Value: f.inputs, Set: f.hasInputs},
	}

	hideTriggerContent(w.triggerDef(f.trigger), &h.Inputs, &h.Outputs)

	w.Histories = append(w.Histories, h)
	trimLogicHistory(w)
}
