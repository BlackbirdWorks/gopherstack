package azurearm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	logicMaxPort     = 65535
	logicMaxHostLen  = 253
	logicTrigWebhook = "httpwebhook"
	logicKSubscribe  = "subscribe"
)

// logicHookRec remembers a successful HttpWebhook subscribe so the matching unsubscribe can be sent later, even
// after the trigger or workflow is gone.
type logicHookRec struct {
	Unsub       map[string]any `json:"unsub,omitempty"`
	Fingerprint string         `json:"fingerprint"`
	Callback    string         `json:"callback"`
}

type logicHookJob struct {
	call        map[string]any
	params      map[string]any
	def         map[string]any
	unsub       map[string]any
	wfKey       string
	wfName      string
	trigger     string
	callback    string
	fingerprint string
	subscribe   bool
}

func validateLogicWebhookTrigger(name string, m map[string]any) error {
	in, _ := objectOf(m["inputs"])

	for _, k := range []string{logicKSubscribe, "unsubscribe"} {
		call, ok := objectOf(in[k])
		if !ok {
			if k == logicKSubscribe {
				return invalidTemplate(
					"the trigger '%s' of type 'HttpWebhook' requires 'subscribe' in its inputs",
					name,
				)
			}

			continue
		}

		if !logicHTTPMethod(stringOf(call["method"])) || stringOf(call["uri"]) == "" {
			return invalidTemplate("the trigger '%s' %s call requires a valid 'method' and a 'uri'", name, k)
		}
	}

	return nil
}

func logicHTTPMethod(m string) bool {
	switch strings.ToUpper(m) {
	case http.MethodGet, http.MethodPut, http.MethodPost, http.MethodPatch, http.MethodDelete, http.MethodHead:
		return true
	}

	return false
}

func hookFingerprint(def map[string]any) string {
	in, _ := objectOf(def["inputs"])

	raw, err := json.Marshal(in[logicKSubscribe], jsonx.V1())
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:])
}

// noteBase remembers the address the caller that wrote this workflow reached the emulator on; webhook callback
// URLs for the workflow are built from it, so one caller's Host header only affects the resource it just wrote.
// A base that is not a plain https host[:port] is ignored.
func (p *LogicProvider) noteBase(wfKey, base string, port int) {
	p.mu.Lock("noteBase")
	defer p.mu.Unlock()

	p.hookPort = port

	if validBase(base) {
		if p.hookBases == nil {
			p.hookBases = map[string]string{}
		}

		p.hookBases[wfKey] = base
	}
}

func (p *LogicProvider) baseForLocked(wfKey string) string {
	if b, ok := p.hookBases[wfKey]; ok {
		return b
	}

	return "https://" + net.JoinHostPort("localhost", strconv.Itoa(p.hookPort))
}

// validBase accepts only https://host[:port] with a DNS name or IP literal host: no userinfo, path or query.
func validBase(base string) bool {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != logicSchemeHTTPS || u.User != nil || u.Path != "" || u.RawQuery != "" ||
		u.Fragment != "" || u.Opaque != "" {
		return false
	}

	return validPort(u.Port()) && validHostName(u.Hostname())
}

func validPort(port string) bool {
	if port == "" {
		return true
	}

	n, err := strconv.Atoi(port)

	return err == nil && n >= 1 && n <= logicMaxPort
}

func validHostName(host string) bool {
	if host == "" || len(host) > logicMaxHostLen {
		return false
	}

	if net.ParseIP(host) != nil {
		return true
	}

	return !strings.ContainsFunc(host, func(r rune) bool {
		return r != '.' && r != '-' && r != '_' && !unicode.IsDigit(r) && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z')
	})
}

func (p *LogicProvider) queueHookLocked(j *logicHookJob) {
	p.hookQ = append(p.hookQ, j)

	if !p.hookRun {
		p.hookRun = true

		p.wg.Go(p.drainHooks)
	}
}

func (p *LogicProvider) hookJobLocked(w *logicWorkflow, name string, subscribe bool, rec *logicHookRec) *logicHookJob {
	def := w.triggerDef(name)
	in, _ := objectOf(def["inputs"])
	unsub, _ := objectOf(in["unsubscribe"])

	j := &logicHookJob{
		params: effectiveLogicParams(w), def: w.Definition, wfKey: w.key(), wfName: w.Name, trigger: name,
		subscribe: subscribe, fingerprint: hookFingerprint(def), unsub: unsub,
	}

	if subscribe {
		j.call, _ = objectOf(in[logicKSubscribe])
		j.callback, _ = w.callbackURL(p.baseForLocked(w.key()), name, "Primary", nil)["value"].(string)

		return j
	}

	j.call, j.callback = rec.Unsub, rec.Callback

	return j
}

// syncWebhooksLocked queues the subscribe and unsubscribe calls that bring every HttpWebhook trigger in line with
// the workflow and trigger state: subscribed while both are enabled and the subscribe call is unchanged.
func (p *LogicProvider) syncWebhooksLocked(w *logicWorkflow) {
	for name, ts := range w.Triggers {
		if ts.HookPending {
			continue
		}

		def := w.triggerDef(name)
		want := typeOf(def) == logicTrigWebhook && w.State == logicStateEnabled && ts.State == logicStateEnabled

		switch {
		case ts.Hook != nil && (!want || ts.Hook.Fingerprint != hookFingerprint(def)):
			ts.HookPending = true

			p.queueHookLocked(p.hookJobLocked(w, name, false, ts.Hook))
		case ts.Hook == nil && want:
			ts.HookPending = true

			p.queueHookLocked(p.hookJobLocked(w, name, true, nil))
		}
	}
}

// unsubscribeAllLocked queues the unsubscribe of every subscribed trigger of a workflow being removed.
func (p *LogicProvider) unsubscribeAllLocked(w *logicWorkflow) {
	for name, ts := range w.Triggers {
		if ts.Hook != nil && !ts.HookPending {
			p.queueHookLocked(p.hookJobLocked(w, name, false, ts.Hook))
		}
	}
}

func (p *LogicProvider) drainHooks() {
	for {
		p.mu.Lock("drainHooks")

		if len(p.hookQ) == 0 || p.rootCtx.Err() != nil {
			p.hookQ, p.hookRun = nil, false
			p.mu.Unlock()

			return
		}

		j := p.hookQ[0]
		p.hookQ = p.hookQ[1:]
		p.mu.Unlock()

		err := p.runHook(j)

		p.mu.Lock("hookResult")
		p.applyHookLocked(j, err)
		p.mu.Unlock()
	}
}

// runHook sends one subscribe or unsubscribe call outside the provider lock, through the Http action's outbound
// rules (loopback-gated unless external hosts are allowed).
func (p *LogicProvider) runHook(j *logicHookJob) error {
	if j.call == nil {
		return nil
	}

	plan := logicPollPlan{params: j.params, wfDef: j.def, wfName: j.wfName}
	rs := p.pollState(plan, p.currentClock().Now)
	rs.callbackURL = j.callback
	env := &logicEnv{local: map[string]*logicActionRec{}}

	in, err := rs.evalInputs(env, j.trigger, map[string]any{"inputs": j.call})
	if err != nil {
		return err
	}

	pol, err := parseRetryPolicy(in)
	if err != nil {
		return err
	}

	req, err := rs.buildHTTPRequest(in, map[string]any{})
	if err != nil {
		return err
	}

	req.async = false

	resp, _, err := rs.doHTTP(req, pol, logicHTTPTimeout)
	if err != nil {
		return err
	}

	if resp.status >= logicHTTPErrorFloor {
		return inputErr(
			"the %s call answered %d",
			hookVerb(j.subscribe),
			resp.status,
		)
	}

	return nil
}

func (p *LogicProvider) applyHookLocked(j *logicHookJob, err error) {
	w, ok := p.wf[j.wfKey]
	if !ok {
		p.orphanUnsubscribeLocked(j, err)

		return
	}

	ts, ok := w.Triggers[j.trigger]
	if !ok {
		p.orphanUnsubscribeLocked(j, err)

		return
	}

	ts.HookPending = false

	switch {
	case err != nil && j.subscribe:
		p.recordPollFailure(w, logicFire{trigger: j.trigger, scheduled: p.clock.Now().UTC()}, p.clock.Now().UTC(), err)
	case err != nil:
		logger.Load(p.rootCtx).Log(p.rootCtx, slog.LevelWarn, "azurearm: logic webhook unsubscribe failed",
			"workflow", j.wfName, "trigger", j.trigger)

		ts.Hook = nil
	case j.subscribe:
		ts.Hook = &logicHookRec{Fingerprint: j.fingerprint, Callback: j.callback}
		if len(j.unsub) > 0 {
			ts.Hook.Unsub = maps.Clone(j.unsub)
		}
	default:
		ts.Hook = nil
	}

	if err == nil || !j.subscribe {
		p.syncWebhooksLocked(w)
	}
}

// orphanUnsubscribeLocked undoes a subscribe that succeeded after its workflow or trigger was removed.
func (p *LogicProvider) orphanUnsubscribeLocked(j *logicHookJob, err error) {
	if err != nil || !j.subscribe || len(j.unsub) == 0 {
		return
	}

	p.queueHookLocked(&logicHookJob{
		call: j.unsub, callback: j.callback, params: j.params, def: j.def, wfKey: j.wfKey, wfName: j.wfName,
		trigger: j.trigger,
	})
}

func hookVerb(subscribe bool) string {
	if subscribe {
		return logicKSubscribe
	}

	return "unsubscribe"
}
