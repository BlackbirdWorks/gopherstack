package azurearm

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	logicInvokeAPIVersion  = "2016-10-01"
	logicContentAPIVersion = "2016-06-01"
	logicSigVersion        = "1.0"
	logicMaxRequestBody    = 100 << 20
	logicSegsInvoke        = 5
	contentTriggerSegs     = 5
	contentDeepSegs        = 7
)

func validCallbackKeyType(k string) bool {
	switch strings.ToLower(k) {
	case "", "notspecified", logicKeyPrimary, "secondary":
		return true
	}

	return false
}

func logicSign(key, canonical string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(canonical))

	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func logicCanonical(path, sp, sv, se string) string {
	return strings.Join([]string{path, sp, sv, se}, "\n")
}

// verifySig reports whether sig matches either access key; expiry is checked against now.
func (w *logicWorkflow) verifySig(path, sp, sv, se, sig string, now time.Time) bool {
	if se != "" {
		exp, err := time.Parse(time.RFC3339, se)
		if err != nil || now.After(exp) {
			return false
		}
	}

	canon := logicCanonical(path, sp, sv, se)

	for _, k := range []string{w.KeyPrimary, w.KeySecondary} {
		if hmac.Equal([]byte(logicSign(k, canon)), []byte(sig)) {
			return true
		}
	}

	return false
}

func (w *logicWorkflow) keyFor(keyType string) string {
	if strings.EqualFold(keyType, "Secondary") {
		return w.KeySecondary
	}

	return w.KeyPrimary
}

func invokePath(flow, trigger string) string {
	return "/workflows/" + flow + "/triggers/" + trigger + "/paths/invoke"
}

// callbackURL builds the trigger's signed invoke URL on the emulator's data plane.
func (w *logicWorkflow) callbackURL(base, trigger, keyType string, notAfter *time.Time) map[string]any {
	def := w.triggerDef(trigger)
	in, _ := objectOf(def["inputs"])
	method := strings.ToUpper(stringOf(in["method"]))

	if method == "" {
		method = http.MethodPost
	}

	sp := "/triggers/" + trigger + "/run"
	se := ""

	if notAfter != nil {
		se = notAfter.UTC().Format(time.RFC3339)
	}

	sig := logicSign(w.keyFor(keyType), logicCanonical(invokePath(w.FlowID, trigger), sp, logicSigVersion, se))
	queries := map[string]any{logicKAPIVersion: logicInvokeAPIVersion, "sp": sp, "sv": logicSigVersion, logicKSig: sig}
	q := url.Values{logicKAPIVersion: {logicInvokeAPIVersion}, "sp": {sp}, "sv": {logicSigVersion}}

	if se != "" {
		queries["se"] = se

		q.Set("se", se)
	}

	q.Set(logicKSig, sig)

	basePath := base + logicDataPrefix + w.FlowID + "/triggers/" + trigger + "/paths/invoke"
	out := map[string]any{"method": method, "basePath": basePath, "queries": queries}

	if rel := stringOf(in["relativePath"]); rel != "" {
		out["relativePath"] = rel
		out["relativePathParameters"] = relativePathParams(rel)
		basePath += "/" + strings.TrimPrefix(rel, "/")
		out["basePath"] = basePath
	}

	out["value"] = basePath + "?" + q.Encode()

	return out
}

func relativePathParams(rel string) []any {
	out := []any{}

	for seg := range strings.SplitSeq(rel, "/") {
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			out = append(out, seg[1:len(seg)-1])
		}
	}

	return out
}

func (w *logicWorkflow) triggerDef(name string) map[string]any {
	defs, _ := objectOf(w.Definition[logicSegTriggers])
	d, _ := objectOf(defs[name])

	return d
}

// contentLink builds a signed read link to stored run content.
func (w *logicWorkflow) contentLink(base, path string, v any) map[string]any {
	sp := path + "/read"
	sig := logicSign(w.KeyPrimary, logicCanonical(path, sp, logicSigVersion, ""))
	q := url.Values{logicKAPIVersion: {logicContentAPIVersion}, "sp": {sp}, "sv": {logicSigVersion}, logicKSig: {sig}}

	return map[string]any{
		"uri": base + logicDataPrefix + w.FlowID + strings.TrimPrefix(
			path,
			"/workflows/"+w.FlowID,
		) + "?" + q.Encode(),
		"contentVersion": "XXXXXXXXXXXXXXXX",
		"contentSize":    marshalLogicSize(v),
	}
}

func (p *LogicProvider) workflowByFlow(flow string) (*logicWorkflow, bool) {
	for _, w := range p.wf {
		if w.FlowID == flow {
			return w, true
		}
	}

	return nil, false
}

// ServeDataPlane answers requests under /_logic/workflows/: trigger invocation and run content reads. It reports
// false for any other path.
func (p *LogicProvider) ServeDataPlane(rw http.ResponseWriter, r *http.Request) bool {
	rest, ok := strings.CutPrefix(r.URL.Path, logicDataPrefix)
	if !ok {
		return false
	}

	segs := strings.Split(strings.Trim(rest, "/"), "/")

	switch {
	case len(segs) >= logicSegsInvoke && segs[1] == logicSegTriggers && segs[3] == "paths" && segs[4] == "invoke":
		p.serveInvoke(rw, r, segs)
	case len(segs) >= 4 && (segs[1] == logicSegRuns || segs[1] == logicSegTriggers) && segs[len(segs)-2] == "contents":
		p.serveContent(rw, r, segs)
	default:
		writeLogicData(rw, http.StatusNotFound, "NotFound", "The requested resource was not found.")
	}

	return true
}

func writeLogicData(rw http.ResponseWriter, status int, code, msg string) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	_ = json.MarshalEncode(
		jsontext.NewEncoder(rw, jsonx.V1()),
		map[string]any{logicKError: map[string]any{logicKCode: code, fieldMessage: msg}},
	)
}

func (p *LogicProvider) authorize(rw http.ResponseWriter, r *http.Request, w *logicWorkflow, canonPath string) bool {
	q := r.URL.Query()
	sig := q.Get(logicKSig)

	if sig == "" {
		writeLogicData(rw, http.StatusUnauthorized, "DirectApiAuthorizationRequired",
			"The request must be authenticated only by Shared Access scheme.")

		return false
	}

	if !w.verifySig(canonPath, q.Get("sp"), q.Get("sv"), q.Get("se"), sig, p.clock.Now()) {
		writeLogicData(rw, http.StatusUnauthorized, "AuthorizationFailed",
			"The provided signature is not valid or has expired.")

		return false
	}

	return true
}

type logicInvokeTarget struct {
	now     time.Time
	def     map[string]any
	wfKey   string
	respond bool
}

func (p *LogicProvider) resolveInvoke(
	rw http.ResponseWriter,
	r *http.Request,
	segs []string,
) (logicInvokeTarget, bool) {
	trigger := segs[2]

	p.mu.RLock("invoke")
	defer p.mu.RUnlock()

	w, ok := p.workflowByFlow(segs[0])
	if !ok {
		writeLogicData(rw, http.StatusNotFound, "WorkflowNotFound", "The workflow was not found.")

		return logicInvokeTarget{}, false
	}

	if !p.authorize(rw, r, w, invokePath(w.FlowID, trigger)) {
		return logicInvokeTarget{}, false
	}

	def := w.triggerDef(trigger)
	if _, has := w.Triggers[trigger]; !has || typeOf(def) != logicTrigRequest && typeOf(def) != logicTrigManual {
		writeLogicData(rw, http.StatusNotFound, "TriggerNotFound", "The trigger '"+trigger+"' was not found.")

		return logicInvokeTarget{}, false
	}

	return logicInvokeTarget{def: def, wfKey: w.key(), respond: w.hasResponse(), now: p.clock.Now().UTC()}, true
}

func (p *LogicProvider) serveInvoke(rw http.ResponseWriter, r *http.Request, segs []string) {
	trigger := segs[2]

	tgt, ok := p.resolveInvoke(rw, r, segs)
	if !ok {
		return
	}

	fire, rej := buildRequestFire(r, trigger, strings.Join(segs[5:], "/"), tgt.def, tgt.respond, tgt.now)
	if rej != nil {
		writeLogicData(rw, rej.status, rej.code, rej.message)

		return
	}

	p.mu.Lock("invoke")

	cur, found := p.wf[tgt.wfKey]
	if !found {
		p.mu.Unlock()
		writeLogicData(rw, http.StatusNotFound, "WorkflowNotFound", "The workflow was not found.")

		return
	}

	started, err := p.startRunLocked(cur, fire)
	name, version := cur.Name, cur.Version
	p.mu.Unlock()

	if err != nil {
		e, _ := logicErrorDetails(err)
		writeLogicData(rw, e.status, e.code, e.message)

		return
	}

	rw.Header().Set("X-Ms-Workflow-Run-Id", started.runName)
	rw.Header().Set("X-Ms-Workflow-Name", name)
	rw.Header().Set("X-Ms-Workflow-Version", version)

	p.respond(rw, r, started.live)
}

func (p *LogicProvider) respond(rw http.ResponseWriter, r *http.Request, live *logicLive) {
	if live.resp == nil {
		rw.WriteHeader(http.StatusAccepted)

		return
	}

	select {
	case resp := <-live.resp:
		writeLogicResponse(rw, resp)
	case <-live.done:
		select {
		case resp := <-live.resp:
			writeLogicResponse(rw, resp)
		default:
			rw.WriteHeader(http.StatusAccepted)
		}
	case <-r.Context().Done():
	}
}

func writeLogicResponse(rw http.ResponseWriter, resp logicResponse) {
	for k, v := range resp.headers {
		rw.Header().Set(k, v)
	}

	var payload []byte

	switch b := resp.body.(type) {
	case nil:
	case string:
		payload = []byte(b)
	default:
		payload, _ = json.Marshal(b, jsonx.V1())

		if rw.Header().Get("Content-Type") == "" {
			rw.Header().Set("Content-Type", "application/json")
		}
	}

	rw.WriteHeader(resp.status)
	_, _ = rw.Write(payload)
}

func (w *logicWorkflow) hasResponse() bool {
	acts, _ := objectOf(w.Definition[logicKActions])

	return containsResponse(acts)
}

func containsResponse(defs map[string]any) bool {
	for _, a := range defs {
		m, _ := objectOf(a)
		typ := strings.ToLower(stringOf(m[fieldType]))

		if typ == logicActResponse {
			return true
		}

		if slices.ContainsFunc(logicChildScopes(typ, m), containsResponse) {
			return true
		}
	}

	return false
}

type logicRejection struct {
	code    string
	message string
	status  int
}

func rejectLogic(status int, code, format string, a ...any) *logicRejection {
	return &logicRejection{status: status, code: code, message: fmt.Sprintf(format, a...)}
}

func matchRelativePath(pattern, rest string) (map[string]any, bool) {
	ps := strings.Split(strings.Trim(pattern, "/"), "/")
	rs := strings.Split(strings.Trim(rest, "/"), "/")

	if strings.Trim(pattern, "/") == "" {
		return map[string]any{}, strings.Trim(rest, "/") == ""
	}

	if len(ps) != len(rs) {
		return nil, false
	}

	params := map[string]any{}

	for i, seg := range ps {
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			v, err := url.PathUnescape(rs[i])
			if err != nil {
				return nil, false
			}

			params[seg[1:len(seg)-1]] = v

			continue
		}

		if !strings.EqualFold(seg, rs[i]) {
			return nil, false
		}
	}

	return params, true
}

func buildRequestFire(
	r *http.Request, trigger, rest string, def map[string]any, respond bool, now time.Time,
) (logicFire, *logicRejection) {
	in, _ := objectOf(def["inputs"])

	want := strings.ToUpper(stringOf(in["method"]))
	if want == "" {
		want = http.MethodPost
	}

	if r.Method != want {
		return logicFire{}, rejectLogic(http.StatusMethodNotAllowed, "TriggerRequestMethodNotValid",
			"The HTTP method '%s' is not valid for trigger '%s'. The expected method is '%s'.", r.Method, trigger, want)
	}

	params, ok := matchRelativePath(stringOf(in["relativePath"]), rest)
	if !ok {
		return logicFire{}, rejectLogic(http.StatusNotFound, "TriggerNotFound",
			"The request path does not match the relative path of trigger '%s'.", trigger)
	}

	body, err := readLogicBody(r)
	if err != nil {
		return logicFire{}, rejectLogic(
			http.StatusBadRequest,
			"InvalidRequestContent",
			"The request body is not valid: %v",
			err,
		)
	}

	if schema, has := objectOf(in["schema"]); has && len(schema) > 0 {
		if verr := validateLogicSchema(schema, body); verr != nil {
			return logicFire{}, rejectLogic(
				http.StatusBadRequest,
				"TriggerInputSchemaMismatch",
				"The input body for trigger '%s' of type 'Request' did not match its schema definition. Error details: '%s'",
				trigger,
				verr.Error(),
			)
		}
	}

	outputs := requestOutputs(r, body, params)

	return logicFire{
		trigger: trigger, scheduled: now, outputs: outputs, respond: respond,
		client: r.Header.Get("X-Ms-Client-Tracking-Id"),
	}, nil
}

func readLogicBody(r *http.Request) (any, error) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, logicMaxRequestBody))
	if err != nil || len(raw) == 0 {
		return nil, err
	}

	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))

	if strings.Contains(mt, "json") {
		var v any
		if jerr := json.Unmarshal(raw, &v, jsonx.V1()); jerr != nil {
			return nil, jerr
		}

		return normLogic(v), nil
	}

	if strings.HasPrefix(mt, "text/") || mt == "" {
		return string(raw), nil
	}

	if strings.HasPrefix(mt, "multipart/") {
		mt = r.Header.Get("Content-Type")
	}

	return map[string]any{"$content-type": mt, "$content": base64.StdEncoding.EncodeToString(raw)}, nil
}

func contentPath(flow string, segs []string) string {
	return "/workflows/" + flow + "/" + strings.Join(segs[1:], "/")
}

// lookupContent resolves a content path to its stored value; caller holds p.mu.
func (w *logicWorkflow) lookupContent(segs []string) (logicContent, bool) {
	kind := segs[len(segs)-1]

	switch {
	case segs[1] == logicSegRuns && len(segs) == contentTriggerSegs:
		return w.runTriggerContent(segs[2], kind)
	case segs[1] == logicSegRuns && len(segs) == contentDeepSegs && segs[3] == logicKActions:
		return w.actionContent(segs[2], segs[4], kind)
	case segs[1] == logicSegTriggers && len(segs) == contentDeepSegs:
		return w.historyContent(segs[2], segs[4], kind)
	}

	return logicContent{}, false
}

func pickContent(inKind, outKind, kind string, in, out logicContent) (logicContent, bool) {
	switch kind {
	case inKind:
		return in, in.Set
	case outKind:
		return out, out.Set
	}

	return logicContent{}, false
}

func (w *logicWorkflow) runTriggerContent(run, kind string) (logicContent, bool) {
	for _, r := range w.Runs {
		if r.Name == run {
			return pickContent("TriggerInputs", "TriggerOutputs", kind, r.Trigger.Inputs, r.Trigger.Outputs)
		}
	}

	return logicContent{}, false
}

func (w *logicWorkflow) actionContent(run, action, kind string) (logicContent, bool) {
	for _, r := range w.Runs {
		if a, ok := r.Actions[action]; ok && r.Name == run {
			return pickContent("ActionInputs", "ActionOutputs", kind, a.Inputs, a.Outputs)
		}
	}

	return logicContent{}, false
}

func (w *logicWorkflow) historyContent(trigger, history, kind string) (logicContent, bool) {
	for _, h := range w.Histories {
		if h.Name == history && h.Trigger == trigger {
			return pickContent("TriggerInputs", "TriggerOutputs", kind, h.Inputs, h.Outputs)
		}
	}

	return logicContent{}, false
}

func (p *LogicProvider) serveContent(rw http.ResponseWriter, r *http.Request, segs []string) {
	p.mu.RLock(logicKContent)
	defer p.mu.RUnlock()

	w, ok := p.workflowByFlow(segs[0])
	if !ok {
		writeLogicData(rw, http.StatusNotFound, "WorkflowNotFound", "The workflow was not found.")

		return
	}

	if !p.authorize(rw, r, w, contentPath(w.FlowID, segs)) {
		return
	}

	c, found := w.lookupContent(segs)
	if !found {
		writeLogicData(rw, http.StatusNotFound, "ContentNotFound", "The requested content was not found.")

		return
	}

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	_ = json.MarshalEncode(jsontext.NewEncoder(rw, jsonx.V1()), c.Value)
}

func requestOutputs(r *http.Request, body any, params map[string]any) map[string]any {
	headers := map[string]any{}
	for k, v := range r.Header {
		headers[strings.ToLower(k)] = strings.Join(v, ", ")
	}

	outputs := map[string]any{logicKHeaders: headers}
	if body != nil {
		outputs[logicKBody] = body
	}

	qs := map[string]any{}

	for k, v := range r.URL.Query() {
		if !reservedQuery(k) {
			qs[k] = strings.Join(v, ",")
		}
	}

	if len(qs) > 0 {
		outputs["queries"] = qs
	}

	if len(params) > 0 {
		outputs["relativePathParameters"] = params
	}

	return outputs
}

func reservedQuery(k string) bool {
	switch k {
	case logicKSig, "sp", "sv", "se", logicKAPIVersion:
		return true
	}

	return false
}
