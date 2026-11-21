package azurearm

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	logicHTTPMaxBody    = 100 << 20
	logicHTTPTimeout    = 2 * time.Minute
	logicRetryDefault   = 4
	logicRetryInterval  = 7 * time.Second
	logicRetryMax       = 90
	logicHTTPErrorFloor = 400
	logicAsyncPollDelay = time.Second
	logicAsyncMaxPolls  = 1000
)

type logicRetryPolicy struct {
	minimum  time.Duration
	maximum  time.Duration
	interval time.Duration
	count    int
	expo     bool
}

func clampRetryCount(n int64) int {
	if n > logicRetryMax {
		return logicRetryMax
	}

	if n < 0 {
		return 0
	}

	return int(n)
}

func parseRetryPolicy(in map[string]any) (logicRetryPolicy, error) {
	rp := logicRetryPolicy{count: logicRetryDefault, interval: logicRetryInterval, expo: true}

	pol, ok := objectOf(in["retryPolicy"])
	if !ok {
		return rp, nil
	}

	switch strings.ToLower(stringOf(pol[fieldType])) {
	case logicNone:
		return logicRetryPolicy{}, nil
	case "fixed":
		rp.expo = false
	case "exponential", "":
	default:
		return rp, inputErr("the retry policy type is not valid")
	}

	if n, has := logicInt(normLogic(pol["count"])); has {
		rp.count = clampRetryCount(n)
	}

	for key, dst := range map[string]*time.Duration{
		"interval": &rp.interval, "minimumInterval": &rp.minimum, "maximumInterval": &rp.maximum,
	} {
		if iv := stringOf(pol[key]); iv != "" {
			d, err := parseISODuration(iv)
			if err != nil {
				return rp, err
			}

			*dst = d
		}
	}

	return rp, nil
}

func (rs *logicRunState) runHTTP(env *logicEnv, name string, def map[string]any) logicOutcome {
	in, err := rs.evalInputs(env, name, def)
	if err != nil {
		return exprFailure(name, err)
	}

	policy, err := parseRetryPolicy(in)
	if err != nil {
		return failOutcome(logicCodeInvalidTmpl, err.Error())
	}

	timeout := logicHTTPTimeout

	if lim, ok := objectOf(def["limit"]); ok {
		if t := stringOf(lim["timeout"]); t != "" {
			d, derr := parseISODuration(t)
			if derr != nil {
				return failOutcome(logicCodeInvalidTmpl, derr.Error())
			}

			timeout = d
		}
	}

	req, err := rs.buildHTTPRequest(in, def)
	if err != nil {
		code := logicCodeInvalidTmpl

		switch {
		case errors.Is(err, errLogicTokenRequest):
			code = logicCodeActionFailed
		case errors.Is(err, errLogicClientCert):
			code = logicCodeBadRequest
		}

		return failOutcome(code, err.Error())
	}

	req.rec = rs.currentRec(env, name)

	resp, history, err := rs.doHTTP(req, policy, timeout)
	out := logicOutcome{inputs: httpInputs(in), hasIn: true}

	if err != nil {
		out.status, out.code = logicStFailed, "ActionFailed"
		out.err = map[string]any{logicKCode: "ActionFailed", fieldMessage: err.Error()}

		if errors.Is(err, context.DeadlineExceeded) {
			out.status, out.code = logicStTimedOut, "ActionTimedOut"
		}

		if rs.ctx.Err() != nil {
			out.status = logicStCancelled
		}

		return out
	}

	outputs := resp.outputs()
	if len(history) > 0 {
		outputs["retryHistory"] = history
	}

	out.outputs, out.hasOut = outputs, true
	out.status = logicStSucceeded

	if resp.status >= logicHTTPErrorFloor {
		code := strings.ReplaceAll(http.StatusText(resp.status), " ", "")
		out.status, out.code = logicStFailed, code
		out.err = map[string]any{logicKCode: code, fieldMessage: logicString(resp.body)}
	}

	return out
}

func httpInputs(in map[string]any) map[string]any {
	out := map[string]any{}

	for k, v := range in {
		if k == "authentication" {
			continue
		}

		out[k] = v
	}

	return out
}

func setRequestHeaders(h http.Header, in map[string]any, method string) {
	if hs, ok := objectOf(in[logicKHeaders]); ok {
		for k, v := range hs {
			h.Set(k, logicString(v))
		}
	}

	dropOmittedHeaders(h, method)

	if c := stringOf(in["cookie"]); c != "" {
		h.Set("Cookie", c)
	}
}

func (rs *logicRunState) buildHTTPRequest(in, def map[string]any) (*logicHTTPRequest, error) {
	method := strings.ToUpper(stringOf(in["method"]))
	switch method {
	case http.MethodGet, http.MethodPut, http.MethodPost, http.MethodPatch, http.MethodDelete, http.MethodHead:
	default:
		return nil, inputErr("the HTTP method '%s' is not valid", stringOf(in["method"]))
	}

	u, err := url.Parse(stringOf(in["uri"]))
	if err != nil || u.Host == "" || u.Scheme != logicSchemeHTTP && u.Scheme != logicSchemeHTTPS {
		return nil, inputErr("the HTTP uri '%s' is not a valid absolute http(s) URI", stringOf(in["uri"]))
	}

	if q, ok := objectOf(in["queries"]); ok {
		vals := u.Query()
		for k, v := range q {
			vals.Set(k, logicString(v))
		}

		u.RawQuery = vals.Encode()
	}

	req := &logicHTTPRequest{
		method: method, url: u.String(), header: http.Header{},
		async: !strings.Contains(strings.ToLower(stringOf(def["operationOptions"])), "disableasyncpattern"),
	}

	setRequestHeaders(req.header, in, method)

	if b, has := in[logicKBody]; has {
		req.body = b
	}

	if auth, ok := objectOf(in["authentication"]); ok {
		if aerr := rs.applyHTTPAuth(req, auth); aerr != nil {
			return nil, aerr
		}
	}

	if !strings.EqualFold(stringOf(def["operationOptions"]), "SuppressWorkflowHeaders") {
		req.header.Set("X-Ms-Workflow-Name", rs.wfName)

		if rs.run.Name != "" {
			req.header.Set("X-Ms-Workflow-Run-Id", rs.run.Name)
		}
	}

	return req, nil
}

func (rs *logicRunState) applyHTTPAuth(req *logicHTTPRequest, auth map[string]any) error {
	switch strings.ToLower(stringOf(auth[fieldType])) {
	case "activedirectoryoauth":
		tok, err := rs.fetchOAuthToken(auth)
		if err != nil {
			return err
		}

		req.header.Set("Authorization", tok)
	case "clientcertificate":
		pfx, err := loadLogicPFX(auth)
		if err != nil {
			return err
		}

		req.cert = &pfx.tls
	case "basic":
		cred := stringOf(auth["username"]) + ":" + stringOf(auth["password"])
		req.header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(cred)))
	case "raw":
		req.header.Set("Authorization", stringOf(auth["value"]))
	case "", logicNone:
	default:
		return inputErr("the authentication type '%s' is not supported by this emulator", stringOf(auth[fieldType]))
	}

	return nil
}

type logicHTTPRequest struct {
	cert   *tls.Certificate
	rec    *logicActionRec
	body   any
	header http.Header
	method string
	url    string
	async  bool
}

type logicHTTPResponse struct {
	body    any
	headers map[string]any
	status  int
}

func (r *logicHTTPResponse) outputs() map[string]any {
	out := map[string]any{logicKStatusCode: int64(r.status), logicKHeaders: r.headers}
	if r.body != nil {
		out[logicKBody] = r.body
	}

	return out
}

// doHTTP sends req, retrying 408, 429, 5xx and transport failures per the policy: fixed waits interval between
// attempts, exponential doubles it, both clamped to minimumInterval and maximumInterval when set. The failed
// attempts are returned as retryHistory entries.
func (rs *logicRunState) doHTTP(req *logicHTTPRequest, pol logicRetryPolicy, timeout time.Duration,
) (*logicHTTPResponse, []any, error) {
	ctx, cancel := context.WithTimeout(rs.ctx, timeout)
	defer cancel()

	delay := pol.interval

	var history []any

	for attempt := 0; ; attempt++ {
		started := rs.clock()
		resp, err := rs.send(ctx, req)
		retryable := err != nil && ctx.Err() == nil || resp != nil && retryableStatus(resp.status)

		if !retryable || attempt >= pol.count {
			if err == nil && req.async {
				resp, err = rs.pollAsync(ctx, req, resp)
			}

			return resp, history, err
		}

		history = append(history, retryEntry(started, rs.clock(), resp, err))

		if !rs.sleep(pol.clamp(delay)) {
			return nil, history, context.Canceled
		}

		if pol.expo {
			delay *= 2
		}
	}
}

func (pol logicRetryPolicy) clamp(d time.Duration) time.Duration {
	if pol.minimum > 0 && d < pol.minimum {
		d = pol.minimum
	}

	if pol.maximum > 0 && d > pol.maximum {
		d = pol.maximum
	}

	return d
}

func retryEntry(start, end time.Time, resp *logicHTTPResponse, err error) map[string]any {
	e := map[string]any{logicKStartTime: logicTime(start), logicKEndTime: logicTime(end)}

	if resp != nil {
		e[logicKCode] = http.StatusText(resp.status)
		e["statusCode"] = int64(resp.status)

		return e
	}

	e[logicKCode] = "ClientError"
	e["error"] = map[string]any{logicKCode: "ClientError", fieldMessage: err.Error()}

	return e
}

func retryableStatus(s int) bool {
	return s == http.StatusRequestTimeout || s == http.StatusTooManyRequests || s >= http.StatusInternalServerError
}

func (p *LogicProvider) sendHTTP(ctx context.Context, r *logicHTTPRequest) (*logicHTTPResponse, error) {
	var body io.Reader

	if r.body != nil {
		switch b := r.body.(type) {
		case string:
			body = strings.NewReader(b)
		default:
			raw, err := json.Marshal(b, jsonx.V1())
			if err != nil {
				return nil, err
			}

			body = bytes.NewReader(raw)

			if r.header.Get("Content-Type") == "" {
				r.header.Set("Content-Type", "application/json")
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, r.method, r.url, body)
	if err != nil {
		return nil, err
	}

	req.Header = r.header.Clone()

	client := p.http

	if r.cert != nil {
		client = p.certClient(r.cert)
		defer client.CloseIdleConnections()
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, logicHTTPMaxBody))
	if err != nil {
		return nil, err
	}

	headers := map[string]any{}

	for k, v := range resp.Header {
		headers[k] = strings.Join(v, ", ")
	}

	return &logicHTTPResponse{
		status:  resp.StatusCode,
		headers: headers,
		body:    decodeHTTPBody(resp.Header.Get("Content-Type"), raw),
	}, nil
}

func decodeHTTPBody(contentType string, raw []byte) any {
	if len(raw) == 0 {
		return nil
	}

	if strings.Contains(strings.ToLower(contentType), "json") {
		var v any
		if err := json.Unmarshal(raw, &v, jsonx.V1()); err == nil {
			return normLogic(v)
		}
	}

	return string(raw)
}

// dropOmittedHeaders removes the headers the HTTP action ignores (Learn: connectors-native-http, "Omitted HTTP
// headers"); Content-* other than Disposition, Encoding and Type is dropped, and those too on GET.
func dropOmittedHeaders(h http.Header, method string) {
	for k := range h {
		if omittedHeader(strings.ToLower(k), method) {
			delete(h, k)
		}
	}
}

func omittedHeader(lk, method string) bool {
	switch lk {
	case acaRuleAllow, "cookie", "expires", logicKHost, "last-modified", "origin", "set-cookie", "transfer-encoding":
		return true
	case "content-disposition", "content-encoding", "content-type":
		return method == http.MethodGet
	case "accept-version":
		return false
	}

	return strings.HasPrefix(lk, "accept-") || strings.HasPrefix(lk, "content-")
}

// pollAsync follows the asynchronous operation pattern (Learn: logic-apps-create-api-app, connectors-native-http):
// a 202 with a Location header is polled, every Retry-After seconds (default one second), until a non-202 answers.
func (rs *logicRunState) pollAsync(ctx context.Context, req *logicHTTPRequest, resp *logicHTTPResponse,
) (*logicHTTPResponse, error) {
	for range logicAsyncMaxPolls {
		if resp.status != http.StatusAccepted {
			return resp, nil
		}

		loc := asyncLocation(req.url, resp.headers["Location"])
		if loc == "" {
			return resp, nil
		}

		if !rs.sleepCtx(ctx, asyncDelay(resp.headers["Retry-After"])) {
			return nil, context.Cause(ctx)
		}

		poll := &logicHTTPRequest{method: http.MethodGet, url: loc, header: pollHeaders(req, loc), rec: req.rec}
		if sameOrigin(req.url, loc) {
			poll.cert = req.cert
		}

		next, err := rs.send(ctx, poll)
		if err != nil {
			return nil, err
		}

		resp = next
	}

	return resp, nil
}

func asyncLocation(base string, v any) string {
	raw, _ := v.(string)
	if raw == "" {
		return ""
	}

	b, err := url.Parse(base)
	if err != nil {
		return ""
	}

	ref, err := url.Parse(raw)
	if err != nil {
		return ""
	}

	abs := b.ResolveReference(ref)
	if abs.Host == "" || abs.Scheme != logicSchemeHTTP && abs.Scheme != logicSchemeHTTPS {
		return ""
	}

	return abs.String()
}

func asyncDelay(v any) time.Duration {
	raw, _ := v.(string)

	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil || n < 0 {
		return logicAsyncPollDelay
	}

	return time.Duration(n) * time.Second
}

// pollHeaders sends a same-origin poll the request headers without the Content-* ones; a poll to another origin
// gets only the workflow headers, since any other header may carry a credential for the original host.
func pollHeaders(req *logicHTTPRequest, loc string) http.Header {
	from, _ := url.Parse(req.url)
	to, _ := url.Parse(loc)
	sameOrigin := from != nil && to != nil && from.Scheme == to.Scheme && from.Host == to.Host
	h := http.Header{}

	for k, v := range req.header {
		lk := strings.ToLower(k)

		switch {
		case sameOrigin && !strings.HasPrefix(lk, "content-"), strings.HasPrefix(lk, "x-ms-workflow-"):
			h[k] = slices.Clone(v)
		}
	}

	return h
}
