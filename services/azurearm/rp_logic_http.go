package azurearm

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	logicHTTPMaxBody    = 100 << 20
	logicHTTPTimeout    = 2 * time.Minute
	logicRetryDefault   = 4
	logicRetryInterval  = 7 * time.Second
	logicRetryMax       = 90
	logicHTTPErrorFloor = 400
)

type logicRetryPolicy struct {
	interval time.Duration
	count    int
	expo     bool
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
		rp.count = int(min(n, logicRetryMax))
	}

	if iv := stringOf(pol["interval"]); iv != "" {
		d, err := parseISODuration(iv)
		if err != nil {
			return rp, err
		}

		rp.interval = d
	}

	return rp, nil
}

func (rs *logicRunState) runHTTP(env *logicEnv, name string, def map[string]any) logicOutcome {
	in, err := rs.evalInputs(env, def)
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
		return failOutcome(logicCodeInvalidTmpl, err.Error())
	}

	resp, err := rs.doHTTP(req, policy, timeout)
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

	out.outputs, out.hasOut = resp.outputs(), true
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

func (rs *logicRunState) buildHTTPRequest(in, def map[string]any) (*logicHTTPRequest, error) {
	method := strings.ToUpper(stringOf(in["method"]))
	switch method {
	case "GET", "PUT", "POST", "PATCH", "DELETE", "HEAD":
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

	req := &logicHTTPRequest{method: method, url: u.String(), header: http.Header{}}

	if h, ok := objectOf(in[logicKHeaders]); ok {
		for k, v := range h {
			req.header.Set(k, logicString(v))
		}
	}

	if b, has := in[logicKBody]; has {
		req.body = b
	}

	if auth, ok := objectOf(in["authentication"]); ok {
		if aerr := applyHTTPAuth(req, auth); aerr != nil {
			return nil, aerr
		}
	}

	if !strings.EqualFold(stringOf(def["operationOptions"]), "SuppressWorkflowHeaders") {
		req.header.Set("X-Ms-Workflow-Name", rs.wfName)
		req.header.Set("X-Ms-Workflow-Run-Id", rs.run.Name)
	}

	return req, nil
}

func applyHTTPAuth(req *logicHTTPRequest, auth map[string]any) error {
	switch strings.ToLower(stringOf(auth[fieldType])) {
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
	body   any
	header http.Header
	method string
	url    string
}

type logicHTTPResponse struct {
	body    any
	headers map[string]any
	status  int
}

func (r *logicHTTPResponse) outputs() map[string]any {
	out := map[string]any{"statusCode": int64(r.status), logicKHeaders: r.headers}
	if r.body != nil {
		out[logicKBody] = r.body
	}

	return out
}

func (rs *logicRunState) doHTTP(req *logicHTTPRequest, pol logicRetryPolicy, timeout time.Duration,
) (*logicHTTPResponse, error) {
	ctx, cancel := context.WithTimeout(rs.ctx, timeout)
	defer cancel()

	delay := pol.interval

	for attempt := 0; ; attempt++ {
		resp, err := rs.p.sendHTTP(ctx, req)
		retryable := err != nil && ctx.Err() == nil || resp != nil && retryableStatus(resp.status)

		if !retryable || attempt >= pol.count {
			return resp, err
		}

		if !rs.sleep(delay) {
			return nil, context.Canceled
		}

		if pol.expo {
			delay *= 2
		}
	}
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
			raw, err := json.Marshal(b)
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

	resp, err := p.http.Do(req)
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
		if err := json.Unmarshal(raw, &v); err == nil {
			return normLogic(v)
		}
	}

	return string(raw)
}
