package wfinterp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	logPayloadArgs     = 3
	paramHeaders       = "headers"
	paramTimeout       = "timeout"
	paramURL           = "url"
	defaultHTTPTimeout = 1800 * time.Second
	defaultCallbackTTL = 43200
	maxResponseBytes   = 2 << 20
	secondsF           = float64(time.Second)
)

func (e *Engine) addHTTP() {
	for _, m := range []string{"get", "post", "put", "patch", "delete"} {
		method := strings.ToUpper(m)

		e.regBlocking("http."+m, []string{paramURL, paramTimeout, paramBody, paramHeaders, "query", "auth"}, 1,
			func(c *callCtx, a []any) (any, error) { return httpCall(c, method, a) })
	}

	e.regBlocking(
		"http.request",
		[]string{"method", paramURL, paramTimeout, paramBody, paramHeaders, "query", "auth"},
		arity2,
		func(c *callCtx, a []any) (any, error) {
			m, err := argString("http.request", "method", a[0])
			if err != nil {
				return nil, err
			}

			return httpCall(c, strings.ToUpper(m), a[1:])
		},
	)
	e.regBlocking("sys.log", []string{"text", "json", paramData, "severity", paramTimeout}, 0, fnLog)
	e.regBlocking("sys.sleep", []string{"seconds"}, 1, fnSleep)
	e.regBlocking("sys.sleep_until", []string{"time"}, 1, fnSleepUntil)
	e.regBlocking("events.create_callback_endpoint", []string{"http_callback_method"}, 1, fnCreateCallback)
	e.regBlocking("events.await_callback", []string{"callback", paramTimeout}, 1, fnAwaitCallback)
}

func fnLog(c *callCtx, a []any) (any, error) {
	given := 0

	for _, v := range a[:logPayloadArgs] {
		if v != nil {
			given++
		}
	}

	if given != 1 {
		return nil, valueErr("sys.log: exactly one of text, json or data is required")
	}

	entry := LogEntry{Severity: "DEFAULT"}

	if a[3] != nil {
		sev, err := argString("sys.log", "severity", a[3])
		if err != nil {
			return nil, err
		}

		switch sev {
		case "DEFAULT", "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL", "ALERT", "EMERGENCY":
			entry.Severity = sev
		default:
			return nil, valueErr("sys.log: unknown severity %q", sev)
		}
	}

	switch {
	case a[1] != nil:
		m, err := argMap("sys.log", "json", a[1])
		if err != nil {
			return nil, err
		}

		entry.JSON = m
	case a[2] != nil:
		if m, ok := a[2].(map[string]any); ok {
			entry.JSON = m
		} else {
			entry.Text = formatValue(a[2])
		}
	default:
		entry.Text = formatValue(a[0])
	}

	if h := c.host(); h.Log != nil {
		h.Log(entry)
	}

	return nil, nil //nolint:nilnil // sys.log returns null
}

func fnSleep(c *callCtx, a []any) (any, error) {
	secs, err := argNumber("sys.sleep", "seconds", a[0])
	if err != nil {
		return nil, err
	}

	if secs < 0 || secs > maxSleep {
		return nil, valueErr("sys.sleep: seconds must be between 0 and %d", maxSleep)
	}

	return nil, c.clock().Sleep(c.ctx, time.Duration(secs*secondsF))
}

func fnSleepUntil(c *callCtx, a []any) (any, error) {
	s, err := argString("sys.sleep_until", "time", a[0])
	if err != nil {
		return nil, err
	}

	t, perr := time.Parse(time.RFC3339Nano, s)
	if perr != nil {
		return nil, valueErr("sys.sleep_until: %q is not an ISO 8601 timestamp", s)
	}

	d := t.Sub(c.clock().Now())
	if d <= 0 {
		return nil, nil //nolint:nilnil // already past
	}

	if d > maxSleep*time.Second {
		return nil, valueErr("sys.sleep_until: the time is more than a year away")
	}

	return nil, c.clock().Sleep(c.ctx, d)
}

func fnCreateCallback(c *callCtx, a []any) (any, error) {
	m, err := argString("events.create_callback_endpoint", "http_callback_method", a[0])
	if err != nil {
		return nil, err
	}

	m = strings.ToUpper(m)
	switch m {
	case "GET", "POST", "PUT", "PATCH", "DELETE":
	default:
		return nil, valueErr("events.create_callback_endpoint: unsupported method %q", m)
	}

	h := c.host()
	if h.CreateCallback == nil {
		return nil, rtErr(TagOperationError, "callbacks are not available")
	}

	u, cerr := h.CreateCallback(c.ctx, m)
	if cerr != nil {
		return nil, rtErr(TagOperationError, "could not create the callback endpoint: %v", cerr)
	}

	return map[string]any{paramURL: u, "http_callback_method": m}, nil
}

func fnAwaitCallback(c *callCtx, a []any) (any, error) {
	cb, err := argMap("events.await_callback", "callback", a[0])
	if err != nil {
		return nil, err
	}

	u, _ := cb[paramURL].(string)
	if u == "" {
		return nil, typeErr("events.await_callback: callback has no url")
	}

	ttl := float64(defaultCallbackTTL)

	if a[1] != nil {
		if ttl, err = argNumber("events.await_callback", paramTimeout, a[1]); err != nil {
			return nil, err
		}
	}

	h := c.host()
	if h.AwaitCallback == nil {
		return nil, rtErr(TagOperationError, "callbacks are not available")
	}

	tctx, cancel := context.WithCancel(c.ctx)

	var timedOut atomic.Bool

	done := make(chan struct{})

	go func() {
		defer close(done)

		if c.clock().Sleep(tctx, time.Duration(ttl*secondsF)) == nil {
			timedOut.Store(true)
			cancel()
		}
	}()

	req, werr := h.AwaitCallback(tctx, u)

	cancel()
	<-done

	switch {
	case werr == nil:
		return map[string]any{"http_request": req}, nil
	case timedOut.Load():
		return nil, rtErr(TagTimeoutError, "timed out waiting for the callback")
	default:
		return nil, werr
	}
}

func httpCall(c *callCtx, method string, a []any) (any, error) {
	target, err := argString("http."+strings.ToLower(method), paramURL, a[0])
	if err != nil {
		return nil, err
	}

	req, err := buildRequest(c, method, target, a)
	if err != nil {
		return nil, err
	}

	timeout := defaultHTTPTimeout

	if a[1] != nil {
		secs, terr := argNumber("http", paramTimeout, a[1])
		if terr != nil {
			return nil, terr
		}

		timeout = time.Duration(secs * secondsF)
	}

	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	client := c.host().HTTP
	if client == nil {
		client = httpclient.New(timeout)
	}

	resp, derr := client.Do(req.WithContext(ctx))
	if derr != nil {
		return nil, transportError(c.ctx, derr)
	}
	defer resp.Body.Close()

	raw, rerr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if rerr != nil {
		return nil, transportError(c.ctx, rerr)
	}

	if len(raw) > maxResponseBytes {
		return nil, rtErr(TagResourceLimitError, "the HTTP response is larger than %d bytes", maxResponseBytes)
	}

	return httpResult(resp, raw)
}

func transportError(parent context.Context, err error) error {
	if parent.Err() != nil {
		return parent.Err()
	}

	var ne net.Error

	switch {
	case errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()):
		return rtErr(TagTimeoutError, "the HTTP request timed out")
	case isDialError(err):
		return rtErr(TagConnectionFailed, "could not connect: %v", err)
	default:
		return rtErr(TagConnectionError, "HTTP connection error: %v", err)
	}
}

func isDialError(err error) bool {
	var oe *net.OpError

	return errors.As(err, &oe) && oe.Op == "dial"
}

func buildRequest(c *callCtx, method, target string, a []any) (*http.Request, error) {
	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, valueErr("http: %q is not an absolute http(s) URL", target)
	}

	if a[4] != nil {
		q, qerr := argMap("http", "query", a[4])
		if qerr != nil {
			return nil, qerr
		}

		vals := u.Query()
		for _, k := range sortedKeys(q) {
			addQuery(vals, k, q[k])
		}

		u.RawQuery = vals.Encode()
	}

	body, ctype, err := encodeBody(a[2])
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(c.ctx, method, u.String(), body)
	if err != nil {
		return nil, valueErr("http: %v", err)
	}

	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}

	if a[3] != nil {
		hm, herr := argMap("http", paramHeaders, a[3])
		if herr != nil {
			return nil, herr
		}

		for k, v := range hm {
			req.Header.Set(k, formatValue(v))
		}
	}

	return req, applyAuth(c, req, a[5])
}

func addQuery(vals url.Values, k string, v any) {
	if l, ok := v.([]any); ok {
		for _, e := range l {
			vals.Add(k, formatValue(e))
		}

		return
	}

	vals.Add(k, formatValue(v))
}

func encodeBody(v any) (io.Reader, string, error) {
	switch b := v.(type) {
	case nil:
		return nil, "", nil
	case string:
		return strings.NewReader(b), "text/plain; charset=utf-8", nil
	case []byte:
		return bytes.NewReader(b), "application/octet-stream", nil
	default:
		raw, err := ToJSON(v)
		if err != nil {
			return nil, "", valueErr("http: body cannot be encoded: %v", err)
		}

		return bytes.NewReader(raw), "application/json", nil
	}
}

func applyAuth(c *callCtx, req *http.Request, auth any) error {
	if auth == nil {
		return nil
	}

	m, err := argMap("http", "auth", auth)
	if err != nil {
		return err
	}

	kind, _ := m["type"].(string)
	if kind != "OIDC" && kind != "OAuth2" {
		return valueErr("http: auth.type must be OIDC or OAuth2")
	}

	aud, _ := m["audience"].(string)
	scope, _ := m["scopes"].(string)

	if l, ok := m["scopes"].([]any); ok {
		parts := make([]string, len(l))
		for i, s := range l {
			parts[i] = formatValue(s)
		}

		scope = strings.Join(parts, " ")
	}

	if h := c.host(); h.Token != nil {
		req.Header.Set("Authorization", "Bearer "+h.Token(kind, aud, scope, req.URL.String()))
	}

	return nil
}

func httpResult(resp *http.Response, raw []byte) (any, error) {
	headers := make(map[string]any, len(resp.Header))
	for k, v := range resp.Header {
		headers[k] = strings.Join(v, ",")
	}

	body, err := decodeBody(resp.Header.Get("Content-Type"), raw)
	if err != nil {
		return nil, err
	}

	result := map[string]any{"code": int64(resp.StatusCode), paramHeaders: headers, "body": body}

	if resp.StatusCode >= http.StatusBadRequest {
		result["message"] = "HTTP server responded with error code " + formatValue(int64(resp.StatusCode))
		result["tags"] = []any{TagHTTPError}

		return nil, &Error{Value: result}
	}

	return result, nil
}

func decodeBody(contentType string, raw []byte) (any, error) {
	if len(raw) == 0 {
		return nil, nil //nolint:nilnil // no body
	}

	ct := strings.ToLower(contentType)

	switch {
	case strings.Contains(ct, "json"):
		v, err := ParseJSON(raw)
		if err != nil {
			return nil, rtErr(TagResponseTypeError, "the response is not valid JSON: %v", err)
		}

		return v, nil
	case strings.HasPrefix(ct, "text/"):
		return string(raw), nil
	default:
		return raw, nil
	}
}
