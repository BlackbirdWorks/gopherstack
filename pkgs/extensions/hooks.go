package extensions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var errHookStatus = errors.New("hook returned error status")

const (
	maxHookBody   = 4 << 20
	maxHookReply  = 8 << 20
	allServices   = "*"
	lifecycleBody = `{"event":%q,"extension":%q}`
)

// lifecycle POSTs a lifecycle event to the extension; failures are logged, never fatal.
// Events are serialized per extension and gated: ready only after startup, nothing after shutdown.
func (m *Manager) lifecycle(ctx context.Context, e *extension, event string) {
	e.seq <- struct{}{}
	defer func() { <-e.seq }()

	path, ep := m.claimEvent(e, event)
	if path == "" || ep == nil {
		return
	}

	body := fmt.Appendf(nil, lifecycleBody, event, e.spec.Name)

	_, err := m.post(ctx, ep, path, body, m.cfg.HookTimeout)
	if err != nil {
		m.log.WarnContext(
			ctx,
			"extension lifecycle hook failed",
			"extension",
			e.spec.Name,
			"event",
			event,
			"error",
			err,
		)
	}
}

// claimEvent decides whether event may be sent now and records it as sent.
func (m *Manager) claimEvent(e *extension, event string) (string, *url.URL) {
	m.mu.Lock("lifecycle")
	defer m.mu.Unlock()

	if e.reg == nil || e.endpoint == nil {
		return "", nil
	}

	switch event {
	case "on_startup":
		if m.stopping.Load() || e.started {
			return "", nil
		}

		e.started = true

		return e.reg.Hooks.OnStartup, e.endpoint
	case "on_ready":
		if m.stopping.Load() || !e.started || e.readySent {
			return "", nil
		}

		e.readySent = true

		return e.reg.Hooks.OnReady, e.endpoint
	case "on_shutdown":
		if !e.started || e.shutdownSent {
			return "", nil
		}

		e.shutdownSent = true

		return e.reg.Hooks.OnShutdown, e.endpoint
	}

	return "", nil
}

func (m *Manager) post(
	ctx context.Context,
	ep *url.URL,
	path string,
	body []byte,
	timeout time.Duration,
) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.JoinPath(path).String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := m.hookHTTP.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	out, err := io.ReadAll(io.LimitReader(resp.Body, maxHookReply))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("%w: %d", errHookStatus, resp.StatusCode)
	}

	return out, nil
}

type boundHook struct {
	ep      *url.URL
	name    string
	pre     string
	post    string
	timeout time.Duration
	open    bool
}

// HookSet is the set of request hooks that apply to one service for one request.
type HookSet struct {
	m     *Manager
	svc   string
	hooks []boundHook
}

// Hooks returns the request hooks registered for service, or nil when there are none.
func (m *Manager) Hooks(service string) *HookSet {
	m.mu.RLock("Hooks")
	defer m.mu.RUnlock()

	var hs []boundHook

	for _, e := range m.exts {
		if e.reg == nil || e.status != StatusReady {
			continue
		}

		for _, rh := range e.reg.Hooks.Request {
			if !appliesTo(rh.Services, service) {
				continue
			}

			t := m.cfg.HookTimeout
			if rh.TimeoutMs > 0 {
				t = time.Duration(rh.TimeoutMs) * time.Millisecond
			}

			open := m.cfg.FailOpenDefault()
			if rh.FailOpen != nil {
				open = *rh.FailOpen
			}

			hs = append(
				hs,
				boundHook{ep: e.endpoint, name: e.spec.Name, pre: rh.Pre, post: rh.Post, timeout: t, open: open},
			)
		}
	}

	if len(hs) == 0 {
		return nil
	}

	return &HookSet{m: m, svc: service, hooks: hs}
}

func appliesTo(services []string, svc string) bool {
	for _, s := range services {
		if s == allServices || strs.Equal(s, svc) {
			return true
		}
	}

	return false
}

// HasPost reports whether any hook wants the response.
func (h *HookSet) HasPost() bool {
	for _, b := range h.hooks {
		if b.post != "" {
			return true
		}
	}

	return false
}

type hookRequest struct {
	Headers         http.Header `json:"headers"`
	ResponseHeaders http.Header `json:"response_headers,omitempty"`
	Phase           string      `json:"phase"`
	Extension       string      `json:"extension"`
	Service         string      `json:"service"`
	Method          string      `json:"method"`
	Path            string      `json:"path"`
	Query           string      `json:"query"`
	Body            []byte      `json:"body"`
	ResponseBody    []byte      `json:"response_body,omitempty"`
	Status          int         `json:"status,omitempty"`
	BodyTruncated   bool        `json:"body_truncated,omitempty"`
}

// HookResponse is a reply an extension (or a failed closed hook) wants sent to the client.
type HookResponse struct {
	Headers map[string]string `json:"headers"`
	Body    []byte            `json:"body"`
	Status  int               `json:"status"`
}

type preReply struct {
	Respond    *HookResponse     `json:"respond"`
	SetHeaders map[string]string `json:"set_headers"`
}

type postReply struct {
	Body       *[]byte           `json:"body"`
	SetHeaders map[string]string `json:"set_headers"`
	Status     int               `json:"status"`
}

// Write sends the response.
func (r *HookResponse) Write(w http.ResponseWriter) {
	for k, v := range r.Headers {
		w.Header().Set(k, v)
	}

	w.WriteHeader(r.Status)
	_, _ = w.Write(r.Body)
}

func failClosed(name string) *HookResponse {
	return &HookResponse{
		Status:  http.StatusBadGateway,
		Headers: map[string]string{"Content-Type": "application/json"},
		Body:    fmt.Appendf(nil, `{"message":"extension hook %s failed"}`, name),
	}
}

// Pre runs the pre-request hooks. A non-nil result short-circuits the request; the request
// body is restored and any set_headers are applied to r.
func (h *HookSet) Pre(r *http.Request) *HookResponse {
	body, truncated := readBody(r)
	base := hookRequest{
		Phase: "pre", Service: h.svc, Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery,
		Headers: r.Header, Body: body, BodyTruncated: truncated,
	}

	for _, b := range h.hooks {
		if b.pre == "" {
			continue
		}

		var rep preReply

		if resp := h.call(r.Context(), b, b.pre, base, &rep); resp != nil {
			return resp
		}

		for k, v := range rep.SetHeaders {
			r.Header.Set(k, v)
		}

		if rep.Respond != nil {
			if rep.Respond.Status == 0 {
				rep.Respond.Status = http.StatusOK
			}

			return rep.Respond
		}
	}

	return nil
}

// Post runs the post-response hooks; replies may replace status, headers and body in rec.
func (h *HookSet) Post(r *http.Request, rec *Recorder) *HookResponse {
	body, truncated := rec.bodyForHook()
	base := hookRequest{
		Phase: "post", Service: h.svc, Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery,
		Headers: r.Header, Status: rec.Status(), ResponseHeaders: rec.Header(),
		ResponseBody: body, BodyTruncated: truncated,
	}

	for _, b := range h.hooks {
		if b.post == "" {
			continue
		}

		var rep postReply

		if resp := h.call(r.Context(), b, b.post, base, &rep); resp != nil {
			return resp
		}

		rec.apply(rep)
	}

	return nil
}

func (h *HookSet) call(ctx context.Context, b boundHook, path string, req hookRequest, rep any) *HookResponse {
	req.Extension = b.name

	payload, err := json.Marshal(req)
	if err == nil {
		var out []byte

		if out, err = h.m.post(ctx, b.ep, path, payload, b.timeout); err == nil && len(bytes.TrimSpace(out)) > 0 {
			err = json.Unmarshal(out, rep)
		}
	}

	if err == nil {
		return nil
	}

	h.m.log.WarnContext(
		ctx,
		"extension request hook failed",
		"extension",
		b.name,
		"service",
		h.svc,
		"fail_open",
		b.open,
		"error",
		err,
	)

	if b.open {
		return nil
	}

	return failClosed(b.name)
}

func readBody(r *http.Request) ([]byte, bool) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, false
	}

	head, _ := io.ReadAll(io.LimitReader(r.Body, maxHookBody+1))
	truncated := len(head) > maxHookBody
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(head), r.Body), r.Body}

	if truncated {
		return nil, true
	}

	return head, false
}
