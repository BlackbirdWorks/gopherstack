package mwaa

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const (
	tokenKindCLI  = "cli"
	tokenKindWeb  = "web"
	tokenSigBytes = 32
)

type airflowHealth struct {
	Metadatabase struct {
		Status string `json:"status"`
	} `json:"metadatabase"`
	Scheduler struct {
		Status string `json:"status"`
	} `json:"scheduler"`
}

// healthy reports whether the webserver answers its health endpoint with a healthy database and scheduler.
func (e *airflowEngine) healthy(ctx context.Context, base string) bool {
	for _, p := range []string{"/health", "/api/v2/monitor/health"} {
		var h airflowHealth

		status, _, err := e.do(ctx, http.MethodGet, base+p, nil, &h)
		if err == nil && status == http.StatusOK && h.Metadatabase.Status == "healthy" &&
			h.Scheduler.Status == "healthy" {
			return true
		}
	}

	return false
}

type airflowResponse struct {
	header  http.Header
	cookies []*http.Cookie
	body    []byte
	status  int
}

// do performs one unauthenticated Airflow HTTP call; a JSON body is decoded into out when set.
func (e *airflowEngine) do(ctx context.Context, method, target string, body []byte, out any) (int, []byte, error) {
	var hdr http.Header
	if body != nil {
		hdr = http.Header{headerContentType: {"application/json"}}
	}

	return e.doWith(ctx, method, target, body, hdr, out)
}

func (e *airflowEngine) doWith(
	ctx context.Context, method, target string, body []byte, hdr http.Header, out any,
) (int, []byte, error) {
	res, err := e.request(ctx, method, target, body, hdr)
	if err != nil {
		return 0, nil, err
	}

	if out != nil && len(res.body) > 0 {
		_ = json.Unmarshal(res.body, out)
	}

	return res.status, res.body, nil
}

// request performs one Airflow HTTP call without following redirects.
func (e *airflowEngine) request(
	ctx context.Context, method, target string, body []byte, hdr http.Header,
) (*airflowResponse, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, target, rd)
	if err != nil {
		return nil, fmt.Errorf("build airflow request: %w", err)
	}

	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	resp, err := e.cfg.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("airflow %s %s: %w", method, target, err)
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, airflowMaxBody))
	if err != nil {
		return nil, fmt.Errorf("read airflow response: %w", err)
	}

	return &airflowResponse{status: resp.StatusCode, header: resp.Header, cookies: resp.Cookies(), body: raw}, nil
}

// newToken issues a short-lived opaque JWT-shaped token bound to one environment.
func (e *airflowEngine) newToken(key, envName, kind string) string {
	e.tokens.Range(func(tok string, t airflowToken) bool {
		if time.Now().After(t.expires) {
			e.tokens.Delete(tok)
		}

		return true
	})

	exp := time.Now().Add(e.cfg.TokenTTL)
	sig := make([]byte, tokenSigBytes)
	_, _ = rand.Read(sig)

	enc := base64.RawURLEncoding
	payload, _ := json.Marshal(map[string]any{"env": envName, "type": kind, "exp": exp.Unix()})
	tok := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + enc.EncodeToString(payload) + "." +
		enc.EncodeToString(sig)

	e.tokens.Set(tok, airflowToken{key: key, kind: kind, expires: exp})

	return tok
}

// airflowTokenLocked issues an engine token for key when the environment is container-backed.
func (b *InMemoryBackend) airflowTokenLocked(key, envName, kind string) (string, bool) {
	if !b.airflowManagedLocked(key) {
		return "", false
	}

	return b.airflow.newToken(key, envName, kind), true
}

// resolveToken maps a valid unexpired token to its running environment.
func (b *InMemoryBackend) resolveToken(token, kind string) (*airflowEngine, *airflowEnv, error) {
	b.mu.RLock("resolveToken")
	defer b.mu.RUnlock()

	e := b.airflow
	if e == nil {
		return nil, nil, errAirflowNoEngine
	}

	t, ok := e.tokens.Get(token)
	if !ok || t.kind != kind || time.Now().After(t.expires) {
		return nil, nil, errAirflowToken
	}

	env, found := b.environments.Get(t.key)
	if !found || env.Status != envStatusAvailable || e.envs[t.key] == nil {
		return nil, nil, ErrEnvironmentNotFound
	}

	return e, e.envs[t.key], nil
}

// ExecCLI runs `airflow <command>` in the environment's container, like MWAA's /aws_mwaa/cli endpoint.
func (b *InMemoryBackend) ExecCLI(ctx context.Context, token, command string) (string, string, error) {
	args := splitCommand(command)
	if len(args) == 0 {
		return "", "", fmt.Errorf("%w: empty airflow command", ErrInvalidParameter)
	}

	if err := checkCLICommand(args); err != nil {
		return "", "", err
	}

	e, ae, err := b.resolveToken(token, tokenKindCLI)
	if err != nil {
		return "", "", err
	}

	b.mu.RLock("ExecCLI")
	id := ae.containerID
	b.mu.RUnlock()

	res, err := e.cfg.Runtime.Exec(ctx, id, append([]string{"airflow"}, args...))
	if err != nil {
		return "", "", fmt.Errorf("airflow cli: %w", err)
	}

	return res.Stdout, res.Stderr, nil
}

// splitCommand splits a command line on whitespace, honouring single and double quotes.
func splitCommand(s string) []string {
	var sp cmdSplitter

	for _, r := range s {
		sp.feed(r)
	}

	sp.flush()

	return sp.args
}

type cmdSplitter struct {
	cur   strings.Builder
	args  []string
	quote rune
	has   bool
}

func (c *cmdSplitter) feed(r rune) {
	switch {
	case c.quote != 0 && r == c.quote:
		c.quote = 0
	case c.quote != 0:
		c.cur.WriteRune(r)
	case r == '\'' || r == '"':
		c.quote, c.has = r, true
	case unicode.IsSpace(r):
		c.flush()
	default:
		c.cur.WriteRune(r)
	}
}

func (c *cmdSplitter) flush() {
	if c.cur.Len() > 0 || c.has {
		c.args = append(c.args, c.cur.String())
		c.cur.Reset()

		c.has = false
	}
}

// restTargetLocked returns the engine and webserver target of an AVAILABLE container-backed env.
func (b *InMemoryBackend) restTargetLocked(key string) (*airflowEngine, airflowTarget, bool) {
	if !b.airflowManagedLocked(key) {
		return nil, airflowTarget{}, false
	}

	env, ok := b.environments.Get(key)
	if !ok {
		return nil, airflowTarget{}, false
	}

	t := b.airflow.envs[key].target()
	t.version = env.AirflowVersion

	return b.airflow, t, true
}

// proxyRest forwards an InvokeRestApi call to the Airflow REST API.
func (e *airflowEngine) proxyRest(
	ctx context.Context, t airflowTarget, req *invokeRestAPIRequest,
) (*InvokeRestAPIResponse, error) {
	apiRoot := "/api/v1"
	if isAirflowV3(t.version) {
		apiRoot = "/api/v2"
	}

	target := t.base + apiRoot + "/" + strings.TrimLeft(req.Path, "/")

	if q := queryString(req.QueryParameters); q != "" {
		target += "?" + q
	}

	var body []byte

	if req.Body != nil {
		var err error
		if body, err = json.Marshal(req.Body); err != nil {
			return nil, fmt.Errorf("%w: Body is not JSON: %w", ErrInvalidParameter, err)
		}
	}

	hdr, err := e.restHeaders(ctx, t)
	if err != nil {
		return nil, err
	}

	if body != nil {
		hdr.Set(headerContentType, "application/json")
	}

	status, raw, err := e.doWith(ctx, req.Method, target, body, hdr, nil)
	if err != nil {
		return nil, err
	}

	doc := any(map[string]any{})
	if len(raw) > 0 {
		if jerr := json.Unmarshal(raw, &doc); jerr != nil {
			doc = string(raw)
		}
	}

	code := int32(status) //nolint:gosec // HTTP status fits int32

	return &InvokeRestAPIResponse{RestAPIStatusCode: code, RestAPIResponse: doc}, nil
}

func queryString(params any) string {
	m, ok := params.(map[string]any)
	if !ok {
		return ""
	}

	v := url.Values{}
	for k, val := range m {
		v.Set(k, fmt.Sprint(val))
	}

	return v.Encode()
}
