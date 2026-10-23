package bedrockagentcore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	hdrSession        = "X-Amzn-Bedrock-Agentcore-Runtime-Session-Id"
	defaultQualifier  = "DEFAULT"
	minSessionIDLen   = 33
	maxSessionIDLen   = 256
	runtimeARNParts   = 6
	flushChunk        = 32 << 10
	runtimeIDPrefix   = "runtime/"
	customHeaderStart = "X-Amzn-Bedrock-Agentcore-Runtime-"
)

// forwardRequestHeaders are copied from the SDK request to the agent container.
func forwardRequestHeaders() []string {
	return []string{
		"Content-Type", "Accept", "Authorization", "Baggage", "Traceparent", "Tracestate", "X-Amzn-Trace-Id",
		"Mcp-Method", "Mcp-Name", "Mcp-Protocol-Version", "Mcp-Session-Id",
	}
}

// forwardResponseHeaders are copied from the agent container to the SDK response.
func forwardResponseHeaders() []string {
	return []string{
		"Content-Type", "Baggage", "Traceparent", "Tracestate", "X-Amzn-Trace-Id", "Mcp-Protocol-Version",
		"Mcp-Session-Id", "Cache-Control",
	}
}

type runtimeTarget struct {
	sc        Scope
	key       string
	sessionID string
	info      RuntimeInfo
}

// parseRuntimeARN splits arn:aws:bedrock-agentcore:<region>:<account>:runtime/<id>.
func parseRuntimeARN(ref string) (Scope, string, error) {
	parts := strings.SplitN(ref, ":", runtimeARNParts)
	if len(parts) != runtimeARNParts || parts[0] != "arn" || parts[2] != arnService ||
		!strings.HasPrefix(parts[5], runtimeIDPrefix) || strings.Contains(parts[5][len(runtimeIDPrefix):], "/") {
		return Scope{}, "", invalid("agentRuntimeArn %q is not a valid agent runtime ARN", ref)
	}

	return Scope{Account: parts[4], Region: parts[3]}, parts[5][len(runtimeIDPrefix):], nil
}

func (b *InMemoryBackend) resolveRuntime(r *http.Request, p map[string]string, session string) (*runtimeTarget, error) {
	sc, id, err := parseRuntimeARN(p["agentRuntimeArn"])
	if err != nil {
		return nil, err
	}

	qualifier := r.URL.Query().Get("qualifier")
	if qualifier == "" {
		qualifier = defaultQualifier
	}

	c := b.ctl()
	if c == nil {
		return nil, internal("the control plane is not wired")
	}

	info, err := c.Runtime(sc, id, qualifier)

	switch {
	case errors.Is(err, ErrNotFound):
		return nil, notFound("agent runtime %q with qualifier %q not found", id, qualifier)
	case errors.Is(err, ErrNotReady):
		return nil, retryableConflict("agent runtime %q is not READY", id)
	case err != nil:
		return nil, internal("resolving the agent runtime failed")
	}

	return &runtimeTarget{sc: sc, info: info, key: runtimeKey(sc, id, qualifier, session), sessionID: session}, nil
}

func sessionHeader(r *http.Request, required bool) (string, error) {
	s := r.Header.Get(hdrSession)
	if s == "" {
		if required {
			return "", invalid("%s header is required", hdrSession)
		}

		return awsid.NewN("", minSessionIDLen), nil
	}

	if len(s) < minSessionIDLen || len(s) > maxSessionIDLen {
		return "", invalid("runtimeSessionId must be between %d and %d characters", minSessionIDLen, maxSessionIDLen)
	}

	return s, nil
}

func (b *InMemoryBackend) invokeAgentRuntime(c *echo.Context, p map[string]string) error {
	r := c.Request()

	session, err := sessionHeader(r, false)
	if err != nil {
		return writeErr(c, err)
	}

	tgt, err := b.resolveRuntime(r, p, session)
	if err != nil {
		return writeErr(c, err)
	}

	if tgt.info.ImageURI == "" {
		return writeErr(c, invalid("only container-based agent runtimes can be invoked"))
	}

	eng, err := b.containerEngine()
	if err != nil {
		return writeErr(c, err)
	}

	eng.reapRuntimes()

	rs, err := eng.session(r.Context(), tgt.key, tgt.info, tgt.sc)
	if err != nil {
		return writeErr(c, err)
	}

	defer eng.release(rs)

	return b.proxyInvocation(c, eng, rs, tgt, http.MethodPost, rs.proto.invokePath)
}

func writeErr(c *echo.Context, err error) error {
	if apiErr, ok := errors.AsType[*restroute.Error](err); ok {
		return restroute.WriteError(c, apiErr.Status, apiErr.Code, apiErr.Message, apiErr.Extra)
	}

	return restroute.WriteError(
		c,
		http.StatusInternalServerError,
		"InternalServerException",
		"Internal server error",
		nil,
	)
}

func (e *engine) outbound(ctx context.Context, r *http.Request, rs *runtimeSession, method, path string) (
	*http.Request, error,
) {
	var body io.Reader = http.NoBody

	if method == http.MethodPost {
		payload, err := httputils.ReadBody(r)
		if err != nil {
			return nil, invalid("the payload could not be read or exceeds %d bytes", httputils.MaxRequestBodyBytes)
		}

		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, rs.base+path, body)
	if err != nil {
		return nil, err
	}

	for _, h := range forwardRequestHeaders() {
		if v := r.Header.Get(h); v != "" && !isSigV4(h, v) {
			req.Header.Set(h, v)
		}
	}

	for k, vs := range r.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), customHeaderStart) {
			req.Header[k] = vs
		}
	}

	return req, nil
}

// isSigV4 keeps the caller's AWS signature out of the agent: only bearer-style Authorization values are forwarded.
func isSigV4(header, value string) bool {
	return header == "Authorization" && strings.HasPrefix(value, "AWS4-")
}

func (b *InMemoryBackend) proxyInvocation(
	c *echo.Context, eng *engine, rs *runtimeSession, tgt *runtimeTarget, method, path string,
) error {
	req, err := eng.outbound(c.Request().Context(), c.Request(), rs, method, path)
	if err != nil {
		return writeErr(c, err)
	}

	req.Header.Set(hdrSession, tgt.sessionID)

	resp, err := eng.cfg.HTTP.Do(req)
	if err != nil {
		return writeErr(c, runtimeClient("the agent runtime could not be reached: %v", err))
	}

	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 { //nolint:mnd // 2xx
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))

		return writeErr(c, runtimeClient("the agent runtime returned HTTP %d: %s", resp.StatusCode, snippet))
	}

	return streamResponse(c, resp, tgt.sessionID)
}

func streamResponse(c *echo.Context, resp *http.Response, session string) error {
	w := c.Response()

	for _, h := range forwardResponseHeaders() {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}

	w.Header().Set(hdrSession, session)
	w.WriteHeader(resp.StatusCode)

	rc := http.NewResponseController(w)
	buf := make([]byte, flushChunk)

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return nil //nolint:nilerr // client went away
			}

			_ = rc.Flush()
		}

		if err != nil {
			return nil //nolint:nilerr // EOF or upstream abort ends the stream
		}
	}
}

func (b *InMemoryBackend) stopRuntimeSession(c *echo.Context, p map[string]string) error {
	r := c.Request()

	session, err := sessionHeader(r, true)
	if err != nil {
		return writeErr(c, err)
	}

	tgt, err := b.resolveRuntime(r, p, session)
	if err != nil {
		return writeErr(c, err)
	}

	eng, err := b.containerEngine()
	if err != nil {
		return writeErr(c, err)
	}

	if !eng.stopRuntimeSession(tgt.key) {
		return writeErr(c, notFound("runtime session %q not found", session))
	}

	c.Response().Header().Set(hdrSession, session)

	return c.NoContent(http.StatusOK)
}

func (b *InMemoryBackend) getAgentCard(c *echo.Context, p map[string]string) error {
	r := c.Request()

	session, err := sessionHeader(r, false)
	if err != nil {
		return writeErr(c, err)
	}

	tgt, err := b.resolveRuntime(r, p, session)
	if err != nil {
		return writeErr(c, err)
	}

	if !strings.EqualFold(tgt.info.Protocol, protoA2A) {
		return writeErr(c, invalid("agent cards are served only by A2A agent runtimes"))
	}

	eng, err := b.containerEngine()
	if err != nil {
		return writeErr(c, err)
	}

	rs, err := eng.session(r.Context(), tgt.key, tgt.info, tgt.sc)
	if err != nil {
		return writeErr(c, err)
	}

	defer eng.release(rs)

	return b.proxyInvocation(c, eng, rs, tgt, http.MethodGet, "/.well-known/agent-card.json")
}
