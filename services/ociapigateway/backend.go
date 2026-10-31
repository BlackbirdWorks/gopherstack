package ociapigateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Default HTTP backend timeouts.
const (
	defaultConnectSec = 60
	defaultReadSec    = 10
	defaultSendSec    = 10
	dialerKeepAlive   = 30 * time.Second
)

var (
	errBackendURL  = errors.New("invalid backend url")
	errNoFunctions = errors.New("OCI Functions is not available")
)

// ErrDestinationNotPermitted is returned when a backend resolves outside the permitted network range.
var ErrDestinationNotPermitted = errors.New(
	"ociapigateway: destination is outside loopback and external backends are not allowed",
)

// FunctionRequest is the request an API gateway hands to OCI Functions.
type FunctionRequest struct {
	Header   http.Header
	Method   string
	Path     string
	RawQuery string
	Body     []byte
}

// FunctionResponse is the function's reply.
type FunctionResponse struct {
	Header http.Header
	Body   []byte
	Status int
}

// FunctionInvoker invokes an OCI function synchronously; wired to OCI Functions in internal/app.
type FunctionInvoker interface {
	InvokeFunction(ctx context.Context, functionID string, in FunctionRequest) (FunctionResponse, error)
}

// guardedDialer refuses connections to non-loopback peers unless external backends are allowed. The check runs
// on the resolved address, so a hostname that resolves elsewhere (or later rebinds) is still refused.
func (b *InMemoryBackend) guardedDialer() *net.Dialer {
	return &net.Dialer{
		KeepAlive: dialerKeepAlive,
		Control: func(_, address string, _ syscall.RawConn) error {
			if b.externalAllowed() {
				return nil
			}

			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}

			if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
				return ErrDestinationNotPermitted
			}

			return nil
		},
	}
}

func seconds(v *float64, def int) time.Duration {
	if v == nil || *v <= 0 {
		return time.Duration(def) * time.Second
	}

	return time.Duration(*v * float64(time.Second))
}

var hopHeaders = map[string]bool{ //nolint:gochecknoglobals // immutable
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Proxy-Connection": true,
}

// backendResponse is what a route backend produced.
type backendResponse struct {
	body   io.ReadCloser
	err    error
	header http.Header
	status int
}

func (b *InMemoryBackend) runBackend(
	ctx context.Context, be *backendDoc, r *http.Request, rv *reqView, body []byte,
) backendResponse {
	switch be.Type {
	case backendStock:
		return stockResponse(be, rv)
	case backendHTTP:
		return b.httpBackend(ctx, be, r, rv, body)
	case backendFn:
		return b.functionBackend(ctx, be, r, rv, body)
	case backendDynamic:
		chosen := selectDynamic(be, rv)
		if chosen == nil {
			return backendResponse{status: http.StatusNotFound, header: http.Header{}, body: http.NoBody}
		}

		return b.runBackend(ctx, chosen, r, rv, body)
	default:
		return backendResponse{err: fmt.Errorf("%w: backend type %s", errKeyFormat, be.Type)}
	}
}

func stockResponse(be *backendDoc, rv *reqView) backendResponse {
	h := http.Header{}
	for _, hs := range be.Headers {
		if hs.Name != "" {
			h.Add(hs.Name, rv.interpolate(hs.Value, nil))
		}
	}

	text := ""
	if be.Body != nil {
		text = rv.interpolate(*be.Body, nil)
	}

	return backendResponse{status: *be.Status, header: h, body: io.NopCloser(strings.NewReader(text))}
}

func (b *InMemoryBackend) httpBackend(
	ctx context.Context, be *backendDoc, r *http.Request, rv *reqView, body []byte,
) backendResponse {
	target := rv.interpolate(*be.URL, escapePath)

	u, err := url.Parse(target)
	if err != nil || !httpURL(u) {
		return backendResponse{err: fmt.Errorf("%w: %q", errBackendURL, target)}
	}

	appendQuery(u, rv.query.Encode())

	total := seconds(be.ConnectTimeout, defaultConnectSec) + seconds(be.ReadTimeout, defaultReadSec) +
		seconds(be.SendTimeout, defaultSendSec)

	ctx, cancel := context.WithTimeout(ctx, total)

	out, err := http.NewRequestWithContext(ctx, r.Method, u.String(), bytes.NewReader(body))
	if err != nil {
		cancel()

		return backendResponse{err: err}
	}

	copyRequestHeaders(out.Header, rv.header)
	out.Header.Set("X-Forwarded-For", clientKey(r))

	resp, err := b.client.Do(out) //nolint:gosec,bodyclose // G704 loopback-only by default; body closed by cancelBody
	if err != nil {
		cancel()

		return backendResponse{err: err}
	}

	return wrapResponse(resp, cancel)
}

// wrapResponse hands the body to the caller; cancelBody.Close closes it and cancels the request context.
func wrapResponse(resp *http.Response, cancel context.CancelFunc) backendResponse {
	h := http.Header{}

	for k, vs := range resp.Header {
		if !hopHeaders[k] {
			h[k] = vs
		}
	}

	return backendResponse{
		status: resp.StatusCode,
		header: h,
		body:   &cancelBody{ReadCloser: resp.Body, cancel: cancel},
	}
}

func appendQuery(u *url.URL, q string) {
	if q == "" {
		return
	}

	if u.RawQuery != "" {
		u.RawQuery += "&"
	}

	u.RawQuery += q
}

func copyRequestHeaders(dst, src http.Header) {
	for k, vs := range src {
		if hopHeaders[k] || k == "Host" || k == "Content-Length" || strings.HasPrefix(k, "X-Forwarded-") {
			continue
		}

		dst[k] = vs
	}
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelBody) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()

	return err
}

func (b *InMemoryBackend) functionBackend(
	ctx context.Context, be *backendDoc, r *http.Request, rv *reqView, body []byte,
) backendResponse {
	b.mu.RLock("functionBackend")
	inv := b.functions
	b.mu.RUnlock()

	if inv == nil {
		return backendResponse{err: errNoFunctions}
	}

	out, err := inv.InvokeFunction(ctx, be.FunctionID, FunctionRequest{
		Method: r.Method, Path: r.URL.Path, RawQuery: rv.query.Encode(), Header: rv.header.Clone(), Body: body,
	})
	if err != nil {
		return backendResponse{err: err}
	}

	h := out.Header
	if h == nil {
		h = http.Header{}
	}

	return backendResponse{status: out.Status, header: h, body: io.NopCloser(bytes.NewReader(out.Body))}
}

// selectDynamic picks the routing backend: exact keys first, then wildcard keys, then the default key.
func selectDynamic(be *backendDoc, rv *reqView) *backendDoc {
	val := rv.lookup(be.SelectionSource.Selector)

	for i := range be.RoutingBackends {
		k := &be.RoutingBackends[i].Key
		if k.Type == anyOf && containsStr(k.Values, val) {
			return be.RoutingBackends[i].Backend
		}
	}

	for i := range be.RoutingBackends {
		k := &be.RoutingBackends[i].Key
		if k.Type == wildcardKey && wildcardMatch(k.Expression, val) {
			return be.RoutingBackends[i].Backend
		}
	}

	for i := range be.RoutingBackends {
		if d := be.RoutingBackends[i].Key.IsDefault; d != nil && *d {
			return be.RoutingBackends[i].Backend
		}
	}

	return nil
}

func containsStr(list []string, s string) bool {
	return slices.Contains(list, s)
}

// wildcardMatch matches a key with one '*' (zero or more characters) or '+' (one or more)
// at its start or end.
func wildcardMatch(expr, val string) bool {
	switch {
	case strings.HasPrefix(expr, "*"):
		return strings.HasSuffix(val, expr[1:])
	case strings.HasPrefix(expr, "+"):
		return len(val) > len(expr)-1 && strings.HasSuffix(val, expr[1:])
	case strings.HasSuffix(expr, "*"):
		return strings.HasPrefix(val, expr[:len(expr)-1])
	case strings.HasSuffix(expr, "+"):
		return len(val) > len(expr)-1 && strings.HasPrefix(val, expr[:len(expr)-1])
	default:
		return val == expr
	}
}
