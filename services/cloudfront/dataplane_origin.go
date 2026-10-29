package cloudfront

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	originDialTimeout     = 10 * time.Second
	originResponseTimeout = 30 * time.Second
	maxOriginBody         = 64 << 20
	defaultHTTPPort       = 80
	defaultHTTPSPort      = 443
	originUserAgent       = "Amazon CloudFront"
)

func newOriginClient() *http.Client {
	return &http.Client{
		Timeout: originResponseTimeout,
		Transport: &http.Transport{
			DisableCompression: true,
			DisableKeepAlives:  true,
			TLSClientConfig:    &tls.Config{MinVersion: tls.VersionTLS12},
			DialContext:        (&net.Dialer{Timeout: originDialTimeout}).DialContext,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

var hopByHopHeaders = map[string]struct{}{ //nolint:gochecknoglobals // static header set
	"Connection": {}, "Keep-Alive": {}, "Proxy-Authenticate": {}, "Proxy-Authorization": {},
	"Te": {}, "Trailer": {}, "Transfer-Encoding": {}, "Upgrade": {},
}

func isHopByHop(name string) bool {
	_, ok := hopByHopHeaders[http.CanonicalHeaderKey(name)]

	return ok
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}

func (o *dpOrigin) targetURL(viewerScheme string) (string, string) {
	scheme := schemeHTTP
	port := defaultHTTPPort

	if o.Custom != nil {
		if o.Custom.HTTPPort > 0 {
			port = o.Custom.HTTPPort
		}

		secure := o.Custom.Policy == originPolicyHTTPS ||
			(o.Custom.Policy == originPolicyMatch && viewerScheme == schemeHTTPS)
		if secure {
			scheme, port = schemeHTTPS, defaultHTTPSPort
			if o.Custom.HTTPSPort > 0 {
				port = o.Custom.HTTPSPort
			}
		}
	}

	host := o.Domain
	if (scheme == schemeHTTP && port != defaultHTTPPort) || (scheme == schemeHTTPS && port != defaultHTTPSPort) {
		host = net.JoinHostPort(o.Domain, strconv.Itoa(port))
	}

	return scheme, host
}

// forwardedQuery keeps the query parameters the cache/origin request policies forward.
func forwardedQuery(raw string, pol *dpPolicy) string {
	if raw == "" || !pol.forwardsAnyQuery() {
		return ""
	}

	var out []string

	for part := range strings.SplitSeq(raw, "&") {
		name, _, _ := strings.Cut(part, "=")
		if dec, err := url.QueryUnescape(name); err == nil {
			name = dec
		}

		if pol.forwardsQuery(name) {
			out = append(out, part)
		}
	}

	return strings.Join(out, "&")
}

func (h *Handler) outboundRequest(
	ctx context.Context, o *dpOrigin, pol *dpPolicy, v *viewerReq, reqPath string,
) (*http.Request, error) {
	scheme, host := o.targetURL(v.scheme)
	rawPath := reqPath
	if reqPath == v.path {
		rawPath = v.rawPath
	}

	target := scheme + "://" + host + strings.TrimSuffix(o.Path, "/") + rawPath
	q := forwardedQuery(v.r.URL.RawQuery, pol)
	if v.edge != nil && v.edge.query != nil {
		q = *v.edge.query
	}

	if q != "" {
		target += "?" + q
	}

	var body io.Reader
	if len(v.body) > 0 {
		body = bytes.NewReader(v.body)
	}

	req, err := http.NewRequestWithContext(ctx, v.r.Method, target, body)
	if err != nil {
		return nil, err
	}

	setOutboundHeaders(req, o, pol, v)

	return req, nil
}

func setOutboundHeaders(req *http.Request, o *dpOrigin, pol *dpPolicy, v *viewerReq) {
	for name, vals := range v.r.Header {
		if skipOutboundHeader(name) {
			continue
		}

		if pol.forwardsHeader(name) || strings.EqualFold(name, headerContentType) {
			req.Header[http.CanonicalHeaderKey(name)] = append([]string(nil), vals...)
		}
	}

	if pol.forwardsHeader("Host") {
		req.Host = v.r.Host
	}

	if c := forwardedCookies(v.r, pol); c != "" {
		req.Header.Set("Cookie", c)
	}

	req.Header.Set("User-Agent", originUserAgent)
	req.Header.Set("Via", "1.1 "+req.Host+" (CloudFront)")
	req.Header.Set("X-Amz-Cf-Id", v.cfID)

	xff := clientIP(v.r)
	if prior := v.r.Header.Get("X-Forwarded-For"); prior != "" {
		xff = prior + ", " + xff
	}

	req.Header.Set("X-Forwarded-For", xff)

	if pol.cloudFrontHdr {
		req.Header["CloudFront-Forwarded-Proto"] = []string{v.scheme}
	}

	for _, hp := range o.Headers {
		if hp.Name != "" {
			req.Header.Set(hp.Name, hp.Value)
		}
	}

	applyEdgeHeaders(req, v.edge)
}

// applyEdgeHeaders makes the origin request carry exactly the headers an origin-request function returned.
func applyEdgeHeaders(req *http.Request, e *edgeOverride) {
	if e == nil || e.header == nil {
		return
	}

	for name := range req.Header {
		if _, kept := e.header[name]; !kept && !edgeDisallowed(name) {
			delete(req.Header, name)
		}
	}

	for name, vals := range e.header {
		switch {
		case strings.EqualFold(name, "Host"):
			req.Host = vals[0]
		case !edgeDisallowed(name) && !isHopByHop(name) && !strings.EqualFold(name, "Content-Length"):
			req.Header[name] = slices.Clone(vals)
		}
	}
}

func skipOutboundHeader(name string) bool {
	return isHopByHop(name) || strings.EqualFold(name, "Host") || strings.EqualFold(name, "Cookie") ||
		strings.EqualFold(name, "Content-Length")
}

func forwardedCookies(r *http.Request, pol *dpPolicy) string {
	var cookies []string

	for _, c := range r.Cookies() {
		if pol.forwardsCookie(c.Name) {
			cookies = append(cookies, c.Name+"="+c.Value)
		}
	}

	return strings.Join(cookies, "; ")
}

type bufferedWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (b *bufferedWriter) Header() http.Header         { return b.header }
func (b *bufferedWriter) Write(p []byte) (int, error) { return b.body.Write(p) }
func (b *bufferedWriter) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}

// forwardALB serves the request from an elbv2 load balancer when the origin domain is one.
func (h *Handler) forwardALB(o *dpOrigin, req *http.Request, v *viewerReq) (originResp, bool) {
	f := h.dp.alb.Load()
	if f == nil || !strings.Contains(strings.ToLower(o.Domain), ".elb.") {
		return originResp{}, false
	}

	fw := (*f)()
	if fw == nil {
		return originResp{}, false
	}

	bw := &bufferedWriter{header: http.Header{}}
	req.RemoteAddr = v.r.RemoteAddr

	if !fw.ServeALB(bw, req) {
		return originResp{}, false
	}

	if bw.status == 0 {
		bw.status = http.StatusOK
	}

	return originResp{status: bw.status, header: bw.header, body: bw.body.Bytes()}, true
}

func (h *Handler) fetchCustom(
	ctx context.Context, o *dpOrigin, pol *dpPolicy, v *viewerReq, reqPath string,
) originResp {
	if o.Custom != nil && o.Custom.ReadTimeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, time.Duration(o.Custom.ReadTimeout)*time.Second)
		defer cancel()
	}

	var resp originResp

	for range max(o.ConnAttempts, 1) {
		var connFailed bool

		if resp, connFailed = h.fetchCustomOnce(ctx, o, pol, v, reqPath); !connFailed {
			break
		}
	}

	return resp
}

// fetchCustomOnce makes one attempt; connFailed reports a transport failure worth retrying.
func (h *Handler) fetchCustomOnce(
	ctx context.Context, o *dpOrigin, pol *dpPolicy, v *viewerReq, reqPath string,
) (originResp, bool) {
	req, err := h.outboundRequest(ctx, o, pol, v, reqPath)
	if err != nil {
		return httpErrorResp(http.StatusBadGateway, "The origin request could not be built."), false
	}

	if resp, ok := h.forwardALB(o, req, v); ok {
		return resp, false
	}

	resp, err := h.dp.client.Do(req)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return httpErrorResp(
				http.StatusGatewayTimeout,
				"CloudFront attempted to establish a connection with the origin, but the attempt failed.",
			), true
		}

		return httpErrorResp(http.StatusBadGateway, "CloudFront wasn't able to connect to the origin."), true
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxOriginBody))
	if err != nil {
		return httpErrorResp(http.StatusBadGateway, "The origin response could not be read."), false
	}

	return originResp{status: resp.StatusCode, header: resp.Header, body: body}, false
}

// cacheKey folds the cache-policy-selected path, query, headers and cookies into one key.
func cacheKey(distID string, v *viewerReq, pol *dpPolicy) string {
	var sb strings.Builder

	sb.WriteString(distID)
	sb.WriteByte('|')
	sb.WriteString(v.path)
	sb.WriteByte('?')
	sb.WriteString(cacheKeyQuery(v.r.URL.Query(), pol))

	var hdrs []string

	for name, vals := range v.r.Header {
		if pol.cacheHeaders.selects(name, true) {
			hdrs = append(hdrs, strings.ToLower(name)+"="+strings.Join(vals, ","))
		}
	}

	sort.Strings(hdrs)
	sb.WriteByte('|')
	sb.WriteString(strings.Join(hdrs, ";"))

	var cks []string

	for _, c := range v.r.Cookies() {
		if pol.cacheCookies.selects(c.Name, false) {
			cks = append(cks, c.Name+"="+c.Value)
		}
	}

	sort.Strings(cks)
	sb.WriteByte('|')
	sb.WriteString(strings.Join(cks, ";"))

	return sb.String()
}

func cacheKeyQuery(q url.Values, pol *dpPolicy) string {
	sel := url.Values{}

	for k, vs := range q {
		if pol.cacheQuery.selects(k, false) {
			sel[k] = vs
		}
	}

	return sel.Encode()
}
