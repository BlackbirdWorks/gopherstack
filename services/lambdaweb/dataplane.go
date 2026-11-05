package lambdaweb

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// Platform error types carried in the lambda-web-error-type response header.
const (
	errMalformedRequest       = "Platform.MalformedRequest"
	errInvalidEndpoint        = "Platform.InvalidEndpoint"
	errUnsupportedHTTPVersion = "Platform.UnsupportedHttpVersion"
	errEndpointNotActive      = "Platform.EndpointNotActive"
	errRevisionNotActive      = "Platform.RevisionNotActive"
	errEndpointDisabled       = "Platform.EndpointDisabled"
	errEndpointRateThrottle   = "Platform.EndpointRequestThrottle"
	errEndpointMaxEnvThrottle = "Platform.EndpointMaxEnvThrottle"
	errRequestTimeout         = "Runtime.RequestTimeout"
	errUnreachable            = "Runtime.Unreachable"
	errTimeoutListen          = "Application.TimeoutListenError"
	errAccountVCPUThrottle    = "Platform.AccountActiveVCPUArm64Throttle"
	errCannotAssumeRoleType   = "Platform.CannotAssumeRole"
	errKMSDecryptionType      = "Platform.KMSDecryptionError"

	hdrRequestID    = "Lambda-Web-Request-Id"
	hdrEndpointName = "Lambda-Web-Endpoint-Name"
	hdrLogStream    = "Lambda-Web-Logstream"
	hdrErrorType    = "Lambda-Web-Error-Type"
	hdrContentSHA   = "X-Amz-Content-Sha256"

	hostInfix     = ".lambda-web."
	hostSuffixAWS = ".on.aws"
	hostSuffixLoc = ".localhost"

	sigV4RegionalAlgo = "AWS4-HMAC-SHA256"
	sigV4aAlgo        = "AWS4-ECDSA-P256-SHA256"
	sigV4ServiceIdx   = 3
	maxHeaderBytes    = 128 * 1024
)

func methodAllowed(m string) bool {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodOptions,
		http.MethodDelete:
		return true
	}

	return false
}

func hopHeaders() []string {
	return []string{
		"Connection", "Keep-Alive", "Upgrade", "Expect", "Te", "Trailer", "Proxy-Authenticate",
		"Proxy-Authorization", "Proxy-Connection", "X-Real-Ip", "X-Http-Method-Override", "X-Http-Method",
		"X-Method-Override",
	}
}

// IsEndpointHost reports whether host names a web function endpoint domain
// (<id>.<shard>.lambda-web.<region>.on.aws, <id>.lambda-web.global.on.aws, or the same under .localhost).
func IsEndpointHost(host string) bool {
	_, ok := canonicalDomain(host)

	return ok
}

// canonicalDomain maps a Host header to the endpoint domain it names, in its ".on.aws" form.
func canonicalDomain(host string) (string, bool) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	host = strings.TrimSuffix(host, ".")

	rest, ok := strings.CutSuffix(strings.ToLower(host), hostSuffixAWS)
	if !ok {
		rest, ok = strings.CutSuffix(strings.ToLower(host), hostSuffixLoc)
	}

	if !ok || !strings.Contains(rest, hostInfix) {
		return "", false
	}

	return rest + hostSuffixAWS, true
}

func (b *InMemoryBackend) findEndpointLocked(domain string) *Endpoint {
	for _, e := range b.endpoints.All() {
		if e.DomainName == domain {
			return e
		}

		for _, d := range e.RegionDomains {
			if d == domain {
				return e
			}
		}
	}

	return nil
}

// ServeEndpoint serves r when its Host names a web function endpoint. It reports false for any other host.
func (b *InMemoryBackend) ServeEndpoint(w http.ResponseWriter, r *http.Request) bool {
	domain, ok := canonicalDomain(r.Host)
	if !ok {
		return false
	}

	reqID := newRequestID()
	w.Header().Set(hdrRequestID, reqID)

	if rejectRequest(w, r, domain) {
		return true
	}

	ep, status, reason := b.activeEndpoint(domain)
	if ep == nil {
		platformError(w, status, reason, "")

		return true
	}

	b.sweepIfDue()

	cw := &captureWriter{ResponseWriter: w}
	rm := &requestMetrics{start: b.run.now(), cw: cw}
	defer b.recordRequest(ep, cw, rm)

	if !b.authorize(cw, r, ep) {
		return true
	}

	key := endpointKey(ep.Region, ep.FunctionName, ep.Name)

	if rejected := b.run.admitRate(key, ep.RateLimit, b.run.now()); rejected != "" {
		code := http.StatusTooManyRequests
		if rejected == errEndpointDisabled {
			code = http.StatusServiceUnavailable
		} else {
			rm.throttle = "EndpointRequestThrottles"
		}

		platformError(cw, code, rejected, "")

		return true
	}

	b.proxy(cw, r, ep, reqID, rm)

	return true
}

func rejectRequest(w http.ResponseWriter, r *http.Request, domain string) bool {
	switch {
	case !methodAllowed(r.Method):
		platformError(w, http.StatusBadRequest, errMalformedRequest, "Unsupported HTTP method.")
	case r.ProtoMajor == 1 && r.ProtoMinor == 0:
		platformError(w, http.StatusHTTPVersionNotSupported, errUnsupportedHTTPVersion, "HTTP/1.0 is not supported.")
	case headerBytes(r) > maxHeaderBytes:
		platformError(w, http.StatusRequestHeaderFieldsTooLarge, "Platform.HeaderTooLarge", "")
	case strings.ToLower(hostOnly(r.Host)) != hostOnly(r.Host) && domain != "":
		platformError(w, http.StatusNotFound, errInvalidEndpoint, "")
	default:
		return false
	}

	return true
}

func hostOnly(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}

	return host
}

func headerBytes(r *http.Request) int {
	n := len(r.Method) + len(r.RequestURI)

	for k, vs := range r.Header {
		for _, v := range vs {
			n += len(k) + len(v) + 4 //nolint:mnd // ": " and CRLF
		}
	}

	return n
}

// activeEndpoint resolves the endpoint a domain names and requires it Active.
func (b *InMemoryBackend) activeEndpoint(domain string) (*Endpoint, int, string) {
	b.mu.Lock("activeEndpoint")
	defer b.mu.Unlock()

	b.advanceLocked()

	e := b.findEndpointLocked(domain)
	if e == nil {
		return nil, http.StatusNotFound, errInvalidEndpoint
	}

	if f, ok := b.functions.Get(functionKey(e.Region, e.FunctionName)); !ok || f.State == stateDeleting {
		return nil, http.StatusNotFound, errInvalidEndpoint
	}

	if e.State != stateActive {
		return nil, http.StatusConflict, errEndpointNotActive
	}

	return b.endpointView(e), 0, ""
}

// SetSigV4Secret sets the secret IamAuth endpoints verify signatures against; empty means "test".
func (b *InMemoryBackend) SetSigV4Secret(secret string) {
	b.mu.Lock("SetSigV4Secret")
	defer b.mu.Unlock()

	b.sigV4Secret = secret
}

// authorize enforces the endpoint's authType; ApplicationManaged requests only lose their AWS signature.
func (b *InMemoryBackend) authorize(w http.ResponseWriter, r *http.Request, ep *Endpoint) bool {
	auth := r.Header.Get("Authorization")

	if ep.AuthType == authIam {
		if !b.validSignature(r, ep, auth) || !b.policyAllowsInvoke(r, ep, auth) {
			w.Header().Set("X-Amzn-Errortype", "AccessDeniedException")
			http.Error(w, `{"Message":"Forbidden"}`, http.StatusForbidden)

			return false
		}
	}

	if strings.HasPrefix(auth, "AWS4-") {
		r.Header.Del("Authorization")
		r.Header.Del("X-Amz-Security-Token")
	}

	return true
}

func (b *InMemoryBackend) validSignature(r *http.Request, ep *Endpoint, auth string) bool {
	if r.Header.Get(hdrContentSHA) == "" || sigService(auth) != serviceName {
		return false
	}

	switch {
	case ep.Type == endpointMultiRegion:
		return strings.HasPrefix(auth, sigV4aAlgo+" ")
	case strings.HasPrefix(auth, sigV4RegionalAlgo+" "):
		b.mu.RLock("sigV4Secret")
		secret := b.sigV4Secret
		b.mu.RUnlock()

		return httputils.NewSigV4Validator(secret).Verify(r) == nil
	}

	return false
}

func sigService(auth string) string {
	_, after, ok := strings.Cut(auth, "Credential=")
	if !ok {
		return ""
	}

	cred, _, _ := strings.Cut(after, ",")
	parts := strings.Split(strings.TrimSpace(cred), "/")

	if len(parts) > sigV4ServiceIdx {
		return parts[sigV4ServiceIdx]
	}

	return ""
}

func platformError(w http.ResponseWriter, status int, errType, msg string) {
	if errType != "" {
		w.Header().Set(hdrErrorType, errType)
	}

	if msg == "" {
		msg = http.StatusText(status)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"message":`+strconv.Quote(msg)+`}`)
}

// proxy picks a revision by weight and forwards the request to one of its execution environments.
func (b *InMemoryBackend) proxy(w http.ResponseWriter, r *http.Request, ep *Endpoint, reqID string, rm *requestMetrics) {
	revID, ok := b.run.pick(ep.Weights)
	if !ok {
		platformError(w, http.StatusConflict, errRevisionNotActive, "")

		return
	}

	b.mu.RLock("proxyRevision")
	rev, found := b.revisions.Get(revisionKey(ep.Region, ep.FunctionName, revID))

	var cp *Revision
	if found {
		cp = rev.clone()
	}
	b.mu.RUnlock()

	if cp == nil || cp.State != stateActive {
		platformError(w, http.StatusConflict, errRevisionNotActive, "")

		return
	}

	timeout := time.Duration(cp.ServiceConfig.TimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	rm.scope = scopeOf(ep)

	lease, th, err := b.run.acquire(ctx, cp, b.capacityFor(ep, cp, reqID))
	if th != nil {
		rm.throttle = th.metric
		platformError(w, http.StatusTooManyRequests, th.errType, "")

		return
	}

	if err != nil {
		b.environmentFailed(ctx, w, cp, err)

		return
	}

	rm.lease = lease
	defer func() { lease.done(rm.cw.bytes) }()

	env := lease.inst.env
	host, port := env.Addr()

	out, err := b.outbound(ctx, r, ep, reqID, net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		platformError(w, http.StatusBadGateway, errTimeoutListen, "")

		return
	}

	resp, err := b.httpClient().Do(out)
	if err != nil {
		upstreamFailed(ctx, w, err)

		return
	}
	defer resp.Body.Close()

	copyResponse(w, resp, reqID, lease.inst.stream)
}

func scopeOf(ep *Endpoint) []string {
	out := make([]string, 0, len(ep.Weights))
	for _, w := range ep.Weights {
		out = append(out, envKey(ep.Region, ep.FunctionName, w.RevisionID))
	}

	return out
}

func (b *InMemoryBackend) capacityFor(ep *Endpoint, rev *Revision, reqID string) capacity {
	_, logs, _, _ := b.environmentServices()

	perEnv := rev.ServiceConfig.MaxConcurrencyPerEnvironment
	if perEnv < 1 {
		perEnv = defaultConcurrency
	}

	return capacity{
		logs: logs, metrics: b.metricEmitter(), preflight: b.preflight, maxEnv: ep.MaxEnvironments,
		scope: scopeOf(ep), reqID: reqID, endpoint: ep.Name, perEnv: perEnv, regionMax: maxTotalArmVCpus / vcpusPerEnv,
	}
}

func (b *InMemoryBackend) environmentFailed(ctx context.Context, w http.ResponseWriter, rev *Revision, err error) {
	switch {
	case errors.Is(err, errEngineUnavailable):
		platformError(w, http.StatusServiceUnavailable, "", err.Error())
	case errors.Is(err, errCannotAssumeRole):
		platformError(w, http.StatusBadGateway, errCannotAssumeRoleType, err.Error())
	case errors.Is(err, errKMSDecryption):
		platformError(w, http.StatusBadGateway, errKMSDecryptionType, err.Error())
	case errors.Is(err, ErrCodeUnavailable):
		b.failRevision(rev.Region, rev.FunctionName, rev.ID, "The deployment package could not be read.")
		platformError(w, http.StatusConflict, errRevisionNotActive, "")
	case ctx.Err() != nil:
		platformError(w, http.StatusGatewayTimeout, errRequestTimeout, "")
	default:
		platformError(w, http.StatusBadGateway, errTimeoutListen, "")
	}
}

func upstreamFailed(ctx context.Context, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		platformError(w, http.StatusGatewayTimeout, errRequestTimeout, "")
	case errors.Is(err, context.Canceled):
		platformError(w, http.StatusBadGateway, "", "")
	default:
		platformError(w, http.StatusServiceUnavailable, errUnreachable, "")
	}
}

func (b *InMemoryBackend) httpClient() *http.Client {
	b.run.mu.Lock("httpClient")
	defer b.run.mu.Unlock()

	if b.run.client == nil {
		c := httpclient.New(0)
		c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		b.run.client = c
	}

	return b.run.client
}

func (b *InMemoryBackend) outbound(
	ctx context.Context, r *http.Request, ep *Endpoint, reqID, addr string,
) (*http.Request, error) {
	target := "http://" + addr + r.URL.RequestURI()
	out, err := http.NewRequestWithContext(ctx, r.Method, target, r.Body) //nolint:gosec // addr is our own environment
	if err != nil {
		return nil, err //nolint:wrapcheck // surfaced as a 502 by the caller
	}

	out.Header = r.Header.Clone()
	out.ContentLength = r.ContentLength

	for _, h := range hopHeaders() {
		out.Header.Del(h)
	}

	for k := range out.Header {
		if strings.HasPrefix(strings.ToLower(k), "lambda-web-") {
			out.Header.Del(k)
		}
	}

	if ip, _, splitErr := net.SplitHostPort(r.RemoteAddr); splitErr == nil {
		out.Header.Add("X-Forwarded-For", ip)
	}

	canonical, _ := canonicalDomain(r.Host)
	out.Host = canonical
	out.Header.Set(hdrRequestID, reqID)
	out.Header.Set(hdrEndpointName, ep.Name)

	return out, nil
}

func copyResponse(w http.ResponseWriter, resp *http.Response, reqID, logStream string) {
	h := w.Header()

	for k, vs := range resp.Header {
		h.Del(k)

		for _, v := range vs {
			h.Add(k, v)
		}
	}

	for _, name := range hopHeaders() {
		h.Del(name)
	}

	h.Del("Trailer")
	h.Del("Transfer-Encoding")

	h.Set(hdrRequestID, reqID)

	if logStream != "" {
		h.Set(hdrLogStream, logStream)
	}

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// EndpointAuthType returns the authType of the endpoint a Host header names, or "" when none matches.
func (b *InMemoryBackend) EndpointAuthType(host string) string {
	domain, ok := canonicalDomain(host)
	if !ok {
		return ""
	}

	b.mu.RLock("EndpointAuthType")
	defer b.mu.RUnlock()

	if e := b.findEndpointLocked(domain); e != nil {
		return e.AuthType
	}

	return ""
}
