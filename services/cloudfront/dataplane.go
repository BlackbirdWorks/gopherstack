package cloudfront

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"

	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	dataPlanePrefix      = "/_gopherstack/cloudfront/"
	dataPlaneAWSSuffix   = ".cloudfront.net"
	dataPlaneLocalSuffix = ".cloudfront.localhost"
	operationDataPlane   = "DataPlaneRequest"
	popLocation          = "GST1-C1"
	maxRequestBody       = 32 << 20
	cfIDBytes            = 42
	viaIDBytes           = 8
)

const (
	xCacheMiss  = "Miss from cloudfront"
	xCacheHit   = "Hit from cloudfront"
	xCacheError = "Error from cloudfront"
)

const (
	schemeHTTP        = "http"
	schemeHTTPS       = "https"
	headerContentType = "Content-Type"
)

// ALBForwarder serves a request in-process when its Host is an elbv2 load balancer DNS name.
type ALBForwarder interface {
	// ServeALB reports false when no load balancer owns r.Host.
	ServeALB(w http.ResponseWriter, r *http.Request) bool
}

type dataPlane struct {
	s3     atomic.Pointer[func() s3backend.StorageBackend]
	alb    atomic.Pointer[func() ALBForwarder]
	edge   atomic.Pointer[func() LambdaEdge]
	cache  *responseCache
	client *http.Client
}

func newDataPlane() *dataPlane {
	return &dataPlane{cache: newResponseCache(defaultCacheBytes), client: newOriginClient()}
}

// SetS3Backend wires a resolver for the S3 backend, called per request so wiring order does not matter.
func (h *Handler) SetS3Backend(f func() s3backend.StorageBackend) { h.dp.s3.Store(&f) }

// SetALBForwarder wires a resolver for the elbv2 data plane used by load balancer origins.
func (h *Handler) SetALBForwarder(f func() ALBForwarder) { h.dp.alb.Store(&f) }

// SetCacheLimit bounds the in-memory edge cache to n bytes, evicting least-recently-used entries.
func (h *Handler) SetCacheLimit(n int64) { h.dp.cache.setMax(n) }

// CacheStats reports the cached entry count and approximate byte size.
func (h *Handler) CacheStats() (int, int64) { return h.dp.cache.stats() }

func hostWithoutPort(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}

	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func canonicalHost(host string) string {
	if base, ok := strings.CutSuffix(host, dataPlaneLocalSuffix); ok {
		return base + dataPlaneAWSSuffix
	}

	return host
}

func isViewerHost(host string) bool {
	return strings.HasSuffix(host, dataPlaneAWSSuffix) || strings.HasSuffix(host, dataPlaneLocalSuffix)
}

func (h *Handler) isDataPlaneRequest(r *http.Request) bool {
	if strings.HasPrefix(r.URL.Path, dataPlanePrefix) {
		return true
	}

	host := hostWithoutPort(r.Host)
	if isViewerHost(host) {
		return true
	}

	if strings.HasPrefix(r.URL.Path, cfPathPrefix) {
		return false
	}

	if !strings.Contains(host, ".") || net.ParseIP(host) != nil {
		return false
	}

	return h.Backend.DistributionByHost(host) != nil
}

func (h *Handler) resolveDistribution(r *http.Request) (*Distribution, string, string) {
	if rest, ok := strings.CutPrefix(r.URL.EscapedPath(), dataPlanePrefix); ok {
		ref, tail, _ := strings.Cut(rest, "/")
		raw := "/" + tail

		return h.Backend.DistributionByIDOrDomain(canonicalHost(strings.ToLower(ref))), decodePath(raw), raw
	}

	raw := r.URL.EscapedPath()
	if raw == "" {
		raw = "/"
	}

	host := canonicalHost(hostWithoutPort(r.Host))

	return h.Backend.DistributionByHost(host), r.URL.Path, raw
}

func decodePath(raw string) string {
	if p, err := url.PathUnescape(raw); err == nil {
		return p
	}

	return raw
}

func (h *Handler) serveDataPlane(c *echo.Context) error {
	h.handleDataPlane(c.Response(), c.Request())

	return nil
}

// viewerReq is one inbound viewer request after distribution resolution.
type viewerReq struct {
	r       *http.Request
	cfID    string
	path    string
	rawPath string
	scheme  string
	origin  *originDirective
	edge    *edgeOverride
	body    []byte
}

func newCFID() string {
	b := make([]byte, cfIDBytes)
	_, _ = rand.Read(b)

	return base64.RawURLEncoding.EncodeToString(b)[:56]
}

func viaValue() string {
	b := make([]byte, viaIDBytes)
	_, _ = rand.Read(b)

	return "1.1 " + hex.EncodeToString(b) + dataPlaneAWSSuffix + " (CloudFront)"
}

func viewerScheme(r *http.Request) string {
	if r.TLS != nil {
		return schemeHTTPS
	}

	for _, k := range []string{"X-Forwarded-Proto", "CloudFront-Forwarded-Proto"} {
		if v := strings.ToLower(
			strings.TrimSpace(strings.Split(r.Header.Get(k), ",")[0]),
		); v == schemeHTTPS ||
			v == schemeHTTP {
			return v
		}
	}

	return schemeHTTP
}

func (h *Handler) handleDataPlane(w http.ResponseWriter, r *http.Request) {
	d, path, rawPath := h.resolveDistribution(r)
	v := &viewerReq{r: r, cfID: newCFID(), path: path, rawPath: rawPath, scheme: viewerScheme(r)}

	if d == nil {
		writeEdgeError(w, v, http.StatusNotFound, "The distribution does not exist.")

		return
	}

	if !d.Enabled {
		writeEdgeError(w, v, http.StatusForbidden, "The distribution is disabled.")

		return
	}

	cfg, err := parseDistConfig(d.RawConfig)
	if err != nil {
		writeEdgeError(w, v, http.StatusInternalServerError, "The distribution configuration is invalid.")

		return
	}

	h.serveBehavior(w, v, d, cfg)
}

// admit applies the viewer protocol policy, allowed methods and signed-request checks.
func (h *Handler) admit(w http.ResponseWriter, v *viewerReq, d *Distribution, beh *dpBehavior) bool {
	r := v.r

	switch {
	case beh.ViewerProtocolPolicy == policyHTTPSOnly && v.scheme != schemeHTTPS:
		writeEdgeError(w, v, http.StatusForbidden, "This distribution only accepts HTTPS requests.")
	case beh.ViewerProtocolPolicy == policyRedirectHTTPS && v.scheme != schemeHTTPS:
		h.redirectToHTTPS(w, v)
	case !beh.allows(r.Method):
		writeEdgeError(w, v, http.StatusForbidden,
			"This distribution is not configured to allow the HTTP request method that was used for this request.")
	case beh.KeyGroups.Enabled && !h.verifySigned(v, d, beh):
		writeEdgeError(w, v, http.StatusForbidden, "Missing Key-Pair-Id query parameter or cookie value")
	default:
		return true
	}

	return false
}

func (h *Handler) serveBehavior(w http.ResponseWriter, v *viewerReq, d *Distribution, cfg *dpDistConfig) {
	r := v.r
	beh := cfg.behaviorFor(v.path)

	if !h.admit(w, v, d, beh) {
		return
	}

	if hasRequestBody(r.Method) && r.Body != nil {
		v.body, _ = io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	}

	if h.applyViewerRequest(w, d, cfg, beh, v) || h.applyEdgeViewerRequest(w, d, beh, v) {
		return
	}

	reqPath := v.path
	if reqPath == "/" && cfg.DefaultRootObject != "" {
		reqPath = "/" + strings.TrimPrefix(cfg.DefaultRootObject, "/")
	}

	pol := h.Backend.resolvePolicy(beh)
	rhp := h.responseHeadersPolicy(beh)
	key := cacheKey(d.ID, v, &pol)

	if beh.caches(r.Method) {
		if e, ok := h.dp.cache.get(key); ok {
			hit := h.applyViewerResponse(
				d,
				beh,
				v,
				originResp{status: e.status, header: e.header.Clone(), body: e.body},
			)

			h.writeResponse(w, v, rhp, &hit, xCacheHit, e)

			return
		}
	}

	resp := h.originPhase(r.Context(), d, cfg, beh, &pol, v, reqPath)
	xcache := xCacheMiss

	if resp.status >= http.StatusBadRequest {
		xcache = xCacheError
		resp = h.applyErrorResponse(r.Context(), d, cfg, v, resp)
	} else if beh.caches(r.Method) {
		h.storeInCache(d.ID, key, v, &pol, &resp)
	}

	resp = h.applyViewerResponse(d, beh, v, resp)

	h.writeResponse(w, v, rhp, &resp, xcache, nil)
}

func (h *Handler) redirectToHTTPS(w http.ResponseWriter, v *viewerReq) {
	status := http.StatusMovedPermanently
	if v.r.Method != http.MethodGet && v.r.Method != http.MethodHead {
		status = http.StatusTemporaryRedirect
	}

	loc := "https://" + v.r.Host + v.r.URL.RequestURI()
	w.Header().Set("Location", loc)
	writeEdgeError(w, v, status, "Moved Permanently")
}

func (h *Handler) applyErrorResponse(
	ctx context.Context, d *Distribution, cfg *dpDistConfig, v *viewerReq, resp originResp,
) originResp {
	for _, er := range cfg.ErrorResponses {
		if er.ErrorCode != resp.status {
			continue
		}

		out := resp

		if er.PagePath != "" {
			pageBeh := cfg.behaviorFor(er.PagePath)
			pol := h.Backend.resolvePolicy(pageBeh)
			pv := &viewerReq{r: v.r, cfID: v.cfID, path: er.PagePath, rawPath: er.PagePath, scheme: v.scheme}

			page := h.fetchOriginGet(ctx, d, cfg, pageBeh, &pol, pv)
			if page.status >= http.StatusBadRequest {
				return resp
			}

			out = page
		}

		out.status = resp.status
		if code, err := strconv.Atoi(er.ResponseCode); err == nil && code > 0 {
			out.status = code
		}

		return out
	}

	return resp
}

func (h *Handler) fetchOriginGet(
	ctx context.Context, d *Distribution, cfg *dpDistConfig, beh *dpBehavior, pol *dpPolicy, v *viewerReq,
) originResp {
	clone := v.r.Clone(ctx)
	clone.Method = http.MethodGet
	clone.Header = http.Header{}
	clone.URL = &url.URL{Path: v.path}
	pv := *v
	pv.r = clone
	pv.body = nil

	return h.fetchOrigin(ctx, d, cfg, beh, pol, &pv, v.path)
}

func (h *Handler) fetchOrigin(
	ctx context.Context,
	d *Distribution,
	cfg *dpDistConfig,
	beh *dpBehavior,
	pol *dpPolicy,
	v *viewerReq,
	reqPath string,
) originResp {
	plan := cfg.plan(beh.TargetOriginID, v.origin)
	if plan == nil {
		return httpErrorResp(http.StatusBadGateway, "The origin for this behavior does not exist.")
	}

	var resp originResp

	for i := range plan.members {
		resp = h.fetchFromOrigin(ctx, &plan.members[i], d, pol, v, reqPath)
		if !plan.shouldFailover(v.r.Method, resp.status) {
			break
		}
	}

	return resp
}

func httpErrorResp(status int, msg string) originResp {
	return originResp{
		status: status,
		header: http.Header{headerContentType: {"text/html"}},
		body:   []byte(edgeErrorBody(status, msg, "")),
	}
}

func edgeErrorBody(status int, msg, reqID string) string {
	code := http.StatusText(status)

	return `<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">` +
		"\n<HTML><HEAD><META HTTP-EQUIV=\"Content-Type\" CONTENT=\"text/html; charset=iso-8859-1\">\n" +
		"<TITLE>ERROR: The request could not be satisfied</TITLE>\n</HEAD><BODY>\n" +
		"<H1>" + strconv.Itoa(status) + " ERROR</H1>\n<H2>" + code + "</H2>\n" + msg +
		"\n<BR clear=\"all\">\n<HR noshade size=\"1px\">\n<PRE>\nGenerated by cloudfront (CloudFront)\nRequest ID: " +
		reqID + "\n</PRE>\n<ADDRESS>\n</ADDRESS>\n</BODY></HTML>"
}

func writeEdgeError(w http.ResponseWriter, v *viewerReq, status int, msg string) {
	writeEdgeErrorCache(w, v, status, msg, xCacheError)
}

func writeEdgeErrorCache(w http.ResponseWriter, v *viewerReq, status int, msg, xcache string) {
	hd := w.Header()
	hd.Set(headerContentType, "text/html")
	hd.Set("X-Cache", xcache)
	hd.Set("Via", viaValue())
	hd.Set("X-Amz-Cf-Pop", popLocation)
	hd.Set("X-Amz-Cf-Id", v.cfID)
	w.WriteHeader(status)

	if v.r.Method != http.MethodHead {
		_, _ = w.Write([]byte(edgeErrorBody(status, msg, v.cfID)))
	}
}

func (h *Handler) writeResponse(
	w http.ResponseWriter, v *viewerReq, rhp *ResponseHeadersPolicy, resp *originResp, xcache string, hit *cacheEntry,
) {
	hd := w.Header()

	if resp.xcache != "" {
		xcache = resp.xcache
	}

	for k, vs := range resp.header {
		if isHopByHop(k) {
			continue
		}

		hd[k] = append([]string(nil), vs...)
	}

	if rhp != nil {
		applyResponseHeadersPolicy(hd, rhp, v.r)
	}

	status := resp.status
	if hit != nil {
		hd.Set("Age", strconv.Itoa(int(time.Since(hit.storedAt).Seconds())))
	}

	if status == http.StatusOK && notModified(v.r, hd) {
		status = http.StatusNotModified
	}

	hd.Set("X-Cache", xcache)
	hd.Set("Via", viaValue())
	hd.Set("X-Amz-Cf-Pop", popLocation)
	hd.Set("X-Amz-Cf-Id", v.cfID)
	hd.Set("Content-Length", strconv.Itoa(len(resp.body)))

	if status == http.StatusNotModified {
		hd.Del("Content-Length")
		w.WriteHeader(status)

		return
	}

	w.WriteHeader(status)

	if v.r.Method != http.MethodHead {
		_, _ = w.Write(resp.body)
	}
}

func notModified(r *http.Request, hd http.Header) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		etag := hd.Get("ETag")

		return etag != "" && (inm == "*" || strings.Contains(inm, etag))
	}

	return false
}

func (h *Handler) fetchFromOrigin(
	ctx context.Context, o *dpOrigin, d *Distribution, pol *dpPolicy, v *viewerReq, reqPath string,
) originResp {
	switch {
	case isS3WebsiteOrigin(o):
		return h.fetchS3Website(ctx, o, d, v.r.Method, reqPath)
	case isS3Origin(o):
		return h.fetchS3(ctx, o, d, v.r.Method, reqPath)
	default:
		return h.fetchCustom(ctx, o, pol, v, reqPath)
	}
}
