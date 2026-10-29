package cloudfront

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	edgeDefaultReadTimeout = 30
	edgeDefaultKeepAlive   = 5
)

var edgeS3Region = regexp.MustCompile(`\.s3[.-]([a-z0-9-]+)\.amazonaws\.com`)

func hasRequestBody(m string) bool {
	return m != http.MethodGet && m != http.MethodHead && m != http.MethodOptions
}

func (e *edgeError) write(w http.ResponseWriter, v *viewerReq) {
	writeEdgeErrorCache(w, v, e.status, e.msg, e.xcache)
}

// applyEdgeViewerRequest runs the viewer-request function; it returns true when the response was written.
func (h *Handler) applyEdgeViewerRequest(w http.ResponseWriter, d *Distribution, beh *dpBehavior, v *viewerReq) bool {
	a := h.edgeAssoc(beh, eventViewerRequest)
	if a == nil {
		return false
	}

	maxBody := edgeLimits(eventViewerRequest).maxBody
	in := edgeVisible(v.r.Header, v.r.Host)
	req := edgeRequestObject(v, in, v.r.URL.RawQuery, v.path)

	if a.IncludeBody && hasRequestBody(v.r.Method) {
		req[keyBody] = edgeBodyObject(v.body, maxBody)
	}

	cf := map[string]any{keyConfig: h.edgeConfig(d, v, eventViewerRequest), keyRequest: req}

	out, ferr := h.edgeInvoke(v.r.Context(), a, eventViewerRequest, cf)
	if ferr != nil {
		ferr.write(w, v)

		return true
	}

	if _, isResp := out["status"]; isResp {
		resp, gerr := parseEdgeResponse(out, eventViewerRequest, http.Header{})
		if gerr != nil {
			gerr.write(w, v)

			return true
		}

		resp.xcache = xCacheEdgeGenerated
		h.writeResponse(w, v, nil, &resp, xCacheEdgeGenerated, nil)

		return true
	}

	res, perr := parseEdgeRequest(out, in, eventViewerRequest, a.IncludeBody, v.path, v.r.URL.RawQuery)
	if perr != nil {
		perr.write(w, v)

		return true
	}

	res.header.Del("Host")
	v.r.Header = res.header
	v.path = res.uri
	v.rawPath = (&url.URL{Path: res.uri}).EscapedPath()
	v.r.URL.Path = res.uri
	v.r.URL.RawQuery = res.query

	if res.replaceBod {
		v.body = res.body
	}

	return false
}

func edgeOriginObject(o *dpOrigin, viewerScheme string) map[string]any {
	hdrs := map[string]any{}

	for _, hp := range o.Headers {
		hdrs[strings.ToLower(hp.Name)] = []map[string]string{{"key": hp.Name, keyValue: hp.Value}}
	}

	if o.Custom == nil {
		auth, region := "none", ""

		if o.S3 != nil && o.S3.OAI != "" {
			auth, region = "origin-access-identity", edgeRegion
			if m := edgeS3Region.FindStringSubmatch(o.Domain); m != nil {
				region = m[1]
			}
		}

		return map[string]any{"s3": map[string]any{
			"authMethod": auth, "customHeaders": hdrs, "domainName": o.Domain, "path": o.Path, "region": region,
		}}
	}

	proto, port := protoHTTPS, o.Custom.HTTPSPort

	switch o.Custom.Policy {
	case originPolicyHTTP:
		proto, port = protoHTTP, o.Custom.HTTPPort
	case originPolicyMatch:
		if viewerScheme == schemeHTTP {
			proto, port = protoHTTP, o.Custom.HTTPPort
		}
	}

	if port == 0 {
		port = defaultHTTPSPort
		if proto == protoHTTP {
			port = defaultHTTPPort
		}
	}

	return map[string]any{keyCustom: map[string]any{
		"customHeaders": hdrs, "domainName": o.Domain, "path": o.Path, "port": port, "protocol": proto,
		"keepaliveTimeout":          max(o.Custom.KeepAlive, edgeDefaultKeepAlive),
		"readTimeout":               max(o.Custom.ReadTimeout, edgeDefaultReadTimeout),
		"responseCompletionTimeout": max(o.Custom.ReadTimeout, edgeDefaultReadTimeout),
		"sslProtocols":              []string{"TLSv1.2"},
	}}
}

func numField(m map[string]any, key string) int {
	f, _ := m[key].(float64)

	return int(f)
}

func parseEdgeOrigin(raw any) (*originParams, *edgeError) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, edgeValidation("The Lambda function returned an origin that is not an object.")
	}

	custom, hasCustom := m[keyCustom].(map[string]any)
	s3o, hasS3 := m["s3"].(map[string]any)

	if hasCustom == hasS3 {
		return nil, edgeValidation("The Lambda function origin must contain exactly one of custom or s3.")
	}

	body := custom
	if hasS3 {
		body = s3o
	}

	domain, _ := body["domainName"].(string)
	if domain == "" || strings.Contains(domain, ":") && hasCustom {
		return nil, edgeValidation("The Lambda function returned an origin with an invalid domainName.")
	}

	p := &originParams{Domain: domain, Headers: map[string]string{}}

	if path, has := body["path"].(string); has {
		p.Path = &path
	}

	hdrs, herr := parseEdgeHeaders(body["customHeaders"])
	if herr != nil {
		return nil, herr
	}

	for name, vs := range hdrs {
		p.Headers[name] = vs[0]
	}

	if hasCustom {
		p.Protocol, _ = custom["protocol"].(string)
		p.Port = numField(custom, "port")
		p.ReadTimeout = numField(custom, "readTimeout")
		p.KeepAlive = numField(custom, "keepaliveTimeout")
	}

	return p, nil
}

// originPhase runs the origin-request function, the origin fetch and the origin-response function.
func (h *Handler) originPhase(
	ctx context.Context,
	d *Distribution,
	cfg *dpDistConfig,
	beh *dpBehavior,
	pol *dpPolicy,
	v *viewerReq,
	reqPath string,
) originResp {
	pv, path, gen, ferr := h.applyEdgeOriginRequest(ctx, d, cfg, beh, pol, v, reqPath)
	if ferr != nil {
		return ferr.resp()
	}

	if gen != nil {
		return *gen
	}

	resp := h.fetchOrigin(ctx, d, cfg, beh, pol, pv, path)

	return h.applyEdgeOriginResponse(ctx, d, cfg, beh, pv, resp)
}

func (h *Handler) applyEdgeOriginRequest(
	ctx context.Context,
	d *Distribution,
	cfg *dpDistConfig,
	beh *dpBehavior,
	pol *dpPolicy,
	v *viewerReq,
	reqPath string,
) (*viewerReq, string, *originResp, *edgeError) {
	a := h.edgeAssoc(beh, eventOriginRequest)
	plan := cfg.plan(beh.TargetOriginID, v.origin)

	if a == nil || plan == nil {
		return v, reqPath, nil, nil
	}

	member := plan.members[0]

	outReq, err := h.outboundRequest(ctx, &member, pol, v, reqPath)
	if err != nil {
		return v, reqPath, nil, nil
	}

	maxBody := edgeLimits(eventOriginRequest).maxBody
	query := forwardedQuery(v.r.URL.RawQuery, pol)
	in := edgeVisible(outReq.Header, outReq.Host)
	req := edgeRequestObject(v, in, query, reqPath)
	req["origin"] = edgeOriginObject(&member, v.scheme)

	if a.IncludeBody && hasRequestBody(v.r.Method) {
		req[keyBody] = edgeBodyObject(v.body, maxBody)
	}

	cf := map[string]any{keyConfig: h.edgeConfig(d, v, eventOriginRequest), keyRequest: req}

	out, ferr := h.edgeInvoke(ctx, a, eventOriginRequest, cf)
	if ferr != nil {
		return nil, "", nil, ferr
	}

	if _, isResp := out["status"]; isResp {
		resp, gerr := parseEdgeResponse(out, eventOriginRequest, http.Header{})
		if gerr != nil {
			return nil, "", nil, gerr
		}

		resp.xcache = xCacheEdgeGenerated

		return v, reqPath, &resp, nil
	}

	res, perr := parseEdgeRequest(out, in, eventOriginRequest, a.IncludeBody, reqPath, query)
	if perr != nil {
		return nil, "", nil, perr
	}

	pv := *v
	pv.edge = &edgeOverride{header: res.header, query: &res.query}

	if res.replaceBod {
		pv.body = res.body
	}

	if res.origin != nil && (v.origin == nil || v.origin.Group == nil) {
		dir := originDirective{}
		if v.origin != nil {
			dir = *v.origin
		}

		dir.Params = res.origin
		pv.origin = &dir
	}

	return &pv, res.uri, nil, nil
}

func (h *Handler) edgeResponseEvent(
	ctx context.Context, a *dpEdgeAssoc, d *Distribution, v *viewerReq, eventType string, resp originResp,
	origin map[string]any,
) (originResp, *edgeError) {
	req := edgeRequestObject(v, edgeVisible(v.r.Header, v.r.Host), v.r.URL.RawQuery, v.path)
	if origin != nil {
		req["origin"] = origin
	}

	in := edgeVisible(resp.header, "")
	cf := map[string]any{
		keyConfig:  h.edgeConfig(d, v, eventType),
		keyRequest: req,
		"response": map[string]any{
			"status": strconv.Itoa(resp.status), "statusDescription": http.StatusText(resp.status),
			keyHeaders: edgeHeaderMap(in),
		},
	}

	out, ferr := h.edgeInvoke(ctx, a, eventType, cf)
	if ferr != nil {
		return originResp{}, ferr
	}

	got, perr := parseEdgeResponse(out, eventType, in)
	if perr != nil {
		return originResp{}, perr
	}

	resp.header = got.header
	if _, has := out[keyBody]; has {
		resp.body = got.body
	}

	if eventType == eventOriginResponse {
		resp.status = got.status
	}

	return resp, nil
}

func (h *Handler) applyEdgeOriginResponse(
	ctx context.Context, d *Distribution, cfg *dpDistConfig, beh *dpBehavior, v *viewerReq, resp originResp,
) originResp {
	a := h.edgeAssoc(beh, eventOriginResponse)
	if a == nil {
		return resp
	}

	var origin map[string]any

	if plan := cfg.plan(beh.TargetOriginID, v.origin); plan != nil {
		origin = edgeOriginObject(&plan.members[0], v.scheme)
	}

	out, ferr := h.edgeResponseEvent(ctx, a, d, v, eventOriginResponse, resp, origin)
	if ferr != nil {
		return ferr.resp()
	}

	return out
}

// applyEdgeViewerResponse runs the viewer-response function; CloudFront skips it for statuses of 400 and above.
func (h *Handler) applyEdgeViewerResponse(d *Distribution, beh *dpBehavior, v *viewerReq, resp originResp) originResp {
	a := h.edgeAssoc(beh, eventViewerResponse)
	if a == nil || resp.status >= http.StatusBadRequest {
		return resp
	}

	out, ferr := h.edgeResponseEvent(v.r.Context(), a, d, v, eventViewerResponse, resp, nil)
	if ferr != nil {
		return ferr.resp()
	}

	return out
}
