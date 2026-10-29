package cloudfront

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	viewerEdgeTimeout = 5 * time.Second
	originEdgeTimeout = 30 * time.Second

	viewerEdgeMaxBody = 40 << 10
	originEdgeMaxBody = 1 << 20
	viewerEdgeMaxB64  = 54476
	originEdgeMaxB64  = 1394606
	edgeMaxURI        = 8192

	xCacheEdgeGenerated  = "LambdaGeneratedResponse from cloudfront"
	xCacheEdgeValidation = "LambdaValidationError from cloudfront"
	xCacheEdgeExecution  = "LambdaExecutionError from cloudfront"
	xCacheEdgeLimit      = "LambdaLimitExceeded from cloudfront"

	bodyActionReplace   = "replace"
	encodingBase64      = "base64"
	encodingText        = "text"
	keyHeaders          = "headers"
	keyBody             = "body"
	keyContentLength    = "content-length"
	keyTransferEncoding = "transfer-encoding"
	keyVia              = "via"
	keyConfig           = "config"
	keyRequest          = "request"
	keyCustom           = "custom"
)

type edgeError struct {
	xcache string
	msg    string
	status int
}

func edgeValidation(msg string) *edgeError {
	return &edgeError{status: http.StatusBadGateway, xcache: xCacheEdgeValidation, msg: msg}
}

func (e *edgeError) resp() originResp {
	r := httpErrorResp(e.status, e.msg)
	r.xcache = e.xcache

	return r
}

type edgeOverride struct {
	header http.Header
	query  *string
}

type edgeLimit struct {
	timeout time.Duration
	maxBody int
	maxB64  int
}

func edgeLimits(eventType string) edgeLimit {
	if eventType == eventViewerRequest || eventType == eventViewerResponse {
		return edgeLimit{viewerEdgeTimeout, viewerEdgeMaxBody, viewerEdgeMaxB64}
	}

	return edgeLimit{originEdgeTimeout, originEdgeMaxBody, originEdgeMaxB64}
}

func (h *Handler) edgeAssoc(beh *dpBehavior, eventType string) *dpEdgeAssoc {
	for i := range beh.Edge {
		if beh.Edge[i].EventType == eventType {
			return &beh.Edge[i]
		}
	}

	return nil
}

func edgeDisallowed(name string) bool {
	n := strings.ToLower(name)
	if strings.HasPrefix(n, "x-amz-cf-") || strings.HasPrefix(n, "x-edge-") {
		return true
	}

	return slices.Contains([]string{
		"connection", "expect", "keep-alive", "proxy-authenticate", "proxy-authorization", "proxy-connection",
		"trailer", "upgrade", "x-accel-buffering", "x-accel-charset", "x-accel-limit-rate", "x-accel-redirect",
		"x-amzn-auth", "x-amzn-cf-billing", "x-amzn-cf-id", "x-amzn-cf-xff", "x-amzn-errortype",
		"x-amzn-fle-profile", "x-amzn-header-count", "x-amzn-header-order", "x-amzn-lambda-integration-tag",
		"x-amzn-requestid", "x-cache", "x-forwarded-proto", "x-real-ip", "cloudfront-viewer-cert-pem",
		"client-cert", "client-cert-chain",
	}, n)
}

func edgeReadOnly(eventType string) []string {
	switch eventType {
	case eventViewerRequest:
		return []string{"cdn-loop", keyContentLength, "host", keyTransferEncoding, keyVia}
	case eventOriginRequest:
		return []string{
			"accept-encoding", "cdn-loop", keyContentLength, "if-modified-since", "if-none-match", "if-range",
			"if-unmodified-since", keyTransferEncoding, keyVia,
		}
	case eventOriginResponse:
		return []string{keyTransferEncoding, keyVia}
	default:
		return []string{"warning", keyVia, keyContentLength, "content-encoding", keyTransferEncoding}
	}
}

func edgeVisible(src http.Header, host string) http.Header {
	out := http.Header{}

	for k, vs := range src {
		if !edgeDisallowed(k) && !strings.EqualFold(k, "Host") {
			out[http.CanonicalHeaderKey(k)] = slices.Clone(vs)
		}
	}

	if host != "" {
		out.Set("Host", host)
	}

	return out
}

func edgeHeaderMap(hd http.Header) map[string]any {
	out := make(map[string]any, len(hd))

	for k, vs := range hd {
		items := make([]map[string]string, len(vs))
		for i, s := range vs {
			items[i] = map[string]string{"key": k, keyValue: s}
		}

		out[strings.ToLower(k)] = items
	}

	return out
}

func parseEdgeHeaders(raw any) (http.Header, *edgeError) {
	out := http.Header{}

	m, ok := raw.(map[string]any)
	if !ok {
		if raw == nil {
			return out, nil
		}

		return nil, edgeValidation("The headers field of the Lambda function result must be an object.")
	}

	for name, v := range m {
		items, isList := v.([]any)
		if !isList {
			return nil, edgeValidation("The header " + name + " must be an array of key/value pairs.")
		}

		for _, it := range items {
			kv, isObj := it.(map[string]any)
			val, isStr := kv[keyValue].(string)

			if !isObj || !isStr {
				return nil, edgeValidation("The header " + name + " has an entry without a string value.")
			}

			key := name
			if k, hasKey := kv["key"].(string); hasKey && k != "" {
				key = k
			}

			out.Add(key, val)
		}
	}

	return out, nil
}

func checkEdgeHeaders(in, out http.Header, eventType string) *edgeError {
	for name := range out {
		if edgeDisallowed(name) {
			return edgeValidation("The Lambda function added the disallowed header " + name + ".")
		}
	}

	for _, ro := range edgeReadOnly(eventType) {
		if !slices.Equal(in.Values(ro), out.Values(ro)) {
			return edgeValidation("The Lambda function modified the read-only header " + ro + ".")
		}
	}

	return nil
}

func (h *Handler) edgeConfig(d *Distribution, v *viewerReq, eventType string) map[string]any {
	return map[string]any{
		"distributionDomainName": d.DomainName, "distributionId": d.ID,
		"eventType": eventType, "requestId": v.cfID,
	}
}

func edgeRequestObject(v *viewerReq, hd http.Header, query, uri string) map[string]any {
	return map[string]any{
		"clientIp": clientIP(v.r), keyHeaders: edgeHeaderMap(hd), "method": v.r.Method,
		keyQuerystring: query, "uri": uri,
	}
}

func edgeBodyObject(body []byte, maxBody int) map[string]any {
	truncated := len(body) > maxBody
	if truncated {
		body = body[:maxBody]
	}

	return map[string]any{
		"inputTruncated": truncated, "action": "read-only", "encoding": encodingBase64,
		"data": base64.StdEncoding.EncodeToString(body),
	}
}

func (h *Handler) edgeInvoke(
	ctx context.Context, a *dpEdgeAssoc, eventType string, cf map[string]any,
) (map[string]any, *edgeError) {
	ctx, cancel := context.WithTimeout(ctx, edgeLimits(eventType).timeout)

	defer cancel()

	arn, _ := parseEdgeARN(a.ARN)
	edge := h.lambdaEdge()

	if edge == nil {
		return nil, &edgeError{
			status: http.StatusServiceUnavailable, xcache: xCacheEdgeExecution,
			msg: "The Lambda function associated with the CloudFront distribution could not be run.",
		}
	}

	payload, _ := json.Marshal(map[string]any{"Records": []any{map[string]any{"cf": cf}}})

	res, fnErr, err := edge.InvokeEdge(ctx, arn.name, arn.version, payload)

	switch {
	case errors.Is(err, ErrEdgeLimitExceeded) || ctx.Err() != nil:
		return nil, &edgeError{
			status: http.StatusServiceUnavailable, xcache: xCacheEdgeLimit,
			msg: "The Lambda function associated with the CloudFront distribution was throttled or timed out.",
		}
	case err != nil || fnErr != "":
		return nil, &edgeError{
			status: http.StatusServiceUnavailable, xcache: xCacheEdgeExecution,
			msg: "The Lambda function associated with the CloudFront distribution failed during execution.",
		}
	}

	var out map[string]any
	if json.Unmarshal(res, &out) != nil || out == nil {
		return nil, edgeValidation(
			"The Lambda function returned a value that is not a valid request or response object.",
		)
	}

	return out, nil
}

func decodeEdgeBody(data, encoding string, maxBody, maxB64 int) ([]byte, *edgeError) {
	if encoding == encodingBase64 {
		if len(data) > maxB64 {
			return nil, edgeValidation("The Lambda function returned a body that is too large.")
		}

		b, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, edgeValidation("The Lambda function returned a body that is not valid base64.")
		}

		return b, nil
	}

	if len(data) > maxBody {
		return nil, edgeValidation("The Lambda function returned a body that is too large.")
	}

	return []byte(data), nil
}

func headerBytes(hd http.Header) int {
	n := 0

	for k, vs := range hd {
		for _, s := range vs {
			n += len(k) + len(s)
		}
	}

	return n
}

func parseEdgeStatus(out map[string]any) (int, *edgeError) {
	var status int

	switch t := out["status"].(type) {
	case string:
		status, _ = strconv.Atoi(t)
	case float64:
		status = int(t)
	}

	if status < http.StatusOK || status > 599 {
		return 0, edgeValidation("The Lambda function returned a status that is not between 200 and 599.")
	}

	return status, nil
}

func parseEdgeResponse(out map[string]any, eventType string, in http.Header) (originResp, *edgeError) {
	lim := edgeLimits(eventType)
	maxBody, maxB64 := lim.maxBody, lim.maxB64

	status, serr := parseEdgeStatus(out)
	if serr != nil {
		return originResp{}, serr
	}

	hd, herr := parseEdgeHeaders(out[keyHeaders])
	if herr != nil {
		return originResp{}, herr
	}

	roEvent := eventType
	if isRequestEvent(eventType) {
		roEvent = eventOriginResponse
	}

	if cerr := checkEdgeHeaders(in, hd, roEvent); cerr != nil {
		return originResp{}, cerr
	}

	resp := originResp{status: status, header: hd}

	if err := setEdgeResponseBody(&resp, out, maxBody, maxB64); err != nil {
		return originResp{}, err
	}

	if status == http.StatusNoContent && len(resp.body) > 0 {
		return originResp{}, edgeValidation("The Lambda function returned a body with a 204 status.")
	}

	if len(resp.body)+headerBytes(hd) > maxBody {
		return originResp{}, edgeValidation("The Lambda function generated a response that is too large.")
	}

	return resp, nil
}

func setEdgeResponseBody(resp *originResp, out map[string]any, maxBody, maxB64 int) *edgeError {
	raw, has := out[keyBody]
	if !has || raw == nil {
		return nil
	}

	data, ok := raw.(string)
	if !ok {
		return edgeValidation("The Lambda function returned a body that is not a string.")
	}

	enc, _ := out["bodyEncoding"].(string)
	if enc != "" && enc != encodingText && enc != encodingBase64 {
		return edgeValidation("The Lambda function returned an unsupported bodyEncoding.")
	}

	b, err := decodeEdgeBody(data, enc, maxBody, maxB64)
	if err != nil {
		return err
	}

	resp.body = b

	return nil
}

type edgeReqResult struct {
	header     http.Header
	origin     *originParams
	uri        string
	query      string
	body       []byte
	replaceBod bool
}

func parseEdgeRequest(
	out map[string]any, in http.Header, eventType string, includeBody bool, curURI, curQuery string,
) (*edgeReqResult, *edgeError) {
	lim := edgeLimits(eventType)
	maxBody, maxB64 := lim.maxBody, lim.maxB64
	res := &edgeReqResult{uri: curURI, query: curQuery}

	if s, ok := out["uri"].(string); ok {
		if !strings.HasPrefix(s, "/") {
			return nil, edgeValidation("The Lambda function returned a uri that does not begin with a forward slash.")
		}

		res.uri = s
	}

	if s, ok := out[keyQuerystring].(string); ok {
		res.query = s
	}

	if strings.ContainsAny(res.query, " #\x00\r\n\t") || len(res.uri)+len(res.query) >= edgeMaxURI {
		return nil, edgeValidation("The Lambda function returned an invalid uri or query string.")
	}

	hd, herr := parseEdgeHeaders(out[keyHeaders])
	if herr != nil {
		return nil, herr
	}

	if cerr := checkEdgeHeaders(in, hd, eventType); cerr != nil {
		return nil, cerr
	}

	res.header = hd

	if berr := parseEdgeRequestBody(res, out, includeBody, maxBody, maxB64); berr != nil {
		return nil, berr
	}

	if raw, has := out["origin"]; has && eventType == eventOriginRequest {
		p, oerr := parseEdgeOrigin(raw)
		if oerr != nil {
			return nil, oerr
		}

		res.origin = p
	}

	return res, nil
}

func parseEdgeRequestBody(res *edgeReqResult, out map[string]any, includeBody bool, maxBody, maxB64 int) *edgeError {
	b, ok := out[keyBody].(map[string]any)
	if !ok || !includeBody {
		return nil
	}

	if action, _ := b["action"].(string); action != bodyActionReplace {
		return nil
	}

	data, _ := b["data"].(string)
	enc, _ := b["encoding"].(string)

	if enc == "" {
		enc = encodingBase64
	}

	if enc != encodingBase64 && enc != encodingText {
		return edgeValidation("The Lambda function returned an unsupported body encoding.")
	}

	dec, err := decodeEdgeBody(data, enc, maxBody, maxB64)
	if err != nil {
		return err
	}

	res.body, res.replaceBod = dec, true

	return nil
}
