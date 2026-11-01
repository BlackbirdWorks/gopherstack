package cloudfront

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

const (
	eventViewerRequest  = "viewer-request"
	eventViewerResponse = "viewer-response"
	fnEventVersion      = "1.0"
)

type dpFunctionAssoc struct {
	FunctionARN string `xml:"FunctionARN"`
	EventType   string `xml:"EventType"`
}

// viewerFunction returns the LIVE function bound to the behavior for eventType.
func (h *Handler) viewerFunction(beh *dpBehavior, eventType string) *Function {
	for _, a := range beh.Functions {
		if a.EventType != eventType {
			continue
		}

		name := a.FunctionARN[strings.LastIndex(a.FunctionARN, "/")+1:]

		fn, err := h.Backend.GetFunctionAtStage(name, functionStageLive)
		if err == nil {
			return fn
		}
	}

	return nil
}

func fnValueMap(pairs map[string][]string, lower bool) map[string]any {
	out := make(map[string]any, len(pairs))

	for k, vs := range pairs {
		if lower {
			k = strings.ToLower(k)
		}

		if len(vs) == 0 {
			continue
		}

		entry := map[string]any{keyValue: vs[0]}
		if len(vs) > 1 {
			mv := make([]map[string]string, len(vs))
			for i, s := range vs {
				mv[i] = map[string]string{keyValue: s}
			}

			entry["multiValue"] = mv
		}

		out[k] = entry
	}

	return out
}

func requestCookies(v *viewerReq) map[string]any {
	out := map[string]any{}

	for _, c := range v.r.Cookies() {
		out[c.Name] = map[string]string{keyValue: c.Value}
	}

	return out
}

func (h *Handler) functionEvent(d *Distribution, v *viewerReq, eventType string, resp *originResp) map[string]any {
	ev := map[string]any{
		"version": fnEventVersion,
		"context": map[string]any{
			"distributionDomainName": d.DomainName, "distributionId": d.ID,
			"eventType": eventType, "requestId": v.cfID,
		},
		"viewer": map[string]any{"ip": clientIP(v.r)},
		keyRequest: map[string]any{
			"method": v.r.Method, "uri": v.path,
			keyQuerystring: fnValueMap(v.r.URL.Query(), false),
			"headers":      fnValueMap(v.r.Header, true),
			"cookies":      requestCookies(v),
		},
	}

	if resp != nil {
		ev["response"] = map[string]any{
			"statusCode": resp.status, "statusDescription": http.StatusText(resp.status),
			"headers": fnValueMap(resp.header, true), "cookies": map[string]any{},
		}
	}

	return ev
}

// runFunction executes fn; ok is false on failure (res.TimedOut distinguishes a compute-limit failure).
func (h *Handler) runFunction(
	fn *Function, eventType string, event map[string]any, cat *originCatalog,
) (map[string]any, fnResult, bool) {
	res := runCloudFrontFunction(fnRun{
		Runtime: fn.Runtime, Code: fn.FunctionCode, Event: event, KVS: h.Backend.kvsReaderFor(fn),
		Origins: cat, EventType: eventType, Multiplier: int(h.Backend.fnMultiplier.Load()),
	})
	if res.ErrMsg != "" {
		return nil, res, false
	}

	var out map[string]any
	if err := json.Unmarshal([]byte(res.Output), &out); err != nil {
		return nil, res, false
	}

	return out, res, true
}

// fnFailure is the edge status of a failed function: 503 when it exceeded its compute limit.
func fnFailure(res fnResult) int {
	if res.TimedOut {
		return http.StatusServiceUnavailable
	}

	return http.StatusBadGateway
}

// applyViewerRequest runs the viewer-request function; it returns true when the
// function answered the request itself or failed (the response was written).
func (h *Handler) applyViewerRequest(
	w http.ResponseWriter, d *Distribution, cfg *dpDistConfig, beh *dpBehavior, v *viewerReq,
) bool {
	fn := h.viewerFunction(beh, eventViewerRequest)
	if fn == nil {
		return false
	}

	out, res, ok := h.runFunction(fn, eventViewerRequest, h.functionEvent(d, v, eventViewerRequest, nil),
		newOriginCatalog(cfg.originIDs()))
	if !ok {
		writeEdgeError(w, v, fnFailure(res), fnFailedMsg)

		return true
	}

	if _, isResp := out["statusCode"]; isResp {
		resp := responseFromFunction(out)
		h.writeResponse(w, v, nil, &resp, xCacheFromFn, nil)

		return true
	}

	mutateRequest(v, out)
	v.origin = res.Origin

	return false
}

const (
	fnFailedMsg    = "The CloudFront function associated with the CloudFront distribution is invalid or could not run."
	keyValue       = "value"
	keyQuerystring = "querystring"
)

const xCacheFromFn = "FunctionGeneratedResponse from cloudfront"

// applyViewerResponse runs the viewer-response function over resp.
func (h *Handler) applyViewerResponse(d *Distribution, beh *dpBehavior, v *viewerReq, resp originResp) originResp {
	fn := h.viewerFunction(beh, eventViewerResponse)
	if fn == nil {
		return h.applyEdgeViewerResponse(d, beh, v, resp)
	}

	out, res, ok := h.runFunction(fn, eventViewerResponse, h.functionEvent(d, v, eventViewerResponse, &resp), nil)
	if !ok {
		return httpErrorResp(fnFailure(res), fnFailedMsg)
	}

	if _, isResp := out["statusCode"]; !isResp {
		return resp
	}

	fnResp := responseFromFunction(out)
	resp.status = fnResp.status
	resp.header = fnResp.header

	if body, has := out["body"]; has && body != nil {
		resp.body = fnResp.body
	}

	return resp
}

func responseFromFunction(out map[string]any) originResp {
	resp := originResp{status: http.StatusOK, header: http.Header{}}

	if sc, ok := out["statusCode"].(float64); ok {
		resp.status = int(sc)
	}

	if hs, ok := out["headers"].(map[string]any); ok {
		for k, e := range hs {
			if val := entryValue(e); val != "" || e != nil {
				resp.header.Set(k, val)
			}
		}
	}

	if cks, ok := out["cookies"].(map[string]any); ok {
		for name, e := range cks {
			resp.header.Add("Set-Cookie", name+"="+entryValue(e))
		}
	}

	resp.body = functionBody(out["body"])

	return resp
}

func functionBody(b any) []byte {
	switch t := b.(type) {
	case string:
		return []byte(t)
	case map[string]any:
		data, _ := t["data"].(string)

		return []byte(data)
	}

	return nil
}

func entryValue(e any) string {
	if m, ok := e.(map[string]any); ok {
		s, _ := m["value"].(string)

		return s
	}

	return ""
}

// mutateRequest applies a viewer-request function's returned request to v.
func mutateRequest(v *viewerReq, out map[string]any) {
	if uri, ok := out["uri"].(string); ok && uri != "" {
		v.path = uri
		v.rawPath = (&url.URL{Path: uri}).EscapedPath()
		v.r.URL.Path = uri
	}

	if qs, ok := out[keyQuerystring].(map[string]any); ok {
		v.r.URL.RawQuery = encodeFnQuery(qs)
	}

	if hs, ok := out["headers"].(map[string]any); ok {
		replaceHeaders(v.r.Header, hs)
	}

	if cks, ok := out["cookies"].(map[string]any); ok {
		v.r.Header.Set("Cookie", cookieHeader(cks))
	}
}

func replaceHeaders(dst http.Header, hs map[string]any) {
	keep := http.Header{}

	for _, k := range []string{"Cookie", "Host"} {
		if vals, ok := dst[k]; ok {
			keep[k] = vals
		}
	}

	clear(dst)

	maps.Copy(dst, keep)

	for k, e := range hs {
		dst.Set(k, entryValue(e))
	}
}

func encodeFnQuery(qs map[string]any) string {
	keys := make([]string, 0, len(qs))
	for k := range qs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	q := url.Values{}

	for _, k := range keys {
		m, _ := qs[k].(map[string]any)
		if mv, ok := m["multiValue"].([]any); ok {
			for _, item := range mv {
				q.Add(k, entryValue(item))
			}

			continue
		}

		q.Add(k, entryValue(m))
	}

	return q.Encode()
}

func cookieHeader(cks map[string]any) string {
	names := make([]string, 0, len(cks))
	for n := range cks {
		names = append(names, n)
	}

	sort.Strings(names)

	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = n + "=" + entryValue(cks[n])
	}

	return strings.Join(parts, "; ")
}
