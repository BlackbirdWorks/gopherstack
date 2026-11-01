package elbv2

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// maxLambdaTargetBodyBytes is the 1 MB request body limit for ALB Lambda targets.
const maxLambdaTargetBodyBytes int64 = 1 << 20

const lambdaMultiValueAttr = "lambda.multi_value_headers.enabled"

type albEvent struct {
	RequestContext                  albRequestContext   `json:"requestContext"`
	QueryStringParameters           map[string]string   `json:"queryStringParameters,omitempty"`
	MultiValueQueryStringParameters map[string][]string `json:"multiValueQueryStringParameters,omitempty"`
	Headers                         map[string]string   `json:"headers,omitempty"`
	MultiValueHeaders               map[string][]string `json:"multiValueHeaders,omitempty"`
	HTTPMethod                      string              `json:"httpMethod"`
	Path                            string              `json:"path"`
	Body                            string              `json:"body"`
	IsBase64Encoded                 bool                `json:"isBase64Encoded"`
}

type albRequestContext struct {
	ELB albELBContext `json:"elb"`
}

type albELBContext struct {
	TargetGroupArn string `json:"targetGroupArn"`
}

type albResponse struct {
	StatusCode        *int                `json:"statusCode"`
	Headers           map[string]string   `json:"headers"`
	MultiValueHeaders map[string][]string `json:"multiValueHeaders"`
	StatusDescription string              `json:"statusDescription"`
	Body              string              `json:"body"`
	IsBase64Encoded   bool                `json:"isBase64Encoded"`
}

func (h *Handler) serveLambda(w http.ResponseWriter, req *dpRequest, tg *TargetGroup, t Target) {
	inv := h.dp.invoker.Load()
	if inv == nil {
		writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

		return
	}

	body, err := httputils.ReadBodyLimit(req.r, maxLambdaTargetBodyBytes)
	if err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			writeDPError(w, http.StatusRequestEntityTooLarge, "413 Request Entity Too Large")

			return
		}

		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	hdr := req.r.Header.Clone()
	setForwardHeaders(hdr, req)
	hdr.Set("Host", req.host)

	payload, err := json.Marshal(buildALBEvent(req, tg, hdr, body))
	if err != nil {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	out, fnErr, err := (*inv).InvokeFunction(req.r.Context(), t.ID, payload)
	if err != nil || fnErr != "" {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	writeLambdaResponse(w, out)
}

func buildALBEvent(req *dpRequest, tg *TargetGroup, hdr http.Header, body []byte) albEvent {
	ev := albEvent{
		RequestContext: albRequestContext{ELB: albELBContext{TargetGroupArn: tg.TargetGroupArn}},
		HTTPMethod:     req.r.Method,
		Path:           req.path,
	}

	ev.Body, ev.IsBase64Encoded = encodeALBBody(body, hdr.Get("Content-Type"))

	rawQuery := parseRawQuery(req.r.URL.RawQuery)

	if tg.TargetGroupAttributes[lambdaMultiValueAttr] == attrValueTrue {
		ev.MultiValueQueryStringParameters = rawQuery
		ev.MultiValueHeaders = lowerHeaders(hdr)

		return ev
	}

	ev.QueryStringParameters = make(map[string]string, len(rawQuery))
	for k, vs := range rawQuery {
		ev.QueryStringParameters[k] = vs[len(vs)-1]
	}

	ev.Headers = make(map[string]string, len(hdr))
	for k, vs := range lowerHeaders(hdr) {
		ev.Headers[k] = vs[len(vs)-1]
	}

	return ev
}

// parseRawQuery splits a query string without decoding: ALB passes query values to Lambda URL-encoded.
func parseRawQuery(raw string) map[string][]string {
	out := map[string][]string{}

	for kv := range strings.SplitSeq(raw, "&") {
		if kv == "" {
			continue
		}

		k, v, _ := strings.Cut(kv, "=")
		out[k] = append(out[k], v)
	}

	return out
}

func lowerHeaders(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		out[strings.ToLower(k)] = append([]string(nil), vs...)
	}

	return out
}

func encodeALBBody(body []byte, contentType string) (string, bool) {
	if len(body) == 0 {
		return "", false
	}

	if isTextContent(contentType) && utf8.Valid(body) {
		return string(body), false
	}

	return base64.StdEncoding.EncodeToString(body), true
}

func isTextContent(ct string) bool {
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}

	return strings.HasPrefix(mt, "text/") || mt == "application/json" || mt == "application/javascript" ||
		mt == "application/xml" || strings.HasSuffix(mt, "+json") || strings.HasSuffix(mt, "+xml")
}

func writeLambdaResponse(w http.ResponseWriter, payload []byte) {
	var resp albResponse
	if err := json.Unmarshal(payload, &resp); err != nil || resp.StatusCode == nil {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	body := []byte(resp.Body)

	if resp.IsBase64Encoded {
		dec, err := base64.StdEncoding.DecodeString(resp.Body)
		if err != nil {
			writeDPError(w, http.StatusBadGateway, msgBadGateway)

			return
		}

		body = dec
	}

	for k, v := range resp.Headers {
		w.Header().Set(k, v)
	}

	for k, vs := range resp.MultiValueHeaders {
		w.Header().Del(k)

		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(*resp.StatusCode)
	_, _ = w.Write(body)
}
