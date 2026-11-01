package azurearm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/textproto"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// gwState carries one gateway request through the policy pipeline.
type gwState struct {
	r          *http.Request
	w          http.ResponseWriter
	p          *APIMProvider
	plan       *gwPlan
	query      url.Values
	header     http.Header
	respHeader http.Header
	extra      http.Header
	respBody   io.ReadCloser
	target     *url.URL
	backend    *gwBackend
	cors       *polNode
	cancel     context.CancelFunc
	rewrite    string
	status     int
	short      bool
	noCopy     bool
}

func existsAction(m map[string][]string, key, action string, vals []string) {
	switch action {
	case apimCDelete:
		delete(m, key)
	case "skip":
		if len(m[key]) == 0 {
			m[key] = append([]string(nil), vals...)
		}
	case "append":
		m[key] = append(m[key], vals...)
	default:
		m[key] = append([]string(nil), vals...)
	}
}

func expandSection(docs []*polNode, section string) []*polNode {
	if len(docs) == 0 {
		if section == polBackend {
			return []*polNode{{Name: apimCForwardRequest, Attrs: map[string]string{}}}
		}

		return nil
	}

	last := docs[len(docs)-1]

	var sec *polNode

	for _, k := range last.Kids {
		if k.Name == section {
			sec = k
		}
	}

	if sec == nil {
		return expandSection(docs[:len(docs)-1], section)
	}

	var out []*polNode

	for _, k := range sec.Kids {
		if k.Name == polBase {
			out = append(out, expandSection(docs[:len(docs)-1], section)...)
		} else {
			out = append(out, k)
		}
	}

	return out
}

func (g *gwState) parseDocs() ([]*polNode, error) {
	var docs []*polNode

	for i, d := range g.plan.docs {
		if d == "" {
			continue
		}

		n, err := parsePolicy(d, g.plan.service+"|"+strconv.Itoa(i))
		if err != nil {
			return nil, err
		}

		docs = append(docs, n)
	}

	return docs, nil
}

func (g *gwState) sub(s string) string {
	return apimNamedRefRE.ReplaceAllStringFunc(s, func(m string) string {
		return g.plan.named[strings.ToLower(m[2:len(m)-2])]
	})
}

func (g *gwState) subAll(in []string) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = g.sub(v)
	}

	return out
}

func (g *gwState) fail(status int, msg string) {
	body, _ := json.Marshal(map[string]any{apimCStatusCode: status, fieldMessage: msg})
	g.setResponse(status, http.Header{apimCContentType: {"application/json"}}, body)
	g.short = true
}

func (g *gwState) setResponse(status int, h http.Header, body []byte) {
	g.status, g.respHeader, g.respBody = status, h, io.NopCloser(bytes.NewReader(body))
}

func (g *gwState) close() {
	if g.respBody != nil {
		_ = g.respBody.Close()
	}

	if g.cancel != nil {
		g.cancel()
	}
}

func (g *gwState) run() {
	defer g.close()

	docs, err := g.parseDocs()
	if err != nil {
		writeGW(g.w, http.StatusInternalServerError, "The policy document could not be loaded.")

		return
	}

	if g.plan.preflght {
		g.preflight(docs)

		return
	}

	g.extra = http.Header{}
	g.header = g.r.Header.Clone()
	g.query = g.r.URL.Query()

	if su := strOf(g.plan.api, apimCServiceURL); su != "" {
		g.target, _ = url.Parse(su)
	}

	for _, section := range []string{polInbound, polBackend} {
		for _, n := range expandSection(docs, section) {
			g.execRequest(n)

			if g.short {
				g.finish()

				return
			}
		}
	}

	if g.respBody == nil {
		g.setResponse(http.StatusOK, http.Header{}, nil)
	}

	for _, n := range expandSection(docs, polOutbound) {
		g.execResponse(n)

		if g.short {
			break
		}
	}

	g.finish()
}

func (g *gwState) finish() {
	h := g.w.Header()

	for _, src := range []http.Header{g.respHeader, g.extra} {
		for k, v := range src {
			for _, e := range v {
				h.Add(k, e)
			}
		}
	}

	g.applyCORS(h)
	g.w.WriteHeader(g.status)

	if g.respBody != nil && g.r.Method != http.MethodHead {
		_, _ = io.Copy(g.w, g.respBody)
	}
}

func (g *gwState) execRequest(n *polNode) {
	switch n.Name {
	case apimCSetHeader:
		existsAction(g.header, textproto.CanonicalMIMEHeaderKey(g.sub(n.Attrs[fieldName])), n.Attrs[apimCExistsAction],
			g.subAll(n.values(fieldValue)))
	case apimCSetQueryParameter:
		existsAction(g.query, g.sub(n.Attrs[fieldName]), n.Attrs[apimCExistsAction], g.subAll(n.values(fieldValue)))
	case "rewrite-uri":
		g.rewrite, g.noCopy = g.sub(n.Attrs[fieldTemplate]), n.Attrs["copy-unmatched-params"] == apimCFalse
	case apimCSetBackendService:
		g.setBackend(n)
	case apimCReturnResponse:
		g.returnResponse(n)
	case apimCMockResponse:
		g.mockResponse(n)
	case apimCRateLimit:
		g.rateLimit(n)
	case apimCCheckHeader:
		g.checkHeader(n)
	case apimCCors:
		if g.cors == nil {
			g.cors = n
		}
	case apimCForwardRequest:
		g.forward(n)
	}
}

func (g *gwState) execResponse(n *polNode) {
	switch n.Name {
	case apimCSetHeader:
		if g.respHeader == nil {
			g.respHeader = http.Header{}
		}

		existsAction(
			g.respHeader,
			textproto.CanonicalMIMEHeaderKey(g.sub(n.Attrs[fieldName])),
			n.Attrs[apimCExistsAction],
			g.subAll(n.values(fieldValue)),
		)
	case apimCReturnResponse:
		g.returnResponse(n)
	case apimCMockResponse:
		g.mockResponse(n)
	case apimCCors:
		if g.cors == nil {
			g.cors = n
		}
	}
}

func (g *gwState) setBackend(n *polNode) {
	if id := n.Attrs[apimCBackendID]; id != "" {
		b, ok := g.plan.backends[strings.ToLower(id)]
		if !ok {
			g.fail(http.StatusInternalServerError, "The backend '"+id+"' no longer exists.")

			return
		}

		g.backend = &b
		g.target, _ = url.Parse(b.url)

		return
	}

	u, err := url.Parse(g.sub(n.Attrs[apimCBaseURL]))
	if err != nil || !validAbsURL(u.String(), acaScaleHTTP, acaSchemeHTTPS) {
		g.fail(http.StatusInternalServerError, "The set-backend-service base-url is not a valid URL.")

		return
	}

	g.target, g.backend = u, nil
}

func (g *gwState) returnResponse(n *polNode) {
	status, hdr, body := http.StatusOK, http.Header{}, []byte(nil)

	for _, k := range n.Kids {
		switch k.Name {
		case apimCSetStatus:
			status, _ = strconv.Atoi(strings.TrimSpace(k.Attrs[aksFieldCode]))
		case apimCSetHeader:
			existsAction(hdr, textproto.CanonicalMIMEHeaderKey(g.sub(k.Attrs[fieldName])), k.Attrs[apimCExistsAction],
				g.subAll(k.values(fieldValue)))
		case apimCSetBody:
			body = []byte(g.sub(strings.TrimSpace(k.Text)))
		}
	}

	g.setResponse(status, hdr, body)
	g.short = true
}

func (g *gwState) mockResponse(n *polNode) {
	code := 200

	if v := n.Attrs[apimCStatusCodeX]; v != "" {
		code, _ = strconv.Atoi(strings.TrimSpace(v))
	}

	want := n.Attrs["content-type"]
	hdr, body := http.Header{}, []byte(nil)

	for _, e := range anySlice(g.plan.op[apimCResponses]) {
		resp := mapOf(e)
		if intOf(resp[apimCStatusCode]) != code {
			continue
		}

		for _, rp := range anySlice(resp["representations"]) {
			rep := mapOf(rp)
			if want != "" && !strings.EqualFold(strOf(rep, apimCContentTypeX), want) {
				continue
			}

			if b, ok := firstExample(mapOf(rep[apimCExamples])); ok {
				hdr.Set(apimCContentType, strOf(rep, apimCContentTypeX))
				body = b
			}

			break
		}
	}

	g.setResponse(code, hdr, body)
	g.short = true
}

func anySlice(v any) []any {
	s, _ := v.([]any)

	return s
}

func firstExample(examples map[string]any) ([]byte, bool) {
	names := make([]string, 0, len(examples))
	for k := range examples {
		names = append(names, k)
	}

	if len(names) == 0 {
		return nil, false
	}

	best := names[0]
	for _, k := range names {
		if k < best {
			best = k
		}
	}

	v := mapOf(examples[best])[fieldValue]
	if s, ok := v.(string); ok {
		return []byte(s), true
	}

	b, err := json.Marshal(v)

	return b, err == nil
}

func (g *gwState) rateLimit(n *polNode) {
	calls, _ := strconv.Atoi(strings.TrimSpace(n.Attrs[apimCCalls]))
	period, _ := strconv.Atoi(strings.TrimSpace(n.Attrs[apimCRenewalPeriod]))
	who := g.plan.subID

	if who == "" {
		return
	}

	key := fmt.Sprintf("%s|%d:%d|%s", n.Scope, n.Line, n.Col, who)

	ok, left, retry := g.p.limiter.take(key, calls, time.Duration(period)*time.Second, g.plan.now())

	if h := n.Attrs["total-calls-header-name"]; h != "" {
		g.extra.Set(h, strconv.Itoa(calls))
	}

	if h := n.Attrs["remaining-calls-header-name"]; h != "" {
		g.extra.Set(h, strconv.Itoa(left))
	}

	if ok {
		return
	}

	secs := strconv.Itoa(int(math.Ceil(retry.Seconds())))
	g.fail(http.StatusTooManyRequests, apimMsgRateLimit)
	g.respHeader.Set(firstNonEmpty(n.Attrs["retry-after-header-name"], apimCRetryAfter), secs)
}

func (g *gwState) checkHeader(n *polNode) {
	name := g.sub(n.Attrs[fieldName])
	have := g.header.Values(name)
	fold := n.Attrs["ignore-case"] == cmpTrue

	for _, h := range have {
		for _, want := range n.values(fieldValue) {
			if h == want || (fold && strings.EqualFold(h, want)) {
				return
			}
		}
	}

	code, _ := strconv.Atoi(strings.TrimSpace(n.Attrs[apimCFailedCheckHttpcode]))
	g.setResponse(
		code,
		http.Header{apimCContentType: {"text/plain"}},
		[]byte(g.sub(n.Attrs["failed-check-error-message"])),
	)
	g.short = true
}

func (g *gwState) buildURL() (*url.URL, error) {
	if g.target == nil {
		return nil, errNoAPIMBackend
	}

	if g.target.Scheme != acaScaleHTTP && g.target.Scheme != acaSchemeHTTPS {
		return nil, errAPIMWebSocket
	}

	path, query := g.suffixPath(), g.query

	if g.rewrite != "" {
		tpl, rawQ, _ := strings.Cut(g.rewrite, "?")
		path = fillTemplate(tpl, g.plan.params)

		if g.noCopy {
			query = url.Values{}
		}

		if rawQ != "" {
			extra, _ := url.ParseQuery(fillTemplate(rawQ, g.plan.params))
			maps.Copy(query, extra)
		}
	}

	u := *g.target
	u.Path = strings.TrimRight(g.target.Path, "/") + "/" + strings.TrimLeft(path, "/")
	u.RawPath = ""

	merged := g.target.Query()
	maps.Copy(merged, query)

	u.RawQuery = merged.Encode()

	return &u, nil
}

func (g *gwState) suffixPath() string { return g.plan.suffix }

func fillTemplate(tpl string, params map[string]string) string {
	return apimTemplateRE.ReplaceAllStringFunc(tpl, func(m string) string {
		return params[strings.Trim(m, "{*}")]
	})
}

var (
	errNoAPIMBackend = errors.New("the API has no serviceUrl and no set-backend-service policy selected a backend")
	errAPIMWebSocket = errors.New("WebSocket backends are not supported")
)

func (g *gwState) forward(n *polNode) {
	u, err := g.buildURL()
	if err != nil {
		g.fail(http.StatusInternalServerError, err.Error())

		return
	}

	timeout := apimForwardTimeout
	if t, convErr := strconv.Atoi(strings.TrimSpace(n.Attrs[apimCTimeout])); convErr == nil {
		timeout = time.Duration(t) * time.Second
	}

	if t, convErr := strconv.Atoi(strings.TrimSpace(n.Attrs["timeout-ms"])); convErr == nil {
		timeout = time.Duration(t) * time.Millisecond
	}

	body, err := io.ReadAll(g.r.Body)
	if err != nil {
		g.fail(http.StatusRequestEntityTooLarge, "The request body is too large.")

		return
	}

	ctx, cancel := context.WithTimeout(g.r.Context(), timeout)
	g.cancel = cancel

	req, err := http.NewRequestWithContext(ctx, g.r.Method, u.String(), bytes.NewReader(body))
	if err != nil {
		g.fail(http.StatusInternalServerError, "The backend request could not be built.")

		return
	}

	copyForwardHeaders(req.Header, g.header)
	req.Header.Set(apimHeaderXForward, hostOnly(g.r.RemoteAddr))
	g.applyCredentials(req)

	resp, err := g.p.client.Do(req) //nolint:bodyclose // closed in close()
	if err != nil {
		g.upstreamError(err)

		return
	}

	h := http.Header{}
	copyForwardHeaders(h, resp.Header)
	g.status, g.respHeader, g.respBody = resp.StatusCode, h, resp.Body
}

func (g *gwState) upstreamError(err error) {
	if errors.Is(err, ErrAPIMDestinationNotPermitted) {
		g.fail(
			http.StatusBadGateway,
			"The backend host is outside loopback and external backends are not allowed (--azure-arm-apim-allow-external).",
		)

		return
	}

	g.fail(http.StatusBadGateway, "The backend request failed.")
}

func (g *gwState) applyCredentials(req *http.Request) {
	if g.backend == nil {
		return
	}

	for k, v := range g.backend.header {
		req.Header[textproto.CanonicalMIMEHeaderKey(k)] = append([]string(nil), v...)
	}

	if len(g.backend.query) > 0 {
		q := req.URL.Query()
		for k, v := range g.backend.query {
			q[k] = append([]string(nil), v...)
		}

		req.URL.RawQuery = q.Encode()
	}

	if g.backend.scheme != "" {
		req.Header.Set(apimCAuthorizationX, g.backend.scheme+" "+g.backend.param)
	}
}

func copyForwardHeaders(dst, src http.Header) {
	for k, v := range src {
		switch k {
		case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te",
			"Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length":
			continue
		}

		dst[k] = append([]string(nil), v...)
	}
}

func (g *gwState) corsAllowed(origin string) bool {
	if g.cors == nil || origin == "" {
		return false
	}

	allowed := g.cors.kid(apimCAllowedOrigins)
	if allowed == nil {
		return false
	}

	for _, o := range allowed.values(apimCOrigin) {
		if o == "*" || o == origin {
			return true
		}
	}

	return false
}

func (g *gwState) corsOrigin(origin string) string {
	if g.cors.Attrs[apimCCorsAllow] != cmpTrue {
		if slices.Contains(g.cors.kid(apimCAllowedOrigins).values(apimCOrigin), "*") {
			return "*"
		}
	}

	return origin
}

func (g *gwState) applyCORS(h http.Header) {
	origin := g.r.Header.Get(apimCOriginX)
	if !g.corsAllowed(origin) {
		return
	}

	h.Set(apimHeaderAllowOrig, g.corsOrigin(origin))
	h.Add("Vary", apimCOriginX)

	if g.cors.Attrs[apimCCorsAllow] == cmpTrue {
		h.Set("Access-Control-Allow-Credentials", cmpTrue)
	}

	if ex := g.cors.kid(apimCExposeHeaders); ex != nil && len(ex.values(apimCHeader)) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(ex.values(apimCHeader), ", "))
	}
}

func (g *gwState) preflight(docs []*polNode) {
	for _, n := range expandSection(docs, polInbound) {
		if n.Name == apimCCors {
			g.cors = n

			break
		}
	}

	h := g.w.Header()
	origin := g.r.Header.Get(apimCOriginX)

	if g.corsAllowed(origin) {
		g.applyCORS(h)
		h.Set("Access-Control-Allow-Methods", corsList(g.cors.kid(apimCAllowedMethods), apimCMethod,
			g.r.Header.Get(apimCAccessControlRequestMethod)))
		h.Set("Access-Control-Allow-Headers", corsList(g.cors.kid(apimCAllowedHeaders), apimCHeader,
			g.r.Header.Get("Access-Control-Request-Headers")))

		if am := g.cors.kid(apimCAllowedMethods); am != nil && am.Attrs[apimCPreflightResultMaxAge] != "" {
			h.Set("Access-Control-Max-Age", am.Attrs[apimCPreflightResultMaxAge])
		}
	}

	g.w.WriteHeader(http.StatusOK)
}

func corsList(n *polNode, kind, requested string) string {
	if n == nil {
		if kind == apimCMethod {
			return "GET, POST"
		}

		return ""
	}

	vals := n.values(kind)
	if slices.Contains(vals, "*") {
		return requested
	}

	return strings.Join(vals, ", ")
}
