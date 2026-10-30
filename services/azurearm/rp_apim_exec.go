package azurearm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/textproto"
	"net/url"
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
	case "delete":
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
			return []*polNode{{Name: "forward-request", Attrs: map[string]string{}}}
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
	body, _ := json.Marshal(map[string]any{"statusCode": status, "message": msg})
	g.setResponse(status, http.Header{"Content-Type": {"application/json"}}, body)
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
	keyNames := mapOf(g.plan.api["subscriptionKeyParameterNames"])
	g.header.Del(firstNonEmpty(strOf(keyNames, "header"), apimHeaderSub))
	g.query = cleanQuery(g.r.URL.Query(), firstNonEmpty(strOf(keyNames, "query"), apimQuerySub))

	if su := strOf(g.plan.api, "serviceUrl"); su != "" {
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
	case "set-header":
		existsAction(g.header, textproto.CanonicalMIMEHeaderKey(g.sub(n.Attrs["name"])), n.Attrs["exists-action"],
			g.subAll(n.values("value")))
	case "set-query-parameter":
		existsAction(g.query, g.sub(n.Attrs["name"]), n.Attrs["exists-action"], g.subAll(n.values("value")))
	case "rewrite-uri":
		g.rewrite, g.noCopy = g.sub(n.Attrs["template"]), n.Attrs["copy-unmatched-params"] == "false"
	case "set-backend-service":
		g.setBackend(n)
	case "return-response":
		g.returnResponse(n)
	case "mock-response":
		g.mockResponse(n)
	case "rate-limit":
		g.rateLimit(n)
	case "check-header":
		g.checkHeader(n)
	case "cors":
		if g.cors == nil {
			g.cors = n
		}
	case "forward-request":
		g.forward(n)
	}
}

func (g *gwState) execResponse(n *polNode) {
	switch n.Name {
	case "set-header":
		if g.respHeader == nil {
			g.respHeader = http.Header{}
		}

		existsAction(g.respHeader, textproto.CanonicalMIMEHeaderKey(g.sub(n.Attrs["name"])), n.Attrs["exists-action"],
			g.subAll(n.values("value")))
	case "return-response":
		g.returnResponse(n)
	case "cors":
		if g.cors == nil {
			g.cors = n
		}
	}
}

func (g *gwState) setBackend(n *polNode) {
	if id := n.Attrs["backend-id"]; id != "" {
		b, ok := g.plan.backends[strings.ToLower(id)]
		if !ok {
			g.fail(http.StatusInternalServerError, "The backend '"+id+"' no longer exists.")

			return
		}

		g.backend = &b
		g.target, _ = url.Parse(b.url)

		return
	}

	u, err := url.Parse(g.sub(n.Attrs["base-url"]))
	if err != nil || !validAbsURL(u.String(), "http", "https") {
		g.fail(http.StatusInternalServerError, "The set-backend-service base-url is not a valid URL.")

		return
	}

	g.target, g.backend = u, nil
}

func (g *gwState) returnResponse(n *polNode) {
	status, hdr, body := http.StatusOK, http.Header{}, []byte(nil)

	for _, k := range n.Kids {
		switch k.Name {
		case "set-status":
			status, _ = strconv.Atoi(strings.TrimSpace(k.Attrs["code"]))
		case "set-header":
			existsAction(hdr, textproto.CanonicalMIMEHeaderKey(g.sub(k.Attrs["name"])), k.Attrs["exists-action"],
				g.subAll(k.values("value")))
		case "set-body":
			body = []byte(g.sub(strings.TrimSpace(k.Text)))
		}
	}

	g.setResponse(status, hdr, body)
	g.short = true
}

func (g *gwState) mockResponse(n *polNode) {
	code := 200

	if v := n.Attrs["status-code"]; v != "" {
		code, _ = strconv.Atoi(strings.TrimSpace(v))
	}

	want := n.Attrs["content-type"]
	hdr, body := http.Header{}, []byte(nil)

	for _, e := range anySlice(g.plan.op["responses"]) {
		resp := mapOf(e)
		if intOf(resp["statusCode"]) != code {
			continue
		}

		for _, rp := range anySlice(resp["representations"]) {
			rep := mapOf(rp)
			if want != "" && !strings.EqualFold(strOf(rep, "contentType"), want) {
				continue
			}

			if b, ok := firstExample(mapOf(rep["examples"])); ok {
				hdr.Set("Content-Type", strOf(rep, "contentType"))
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
	calls, _ := strconv.Atoi(strings.TrimSpace(n.Attrs["calls"]))
	period, _ := strconv.Atoi(strings.TrimSpace(n.Attrs["renewal-period"]))
	who := g.plan.subID

	if who == "" {
		return
	}

	key := fmt.Sprintf("%s|%d:%d|%s", n.Scope, n.Line, n.Col, who)

	ok, left, retry := g.p.limiter.take(key, calls, time.Duration(period)*time.Second, g.plan.now())
	if !ok {
		secs := int(math.Ceil(retry.Seconds()))
		g.fail(http.StatusTooManyRequests, fmt.Sprintf("Rate limit is exceeded. Try again in %d seconds.", secs))
		g.respHeader.Set("Retry-After", strconv.Itoa(secs))

		if h := n.Attrs["retry-after-header-name"]; h != "" {
			g.respHeader.Set(h, strconv.Itoa(secs))
		}

		return
	}

	if h := n.Attrs["remaining-calls-header-name"]; h != "" {
		g.extra.Set(h, strconv.Itoa(left))
	}
}

func (g *gwState) checkHeader(n *polNode) {
	name := g.sub(n.Attrs["name"])
	have := g.header.Values(name)
	fold := n.Attrs["ignore-case"] == "true"

	for _, h := range have {
		for _, want := range n.values("value") {
			if h == want || (fold && strings.EqualFold(h, want)) {
				return
			}
		}
	}

	code, _ := strconv.Atoi(strings.TrimSpace(n.Attrs["failed-check-httpcode"]))
	g.setResponse(code, http.Header{"Content-Type": {"text/plain"}}, []byte(g.sub(n.Attrs["failed-check-error-message"])))
	g.short = true
}

func (g *gwState) buildURL() (*url.URL, error) {
	if g.target == nil {
		return nil, errNoAPIMBackend
	}

	if g.target.Scheme != "http" && g.target.Scheme != "https" {
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
			for k, v := range extra {
				query[k] = v
			}
		}
	}

	u := *g.target
	u.Path = strings.TrimRight(g.target.Path, "/") + "/" + strings.TrimLeft(path, "/")
	u.RawPath = ""

	merged := g.target.Query()
	for k, v := range query {
		merged[k] = v
	}

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
	if t, convErr := strconv.Atoi(n.Attrs["timeout"]); convErr == nil && t > 0 {
		timeout = time.Duration(t) * time.Second
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

	resp, err := g.p.client.Do(req) //nolint:gosec,bodyclose // G704: dialer refuses non-loopback peers by default; closed in close()
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
		g.fail(http.StatusBadGateway,
			"The backend host is outside loopback and external backends are not allowed (--azure-arm-apim-allow-external).")

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
		req.Header.Set("Authorization", g.backend.scheme+" "+g.backend.param)
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

	allowed := g.cors.kid("allowed-origins")
	if allowed == nil {
		return false
	}

	for _, o := range allowed.values("origin") {
		if o == "*" || o == origin {
			return true
		}
	}

	return false
}

func (g *gwState) corsOrigin(origin string) string {
	if g.cors.Attrs["allow-credentials"] != "true" {
		for _, o := range g.cors.kid("allowed-origins").values("origin") {
			if o == "*" {
				return "*"
			}
		}
	}

	return origin
}

func (g *gwState) applyCORS(h http.Header) {
	origin := g.r.Header.Get("Origin")
	if !g.corsAllowed(origin) {
		return
	}

	h.Set(apimHeaderAllowOrig, g.corsOrigin(origin))
	h.Add("Vary", "Origin")

	if g.cors.Attrs["allow-credentials"] == "true" {
		h.Set("Access-Control-Allow-Credentials", "true")
	}

	if ex := g.cors.kid("expose-headers"); ex != nil && len(ex.values("header")) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(ex.values("header"), ", "))
	}
}

func (g *gwState) preflight(docs []*polNode) {
	for _, n := range expandSection(docs, polInbound) {
		if n.Name == "cors" {
			g.cors = n

			break
		}
	}

	h := g.w.Header()
	origin := g.r.Header.Get("Origin")

	if g.corsAllowed(origin) {
		g.applyCORS(h)
		h.Set("Access-Control-Allow-Methods", corsList(g.cors.kid("allowed-methods"), "method",
			g.r.Header.Get("Access-Control-Request-Method")))
		h.Set("Access-Control-Allow-Headers", corsList(g.cors.kid("allowed-headers"), "header",
			g.r.Header.Get("Access-Control-Request-Headers")))

		if am := g.cors.kid("allowed-methods"); am != nil && am.Attrs["preflight-result-max-age"] != "" {
			h.Set("Access-Control-Max-Age", am.Attrs["preflight-result-max-age"])
		}
	}

	g.w.WriteHeader(http.StatusOK)
}

func corsList(n *polNode, kind, requested string) string {
	if n == nil {
		return ""
	}

	vals := n.values(kind)
	for _, v := range vals {
		if v == "*" {
			return requested
		}
	}

	return strings.Join(vals, ", ")
}
