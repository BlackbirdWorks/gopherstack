package azurearm

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	apimGatewayPrefix  = "/_apim/"
	apimMaxRequestBody = 8 << 20
	apimDialKeepAlive  = 30 * time.Second
	apimDialTimeout    = 10 * time.Second
	apimForwardTimeout = 300 * time.Second
	apimMsgNotFound    = "Resource not found"
	apimMsgNoOperation = "Unable to match incoming request to an operation."
	apimMsgRateLimit   = "Rate limit is exceeded"
	apimMsgMissingKey  = "Access denied due to missing subscription key. " +
		"Make sure to include subscription key when making requests to this API."
	apimMsgInvalidKey = "Access denied due to invalid subscription key. " +
		"Make sure to provide a valid key for an active subscription."
	apimMsgNotActive    = "The API Management service is not active."
	apimLimiterSweepAt  = 10000
	apimHeaderXForward  = "X-Forwarded-For"
	apimHeaderAllowOrig = "Access-Control-Allow-Origin"
)

// ErrAPIMDestinationNotPermitted is returned by the gateway's dialer for a backend outside loopback.
var ErrAPIMDestinationNotPermitted = errors.New(
	"azurearm: apim backend is outside loopback and external backends are not allowed",
)

type apimLimiter struct {
	mu  *lockmetrics.RWMutex
	win map[string][]time.Time
}

func newAPIMLimiter() *apimLimiter {
	return &apimLimiter{mu: lockmetrics.New("azurearm.apim.limiter"), win: map[string][]time.Time{}}
}

func (l *apimLimiter) reset() {
	l.mu.Lock("reset")
	l.win = map[string][]time.Time{}
	l.mu.Unlock()
}

// take counts one call in a sliding window of period; it reports whether the call is allowed, the calls left and,
// when denied, the time until the oldest counted call leaves the window.
func (l *apimLimiter) take(key string, calls int, period time.Duration, now time.Time) (bool, int, time.Duration) {
	l.mu.Lock("take")
	defer l.mu.Unlock()

	if len(l.win) > apimLimiterSweepAt {
		for k, w := range l.win {
			if len(w) == 0 || !now.Before(w[len(w)-1].Add(period)) {
				delete(l.win, k)
			}
		}
	}

	w := l.win[key]
	for len(w) > 0 && !now.Before(w[0].Add(period)) {
		w = w[1:]
	}

	if len(w) >= calls {
		l.win[key] = w

		return false, 0, w[len(w)-calls].Add(period).Sub(now)
	}

	w = append(w, now)
	l.win[key] = w

	return true, calls - len(w), 0
}

func (p *APIMProvider) newBackendClient() *http.Client {
	tr := httpclient.NewTransport()
	tr.Proxy = nil
	tr.DialContext = (&net.Dialer{
		KeepAlive: apimDialKeepAlive,
		Timeout:   apimDialTimeout,
		Control: func(_, address string, _ syscall.RawConn) error {
			if p.allowExternal.Load() {
				return nil
			}

			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}

			if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
				return ErrAPIMDestinationNotPermitted
			}

			return nil
		},
	}).DialContext

	return &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

type gwFail struct {
	msg    string
	status int
}

type gwBackend struct {
	header map[string][]string
	query  map[string][]string
	url    string
	scheme string
	param  string
}

// gwPlan is the immutable view of the service the gateway needs for one request.
type gwPlan struct {
	now      func() time.Time
	api      map[string]any
	op       map[string]any
	named    map[string]string
	backends map[string]gwBackend
	params   map[string]string
	service  string
	subID    string
	suffix   string
	apiID    string
	docs     [3]string
	preflght bool
}

func gatewayTarget(r *http.Request) (string, string, bool) {
	if rest, ok := strings.CutPrefix(r.URL.Path, apimGatewayPrefix); ok {
		name, path, _ := strings.Cut(rest, "/")

		return name, "/" + path, name != ""
	}

	host := strings.ToLower(hostFromHostHeader(r.Host))
	if name, ok := strings.CutSuffix(host, apimGatewaySuffix); ok && name != "" && !strings.Contains(name, ".") {
		return name, r.URL.Path, true
	}

	return "", "", false
}

func writeGW(w http.ResponseWriter, status int, msg string) {
	w.Header().Set(apimCContentType, "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{apimCStatusCode: status, fieldMessage: msg})
}

// ServeGateway answers a request addressed to a service's gateway (Host <name>.azure-api.net, or the path prefix
// /_apim/<name>/); it reports false for any other request.
func (p *APIMProvider) ServeGateway(w http.ResponseWriter, r *http.Request) bool {
	name, path, ok := gatewayTarget(r)
	if !ok {
		return false
	}

	plan, fail := p.resolveGateway(name, path, r)
	if fail != nil {
		writeGW(w, fail.status, fail.msg)

		return true
	}

	r.Body = http.MaxBytesReader(w, r.Body, apimMaxRequestBody)

	(&gwState{p: p, plan: plan, w: w, r: r}).run()

	return true
}

func (p *APIMProvider) findService(name string) *apimService {
	for _, s := range p.svcs {
		if strings.EqualFold(s.Name, name) {
			return s
		}
	}

	return nil
}

func (p *APIMProvider) resolveGateway(name, path string, r *http.Request) (*gwPlan, *gwFail) {
	p.mu.RLock("resolveGateway")
	defer p.mu.RUnlock()

	s := p.findService(name)
	if s == nil {
		return nil, &gwFail{status: http.StatusNotFound, msg: apimMsgNotFound}
	}

	if s.state(p.now()) != provisioningStateSucceeded {
		return nil, &gwFail{status: http.StatusServiceUnavailable, msg: apimMsgNotActive}
	}

	api, suffix := s.matchAPI(path)
	if api == nil || !apiAllowsScheme(api.Props, r.TLS != nil) {
		return nil, &gwFail{status: http.StatusNotFound, msg: apimMsgNoOperation}
	}

	plan := &gwPlan{
		now:      p.now,
		service:  s.Name,
		api:      api.Props,
		apiID:    api.ID,
		suffix:   suffix,
		named:    s.namedMap(),
		backends: s.backendMap(),
	}
	plan.docs[0], plan.docs[1] = s.policyDoc(""), s.policyDoc("apis/"+api.ID)

	if r.Method == http.MethodOptions && r.Header.Get(apimCOriginX) != "" &&
		r.Header.Get(apimCAccessControlRequestMethod) != "" {
		plan.preflght = true
		plan.op, _, _ = s.matchOperation(api.ID, r.Header.Get(apimCAccessControlRequestMethod), suffix)

		return plan, nil
	}

	op, params, opID := s.matchOperation(api.ID, r.Method, suffix)
	if op == nil {
		return nil, &gwFail{status: http.StatusNotFound, msg: apimMsgNoOperation}
	}

	plan.op, plan.params = op, params
	plan.docs[2] = s.policyDoc("apis/" + api.ID + "/operations/" + opID)

	if fail := p.checkKey(s, api, plan, r); fail != nil {
		return nil, fail
	}

	return plan, nil
}

func (s *apimService) policyDoc(scope string) string {
	if e, ok := s.Policies[entKey(scope, apimPolicyID)]; ok {
		return strOf(e.Props, fieldValue)
	}

	return ""
}

func (s *apimService) namedMap() map[string]string {
	out := make(map[string]string, len(s.Named))
	for _, e := range s.Named {
		out[strings.ToLower(strOf(e.Props, apimCDisplayName))] = strOf(e.Props, fieldValue)
	}

	return out
}

func (s *apimService) backendMap() map[string]gwBackend {
	out := make(map[string]gwBackend, len(s.Backends))

	for _, e := range s.Backends {
		b := gwBackend{url: strOf(e.Props, apimCUrl), header: map[string][]string{}, query: map[string][]string{}}
		c := mapOf(e.Props[apimCCredentials])

		for k, v := range mapOf(c[apimCHeader]) {
			b.header[k] = strList(v)
		}

		for k, v := range mapOf(c[apimCQuery]) {
			b.query[k] = strList(v)
		}

		if a := mapOf(c[apimCAuthorization]); a != nil {
			b.scheme, b.param = strOf(a, "scheme"), strOf(a, "parameter")
		}

		out[strings.ToLower(e.ID)] = b
	}

	return out
}

func apiAllowsScheme(props map[string]any, tls bool) bool {
	want := acaScaleHTTP
	if tls {
		want = acaSchemeHTTPS
	}

	for _, v := range strList(props[apimCProtocols]) {
		if strings.EqualFold(v, want) {
			return true
		}
	}

	return false
}

func (s *apimService) matchAPI(path string) (*apimEntity, string) {
	norm := strings.Trim(path, "/")

	var (
		best   *apimEntity
		bestAP string
	)

	for _, e := range s.APIs {
		ap := strings.ToLower(strOf(e.Props, apimCPath))
		if len(norm) < len(ap) || !strings.EqualFold(norm[:len(ap)], ap) {
			continue
		}

		if ap != "" && len(norm) > len(ap) && norm[len(ap)] != '/' {
			continue
		}

		if best == nil || len(ap) > len(bestAP) {
			best, bestAP = e, ap
		}
	}

	if best == nil {
		return nil, ""
	}

	return best, "/" + strings.TrimPrefix(norm[len(bestAP):], "/")
}

// matchOperation picks the most specific operation of an API whose method and URL template fit.
func (s *apimService) matchOperation(apiID, method, suffix string) (map[string]any, map[string]string, string) {
	var (
		bestProps  map[string]any
		bestParams map[string]string
		bestID     string
		bestScore  = -1
	)

	segs := splitARMPath(suffix)

	for _, e := range s.Ops {
		if !strings.EqualFold(e.Parent, apiID) || !strings.EqualFold(strOf(e.Props, apimCMethod), method) {
			continue
		}

		tpl, _, _ := strings.Cut(strOf(e.Props, apimCUrlTemplate), "?")

		params, score, ok := matchTemplate(splitARMPath(tpl), segs)
		if ok && (score > bestScore || (score == bestScore && e.ID < bestID)) {
			bestProps, bestParams, bestID, bestScore = e.Props, params, e.ID, score
		}
	}

	return bestProps, bestParams, bestID
}

func matchTemplate(tpl, segs []string) (map[string]string, int, bool) {
	params := map[string]string{}
	score := 0

	for i, t := range tpl {
		isParam := strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}")

		if isParam && strings.HasPrefix(t, "{*") {
			if i != len(tpl)-1 {
				return nil, 0, false
			}

			params[strings.Trim(t, "{*}")] = strings.Join(segs[min(i, len(segs)):], "/")

			return params, score, true
		}

		if i >= len(segs) {
			return nil, 0, false
		}

		if isParam {
			params[strings.Trim(t, "{}")] = segs[i]

			continue
		}

		if !strings.EqualFold(t, segs[i]) {
			return nil, 0, false
		}

		score++
	}

	return params, score, len(segs) == len(tpl)
}

// checkKey applies the subscription rules of api-management-subscriptions: a key for an API that does not require
// a subscription is ignored, and an invalid or missing key falls back to an open product when one includes the API.
func (p *APIMProvider) checkKey(s *apimService, api *apimEntity, plan *gwPlan, r *http.Request) *gwFail {
	if req, _ := api.Props[apimCSubscriptionRequired].(bool); !req {
		return nil
	}

	names := mapOf(api.Props[apimCSubscriptionKeyParameterNames])

	key := r.Header.Get(firstNonEmpty(strOf(names, apimCHeader), apimHeaderSub))
	if key == "" {
		key = r.URL.Query().Get(firstNonEmpty(strOf(names, apimCQuery), apimQuerySub))
	}

	if key != "" {
		if sub := s.activeSubscription(key, api.ID); sub != nil {
			plan.subID = sub.ID

			return nil
		}
	}

	switch {
	case s.apiInOpenProduct(api.ID):
		return nil
	case key == "":
		return &gwFail{status: http.StatusUnauthorized, msg: apimMsgMissingKey}
	}

	return &gwFail{status: http.StatusUnauthorized, msg: apimMsgInvalidKey}
}

func (s *apimService) activeSubscription(key, apiID string) *apimEntity {
	for _, sub := range s.Subs {
		if strOf(sub.Props, sqlPropState) != apimStateActive {
			continue
		}

		if !keysEqual(strOf(sub.Props, redisPropPrimaryKey), key) &&
			!keysEqual(strOf(sub.Props, redisPropSecondaryKey), key) {
			continue
		}

		if s.scopeCovers(strOf(sub.Props, acrPropScope), apiID) {
			return sub
		}
	}

	return nil
}

func (s *apimService) scopeCovers(scope, apiID string) bool {
	base := s.armBase()

	switch {
	case strings.EqualFold(scope, base+"/"), strings.EqualFold(scope, base+"/apis"):
		return true
	case strings.EqualFold(scope, base+"/apis/"+apiID):
		return true
	}

	pid, ok := strings.CutPrefix(strings.ToLower(scope), strings.ToLower(base)+"/products/")

	return ok && s.ProductAPIs[pid+"/"+strings.ToLower(apiID)]
}

func (s *apimService) apiInOpenProduct(apiID string) bool {
	for link := range s.ProductAPIs {
		pid, aid, _ := strings.Cut(link, "/")
		if aid != strings.ToLower(apiID) {
			continue
		}

		if prod := s.Products[pid]; prod != nil {
			if req, _ := prod.Props[apimCSubscriptionRequired].(bool); !req {
				return true
			}
		}
	}

	return false
}

func hostOnly(addr string) string {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	return h
}
