package dashboard

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
)

const (
	ociLBPrefix      = ociProxyPrefix + "/loadbalancer"
	ociLBAPIPrefix   = "/20170115"
	ociLBColl        = "loadBalancers"
	ociLBSets        = "backendSets"
	ociLBBackends    = "backends"
	ociLBListeners   = "listeners"
	ociLBCerts       = "certificates"
	ociLBHealth      = "health"
	ociLBWorkReqs    = "workRequests"
	ociLBInvoke      = "invoke"
	ociLBMaxReqBody  = 1 << 16
	ociLBMaxRespBody = 1 << 18
	ociLBMaxHeaders  = 32
	ociLBMaxTryJSON  = 2 * ociLBMaxReqBody
	ociLBMaxName     = 255
	ociLBMaxPort     = 65535
	ociLBMinMbps     = 10
	ociLBMaxMbps     = 8000
	ociLBTryTimeout  = 10 * time.Second
	ociLBItemSegs    = 2
	ociLBSubSegs     = 3
	ociLBSetHealth   = 5
	ociLBBackendSegs = 6
	ociLBBackendHlth = 7
	ociLBProbeMillis = 1000
	ociLBFlexShape   = "flexible"
	ociLBActive      = "ACTIVE"
	ociLBFirstSet    = "backend-set-1"
	ociLBFirstLsnr   = "listener-1"
	ociLBMaxHostLen  = 253
	ociLBHTTP        = "HTTP"
	ociLBBackup      = "backup"
	ociLBTCP         = "TCP"
)

// registerOCILoadBalancerRoutes serves the Load Balancer page by dispatching in-process to the emulator's handler
// without request signing. Reads, backend add/update/remove and load balancer create/delete are reachable, each
// write rebuilt from fixed fields; a "try it" call goes to the loopback data-plane address of one existing
// plain-text listener chosen by id, never to a caller-supplied host. Certificate bodies are reduced to subjects and
// expiry before they leave the proxy.
func (h *DashboardHandler) registerOCILoadBalancerRoutes() {
	var inner *echo.Echo

	ops := h.config.OCILoadBalancerOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI Load Balancer", next) }

	h.SubRouter.Add(http.MethodGet, ociLBPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: ops.Backend.Compartments()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociLBPrefix+"/api")

		parts, ok := ociSplitPath(raw)
		if !ok || !ociLBAllowed(r.Method, parts) {
			return ociInvalid(c, "unsupported Load Balancer proxy request")
		}

		if r.Method == http.MethodPost && parts[len(parts)-1] == ociLBInvoke {
			return ociLBTryIt(c, ops.Backend, unescapedPath(parts[1]), unescapedPath(parts[3]))
		}

		if r.Method != http.MethodGet {
			r.URL.RawQuery = ""

			return ociLBWrite(c, inner, raw, parts)
		}

		return ociLBRead(c, inner, raw, parts)
	})

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(m, ociLBPrefix+"/api/*", serve)
	}
}

func ociLBAllowed(method string, parts []string) bool {
	if parts[0] != ociLBColl {
		return false
	}

	switch method {
	case http.MethodGet:
		return ociLBReadable(parts)
	case http.MethodPost:
		return len(parts) == 1 || ociLBChild(parts, ociLBSetHealth, ociLBSets, ociLBBackends) ||
			ociLBChild(parts, ociLBSetHealth, ociLBListeners, ociLBInvoke)
	case http.MethodPut:
		return ociLBChild(parts, ociLBBackendSegs, ociLBSets, ociLBBackends)
	case http.MethodDelete:
		return len(parts) == ociLBItemSegs || ociLBChild(parts, ociLBBackendSegs, ociLBSets, ociLBBackends)
	}

	return false
}

// ociLBChild reports whether parts has n segments with the given collection at 2 and sub-collection at 4.
func ociLBChild(parts []string, n int, coll, sub string) bool {
	return len(parts) == n && parts[2] == coll && parts[4] == sub
}

func ociLBReadable(parts []string) bool {
	switch len(parts) {
	case 1, ociLBItemSegs:
		return true
	case ociLBSubSegs:
		return slices.Contains([]string{ociLBHealth, ociLBWorkReqs, ociLBCerts}, parts[2])
	case ociLBSetHealth:
		return parts[2] == ociLBSets && parts[4] == ociLBHealth
	case ociLBBackendHlth:
		return parts[2] == ociLBSets && parts[4] == ociLBBackends && parts[6] == ociLBHealth
	}

	return false
}

func ociLBRead(c *echo.Context, inner *echo.Echo, raw string, parts []string) error {
	r := c.Request()
	fwd := r.Clone(r.Context())
	fwd.URL.Path, fwd.URL.RawPath = unescapedPath(ociLBAPIPrefix+raw), ociLBAPIPrefix+raw
	fwd.RequestURI, fwd.Body = "", http.NoBody

	rec := httptest.NewRecorder()
	inner.ServeHTTP(rec, fwd)

	if rec.Code == http.StatusOK &&
		(len(parts) == ociLBItemSegs || (len(parts) == ociLBSubSegs && parts[2] == ociLBCerts)) {
		rec.Body = bytes.NewBuffer(ociLBScrubCerts(rec.Body.Bytes()))
	}

	return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id")
}

func ociLBWrite(c *echo.Context, inner *echo.Echo, raw string, parts []string) error {
	var (
		body []byte
		ok   = true
	)

	switch {
	case c.Request().Method == http.MethodPost && len(parts) == 1:
		body, ok = ociLBCreateBody(c)
	case c.Request().Method == http.MethodPost:
		body, ok = ociLBBackendBody(c, false)
	case c.Request().Method == http.MethodPut:
		body, ok = ociLBBackendBody(c, true)
	default:
		body = []byte{}
	}

	if !ok {
		return ociInvalid(c, "invalid Load Balancer request")
	}

	return ociDispatch(c, inner, ociLBAPIPrefix+raw, body)
}

func ociLBName(s string) bool { return s != "" && len(s) <= ociLBMaxName }

func ociLBPortOK(p *int) bool { return p != nil && *p >= 1 && *p <= ociLBMaxPort }

// ociLBCreateBody rebuilds a CreateLoadBalancer request from the form fields alone: a flexible shape, one subnet and
// an optional first HTTP listener with a round-robin backend set.
func ociLBCreateBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		Private      *bool  `json:"isPrivate"`
		ListenerPort *int   `json:"listenerPort"`
		MinMbps      *int   `json:"minimumBandwidthInMbps"`
		MaxMbps      *int   `json:"maximumBandwidthInMbps"`
		Compartment  string `json:"compartmentId"`
		DisplayName  string `json:"displayName"`
		SubnetID     string `json:"subnetId"`
	}

	if !ociDecodeBody(c, &in) || !ociLBName(in.Compartment) || !ociLBName(in.DisplayName) || !ociLBName(in.SubnetID) {
		return nil, false
	}

	lo, hi := ociLBMinMbps, ociLBMinMbps
	if in.MinMbps != nil {
		lo = *in.MinMbps
	}

	if in.MaxMbps != nil {
		hi = *in.MaxMbps
	}

	if lo < ociLBMinMbps || hi > ociLBMaxMbps || lo > hi || (in.ListenerPort != nil && !ociLBPortOK(in.ListenerPort)) {
		return nil, false
	}

	out := map[string]any{
		"compartmentId": in.Compartment, "displayName": in.DisplayName, "shapeName": ociLBFlexShape,
		"shapeDetails": map[string]any{"minimumBandwidthInMbps": lo, "maximumBandwidthInMbps": hi},
		"subnetIds":    []string{in.SubnetID}, "isPrivate": in.Private != nil && *in.Private,
	}

	if in.ListenerPort != nil {
		out["backendSets"] = map[string]any{ociLBFirstSet: map[string]any{
			"policy": "ROUND_ROBIN",
			"healthChecker": map[string]any{
				"protocol":         ociLBHTTP,
				"urlPath":          "/",
				"intervalInMillis": ociLBProbeMillis,
				"retries":          1,
			},
		}}
		out["listeners"] = map[string]any{ociLBFirstLsnr: map[string]any{
			"defaultBackendSetName": ociLBFirstSet, "protocol": ociLBHTTP, "port": *in.ListenerPort,
		}}
	}

	b, err := json.Marshal(out)

	return b, err == nil
}

type ociLBBackendIn struct {
	Port    *int   `json:"port"`
	Weight  *int   `json:"weight"`
	MaxConn *int   `json:"maxConnections"`
	Backup  *bool  `json:"backup"`
	Drain   *bool  `json:"drain"`
	Offline *bool  `json:"offline"`
	IP      string `json:"ipAddress"`
}

// fields returns the fixed update fields, or false when an UpdateBackend omits one the OCI API requires.
func (in ociLBBackendIn) update() (map[string]any, bool) {
	if in.Weight == nil || in.Backup == nil || in.Drain == nil || in.Offline == nil {
		return nil, false
	}

	out := map[string]any{"weight": *in.Weight, "backup": *in.Backup, "drain": *in.Drain, "offline": *in.Offline}
	if in.MaxConn != nil {
		out["maxConnections"] = *in.MaxConn
	}

	return out, true
}

func (in ociLBBackendIn) create() (map[string]any, bool) {
	if _, err := netip.ParseAddr(in.IP); err != nil || !ociLBPortOK(in.Port) {
		return nil, false
	}

	out := map[string]any{"ipAddress": in.IP, "port": *in.Port}
	if in.Weight != nil {
		out["weight"] = *in.Weight
	}

	if in.MaxConn != nil {
		out["maxConnections"] = *in.MaxConn
	}

	for k, v := range map[string]*bool{ociLBBackup: in.Backup, "drain": in.Drain, "offline": in.Offline} {
		if v != nil {
			out[k] = *v
		}
	}

	return out, true
}

// ociLBBackendBody rebuilds a CreateBackend (update=false) or UpdateBackend (update=true) request from fixed fields.
func ociLBBackendBody(c *echo.Context, update bool) ([]byte, bool) {
	var in ociLBBackendIn

	if !ociDecodeBody(c, &in) || (in.Weight != nil && *in.Weight < 1) || (in.MaxConn != nil && *in.MaxConn < 1) {
		return nil, false
	}

	build := in.create
	if update {
		build = in.update
	}

	out, ok := build()
	if !ok {
		return nil, false
	}

	b, err := json.Marshal(out)

	return b, err == nil
}

// ociLBScrubCerts reduces certificate bodies (a name-keyed map in a load balancer, a list from the listing) to
// subjects and validity; PEM text never reaches the browser.
func ociLBScrubCerts(raw []byte) []byte {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return raw
	}

	switch t := v.(type) {
	case []any:
		for i, e := range t {
			t[i] = ociLBCertSummary(e)
		}
	case map[string]any:
		if certs, ok := t["certificates"].(map[string]any); ok {
			for k, e := range certs {
				certs[k] = ociLBCertSummary(e)
			}
		}
	}

	out, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}

	return out
}

func ociLBCertSummary(e any) map[string]any {
	m, _ := e.(map[string]any)
	name, _ := m["certificateName"].(string)
	pub, _ := m["publicCertificate"].(string)
	ca, _ := m["caCertificate"].(string)
	out := map[string]any{"certificateName": name, "hasCaCertificate": ca != ""}

	if cert := ociLBFirstCert(pub); cert != nil {
		names := append([]string{}, cert.DNSNames...)
		if cert.Subject.CommonName != "" && !slices.Contains(names, cert.Subject.CommonName) {
			names = append([]string{cert.Subject.CommonName}, names...)
		}

		out["subjectNames"], out["issuer"] = names, cert.Issuer.CommonName
		out["notBefore"], out["notAfter"] = cert.NotBefore.UTC().
			Format(time.RFC3339),
			cert.NotAfter.UTC().
				Format(time.RFC3339)
	}

	return out
}

func ociLBFirstCert(pemText string) *x509.Certificate {
	rest := []byte(pemText)

	for {
		var blk *pem.Block

		blk, rest = pem.Decode(rest)
		if blk == nil {
			return nil
		}

		if blk.Type == "CERTIFICATE" {
			if c, err := x509.ParseCertificate(blk.Bytes); err == nil {
				return c
			}
		}
	}
}

type ociLBTryIn struct {
	Method      string              `json:"method"`
	Path        string              `json:"path"`
	Query       string              `json:"query"`
	Body        string              `json:"body"`
	VirtualHost string              `json:"virtualHost"`
	Headers     []ociAPIGWTryHeader `json:"headers"`
}

func ociLBTryValid(in ociLBTryIn) (string, bool) {
	rel, ok := ociAPIGWTryPath(ociAPIGWTryIn{Method: in.Method, Path: in.Path, Query: in.Query})

	return rel, ok && ociAPIGWMethodOK(in.Method) && len(in.Headers) <= ociLBMaxHeaders &&
		len(in.Body) <= ociLBMaxReqBody && ociLBHostOK(in.VirtualHost)
}

// ociLBHostOK accepts an empty or plain DNS-style name; it only ever becomes the Host header, never the destination.
func ociLBHostOK(h string) bool {
	if len(h) > ociLBMaxHostLen {
		return false
	}

	for i, r := range h {
		alnum := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		if !alnum && (i == 0 || i == len(h)-1 || (r != '.' && r != '-')) {
			return false
		}
	}

	return true
}

// ociLBTryIt replays a request against the loopback data-plane address of one plain-text listener. The destination
// comes from the backend, never from the request, and every dial is pinned to it.
func ociLBTryIt(c *echo.Context, ops lbTargets, lbID, listener string) error {
	var in ociLBTryIn
	if !ociDecodeBodyN(c, &in, ociLBMaxTryJSON) {
		return ociInvalid(c, "invalid Load Balancer try-it request")
	}

	rel, ok := ociLBTryValid(in)
	if !ok {
		return ociInvalid(c, "invalid Load Balancer try-it request")
	}

	state, exists := ops.LoadBalancerState(lbID)
	if !exists {
		return cloudJSONFail(c, http.StatusNotFound, "NotAuthorizedOrNotFound", "Load balancer not found")
	}

	if state != ociLBActive {
		return cloudJSONFail(c, http.StatusConflict, "IncorrectState", "Load balancer is in state "+state)
	}

	t, found := ops.ListenerTarget(lbID, listener)
	if !found {
		return cloudJSONFail(c, http.StatusNotFound, "NotAuthorizedOrNotFound", "Listener not found or not serving")
	}

	if t.TLS || (t.Protocol != ociLBHTTP && t.Protocol != ociLBTCP) {
		return cloudJSONFail(c, http.StatusConflict, "IncorrectState",
			"Try it supports plain-text HTTP and TCP listeners only")
	}

	if !ociLBLoopback(t.Address) {
		return cloudJSONFail(c, http.StatusBadGateway, "InternalServerError", "listener address is not loopback")
	}

	return ociLBSend(c, t.Address, rel, in)
}

type lbTargets interface {
	LoadBalancerState(id string) (string, bool)
	ListenerTarget(lbID, listener string) (ociloadbalancer.Target, bool)
}

func ociLBLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}

	ip, err := netip.ParseAddr(host)

	return err == nil && ip.IsLoopback()
}

func ociLBSend(c *echo.Context, addr, rel string, in ociLBTryIn) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), ociLBTryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, in.Method, "http://"+addr+rel, strings.NewReader(in.Body))
	if err != nil {
		return ociInvalid(c, "invalid Load Balancer try-it request")
	}

	if in.VirtualHost != "" {
		req.Host = in.VirtualHost
	}

	for _, hd := range in.Headers {
		if ociAPIGWHeaderOK(hd.Name) {
			req.Header.Add(hd.Name, hd.Value)
		}
	}

	tr := httpclient.NewTransport()
	tr.Proxy = nil
	tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer

		return d.DialContext(ctx, network, addr)
	}

	defer tr.CloseIdleConnections()

	client := &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	resp, err := client.Do(req)
	if err != nil {
		return cloudJSONFail(
			c,
			http.StatusBadGateway,
			"InternalServerError",
			"request to listener failed: "+err.Error(),
		)
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, ociLBMaxRespBody+1))

	rec := httptest.NewRecorder()
	rec.Code = resp.StatusCode
	maps.Copy(rec.Header(), resp.Header)
	_, _ = rec.Body.Write(body)

	return c.JSON(http.StatusOK, ociAPIGWTryOut(rec))
}
