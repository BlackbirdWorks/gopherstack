package dashboard

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	mdns "github.com/miekg/dns"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	idbackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

const (
	ociDNSPrefix     = ociProxyPrefix + "/dns"
	ociDNSAPIPrefix  = "/20180115"
	ociDNSZones      = "zones"
	ociDNSZoneItem   = "zone"
	ociDNSViews      = "views"
	ociDNSResolvers  = "resolvers"
	ociDNSEndpoints  = "endpoints"
	ociDNSRecords    = "records"
	ociDNSTsigKeys   = "tsigKeys"
	ociDNSPolicies   = "steeringPolicies"
	ociDNSMaxBody    = 1 << 16
	ociDNSMaxName    = 255
	ociDNSMaxRdata   = 4096
	ociDNSMaxItems   = 50
	ociDNSMaxTTL     = 2147483647
	ociDNSMaxFQDN    = 253
	ociDNSMaxLabel   = 63
	ociDNSItemSegs   = 2
	ociDNSSubSegs    = 3
	ociDNSQueryWait  = 3 * time.Second
	ociDNSKeyLimit   = "limit"
	ociDNSKeyPage    = "page"
	ociDNSKeyScope   = "scope"
	ociDNSKeyViewID  = "viewId"
	ociDNSKeySortBy  = "sortBy"
	ociDNSKeySortOrd = "sortOrder"
	ociDNSKeyName    = "name"
	ociDNSKeyState   = "lifecycleState"
	ociDNSKeyDomain  = "domain"
	ociDNSKeyRtype   = "rtype"
	ociDNSKeyDisplay = "displayName"
	ociDNSKeyComp    = "compartmentId"
	ociDNSKeySecret  = "secret"
	ociDNSKeyPort    = "port"
	ociDNSKeyType    = "type"
	ociDNSKeyEnabled = "enabled"
	ociDNSOpAdd      = "ADD"
	ociDNSOpRemove   = "REMOVE"
	ociDNSScopeGlob  = "GLOBAL"
	ociDNSScopePriv  = "PRIVATE"
	ociDNSLoopback   = "127.0.0.1"
)

var (
	ociDNSIDRe     = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	ociDNSDomainRe = regexp.MustCompile(`^[A-Za-z0-9_*.-]{1,255}$`)
	ociDNSLabelRe  = regexp.MustCompile(`^[A-Za-z0-9_*]([A-Za-z0-9_-]*[A-Za-z0-9_])?$`)
)

// ociDNSQueryKeys are the only query parameters each listing forwards; everything else is dropped.
//
//nolint:gochecknoglobals // immutable allow-list
var ociDNSQueryKeys = map[string][]string{
	ociDNSZones: {
		ociDNSKeyComp, ociDNSKeyScope, ociDNSKeyViewID, ociDNSKeyName, "nameContains", "zoneType", ociDNSKeyState,
		ociDNSKeyLimit, ociDNSKeyPage, ociDNSKeySortBy, ociDNSKeySortOrd,
	},
	ociDNSRecords: {
		ociDNSKeyScope, ociDNSKeyViewID, ociDNSKeyDomain, "domainContains", ociDNSKeyRtype, "zoneVersion",
		ociDNSKeyLimit, ociDNSKeyPage, ociDNSKeySortBy, ociDNSKeySortOrd,
	},
	ociDNSViews:     {ociDNSKeyComp, ociDNSKeyDisplay, ociDNSKeyLimit, ociDNSKeyPage},
	ociDNSResolvers: {ociDNSKeyComp, ociDNSKeyDisplay, ociDNSKeyLimit, ociDNSKeyPage},
	ociDNSEndpoints: {ociDNSKeyLimit, ociDNSKeyPage},
	ociDNSTsigKeys:  {ociDNSKeyComp, ociDNSKeyName, ociDNSKeyLimit, ociDNSKeyPage},
	ociDNSPolicies:  {ociDNSKeyComp, ociDNSKeyDisplay, ociDNSKeyLimit, ociDNSKeyPage},
	ociDNSZoneItem:  {ociDNSKeyScope, ociDNSKeyViewID},
}

// ociDNSQueryTypes are the record types the Try it panel may ask for.
//
//nolint:gochecknoglobals // immutable allow-list
var ociDNSQueryTypes = []string{"A", "AAAA", "CNAME", "MX", "NS", "TXT", "SOA", "SRV", "PTR", "CAA"}

type ociDNSCall struct {
	body func(c *echo.Context) ([]byte, bool)
	path string
	coll string
	cond bool
}

// registerOCIDNSRoutes serves the OCI DNS page by dispatching in-process to the emulator's control plane without
// request signing. Only the zone, record, view, resolver, TSIG key and steering policy operations the page uses are
// reachable, each write rebuilt from fixed fields; TSIG secrets are removed from every answer, and the Try it query
// goes to the emulator's own DNS data plane on the loopback address only.
func (h *DashboardHandler) registerOCIDNSRoutes() {
	var inner *echo.Echo

	ops := h.config.OCIDNSOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(inner != nil, "OCI DNS", next)
	}

	h.SubRouter.Add(http.MethodGet, ociDNSPrefix+"/compartments", guard(func(c *echo.Context) error {
		comps := ops.Backend.Compartments()
		if !slices.Contains(comps, idbackend.TenancyID) {
			comps = slices.Insert(comps, 0, idbackend.TenancyID)
		}

		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps})
	}))

	h.SubRouter.Add(http.MethodGet, ociDNSPrefix+"/dataplane", guard(func(c *echo.Context) error {
		port := ops.DataPlanePort()

		return c.JSON(http.StatusOK, map[string]any{ociDNSKeyEnabled: port > 0, ociDNSKeyPort: port})
	}))

	h.SubRouter.Add(http.MethodPost, ociDNSPrefix+"/query", guard(func(c *echo.Context) error {
		return ociDNSTryIt(c, ops.DataPlanePort())
	}))

	serve := guard(func(c *echo.Context) error { return ociDNSServe(c, inner) })

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete} {
		h.SubRouter.Add(m, ociDNSPrefix+"/api/*", serve)
	}
}

// ociDNSServe forwards one allow-listed control-plane call.
func ociDNSServe(c *echo.Context, inner *echo.Echo) error {
	r := c.Request()

	parts, ok := ociSplitPath(strings.TrimPrefix(r.URL.EscapedPath(), ociDNSPrefix+"/api"))
	if !ok {
		return ociInvalid(c, "unsupported OCI DNS proxy request")
	}

	call, ok := ociDNSResolve(r.Method, parts)
	if !ok {
		return ociInvalid(c, "unsupported OCI DNS proxy request")
	}

	var body []byte

	if call.body != nil {
		if body, ok = call.body(c); !ok {
			return ociInvalid(c, "invalid OCI DNS request")
		}
	}

	ifMatch := ""
	if call.cond {
		ifMatch = r.Header.Get("If-Match")
		if len(ifMatch) > ociDNSMaxName {
			return ociInvalid(c, "invalid If-Match header")
		}
	}

	query := ociArtQuery(r.URL.Query(), ociDNSQueryKeys[call.coll])
	r.Header = http.Header{}

	if ifMatch != "" {
		r.Header.Set("If-Match", ifMatch)
	}

	r.URL.RawQuery = query

	rec := ociArtForward(c, inner, call.path, body)
	if rec.Code < http.StatusMultipleChoices {
		rec.Body = bytes.NewBuffer(ociDNSScrub(rec.Body.Bytes()))
	}

	return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id", "opc-total-items")
}

// ociDNSResolve maps a proxy request to its emulator call; false means the request is outside the allow-list.
func ociDNSResolve(method string, parts []string) (ociDNSCall, bool) {
	if len(parts) > ociDNSSubSegs || (len(parts) >= ociDNSItemSegs && !ociDNSIDOK(parts[1])) {
		return ociDNSCall{}, false
	}

	var (
		call ociDNSCall
		ok   bool
	)

	switch parts[0] {
	case ociDNSZones:
		call, ok = ociDNSZoneCall(method, parts)
	case ociDNSViews, ociDNSTsigKeys, ociDNSPolicies:
		call, ok = ociDNSCall{path: "/" + parts[0]}, method == http.MethodGet && len(parts) == 1
	case ociDNSResolvers:
		call, ok = ociDNSResolverCall(method, parts)
	}

	if !ok {
		return ociDNSCall{}, false
	}

	if call.coll == "" {
		call.coll = parts[0]
	}

	call.path = ociDNSAPIPrefix + call.path

	return call, true
}

func ociDNSZoneCall(method string, parts []string) (ociDNSCall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociDNSCall{path: "/zones"}, true
	case len(parts) == 1 && method == http.MethodPost:
		return ociDNSCall{path: "/zones", body: ociDNSZoneBody}, true
	case len(parts) == ociDNSItemSegs && method == http.MethodGet:
		return ociDNSCall{path: "/zones/" + parts[1], coll: ociDNSZoneItem}, true
	case len(parts) == ociDNSItemSegs && method == http.MethodDelete:
		return ociDNSCall{path: "/zones/" + parts[1], coll: ociDNSZoneItem, cond: true}, true
	case len(parts) == ociDNSSubSegs && parts[2] == ociDNSRecords && method == http.MethodGet:
		return ociDNSCall{path: "/zones/" + parts[1] + "/records", coll: ociDNSRecords}, true
	case len(parts) == ociDNSSubSegs && parts[2] == ociDNSRecords && method == http.MethodPatch:
		return ociDNSCall{
			path: "/zones/" + parts[1] + "/records", coll: ociDNSRecords, cond: true, body: ociDNSRecordsBody,
		}, true
	}

	return ociDNSCall{}, false
}

func ociDNSResolverCall(method string, parts []string) (ociDNSCall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociDNSCall{path: "/resolvers"}, true
	case len(parts) == ociDNSSubSegs && parts[2] == ociDNSEndpoints && method == http.MethodGet:
		return ociDNSCall{path: "/resolvers/" + parts[1] + "/endpoints", coll: ociDNSEndpoints}, true
	}

	return ociDNSCall{}, false
}

func ociDNSIDOK(seg string) bool { return ociDNSIDRe.MatchString(unescapedPath(seg)) }

func ociDNSName(s string) bool { return s != "" && len(s) <= ociDNSMaxName }

func ociDNSZoneBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		Name        string `json:"name"`
		Compartment string `json:"compartmentId"`
		ZoneType    string `json:"zoneType"`
		Scope       string `json:"scope"`
		ViewID      string `json:"viewId"`
	}

	if !ociDecodeBodyN(c, &in, ociDNSMaxBody) {
		return nil, false
	}

	scope := in.Scope
	if scope == "" {
		scope = ociDNSScopeGlob
	}

	if !ociDNSName(in.Name) || !ociDNSName(in.Compartment) || (in.ZoneType != "" && in.ZoneType != "PRIMARY") ||
		(scope != ociDNSScopeGlob && scope != ociDNSScopePriv) ||
		(scope == ociDNSScopePriv) != (in.ViewID != "") || len(in.ViewID) > ociDNSMaxName {
		return nil, false
	}

	out := map[string]any{
		ociDNSKeyName: in.Name, ociDNSKeyComp: in.Compartment, "zoneType": "PRIMARY", ociDNSKeyScope: scope,
	}

	if in.ViewID != "" {
		out[ociDNSKeyViewID] = in.ViewID
	}

	return ociArtMarshal(out)
}

type ociDNSRecordIn struct {
	TTL       *int   `json:"ttl"`
	Operation string `json:"operation"`
	Domain    string `json:"domain"`
	Rtype     string `json:"rtype"`
	Rdata     string `json:"rdata"`
}

func (in ociDNSRecordIn) build() (map[string]any, bool) {
	if (in.Operation != ociDNSOpAdd && in.Operation != ociDNSOpRemove) || !ociDNSDomainRe.MatchString(in.Domain) ||
		!slices.Contains(ociDNSQueryTypes, in.Rtype) || in.Rdata == "" || len(in.Rdata) > ociDNSMaxRdata ||
		strings.ContainsFunc(in.Rdata, func(r rune) bool { return r < ' ' }) {
		return nil, false
	}

	out := map[string]any{
		"operation": in.Operation, ociDNSKeyDomain: in.Domain, ociDNSKeyRtype: in.Rtype, "rdata": in.Rdata,
	}

	if in.TTL != nil {
		if *in.TTL < 0 || *in.TTL > ociDNSMaxTTL {
			return nil, false
		}

		out["ttl"] = *in.TTL
	} else if in.Operation == ociDNSOpAdd {
		return nil, false
	}

	return out, true
}

func ociDNSRecordsBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		Items []ociDNSRecordIn `json:"items"`
	}

	if !ociDecodeBodyN(c, &in, ociDNSMaxBody) || len(in.Items) == 0 || len(in.Items) > ociDNSMaxItems {
		return nil, false
	}

	items := make([]map[string]any, 0, len(in.Items))

	for _, it := range in.Items {
		built, ok := it.build()
		if !ok {
			return nil, false
		}

		items = append(items, built)
	}

	return ociArtMarshal(map[string]any{"items": items})
}

// ociDNSScrub removes TSIG secrets from any answer.
func ociDNSScrub(raw []byte) []byte {
	if len(bytes.TrimSpace(raw)) == 0 {
		return raw
	}

	var doc any
	if json.Unmarshal(raw, &doc, jsonx.V1()) != nil {
		return []byte("{}")
	}

	ociDNSStrip(doc)

	out, err := json.Marshal(doc, jsonx.V1())
	if err != nil {
		return []byte("{}")
	}

	return out
}

func ociDNSStrip(v any) {
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			ociDNSStrip(x)
		}
	case map[string]any:
		delete(t, ociDNSKeySecret)

		for _, x := range t {
			ociDNSStrip(x)
		}
	}
}

// ociDNSQName validates a query name: a dotted host name of at most 253 characters with valid labels.
func ociDNSQName(name string) (string, bool) {
	n := strings.TrimSuffix(name, ".")
	if n == "" || len(n) > ociDNSMaxFQDN {
		return "", false
	}

	for label := range strings.SplitSeq(n, ".") {
		if len(label) > ociDNSMaxLabel || !ociDNSLabelRe.MatchString(label) {
			return "", false
		}
	}

	return mdns.Fqdn(n), true
}

type ociDNSRR struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Data string `json:"data"`
	TTL  uint32 `json:"ttl"`
}

func ociDNSRRs(in []mdns.RR) []ociDNSRR {
	out := make([]ociDNSRR, 0, len(in))

	for _, rr := range in {
		hdr := rr.Header()
		out = append(out, ociDNSRR{
			Name: hdr.Name, Type: mdns.TypeToString[hdr.Rrtype], TTL: hdr.Ttl,
			Data: strings.TrimSpace(strings.TrimPrefix(rr.String(), hdr.String())),
		})
	}

	return out
}

// ociDNSTryIt runs one query against the emulator's DNS data plane. The address is always the loopback host with
// the configured data plane port; nothing in the request selects where the query goes.
func ociDNSTryIt(c *echo.Context, port int) error {
	if port <= 0 {
		return cloudJSONFail(c, http.StatusServiceUnavailable, "DataPlaneDisabled",
			"the OCI DNS data plane is not running (OCI_DNS_PORT_DNS is 0)")
	}

	var in struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	if !ociDecodeBodyN(c, &in, ociDNSMaxBody) {
		return ociInvalid(c, "invalid query request")
	}

	fqdn, ok := ociDNSQName(in.Name)
	if !ok || !slices.Contains(ociDNSQueryTypes, in.Type) {
		return ociInvalid(c, "query name or type is not valid")
	}

	msg := new(mdns.Msg)
	msg.SetQuestion(fqdn, mdns.StringToType[in.Type])

	ctx, cancel := context.WithTimeout(c.Request().Context(), ociDNSQueryWait)
	defer cancel()

	addr := net.JoinHostPort(ociDNSLoopback, strconv.Itoa(port))

	resp, _, err := (&mdns.Client{Net: "udp", Timeout: ociDNSQueryWait}).ExchangeContext(ctx, msg, addr)
	if err == nil && resp.Truncated {
		resp, _, err = (&mdns.Client{Net: "tcp", Timeout: ociDNSQueryWait}).ExchangeContext(ctx, msg, addr)
	}

	if err != nil {
		return cloudJSONFail(c, http.StatusBadGateway, "QueryFailed", "the DNS data plane did not answer")
	}

	return c.JSON(http.StatusOK, map[string]any{
		"rcode": mdns.RcodeToString[resp.Rcode], "authoritative": resp.Authoritative,
		"answer": ociDNSRRs(resp.Answer), "authority": ociDNSRRs(resp.Ns), "additional": ociDNSRRs(resp.Extra),
		ociDNSKeyName: fqdn, ociDNSKeyType: in.Type, "server": addr,
	})
}
