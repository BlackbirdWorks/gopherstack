package azurearm

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	netWAFs        = "ApplicationGatewayWebApplicationFirewallPolicies"
	wafMinBodyKb   = 8
	wafMaxBodyKb   = 2000
	wafMaxUploadMb = 4000
	wafMaxRatePri  = 100
	wafStateOn     = netEnabled
	wafRuleInvalid = "Invalid"
	agwActionStart = "start"
	agwActionStop  = "stop"
	wafPropRules   = "customRules"
	wafPropManaged = "managedRules"
	wafPropPS      = "policySettings"
	probeMaxWait   = 10 * time.Second
	probeBodyMax   = 1 << 16
	healthUp       = "Up"
	healthDown     = "Down"
	healthUnknown  = "Unknown"
	defaultStatus  = "200-399"
	statusRangeLen = 2
)

// --- start / stop ---

// AGWAction runs start or stop against an application gateway.
func (p *NetworkProvider) AGWAction(id ResourceID, action string) error {
	p.mu.Lock("AGWAction")
	defer p.mu.Unlock()

	gw, ok := p.top(netAGWs, id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrResourceNotFound
	}

	switch strings.ToLower(action) {
	case agwActionStart:
		gw.Obj.Aux[agwStateKey] = agwRunning
	case agwActionStop:
		gw.Obj.Aux[agwStateKey] = agwStopped
	default:
		return ErrResourceNotFound
	}

	p.touch(gw)

	return nil
}

// --- backend health ---

type agwServer struct {
	address, ipConfig string
	probe             agwProbeSpec
}

type agwProbeSpec struct {
	scheme, host, path, body string
	statuses                 string
	port                     int
	timeout                  time.Duration
}

type agwHealthGroup struct {
	poolID, settingsID string
	servers            []agwServer
}

// BackendHealth probes loopback backends over real HTTP; every other backend is reported Unknown because the
// emulator has no route to it.
func (p *NetworkProvider) BackendHealth(ctx context.Context, id ResourceID) (map[string]any, error) {
	groups, err := p.healthGroups(id)
	if err != nil {
		return nil, err
	}

	pools := []any{}
	index := map[string]int{}

	for _, g := range groups {
		servers := make([]any, 0, len(g.servers))
		for _, s := range g.servers {
			servers = append(servers, healthOf(ctx, s))
		}

		entry := map[string]any{"backendHttpSettings": idRef(g.settingsID), "servers": servers}

		i, seen := index[strings.ToLower(g.poolID)]
		if !seen {
			index[strings.ToLower(g.poolID)] = len(pools)
			pools = append(pools, map[string]any{
				"backendAddressPool":            idRef(g.poolID),
				"backendHttpSettingsCollection": []any{entry},
			})

			continue
		}

		pm, _ := pools[i].(map[string]any)
		pm["backendHttpSettingsCollection"] = append(anyList(pm["backendHttpSettingsCollection"]), entry)
	}

	return map[string]any{agwPools: pools}, nil
}

func (p *NetworkProvider) healthGroups(id ResourceID) ([]agwHealthGroup, error) {
	p.mu.RLock("BackendHealth")
	defer p.mu.RUnlock()

	gw, ok := p.top(netAGWs, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	if gw.Obj.Aux[agwStateKey] == agwStopped {
		return nil, netErr(http.StatusBadRequest, "ApplicationGatewayNotRunning",
			"Application gateway %s is stopped; backend health is available only while it is running.", gw.armID())
	}

	var (
		out  []agwHealthGroup
		seen = map[string]bool{}
	)

	add := func(pool, set string) {
		key := strings.ToLower(pool + "|" + set)
		if pool == "" || set == "" || seen[key] {
			return
		}

		seen[key] = true

		out = append(out, p.healthGroup(gw, pool, set))
	}

	for _, r := range gw.Kids[agwRules] {
		add(refIDOf(r.Props[agwPropPool]), refIDOf(r.Props[agwPropHTTPS]))

		if pm := findObj(gw.Kids[agwPathMaps], agwLeaf(refIDOf(r.Props["urlPathMap"]))); pm != nil {
			add(refIDOf(pm.Props["defaultBackendAddressPool"]), refIDOf(pm.Props["defaultBackendHttpSettings"]))

			for _, e := range anyList(pm.Props[agwPropPathRs]) {
				em, _ := e.(map[string]any)
				rp := propsOf(em)
				add(refIDOf(rp[agwPropPool]), refIDOf(rp[agwPropHTTPS]))
			}
		}
	}

	return out, nil
}

func agwLeaf(id string) string {
	_, leaf, found := strings.CutLast(id, "/")
	if !found {
		return id
	}

	return leaf
}

func (p *NetworkProvider) healthGroup(gw *netParent, poolID, setID string) agwHealthGroup {
	g := agwHealthGroup{poolID: poolID, settingsID: setID}
	pool := findObj(gw.Kids[agwPools], agwLeaf(poolID))
	set := findObj(gw.Kids[agwSettings], agwLeaf(setID))

	if pool == nil || set == nil {
		return g
	}

	spec := probeSpecFor(gw, set)

	for _, a := range anyList(pool.Props["backendAddresses"]) {
		m, _ := a.(map[string]any)
		s := agwServer{address: firstNonEmpty(strOf(m, propIPAddress), strOf(m, propFQDN)), probe: spec}
		g.servers = append(g.servers, s)
	}

	for _, c := range p.allIPConfigs() {
		if hasRef(c.props, propNICAGWPools, poolID) {
			g.servers = append(g.servers, agwServer{
				address: strOf(c.props, propPrivateIP), ipConfig: c.id, probe: spec,
			})
		}
	}

	return g
}

func probeSpecFor(gw *netParent, set *netObj) agwProbeSpec {
	spec := agwProbeSpec{
		scheme: strings.ToLower(strOf(set.Props, propProtocol)), path: "/", statuses: defaultStatus,
		port: intOf(set.Props["port"]), timeout: probeMaxWait,
	}

	pr := findObj(gw.Kids[agwProbes], agwLeaf(refIDOf(set.Props["probe"])))
	if pr == nil {
		return spec
	}

	spec.scheme, spec.path = strings.ToLower(strOf(pr.Props, propProtocol)), strOf(pr.Props, "path")
	spec.host = strOf(pr.Props, "host")

	if pt := intOf(pr.Props["port"]); pt > 0 {
		spec.port = pt
	}

	if t := intOf(pr.Props["timeout"]); t > 0 {
		spec.timeout = min(time.Duration(t)*time.Second, probeMaxWait)
	}

	if m, ok := pr.Props["match"].(map[string]any); ok {
		if codes := strList(m["statusCodes"]); len(codes) > 0 {
			spec.statuses = strings.Join(codes, ",")
		}

		spec.body = strOf(m, "body")
	}

	return spec
}

func healthOf(ctx context.Context, s agwServer) map[string]any {
	out := map[string]any{"address": s.address}

	if s.ipConfig != "" {
		out["ipConfiguration"] = idRef(s.ipConfig)
	}

	ip, err := netip.ParseAddr(s.address)
	if err != nil || !ip.IsLoopback() {
		out["health"] = healthUnknown
		out["healthProbeLog"] = "Backend " + s.address + " is not a loopback address; " +
			"the emulator has no data plane route to it."

		return out
	}

	status, body, perr := runAGWProbe(ctx, s)

	switch {
	case perr != nil:
		out["health"], out["healthProbeLog"] = healthDown, "Cannot connect to backend server: "+perr.Error()
	case !statusMatches(s.probe.statuses, status):
		out["health"] = healthDown
		out["healthProbeLog"] = fmt.Sprintf("Received invalid status code: %d in the backend server's HTTP response. "+
			"Expected: %s.", status, s.probe.statuses)
	case s.probe.body != "" && !strings.Contains(body, s.probe.body):
		out["health"] = healthDown
		out["healthProbeLog"] = "Body of the backend server's HTTP response did not match the probe body."
	default:
		out["health"] = healthUp
		out["healthProbeLog"] = "Success. Received 200 status code"
	}

	return out
}

func runAGWProbe(ctx context.Context, s agwServer) (int, string, error) {
	host := s.address
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}

	target := s.probe.scheme + "://" + host + ":" + strconv.Itoa(s.probe.port) + s.probe.path

	tr := httpclient.NewTransport()
	tr.TLSClientConfig = aciProbeTLSConfig(target)

	client := &http.Client{Transport: tr, Timeout: s.probe.timeout}
	defer tr.CloseIdleConnections()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
	if err != nil {
		return 0, "", fmt.Errorf("build probe request: %w", err)
	}

	if s.probe.host != "" {
		req.Host = s.probe.host
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("probe request: %w", err)
	}

	defer resp.Body.Close()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, probeBodyMax))

	return resp.StatusCode, string(raw), nil
}

func statusMatches(spec string, status int) bool {
	for part := range strings.SplitSeq(spec, ",") {
		part = strings.TrimSpace(part)

		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil {
			continue
		}

		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil {
				continue
			}
		}

		if status >= a && status <= b {
			return true
		}
	}

	return false
}

// --- WAF policies ---

func (p *NetworkProvider) putWAF(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)

	ps, err := normWAFSettings(props)
	if err != nil {
		return nil, false, err
	}

	rules, err := normWAFRules(anyList(props[wafPropRules]))
	if err != nil {
		return nil, false, err
	}

	managed, err := normWAFManaged(props[wafPropManaged])
	if err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netWAFs, id, body)
	obj.Props = map[string]any{wafPropPS: ps, wafPropManaged: managed}

	if len(rules) > 0 {
		obj.Props[wafPropRules] = rules
	}

	return p.bodyWAF(id, it), created, nil
}

func normWAFSettings(props map[string]any) (map[string]any, error) {
	in, _ := props[wafPropPS].(map[string]any)
	out := map[string]any{}

	maps.Copy(out, in)

	state, ok := canonEnum(firstNonEmpty(strOf(in, "state"), wafStateOn), []string{wafStateOn, netDisabled})
	if !ok {
		return nil, netBad("policySettings.state must be Enabled or Disabled.")
	}

	mode, ok := canonEnum(firstNonEmpty(strOf(in, "mode"), "Detection"), []string{"Detection", "Prevention"})
	if !ok {
		return nil, netBad("policySettings.mode must be Detection or Prevention.")
	}

	out["state"], out["mode"] = state, mode

	if _, has := out["requestBodyCheck"]; !has {
		out["requestBodyCheck"] = true
	}

	for _, spec := range []struct {
		key    string
		lo, hi int
	}{{"maxRequestBodySizeInKb", wafMinBodyKb, wafMaxBodyKb}, {"fileUploadLimitInMb", 1, wafMaxUploadMb}} {
		if _, has := out[spec.key]; has {
			if _, err := intIn(out, spec.key, spec.lo, spec.hi, -1); err != nil {
				return nil, err
			}
		}
	}

	return out, nil
}

func normWAFRules(raw []any) ([]any, error) {
	out := make([]any, 0, len(raw))
	names, prios := map[string]bool{}, map[int]bool{}

	for _, e := range raw {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if name != "" && names[strings.ToLower(name)] {
			return nil, netBad("Custom rule name %s is used more than once.", name)
		}

		names[strings.ToLower(name)] = true

		pr, err := intIn(m, netPropPriority, 1, maxCount, -1)
		if err != nil {
			return nil, err
		}

		if prios[pr] {
			return nil, netBad("Custom rule priority %d is used more than once.", pr)
		}

		prios[pr] = true

		rule := map[string]any{}

		maps.Copy(rule, m)
		delete(rule, netFieldEtag)

		if err = checkWAFRule(rule); err != nil {
			return nil, err
		}

		out = append(out, rule)
	}

	return out, nil
}

func checkWAFRule(rule map[string]any) error {
	act, ok := canonEnum(strOf(rule, "action"), []string{ruleAllow, "Block", "Log", "JSChallenge"})
	if !ok {
		return netBad("Custom rule action must be Allow, Block, Log or JSChallenge.")
	}

	rt, ok := canonEnum(strOf(rule, "ruleType"), []string{"MatchRule", "RateLimitRule", wafRuleInvalid})
	if !ok {
		return netBad("Custom rule ruleType must be MatchRule, RateLimitRule or Invalid.")
	}

	rule["action"], rule["ruleType"] = act, rt

	conds := anyList(rule["matchConditions"])
	if len(conds) == 0 {
		return netBad("Custom rule requires at least one match condition.")
	}

	if rt == "RateLimitRule" {
		if _, err := intIn(rule, "rateLimitThreshold", 1, maxCount, -1); err != nil {
			return err
		}
	}

	for _, c := range conds {
		cm, _ := c.(map[string]any)
		if err := checkWAFCondition(cm); err != nil {
			return err
		}
	}

	return nil
}

func checkWAFCondition(cm map[string]any) error {
	if _, ok := canonEnum(strOf(cm, "operator"), wafOperators); !ok {
		return netBad("Match condition operator %s is invalid.", strOf(cm, "operator"))
	}

	if len(anyList(cm["matchVariables"])) == 0 || len(anyList(cm["matchValues"])) == 0 {
		return netBad("Match condition requires matchVariables and matchValues.")
	}

	for _, v := range anyList(cm["matchVariables"]) {
		vm, _ := v.(map[string]any)
		if _, ok := canonEnum(strOf(vm, "variableName"), wafVariables); !ok {
			return netBad("Match variable %s is invalid.", strOf(vm, "variableName"))
		}
	}

	for _, t := range strList(cm["transforms"]) {
		if _, ok := canonEnum(t, wafTransforms); !ok {
			return netBad("Transform %s is invalid.", t)
		}
	}

	return nil
}

//nolint:gochecknoglobals // read-only tables
var (
	wafOperators = []string{
		fwAny, "BeginsWith", "Contains", "EndsWith", "Equal", "GeoMatch", "GreaterThan", "GreaterThanOrEqual",
		"IPMatch", "LessThan", "LessThanOrEqual", "Regex",
	}
	wafVariables = []string{
		"PostArgs", "QueryString", "RemoteAddr", "RequestBody", "RequestCookies", "RequestHeaders", "RequestMethod",
		"RequestUri",
	}
	wafTransforms = []string{
		"HtmlEntityDecode",
		"Lowercase",
		"RemoveNulls",
		"Trim",
		"UrlDecode",
		"UrlEncode",
		"Uppercase",
	}
)

func normWAFManaged(raw any) (map[string]any, error) {
	m, _ := raw.(map[string]any)
	sets := anyList(m["managedRuleSets"])

	if len(sets) == 0 {
		return nil, netBad("managedRules.managedRuleSets requires at least one rule set.")
	}

	out := map[string]any{}

	maps.Copy(out, m)

	list := make([]any, 0, len(sets))
	seen := map[string]bool{}

	for _, s := range sets {
		sm, _ := s.(map[string]any)
		typ, ver := strOf(sm, "ruleSetType"), strOf(sm, "ruleSetVersion")

		if typ == "" || ver == "" {
			return nil, netBad("Managed rule set requires ruleSetType and ruleSetVersion.")
		}

		if seen[strings.ToLower(typ)] {
			return nil, netBad("Managed rule set %s is listed more than once.", typ)
		}

		seen[strings.ToLower(typ)] = true

		cp := map[string]any{}

		maps.Copy(cp, sm)
		delete(cp, "computedDisabledRules")

		list = append(list, cp)
	}

	out["managedRuleSets"] = list

	return out, nil
}

func (p *NetworkProvider) wafUsers(polID string) ([]string, []string, []string) {
	var gws, listeners, pathRules []string

	for _, gw := range p.ofType(netAGWs) {
		hit := strings.EqualFold(subID(gw.Obj.Props, agwPropPolicy), polID)

		for _, l := range gw.Kids[agwListeners] {
			if strings.EqualFold(subID(l.Props, agwPropPolicy), polID) {
				hit = true

				listeners = append(listeners, gw.armID()+"/"+agwListeners+"/"+l.Name)
			}
		}

		for _, pm := range gw.Kids[agwPathMaps] {
			for _, e := range anyList(pm.Props[agwPropPathRs]) {
				m, _ := e.(map[string]any)
				if strings.EqualFold(subID(propsOf(m), agwPropPolicy), polID) {
					hit = true

					pathRules = append(
						pathRules,
						gw.armID()+"/"+agwPathMaps+"/"+pm.Name+"/"+agwPropPathRs+"/"+strOf(m, fieldName),
					)
				}
			}
		}

		if hit {
			gws = append(gws, gw.armID())
		}
	}

	return gws, listeners, pathRules
}

func (p *NetworkProvider) bodyWAF(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	gws, ls, prs := p.wafUsers(it.armID())
	props["applicationGateways"] = idRefs(gws)
	props[agwListeners] = idRefs(ls)
	props["pathBasedRules"] = idRefs(prs)
	props[fieldProvisioningState] = netStatusSucceeded
	props["resourceState"] = netEnabled

	return p.envelope(id, netWAFs, &it.Obj, props)
}
