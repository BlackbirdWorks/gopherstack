package azurearm

import (
	"encoding/base64"
	"maps"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

const (
	netAGWs        = "applicationGateways"
	agwTypePrefix  = "Microsoft.Network/applicationGateways/"
	agwGatewayIPs  = "gatewayIPConfigurations"
	agwFrontends   = "frontendIPConfigurations"
	agwPorts       = "frontendPorts"
	agwPools       = "backendAddressPools"
	agwSettings    = "backendHttpSettingsCollection"
	agwListeners   = "httpListeners"
	agwRules       = "requestRoutingRules"
	agwProbes      = "probes"
	agwCerts       = "sslCertificates"
	agwPathMaps    = "urlPathMaps"
	agwRedirects   = "redirectConfigurations"
	agwRewrites    = "rewriteRuleSets"
	agwAuthCerts   = "authenticationCertificates"
	agwRootCerts   = "trustedRootCertificates"
	agwClientCerts = "trustedClientCertificates"
	agwSSLProfiles = "sslProfiles"

	agwSKUStdV2   = "Standard_v2"
	agwSKUWAFV2   = "WAF_v2"
	agwRuleBasic  = "Basic"
	agwRulePath   = "PathBasedRouting"
	agwMaxPrio    = 20000
	agwMaxPort    = 65535
	agwMaxHosts   = 5
	agwMaxTimeout = 86400
	agwMinCap     = 1
	agwMaxCap     = 125
	agwMaxMin     = 100
	agwMinMax     = 2
	agwStateKey   = "opstate"
	agwRunning    = "Running"
	agwStopped    = "Stopped"
	agwProtoHTTP  = "Http"
	agwProtoHTTPS = "Https"
	agwPropPolicy = "firewallPolicy"
	agwPropSKU    = "sku"
	agwPropPort   = "port"
	agwPropData   = "data"
	agwPropKV     = "keyVaultSecretId"
	agwPropPub    = "publicCertData"
	agwPropAuto   = "autoscaleConfiguration"
	agwPropWAFCfg = "webApplicationFirewallConfiguration"
	agwPropPathRs = "pathRules"
	agwPropHTTPS  = "backendHttpSettings"
	agwPropRedir  = "redirectConfiguration"
	agwPropRewr   = "rewriteRuleSet"
	agwPropPool   = "backendAddressPool"
)

// agwKidTypes are the gateway child collections in body order.
//
//nolint:gochecknoglobals // read-only table
var agwKidTypes = []string{
	agwGatewayIPs, agwFrontends, agwPorts, agwPools, agwSettings, agwListeners, agwRules, agwProbes, agwCerts,
	agwPathMaps, agwRedirects, agwRewrites, agwAuthCerts, agwRootCerts, agwClientCerts, agwSSLProfiles,
}

// agwUnsupported lists collections the emulator rejects instead of storing without behaviour.
//
//nolint:gochecknoglobals // read-only table
var agwUnsupported = []string{
	"backendSettingsCollection", "listeners", "routingRules", "loadDistributionPolicies",
	"entraJWTValidationConfigs", "privateLinkConfigurations",
}

type agwRefSpec struct{ owner, key, target string }

// agwRefSpecs are the single references between children of one gateway; every one must name an existing child.
//
//nolint:gochecknoglobals // read-only table
var agwRefSpecs = []agwRefSpec{
	{agwListeners, "frontendIPConfiguration", agwFrontends},
	{agwListeners, "frontendPort", agwPorts},
	{agwListeners, "sslCertificate", agwCerts},
	{agwListeners, "sslProfile", agwSSLProfiles},
	{agwSettings, "probe", agwProbes},
	{agwRules, "httpListener", agwListeners},
	{agwRules, agwPropPool, agwPools},
	{agwRules, agwPropHTTPS, agwSettings},
	{agwRules, agwPropRedir, agwRedirects},
	{agwRules, "urlPathMap", agwPathMaps},
	{agwRules, agwPropRewr, agwRewrites},
	{agwPathMaps, "defaultBackendAddressPool", agwPools},
	{agwPathMaps, "defaultBackendHttpSettings", agwSettings},
	{agwPathMaps, "defaultRedirectConfiguration", agwRedirects},
	{agwPathMaps, "defaultRewriteRuleSet", agwRewrites},
	{agwRedirects, "targetListener", agwListeners},
}

// agwListRefSpecs are the list-valued references.
//
//nolint:gochecknoglobals // read-only table
var agwListRefSpecs = []agwRefSpec{
	{agwSettings, agwRootCerts, agwRootCerts},
	{agwSettings, agwAuthCerts, agwAuthCerts},
	{agwSSLProfiles, agwClientCerts, agwClientCerts},
}

var agwFQDNRe = regexp.MustCompile(
	`^([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`,
)

type agwCtx struct {
	taken              map[string]map[string]string
	pips               map[string]string
	id, loc, sku, tier string
	gwSubnet           string
	hasAutoscale       bool
	v2                 bool
}

func (c *agwCtx) kidID(kt, name string) string { return c.id + "/" + kt + "/" + name }

func (c *agwCtx) childName(ref, kt string) string {
	prefix := strings.ToLower(c.id + "/" + kt + "/")
	if !strings.HasPrefix(strings.ToLower(ref), prefix) {
		return ""
	}

	return ref[len(prefix):]
}

func agwNotFoundRef(c *agwCtx, owner, ref, kt string) error {
	return netErr(http.StatusBadRequest, netCodeRef,
		"Resource %s referenced by %s of application gateway %s was not found. An application gateway can only "+
			"reference its own %s.", ref, owner, c.id, kt)
}

func parseAGWSKU(props map[string]any) (string, string, error) {
	in, _ := props[agwPropSKU].(map[string]any)

	name, ok := canonEnum(strOf(in, fieldName), []string{agwSKUStdV2, agwSKUWAFV2})
	if !ok {
		if v1, isV1 := canonEnum(strOf(in, fieldName), []string{
			"Standard_Small", "Standard_Medium", "Standard_Large", "WAF_Medium", "WAF_Large",
		}); isV1 {
			return "", "", netErr(http.StatusBadRequest, "ApplicationGatewayV1SkuRetired",
				"Application Gateway V1 SKU %s was retired on April 28, 2026; use Standard_v2 or WAF_v2.", v1)
		}

		return "", "", netBad("Application gateway SKU name must be Standard_v2 or WAF_v2.")
	}

	tier := firstNonEmpty(strOf(in, netFieldTier), name)
	if !strings.EqualFold(tier, name) {
		return "", "", netBad("Application gateway SKU tier %s does not match SKU name %s.", tier, name)
	}

	auto, hasAuto := props[agwPropAuto].(map[string]any)
	_, hasCap := in["capacity"]

	switch {
	case hasAuto && hasCap:
		return "", "", netBad("Capacity must not be set when autoscaleConfiguration is specified.")
	case hasAuto:
		lo, err := intIn(auto, "minCapacity", 0, agwMaxMin, -1)
		if err != nil {
			return "", "", err
		}

		hi, err := intIn(auto, "maxCapacity", agwMinMax, agwMaxCap, 0)
		if err != nil {
			return "", "", err
		}

		if hi != 0 && hi < lo {
			return "", "", netBad("maxCapacity %d must not be lower than minCapacity %d.", hi, lo)
		}
	default:
		if _, err := intIn(in, "capacity", agwMinCap, agwMaxCap, -1); err != nil {
			return "", "", err
		}
	}

	return name, name, nil
}

func (p *NetworkProvider) putAGW(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)

	for _, k := range agwUnsupported {
		if len(anyList(props[k])) > 0 {
			return nil, false, netBad("%s is not supported by this emulator.", k)
		}
	}

	name, tier, err := parseAGWSKU(props)
	if err != nil {
		return nil, false, err
	}

	old, existed := p.top(netAGWs, id.ResourceGroup, id.Names[0])
	loc := normLocation(strOf(body, fieldLocation))

	if loc == "" && existed {
		loc = old.Obj.Location
	}

	c := &agwCtx{
		id: id.ARMID(), loc: loc, sku: name, tier: tier, v2: true,
		taken: p.usedAddrsSkip("", id.ARMID()), pips: map[string]string{},
		hasAutoscale: props[agwPropAuto] != nil,
	}

	kids := map[string][]*netObj{}

	for _, kt := range agwKidTypes {
		var prev []*netObj
		if old != nil {
			prev = old.Kids[kt]
		}

		list, berr := p.buildAGWKids(c, kt, props[kt], prev)
		if berr != nil {
			return nil, false, berr
		}

		kids[kt] = list
	}

	if err = p.validateAGW(c, props, kids); err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netAGWs, id, body)
	drop := append([]string{
		agwPropSKU, fieldProvisioningState, netFieldResourceGUID, "operationalState", acaFieldPEC,
		"defaultPredefinedSslPolicy", agwPropPolicy,
	}, agwKidTypes...)
	drop = append(drop, agwUnsupported...)
	obj.Props = takeProps(body, drop...)

	if pol := subID(props, agwPropPolicy); pol != "" {
		obj.Props[agwPropPolicy] = idRef(pol)
	}

	obj.Props[agwPropSKU] = skuOut(props, name, tier)
	obj.Extra = map[string]any{}

	for _, k := range []string{"identity", netFieldZones} {
		if v, has := body[k]; has {
			obj.Extra[k] = v
		}
	}

	if !existed {
		obj.Aux[agwStateKey] = agwRunning
	}

	it.Kids = kids

	return p.bodyAGW(id, it), created, nil
}

func skuOut(props map[string]any, name, tier string) map[string]any {
	out := map[string]any{fieldName: name, netFieldTier: tier}

	if in, ok := props[agwPropSKU].(map[string]any); ok {
		if c, has := in["capacity"]; has {
			out["capacity"] = c
		}
	}

	return out
}

func (p *NetworkProvider) buildAGWKids(c *agwCtx, kt string, raw any, prevList []*netObj) ([]*netObj, error) {
	if raw == nil {
		return nil, nil
	}

	arr, ok := raw.([]any)
	if !ok {
		return nil, netBad("%s must be an array.", kt)
	}

	out := make([]*netObj, 0, len(arr))

	for _, e := range arr {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if err := validNetName(name, netMaxName); err != nil {
			return nil, err
		}

		if hasObj(out, name) {
			return nil, netBad("%s name %s is used more than once.", kt, name)
		}

		prev := findObj(prevList, name)

		props, err := p.normAGWKid(c, kt, name, propsOf(m), prev)
		if err != nil {
			return nil, err
		}

		o := &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: props}
		if prev != nil {
			o.GUID = prev.GUID
		}

		out = append(out, o)
	}

	return out, nil
}

func (p *NetworkProvider) normAGWKid(
	c *agwCtx,
	kt, name string,
	props map[string]any,
	prev *netObj,
) (map[string]any, error) {
	out := takeProps(map[string]any{fieldProperties: props}, fieldProvisioningState, agwPropPub)

	var prevProps map[string]any
	if prev != nil {
		prevProps = prev.Props
	}

	switch kt {
	case agwGatewayIPs:
		return p.normAGWGatewayIP(c, name, out)
	case agwFrontends:
		return p.normAGWFrontend(c, name, out, prevProps)
	case agwPorts:
		_, err := intIn(out, agwPropPort, 1, agwMaxPort, -1)

		return out, err
	case agwPools:
		return normAGWPool(name, out)
	case agwSettings:
		return normAGWSettings(name, out)
	case agwListeners:
		return normAGWListener(name, out)
	case agwRules:
		return normAGWRule(c, name, out)
	case agwProbes:
		return normAGWProbe(name, out)
	case agwCerts:
		return normAGWCert(name, props, prevProps)
	case agwRedirects:
		return normAGWRedirect(name, out)
	case agwRootCerts, agwClientCerts, agwAuthCerts:
		return normAGWTrustCert(name, out)
	case agwRewrites:
		return normAGWRewrite(name, out)
	}

	return out, nil
}

func (p *NetworkProvider) normAGWGatewayIP(c *agwCtx, name string, out map[string]any) (map[string]any, error) {
	sid := subID(out, netFieldSubnet)
	if sid == "" {
		return nil, netBad("Gateway IP configuration %s requires subnet.id.", name)
	}

	vnet, _, found := p.subnetByID(sid)
	if !found || (c.loc != "" && vnet.Obj.Location != c.loc) {
		return nil, refMissing(sid)
	}

	out[netFieldSubnet] = idRef(sid)
	c.gwSubnet = sid

	return out, nil
}

func (p *NetworkProvider) normAGWFrontend(
	c *agwCtx, name string, out, prev map[string]any,
) (map[string]any, error) {
	pipID, sid := subID(out, "publicIPAddress"), subID(out, netFieldSubnet)
	self := c.kidID(agwFrontends, name)

	if (pipID == "") == (sid == "") {
		return nil, netBad(
			"Frontend IP configuration %s must reference exactly one of publicIPAddress or subnet.",
			name,
		)
	}

	delete(out, "privateLinkConfiguration")

	if pipID != "" {
		pip, ok := p.netRef(pipID, netPIPs)
		if !ok || (c.loc != "" && pip.Obj.Location != c.loc) {
			return nil, refMissing(pipID)
		}

		if sk, _ := pip.Obj.Extra[netFieldSKU].(map[string]any); strOf(sk, fieldName) != skuStandard ||
			strOf(pip.Obj.Props, "publicIPAllocationMethod") != methodStatic {
			return nil, netErr(
				http.StatusBadRequest,
				"ApplicationGatewayV2PublicIpNotStandardStatic",
				"Public IP %s must use the Standard SKU with Static allocation to be used by an Application Gateway v2.",
				pipID,
			)
		}

		if user := p.pipHolder(pip.armID()); user != "" && !strings.EqualFold(user, self) {
			return nil, netErr(http.StatusBadRequest, "PublicIPAddressInUse",
				"Public IP address %s is already allocated to resource %s.", pipID, user)
		}

		if other, dup := c.pips[strings.ToLower(pipID)]; dup {
			return nil, netErr(http.StatusBadRequest, "PublicIPAddressInUse",
				"Public IP address %s is already allocated to %s.", pipID, other)
		}

		c.pips[strings.ToLower(pipID)] = self
		out["publicIPAddress"] = idRef(pipID)
		out[netPropIPMethod] = methodDynamic
		delete(out, propPrivateIP)
		delete(out, netFieldSubnet)

		return out, nil
	}

	return p.bindAGWPrivate(c, self, out, prev)
}

func (p *NetworkProvider) bindAGWPrivate(c *agwCtx, self string, out, prev map[string]any) (map[string]any, error) {
	sid := subID(out, netFieldSubnet)

	_, sn, found := p.subnetByID(sid)
	if !found {
		return nil, refMissing(sid)
	}

	method, _, err := parseIPConfigMode(self, out)
	if err != nil {
		return nil, err
	}

	key := strings.ToLower(sid)
	if c.taken[key] == nil {
		c.taken[key] = map[string]string{}
	}

	prevAddr := ""
	if prev != nil && strings.EqualFold(subID(prev, netFieldSubnet), sid) {
		prevAddr = strOf(prev, propPrivateIP)
	}

	addr, err := assignIP(self, method, strOf(out, propPrivateIP), subnetPrefixes(sn), c.taken[key], prevAddr)
	if err != nil {
		return nil, err
	}

	c.taken[key][addr] = self
	out[netFieldSubnet] = idRef(sid)
	out[propPrivateIP], out[netPropIPMethod] = addr, method

	delete(out, netPropIPVersion)

	return out, nil
}

func normAGWPool(name string, out map[string]any) (map[string]any, error) {
	delete(out, "backendIPConfigurations")

	raw, has := out["backendAddresses"]
	if !has {
		return out, nil
	}

	list := make([]any, 0, len(anyList(raw)))

	for _, e := range anyList(raw) {
		m, _ := e.(map[string]any)
		ip, fqdn := strOf(m, propIPAddress), strOf(m, propFQDN)

		switch {
		case (ip == "") == (fqdn == ""):
			return nil, netBad("Backend address of pool %s must set exactly one of ipAddress or fqdn.", name)
		case ip != "":
			a, err := netip.ParseAddr(ip)
			if err != nil {
				return nil, netBad("Backend address %s of pool %s is not a valid IP address.", ip, name)
			}

			list = append(list, map[string]any{propIPAddress: a.String()})
		case !agwFQDNRe.MatchString(fqdn) || len(fqdn) > fqdnMax:
			return nil, netBad("Backend address %s of pool %s is not a valid FQDN.", fqdn, name)
		default:
			list = append(list, map[string]any{propFQDN: fqdn})
		}
	}

	out["backendAddresses"] = list

	return out, nil
}

const fqdnMax = 253

func normAGWSettings(name string, out map[string]any) (map[string]any, error) {
	if _, err := intIn(out, agwPropPort, 1, agwMaxPort, -1); err != nil {
		return nil, err
	}

	proto, ok := canonEnum(strOf(out, propProtocol), []string{agwProtoHTTP, agwProtoHTTPS})
	if !ok {
		return nil, netBad("Backend HTTP settings %s: protocol must be Http or Https.", name)
	}

	out[propProtocol] = proto

	aff, ok := canonEnum(
		firstNonEmpty(strOf(out, "cookieBasedAffinity"), netDisabled),
		[]string{netEnabled, netDisabled},
	)
	if !ok {
		return nil, netBad("Backend HTTP settings %s: cookieBasedAffinity must be Enabled or Disabled.", name)
	}

	out["cookieBasedAffinity"] = aff

	to, err := intIn(out, "requestTimeout", 1, agwMaxTimeout, defaultAGWTimeout)
	if err != nil {
		return nil, err
	}

	out["requestTimeout"] = to

	if _, has := out["pickHostNameFromBackendAddress"]; !has {
		out["pickHostNameFromBackendAddress"] = false
	}

	if p := strOf(out, "path"); p != "" && !strings.HasPrefix(p, "/") {
		return nil, netBad("Backend HTTP settings %s: path must start with '/'.", name)
	}

	if cd, isMap := out["connectionDraining"].(map[string]any); isMap {
		if _, err = intIn(cd, "drainTimeoutInSec", 1, connDrainMax, -1); err != nil {
			return nil, err
		}
	}

	return out, nil
}

const (
	defaultAGWTimeout = 30
	connDrainMax      = 3600
	probeMaxThreshold = 20
)

func normAGWListener(name string, out map[string]any) (map[string]any, error) {
	proto, ok := canonEnum(strOf(out, propProtocol), []string{agwProtoHTTP, agwProtoHTTPS})
	if !ok {
		return nil, netBad("HTTP listener %s: protocol must be Http or Https.", name)
	}

	out[propProtocol] = proto

	hosts := strList(out["hostNames"])
	if strOf(out, "hostName") != "" && len(hosts) > 0 {
		return nil, netBad("HTTP listener %s: hostName and hostNames cannot both be set.", name)
	}

	if len(hosts) > agwMaxHosts {
		return nil, netBad("HTTP listener %s: at most %d host names are allowed.", name, agwMaxHosts)
	}

	if proto == agwProtoHTTPS && subID(out, "sslCertificate") == "" && subID(out, "sslProfile") == "" {
		return nil, netBad("HTTP listener %s uses Https and requires sslCertificate.", name)
	}

	if _, has := out["requireServerNameIndication"]; !has {
		out["requireServerNameIndication"] = false
	}

	return out, nil
}

func normAGWRule(c *agwCtx, name string, out map[string]any) (map[string]any, error) {
	rt, ok := canonEnum(firstNonEmpty(strOf(out, "ruleType"), agwRuleBasic), []string{agwRuleBasic, agwRulePath})
	if !ok {
		return nil, netBad("Routing rule %s: ruleType must be Basic or PathBasedRouting.", name)
	}

	out["ruleType"] = rt

	if c.v2 {
		if _, set := out[netPropPriority]; !set {
			return nil, netErr(http.StatusBadRequest, "ApplicationGatewayRequestRoutingRulePriorityRequired",
				"Request routing rule %s requires a priority on an Application Gateway v2 SKU.", name)
		}

		if _, err := intIn(out, netPropPriority, 1, agwMaxPrio, -1); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func normAGWProbe(name string, out map[string]any) (map[string]any, error) {
	proto, ok := canonEnum(strOf(out, propProtocol), []string{agwProtoHTTP, agwProtoHTTPS})
	if !ok {
		return nil, netBad("Probe %s: protocol must be Http or Https.", name)
	}

	out[propProtocol] = proto

	if !strings.HasPrefix(strOf(out, "path"), "/") {
		return nil, netBad("Probe %s: path is required and must start with '/'.", name)
	}

	pick, _ := out["pickHostNameFromBackendHttpSettings"].(bool)
	if strOf(out, "host") == "" && !pick {
		return nil, netBad("Probe %s requires host or pickHostNameFromBackendHttpSettings.", name)
	}

	for _, spec := range []struct {
		key    string
		lo, hi int
	}{
		{"interval", 1, agwMaxTimeout},
		{"timeout", 1, agwMaxTimeout},
		{"unhealthyThreshold", 1, probeMaxThreshold},
		{agwPropPort, 1, agwMaxPort},
	} {
		if _, has := out[spec.key]; !has {
			continue
		}

		if _, err := intIn(out, spec.key, spec.lo, spec.hi, -1); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func normAGWRedirect(name string, out map[string]any) (map[string]any, error) {
	rt, ok := canonEnum(strOf(out, "redirectType"), []string{"Permanent", "Found", "SeeOther", "Temporary"})
	if !ok {
		return nil, netBad(
			"Redirect configuration %s: redirectType must be Permanent, Found, SeeOther or Temporary.",
			name,
		)
	}

	out["redirectType"] = rt

	if (subID(out, "targetListener") == "") == (strOf(out, "targetUrl") == "") {
		return nil, netBad("Redirect configuration %s must set exactly one of targetListener or targetUrl.", name)
	}

	for _, k := range []string{"requestRoutingRules", agwPathMaps, agwPropPathRs} {
		delete(out, k)
	}

	return out, nil
}

func normAGWTrustCert(name string, out map[string]any) (map[string]any, error) {
	data, kv := strOf(out, agwPropData), strOf(out, agwPropKV)
	if (data == "") == (kv == "") {
		return nil, netBad("Certificate %s must set exactly one of data or keyVaultSecretId.", name)
	}

	if data != "" {
		if _, err := base64.StdEncoding.DecodeString(data); err != nil {
			return nil, netBad("Certificate %s: data is not valid base64.", name)
		}
	}

	return out, nil
}

func normAGWRewrite(name string, out map[string]any) (map[string]any, error) {
	seen := map[string]bool{}

	for _, e := range anyList(out["rewriteRules"]) {
		m, _ := e.(map[string]any)
		rn := strOf(m, fieldName)

		if rn == "" || seen[strings.ToLower(rn)] {
			return nil, netBad("Rewrite rule set %s requires unique, non-empty rule names.", name)
		}

		seen[strings.ToLower(rn)] = true

		if _, has := m["ruleSequence"]; has {
			if _, err := intIn(m, "ruleSequence", 1, agwMaxTimeout, -1); err != nil {
				return nil, err
			}
		}
	}

	return out, nil
}

// normAGWCert accepts a PFX (data, password) or a Key Vault secret id and keeps only the public certificate.
func normAGWCert(name string, in, prev map[string]any) (map[string]any, error) {
	data, kv := strOf(in, agwPropData), strOf(in, agwPropKV)
	out := map[string]any{}

	switch {
	case data != "" && kv != "":
		return nil, netBad("SSL certificate %s must set exactly one of data or keyVaultSecretId.", name)
	case data != "":
		pub, err := publicCertOf(name, data, strOf(in, "password"))
		if err != nil {
			return nil, err
		}

		out[agwPropPub] = pub
	case kv != "":
		out[agwPropKV] = kv
	case prev != nil:
		maps.Copy(out, prev)
	default:
		return nil, netBad("SSL certificate %s must set data or keyVaultSecretId.", name)
	}

	return out, nil
}

func publicCertOf(name, data, password string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return "", netBad("SSL certificate %s: data is not valid base64.", name)
	}

	_, leaf, _, err := pkcs12.DecodeChain(raw, password)
	if err != nil {
		return "", netBad("SSL certificate %s: data is not a valid PFX or the password is wrong.", name)
	}

	return base64.StdEncoding.EncodeToString(leaf.Raw), nil
}

// --- cross-child validation ---

func (p *NetworkProvider) validateAGW(c *agwCtx, props map[string]any, kids map[string][]*netObj) error {
	if len(kids[agwGatewayIPs]) != 1 {
		return netBad("Application gateway %s requires exactly one gatewayIPConfiguration.", c.id)
	}

	if err := p.checkAGWSubnet(c, kids); err != nil {
		return err
	}

	if err := checkAGWFrontends(c, kids[agwFrontends]); err != nil {
		return err
	}

	if err := checkAGWRefs(c, kids); err != nil {
		return err
	}

	if err := checkAGWRules(kids); err != nil {
		return err
	}

	if err := checkAGWListeners(kids); err != nil {
		return err
	}

	if err := checkAGWPathMaps(c, kids); err != nil {
		return err
	}

	return p.checkAGWWAF(c, props, kids)
}

func (p *NetworkProvider) checkAGWSubnet(c *agwCtx, kids map[string][]*netObj) error {
	if others := append(p.subnetForeignUsers(c.gwSubnet), p.fwSubnetUsers(c.gwSubnet)...); len(others) > 0 {
		return netErr(http.StatusBadRequest, "ApplicationGatewaySubnetNotDedicated",
			"Subnet %s contains %s; a subnet used by an application gateway can contain only application gateways.",
			c.gwSubnet, others[0])
	}

	for _, f := range kids[agwFrontends] {
		if sid := subID(f.Props, netFieldSubnet); sid != "" && !strings.EqualFold(sid, c.gwSubnet) {
			return netBad(
				"Private frontend IP configuration %s must use the subnet of the gateway IP configuration.",
				f.Name,
			)
		}
	}

	return nil
}

func agwSubnet(gw *netParent) string {
	if len(gw.Kids[agwGatewayIPs]) == 0 {
		return ""
	}

	return subID(gw.Kids[agwGatewayIPs][0].Props, netFieldSubnet)
}

func checkAGWFrontends(c *agwCtx, fronts []*netObj) error {
	pub := 0

	for _, f := range fronts {
		if subID(f.Props, "publicIPAddress") != "" {
			pub++
		}
	}

	if pub == 0 {
		return netBad("Application gateway %s requires at least one public frontend IP configuration; private-only "+
			"deployments (network isolation) are not modelled.", c.id)
	}

	return nil
}

func checkAGWRefs(c *agwCtx, kids map[string][]*netObj) error {
	for _, s := range agwRefSpecs {
		for _, o := range kids[s.owner] {
			if err := agwCheckRef(c, kids, o, o.Props[s.key], s.target); err != nil {
				return err
			}
		}
	}

	for _, s := range agwListRefSpecs {
		for _, o := range kids[s.owner] {
			for _, v := range anyList(o.Props[s.key]) {
				if err := agwCheckRef(c, kids, o, v, s.target); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func agwCheckRef(c *agwCtx, kids map[string][]*netObj, owner *netObj, v any, target string) error {
	ref := refIDOf(v)
	if ref == "" {
		return nil
	}

	n := c.childName(ref, target)
	if n == "" || findObj(kids[target], n) == nil {
		return agwNotFoundRef(c, owner.Name, ref, target)
	}

	return nil
}

func checkAGWRules(kids map[string][]*netObj) error {
	prios := map[int]string{}
	used := map[string]string{}

	for _, r := range kids[agwRules] {
		pr := intOf(r.Props[netPropPriority])
		if other, dup := prios[pr]; dup {
			return netErr(http.StatusBadRequest, "ApplicationGatewayRequestRoutingRulePriorityDuplicate",
				"Request routing rules %s and %s have the same priority %d.", other, r.Name, pr)
		}

		prios[pr] = r.Name

		l := strings.ToLower(refIDOf(r.Props["httpListener"]))
		if other, dup := used[l]; dup {
			return netBad("HTTP listener %s is used by more than one routing rule (%s and %s).",
				refIDOf(r.Props["httpListener"]), other, r.Name)
		}

		used[l] = r.Name

		if err := checkAGWRuleTarget(r); err != nil {
			return err
		}
	}

	return nil
}

func checkAGWRuleTarget(r *netObj) error {
	hasPool, hasSet := r.Props[agwPropPool] != nil, r.Props[agwPropHTTPS] != nil
	hasRedir, hasMap := r.Props[agwPropRedir] != nil, r.Props["urlPathMap"] != nil

	if r.Props["httpListener"] == nil {
		return netBad("Routing rule %s requires httpListener.", r.Name)
	}

	switch strOf(r.Props, "ruleType") {
	case agwRulePath:
		if !hasMap || hasPool || hasSet || hasRedir {
			return netBad("Path-based routing rule %s requires urlPathMap and no direct backend or redirect.", r.Name)
		}
	default:
		if hasMap || hasRedir == (hasPool || hasSet) || hasPool != hasSet {
			return netBad("Basic routing rule %s requires backendAddressPool with backendHttpSettings, or "+
				"redirectConfiguration.", r.Name)
		}
	}

	return nil
}

func checkAGWListeners(kids map[string][]*netObj) error {
	ports := map[int]string{}

	for _, po := range kids[agwPorts] {
		pn := intOf(po.Props[agwPropPort])
		if other, dup := ports[pn]; dup {
			return netBad("Frontend ports %s and %s use the same port %d.", other, po.Name, pn)
		}

		ports[pn] = po.Name
	}

	seen := map[string]string{}

	for _, l := range kids[agwListeners] {
		hosts := strList(l.Props["hostNames"])
		if h := strOf(l.Props, "hostName"); h != "" {
			hosts = append(hosts, h)
		}

		slices.Sort(hosts)

		key := strings.ToLower(
			refIDOf(l.Props["frontendIPConfiguration"]) + "|" + refIDOf(l.Props["frontendPort"]) + "|" +
				strings.Join(hosts, ","),
		)
		if other, dup := seen[key]; dup {
			return netBad("HTTP listeners %s and %s use the same frontend IP, port and host names.", other, l.Name)
		}

		seen[key] = l.Name
	}

	return nil
}

func checkAGWPathMaps(c *agwCtx, kids map[string][]*netObj) error {
	for _, pm := range kids[agwPathMaps] {
		if err := checkAGWDefaultTarget(pm.Name, pm.Props, "default"); err != nil {
			return err
		}

		seen := map[string]bool{}

		for _, e := range anyList(pm.Props[agwPropPathRs]) {
			m, _ := e.(map[string]any)
			if err := checkAGWPathRule(c, kids, pm, seen, m); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkAGWPathRule(c *agwCtx, kids map[string][]*netObj, pm *netObj, seen map[string]bool, m map[string]any) error {
	rn := strOf(m, fieldName)

	if err := validNetName(rn, netMaxName); err != nil {
		return err
	}

	if seen[strings.ToLower(rn)] {
		return netBad("URL path map %s uses path rule name %s more than once.", pm.Name, rn)
	}

	seen[strings.ToLower(rn)] = true

	rp := propsOf(m)
	if len(strList(rp["paths"])) == 0 {
		return netBad("Path rule %s of URL path map %s requires at least one path.", rn, pm.Name)
	}

	for _, pth := range strList(rp["paths"]) {
		if !strings.HasPrefix(pth, "/") {
			return netBad("Path %s of path rule %s must start with '/'.", pth, rn)
		}
	}

	if err := checkAGWDefaultTarget(rn, rp, ""); err != nil {
		return err
	}

	for _, s := range []agwRefSpec{
		{key: agwPropPool, target: agwPools},
		{key: agwPropHTTPS, target: agwSettings},
		{key: agwPropRedir, target: agwRedirects},
		{key: agwPropRewr, target: agwRewrites},
	} {
		if err := agwCheckRef(c, kids, pm, rp[s.key], s.target); err != nil {
			return err
		}
	}

	return nil
}

// checkAGWDefaultTarget requires a pool with settings, or a redirect; prefix "default" selects the url-path-map keys.
func checkAGWDefaultTarget(name string, props map[string]any, prefix string) error {
	key := func(k string) string {
		if prefix == "" {
			return k
		}

		return prefix + strings.ToUpper(k[:1]) + k[1:]
	}

	hasPool, hasSet := props[key(agwPropPool)] != nil, props[key(agwPropHTTPS)] != nil
	hasRedir := props[key(agwPropRedir)] != nil

	if hasRedir == (hasPool || hasSet) || hasPool != hasSet {
		return netBad("%s requires a backend pool with backend HTTP settings, or a redirect configuration.", name)
	}

	return nil
}

func (p *NetworkProvider) checkAGWWAF(c *agwCtx, props map[string]any, kids map[string][]*netObj) error {
	refs := agwPolicyRefs(props, kids)

	for _, ref := range refs {
		pol, ok := p.netRef(ref, netWAFs)
		if !ok || (c.loc != "" && pol.Obj.Location != c.loc) {
			return refMissing(ref)
		}
	}

	waf, _ := props[agwPropWAFCfg].(map[string]any)
	enabled, _ := waf["enabled"].(bool)

	if c.tier != agwSKUWAFV2 && (len(refs) > 0 || enabled) {
		return netErr(http.StatusBadRequest, "ApplicationGatewayFirewallPolicyRequiresWafSku",
			"Web application firewall settings can only be used with the WAF_v2 SKU; gateway %s uses %s.", c.id, c.tier)
	}

	if !enabled {
		return nil
	}

	if _, ok := canonEnum(strOf(waf, "firewallMode"), []string{"Detection", "Prevention"}); !ok {
		return netBad("webApplicationFirewallConfiguration.firewallMode must be Detection or Prevention.")
	}

	if strOf(waf, "ruleSetType") == "" || strOf(waf, "ruleSetVersion") == "" {
		return netBad("webApplicationFirewallConfiguration requires ruleSetType and ruleSetVersion.")
	}

	return nil
}

// agwPolicyRefs lists the firewall policy ids set on the gateway, its listeners and its path rules.
func agwPolicyRefs(props map[string]any, kids map[string][]*netObj) []string {
	var refs []string

	if pol := subID(props, agwPropPolicy); pol != "" {
		refs = append(refs, pol)
	}

	for _, l := range kids[agwListeners] {
		if pol := subID(l.Props, agwPropPolicy); pol != "" {
			refs = append(refs, pol)
		}
	}

	for _, pm := range kids[agwPathMaps] {
		for _, e := range anyList(pm.Props[agwPropPathRs]) {
			m, _ := e.(map[string]any)
			if pol := subID(propsOf(m), agwPropPolicy); pol != "" {
				refs = append(refs, pol)
			}
		}
	}

	return refs
}

// --- body ---

func (p *NetworkProvider) bodyAGW(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	for _, kt := range agwKidTypes {
		list := make([]any, 0, len(it.Kids[kt]))
		for _, k := range it.Kids[kt] {
			list = append(list, p.bodyAGWKid(it, kt, k))
		}

		props[kt] = list
	}

	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID
	props["operationalState"] = firstNonEmpty(it.Obj.Aux[agwStateKey], agwRunning)

	return p.envelope(id, netAGWs, &it.Obj, props)
}

func (p *NetworkProvider) bodyAGWKid(gw *netParent, kt string, k *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, k.Props)

	self := gw.armID() + "/" + kt + "/" + k.Name

	switch kt {
	case agwPools:
		props["backendIPConfigurations"] = idRefs(p.agwPoolMembers(self))
	case agwRedirects:
		props["requestRoutingRules"] = idRefs(refsTo(gw.Kids[agwRules], agwRules, gw.armID(), agwPropRedir, self))
		props[agwPathMaps] = idRefs(
			refsTo(gw.Kids[agwPathMaps], agwPathMaps, gw.armID(), "defaultRedirectConfiguration", self),
		)
	}

	props[fieldProvisioningState] = netStatusSucceeded

	return map[string]any{
		netFieldID: self, fieldName: k.Name, fieldType: agwTypePrefix + kt, netFieldEtag: k.Etag,
		fieldProperties: props,
	}
}

func (p *NetworkProvider) agwPoolMembers(poolID string) []string {
	var out []string

	for _, c := range p.allIPConfigs() {
		if hasRef(c.props, propNICAGWPools, poolID) {
			out = append(out, c.id)
		}
	}

	return out
}

// --- holders and lookups ---

const propNICAGWPools = "applicationGatewayBackendAddressPools"

// agwFrontendUsers lists gateway frontends whose property key references target.
func (p *NetworkProvider) agwFrontendUsers(key, target string) []string {
	var out []string

	for _, gw := range p.ofType(netAGWs) {
		for _, f := range gw.Kids[agwFrontends] {
			if strings.EqualFold(subID(f.Props, key), target) {
				out = append(out, gw.armID()+"/"+agwFrontends+"/"+f.Name)
			}
		}
	}

	return out
}

func (p *NetworkProvider) agwSubnetUsers(subnetID string) []string {
	var out []string

	for _, gw := range p.ofType(netAGWs) {
		if strings.EqualFold(agwSubnet(gw), subnetID) {
			out = append(out, gw.armID())
		}
	}

	return out
}

func (p *NetworkProvider) agwPoolExists(idStr string) bool {
	rid, err := ParseGenericResourcePath(idStr)
	if err != nil || len(rid.Types) != 2 || !strings.EqualFold(rid.Types[0], netAGWs) ||
		!strings.EqualFold(rid.Types[1], agwPools) || !strings.EqualFold(rid.Namespace, namespaceMicrosoftNetwork) {
		return false
	}

	gw, ok := p.top(netAGWs, rid.ResourceGroup, rid.Names[0])
	if !ok {
		return false
	}

	_, idx := gw.kid(agwPools, rid.Names[1])

	return idx >= 0
}
