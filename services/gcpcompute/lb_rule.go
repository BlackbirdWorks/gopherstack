package gcpcompute

import (
	"net/netip"
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	maxRulePorts  = 5
	portHTTPAlt   = 8080
	protoL3       = "L3_DEFAULT"
	targetKindCnt = 4
)

var (
	tcpProxyPorts = []int64{25, 43, 110, 143, 195, 443, 465, 587, 700, 993, 995, 1883, 5222} //nolint:gochecknoglobals // read-only lookup table
	ruleProtocols = []string{protoTCP, "UDP", "ESP", "AH", "SCTP", "ICMP", protoL3}          //nolint:gochecknoglobals // read-only lookup table
)

func fwdRuleSpec() lbSpec[computeapi.ForwardingRule] {
	return lbSpec[computeapi.ForwardingRule]{
		coll: collFwdRules, kind: "compute#forwardingRule", noun: "forwarding_rule",
		m: func(b *InMemoryBackend) map[string]*computeapi.ForwardingRule { return b.fwdRules },
		meta: func(v *computeapi.ForwardingRule) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, fp: &v.Fingerprint, id: &v.Id,
			}
		},
		prep: prepForwardingRule,
		refs: func(v *computeapi.ForwardingRule) []string { return []string{v.Target, v.BackendService} },
	}
}

func validRuleScheme(c lbCtx, s string) bool {
	switch s {
	case schemeExternal, schemeExternalManaged:
		return true
	case schemeSelfManaged:
		return c.scope == scopeGlobal
	case schemeInternal, schemeInternalManaged:
		return c.scope == scopeRegion
	}

	return false
}

func parsePortRange(s string) (int64, int64, bool) {
	lo, hi, found := strings.Cut(s, "-")

	l, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || l < 1 || l > hcMaxPort {
		return 0, 0, false
	}

	if !found {
		return l, l, true
	}

	h, err := strconv.ParseInt(hi, 10, 64)
	if err != nil || h < l || h > hcMaxPort {
		return 0, 0, false
	}

	return l, h, true
}

// ruleTargetKind identifies the collection a forwarding rule target points into.
func ruleTargetKind(raw string) string {
	parts := strings.Split(trimURL(raw), "/")
	if len(parts) < nameInRegionOrZone {
		return ""
	}

	return parts[len(parts)-2]
}

func (b *InMemoryBackend) resolveRuleTargetLocked(c lbCtx, in *computeapi.ForwardingRule) (string, *APIError) {
	switch k := ruleTargetKind(in.Target); k {
	case collHTTPProxy:
		return lbResolve(b, httpProxySpec(), c, "resource.target", in.Target)
	case collHTTPSProxy:
		return lbResolve(b, httpsProxySpec(), c, "resource.target", in.Target)
	case collTCPProxy:
		return lbResolve(b, tcpProxySpec(), c, "resource.target", in.Target)
	case collSSLProxy:
		return lbResolve(b, sslProxySpec(), c, "resource.target", in.Target)
	default:
		return "", errField("resource.target", in.Target, "Must be a target HTTP, HTTPS, TCP or SSL proxy; "+
			"target pools, target instances and service attachments are not supported by this emulator.")
	}
}

// proxyServiceLocked returns the backend service a proxy ultimately sends traffic to, if it is a service.
func (b *InMemoryBackend) proxyServiceLocked(target string) *computeapi.BackendService {
	var svc string

	switch ruleTargetKind(target) {
	case collHTTPProxy:
		for _, p := range b.httpProxies {
			if p.SelfLink == target {
				svc = b.urlMapDefaultLocked(p.UrlMap)
			}
		}
	case collHTTPSProxy:
		for _, p := range b.httpsProxies {
			if p.SelfLink == target {
				svc = b.urlMapDefaultLocked(p.UrlMap)
			}
		}
	case collTCPProxy:
		for _, p := range b.tcpProxies {
			if p.SelfLink == target {
				svc = p.Service
			}
		}
	case collSSLProxy:
		for _, p := range b.sslProxies {
			if p.SelfLink == target {
				svc = p.Service
			}
		}
	}

	for _, bs := range b.backendSvcs {
		if bs.SelfLink == svc {
			return bs
		}
	}

	return nil
}

func (b *InMemoryBackend) urlMapDefaultLocked(link string) string {
	for _, m := range b.urlMaps {
		if m.SelfLink == link {
			return firstService(m.DefaultService, m.DefaultRouteAction)
		}
	}

	return ""
}

func prepForwardingRule(b *InMemoryBackend, c lbCtx, in *computeapi.ForwardingRule, raw map[string]any) *APIError {
	if in.LoadBalancingScheme == "" {
		in.LoadBalancingScheme = schemeExternal
	}

	if !validRuleScheme(c, in.LoadBalancingScheme) {
		return errField("resource.loadBalancingScheme", in.LoadBalancingScheme,
			"Global rules support EXTERNAL, EXTERNAL_MANAGED and INTERNAL_SELF_MANAGED; "+
				"regional rules EXTERNAL, EXTERNAL_MANAGED, INTERNAL and INTERNAL_MANAGED.")
	}

	if (in.Target == "") == (in.BackendService == "") {
		return errInvalid("Invalid value for field 'resource': exactly one of target or backendService must be specified.")
	}

	if err := b.resolveRuleBackendLocked(c, in); err != nil {
		return err
	}

	if raw == nil {
		return validateLabelsOf(in)
	}

	if err := b.prepRuleNetworkLocked(c, in); err != nil {
		return err
	}

	if err := prepRulePorts(in); err != nil {
		return err
	}

	if err := b.prepRuleIPLocked(c, in); err != nil {
		return err
	}

	if err := b.ruleConflictLocked(in); err != nil {
		return err
	}

	in.SelfLinkWithId = apiBase + "projects/" + c.project + "/" + c.scope + "/" +
		strings.TrimPrefix(c.loc+"/", "/") + collFwdRules + "/" + strconv.FormatUint(in.Id, 10)

	return validateLabelsOf(in)
}

func validateLabelsOf(in *computeapi.ForwardingRule) *APIError {
	if err := validLabels(in.Labels); err != nil {
		return err
	}

	in.LabelFingerprint = labelFP(in.Labels)

	return nil
}

func (b *InMemoryBackend) resolveRuleBackendLocked(c lbCtx, in *computeapi.ForwardingRule) *APIError {
	scheme := in.LoadBalancingScheme

	if in.BackendService != "" {
		if c.scope != scopeRegion || scheme != schemeExternal && scheme != schemeInternal {
			return errField("resource.backendService", in.BackendService,
				"Only regional EXTERNAL or INTERNAL pass-through rules take a backend service.")
		}

		l, err := lbResolve(b, bsSpec(), c, "resource.backendService", in.BackendService)
		if err != nil {
			return err
		}

		in.BackendService = l

		return b.schemeMatchesLocked(l, scheme)
	}

	l, err := b.resolveRuleTargetLocked(c, in)
	if err != nil {
		return err
	}

	in.Target = l

	if c.scope == scopeRegion && scheme == schemeExternal || scheme == schemeInternal {
		return errField("resource.loadBalancingScheme", scheme, "A proxy target needs a managed or self-managed scheme.")
	}

	return b.schemeMatchesLocked(b.proxySvcLink(l), scheme)
}

func (b *InMemoryBackend) proxySvcLink(target string) string {
	if bs := b.proxyServiceLocked(target); bs != nil {
		return bs.SelfLink
	}

	return ""
}

func (b *InMemoryBackend) schemeMatchesLocked(svcLink, scheme string) *APIError {
	for _, bs := range b.backendSvcs {
		if bs.SelfLink == svcLink && bs.LoadBalancingScheme != scheme {
			return errInvalid("Invalid value for field 'resource.loadBalancingScheme': '%s'. The backend service '%s' "+
				"has load balancing scheme %s.", scheme, bs.Name, bs.LoadBalancingScheme)
		}
	}

	return nil
}

func (b *InMemoryBackend) prepRuleNetworkLocked(c lbCtx, in *computeapi.ForwardingRule) *APIError {
	switch in.LoadBalancingScheme {
	case schemeInternal, schemeInternalManaged:
		if in.Subnetwork == "" {
			return errRequired("resource.subnetwork")
		}

		s, err := b.resolveSubnetLocked(c.project, c.loc, in.Subnetwork)
		if err != nil {
			return err
		}

		if in.Network != "" {
			n, nerr := b.resolveNetworkLocked(c.project, in.Network)
			if nerr != nil {
				return nerr
			}

			if n.SelfLink != s.Network {
				return errField("resource.network", in.Network, "The subnetwork is in a different network.")
			}
		}

		in.Subnetwork, in.Network = s.SelfLink, s.Network
	case schemeSelfManaged:
		if in.Network == "" {
			return errRequired("resource.network")
		}

		n, err := b.resolveNetworkLocked(c.project, in.Network)
		if err != nil {
			return err
		}

		in.Network = n.SelfLink
	default:
		if in.Network != "" || in.Subnetwork != "" {
			return errInvalid("Invalid value for field 'resource.network': external rules take no network or subnetwork.")
		}
	}

	return nil
}

func prepRulePorts(in *computeapi.ForwardingRule) *APIError {
	if in.IPProtocol == "" {
		in.IPProtocol = protoTCP
	}

	if !slices.Contains(ruleProtocols, in.IPProtocol) {
		return errField("resource.IPProtocol", in.IPProtocol, "")
	}

	if in.Target != "" {
		return prepProxyRulePorts(in)
	}

	set := 0

	for _, ok := range []bool{in.PortRange != "", len(in.Ports) > 0, in.AllPorts} {
		if ok {
			set++
		}
	}

	if set != 1 {
		return errInvalid("Invalid value for field 'resource': exactly one of portRange, ports or allPorts must be specified.")
	}

	if in.PortRange != "" && in.LoadBalancingScheme == schemeInternal {
		return errField("resource.portRange", in.PortRange, "INTERNAL rules use ports or allPorts.")
	}

	return normalizeRulePorts(in)
}

func normalizeRulePorts(in *computeapi.ForwardingRule) *APIError {
	if in.PortRange != "" {
		lo, hi, ok := parsePortRange(in.PortRange)
		if !ok {
			return errField("resource.portRange", in.PortRange, "Must be a port or a range such as 80-90.")
		}

		in.PortRange = strconv.FormatInt(lo, 10) + "-" + strconv.FormatInt(hi, 10)
	}

	if len(in.Ports) > maxRulePorts {
		return errField("resource.ports", "", "At most 5 ports are allowed.")
	}

	for _, p := range in.Ports {
		if _, _, ok := parsePortRange(p); !ok || strings.Contains(p, "-") {
			return errField("resource.ports", p, "Must be a port number.")
		}
	}

	return nil
}

func prepProxyRulePorts(in *computeapi.ForwardingRule) *APIError {
	if in.IPProtocol != protoTCP {
		return errField("resource.IPProtocol", in.IPProtocol, "A proxy target requires TCP.")
	}

	if len(in.Ports) > 0 || in.AllPorts {
		return errInvalid("Invalid value for field 'resource.ports': a proxy target takes a portRange.")
	}

	if in.PortRange == "" {
		return errRequired("resource.portRange")
	}

	if err := normalizeRulePorts(in); err != nil {
		return err
	}

	lo, hi, _ := parsePortRange(in.PortRange)
	if in.LoadBalancingScheme != schemeExternal {
		return nil
	}

	if lo != hi || !classicPortOK(ruleTargetKind(in.Target), lo) {
		return errField("resource.portRange", in.PortRange, "Not a port the target proxy type supports for EXTERNAL rules.")
	}

	return nil
}

func classicPortOK(kind string, p int64) bool {
	switch kind {
	case collHTTPProxy:
		return p == portHTTP || p == portHTTPAlt
	case collHTTPSProxy:
		return p == portHTTPS
	}

	return slices.Contains(tcpProxyPorts, p)
}

func (b *InMemoryBackend) prepRuleIPLocked(c lbCtx, in *computeapi.ForwardingRule) *APIError {
	if in.LoadBalancingScheme == schemeSelfManaged {
		if in.IPAddress == "" {
			in.IPAddress = "0.0.0.0"
		}

		if _, err := netip.ParseAddr(in.IPAddress); err != nil {
			return errField("resource.IPAddress", in.IPAddress, "Must be a valid IPv4 address.")
		}

		return nil
	}

	internal := in.LoadBalancingScheme == schemeInternal || in.LoadBalancingScheme == schemeInternalManaged

	if in.IPAddress != "" {
		ip, err := b.reservedRuleIPLocked(c, in.IPAddress, internal)
		if err != nil {
			return err
		}

		in.IPAddress = ip

		return nil
	}

	if !internal {
		region := ""
		if c.scope == scopeRegion {
			region = c.loc
		}

		ip, err := b.allocExternalLocked(region)
		in.IPAddress = ip

		return err
	}

	s, err := b.resolveSubnetLocked(c.project, c.loc, in.Subnetwork)
	if err != nil {
		return err
	}

	ip, ok := nextFree(netip.MustParsePrefix(s.IpCidrRange), b.usedInternalLocked(s.SelfLink))
	if !ok {
		return errInvalid("No free IP addresses are available in subnetwork '%s'.", s.Name)
	}

	in.IPAddress = ip.String()

	return nil
}

func (b *InMemoryBackend) reservedRuleIPLocked(c lbCtx, raw string, internal bool) (string, *APIError) {
	want := addrExternal
	if internal {
		want = addrInternal
	}

	var found *computeapi.Address

	if ip, err := netip.ParseAddr(raw); err == nil && ip.Is4() {
		for _, a := range b.addresses {
			if a.Address == raw && addrInScope(a, c) && a.AddressType != "" {
				found = a
			}
		}

		if found == nil {
			return "", errField("resource.IPAddress", raw, "The address is not a reserved address in this scope.")
		}
	} else {
		r, ok := parseRef(raw, collAddresses, c.project, c.loc)
		if !ok {
			return "", errField("resource.IPAddress", raw, "Not an IP address or reserved address reference.")
		}

		a, aerr := b.getAddressLocked(r.project, c.scope, c.loc, r.name)
		if aerr != nil {
			return "", aerr
		}

		found = a
	}

	if found.AddressType != want {
		return "", errField("resource.IPAddress", raw, "The address type does not fit the load balancing scheme.")
	}

	return found.Address, nil
}

func addrInScope(a *computeapi.Address, c lbCtx) bool {
	if c.scope == scopeGlobal {
		return a.Region == ""
	}

	return lastSegment(a.Region) == c.loc
}

func rulePortSpan(r *computeapi.ForwardingRule) (int64, int64) {
	switch {
	case r.AllPorts:
		return 1, hcMaxPort
	case r.PortRange != "":
		lo, hi, _ := parsePortRange(r.PortRange)

		return lo, hi
	}

	lo, hi := int64(hcMaxPort), int64(0)

	for _, p := range r.Ports {
		n, _ := strconv.ParseInt(p, 10, 64)
		lo, hi = min(lo, n), max(hi, n)
	}

	return lo, hi
}

func (b *InMemoryBackend) ruleConflictLocked(in *computeapi.ForwardingRule) *APIError {
	lo, hi := rulePortSpan(in)

	for _, o := range b.fwdRules {
		if o.IPAddress != in.IPAddress || o.IPProtocol != in.IPProtocol || o.Region != in.Region || o.SelfLink == in.SelfLink {
			continue
		}

		olo, ohi := rulePortSpan(o)
		if lo <= ohi && olo <= hi {
			return errInvalid("Invalid value for field 'resource.portRange': the address %s with protocol %s and "+
				"these ports is already used by forwarding rule '%s'.", in.IPAddress, in.IPProtocol, o.Name)
		}
	}

	return nil
}

func (b *InMemoryBackend) setRuleTarget(c lbCtx, name, target string) (*computeapi.Operation, error) {
	return lbMutate(b, fwdRuleSpec(), c, name, "setTarget", nil, func(cur *computeapi.ForwardingRule) (*computeapi.ForwardingRule, *APIError) {
		if cur.Target == "" {
			return nil, errInvalid("The forwarding rule targets a backend service; setTarget needs a target.")
		}

		cur.Target = target

		return cur, nil
	})
}

func (b *InMemoryBackend) setRuleLabels(c lbCtx, name string, req labelsRequest) (*computeapi.Operation, error) {
	return lbMutate(b, fwdRuleSpec(), c, name, "setLabels", nil, func(cur *computeapi.ForwardingRule) (*computeapi.ForwardingRule, *APIError) {
		labels, _, err := applyLabels(cur.Labels, cur.LabelFingerprint, req)
		if err != nil {
			return nil, err
		}

		cur.Labels = labels

		return cur, nil
	})
}
