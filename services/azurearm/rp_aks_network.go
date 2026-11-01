package azurearm

import (
	"net/netip"
	"slices"
	"strings"
)

const (
	aksPluginKubenet = "kubenet"
	aksPluginAzure   = "azure"
	aksPluginNone    = "none"
	aksModeOverlay   = "overlay"
	aksDataplaneCil  = "cilium"
	aksPolicyCalico  = "calico"
	aksPolicyAzure   = "azure"
	aksPolicyCilium  = "cilium"
	aksDefaultPod    = "10.244.0.0/16"
	aksDefaultSvc    = "10.0.0.0/16"
	aksDefaultDNS    = "10.0.0.10"
	aksMinServiceLen = 12
	aksMaxPodsMin    = 10
	aksMaxPodsMax    = 250
	aksMinClusterPod = 30
	aksNetPluginKey  = "networkPlugin"
)

// aksCheckNetwork validates networkProfile and returns it with the documented defaults filled in.
func aksCheckNetwork(in map[string]any) (map[string]any, error) {
	np := acaClone(in)
	if np == nil {
		np = map[string]any{}
	}

	plugin := strings.ToLower(strOf(np, aksNetPluginKey))
	if plugin == "" {
		plugin = aksPluginKubenet
	}

	if !slices.Contains([]string{aksPluginKubenet, aksPluginAzure, aksPluginNone}, plugin) {
		return nil, aksBad("networkProfile.networkPlugin '%s' must be one of azure, kubenet or none.",
			strOf(np, aksNetPluginKey))
	}

	np[aksNetPluginKey] = plugin

	if err := aksCheckPluginModes(np, plugin); err != nil {
		return nil, err
	}

	overlay := strings.EqualFold(strOf(np, "networkPluginMode"), aksModeOverlay)
	if plugin == aksPluginKubenet || overlay {
		aksDefault(np, aksFieldPodCidr, aksDefaultPod)
	} else if strOf(np, aksFieldPodCidr) != "" {
		return nil, aksBad("networkProfile.podCidr can only be set with the kubenet plugin or Azure CNI overlay.")
	}

	if plugin != aksPluginNone {
		aksDefault(np, aksFieldSvcCidr, aksDefaultSvc)
		aksDefault(np, "dnsServiceIP", aksDefaultDNS)
	}

	if err := aksCheckCIDRs(np); err != nil {
		return nil, err
	}

	aksDefault(np, "loadBalancerSku", "standard")
	aksDefault(np, "outboundType", "loadBalancer")
	aksFillLists(np)

	return np, nil
}

func aksDefault(m map[string]any, k, v string) {
	if strOf(m, k) == "" {
		m[k] = v
	}
}

func aksFillLists(np map[string]any) {
	if s := strOf(np, aksFieldSvcCidr); s != "" {
		np["serviceCidrs"] = []any{s}
	}

	if s := strOf(np, aksFieldPodCidr); s != "" {
		np["podCidrs"] = []any{s}
	}

	np["ipFamilies"] = []any{"IPv4"}
}

func aksCheckPluginModes(np map[string]any, plugin string) error {
	if mode := strOf(np, "networkPluginMode"); mode != "" {
		if !strings.EqualFold(mode, aksModeOverlay) {
			return aksBad("networkProfile.networkPluginMode '%s' must be overlay.", mode)
		}

		if plugin != aksPluginAzure {
			return aksBad("networkProfile.networkPluginMode overlay requires networkPlugin azure.")
		}

		np["networkPluginMode"] = aksModeOverlay
	}

	dp := strings.ToLower(strOf(np, "networkDataplane"))
	if dp != "" && dp != aksPluginAzure && dp != aksDataplaneCil {
		return aksBad("networkProfile.networkDataplane '%s' must be azure or cilium.", strOf(np, "networkDataplane"))
	}

	if dp == aksDataplaneCil && plugin != aksPluginAzure {
		return aksBad("networkProfile.networkDataplane cilium requires networkPlugin azure.")
	}

	return aksCheckPolicy(np, plugin, dp)
}

func aksCheckPolicy(np map[string]any, plugin, dataplane string) error {
	pol := strings.ToLower(strOf(np, "networkPolicy"))

	switch {
	case pol == "" || pol == aksPluginNone:
		return nil
	case plugin == aksPluginNone:
		return aksBad("networkProfile.networkPolicy cannot be set with networkPlugin none.")
	case pol == aksPolicyCalico:
		return nil
	case pol == aksPolicyAzure && plugin == aksPluginAzure:
		return nil
	case pol == aksPolicyCilium && plugin == aksPluginAzure && dataplane == aksDataplaneCil:
		return nil
	}

	return aksBad("networkProfile.networkPolicy '%s' is not supported with networkPlugin %s and dataplane '%s'.",
		strOf(np, "networkPolicy"), plugin, dataplane)
}

func aksParseCIDR(field, v string) (netip.Prefix, error) {
	p, err := netip.ParsePrefix(v)
	if err != nil || !p.Addr().Is4() || p.Masked() != p {
		return netip.Prefix{}, aksBad("networkProfile.%s '%s' is not a valid IPv4 CIDR block.", field, v)
	}

	return p, nil
}

func aksCheckCIDRs(np map[string]any) error {
	var svc, pod netip.Prefix

	if v := strOf(np, aksFieldSvcCidr); v != "" {
		p, err := aksParseCIDR(aksFieldSvcCidr, v)
		if err != nil {
			return err
		}

		if p.Bits() < aksMinServiceLen {
			return aksBad("networkProfile.serviceCidr '%s' must be smaller than /%d.", v, aksMinServiceLen)
		}

		svc = p
	}

	if v := strOf(np, aksFieldPodCidr); v != "" {
		p, err := aksParseCIDR(aksFieldPodCidr, v)
		if err != nil {
			return err
		}

		pod = p
	}

	if svc.IsValid() && pod.IsValid() && svc.Overlaps(pod) {
		return aksBad("networkProfile.podCidr '%s' overlaps serviceCidr '%s'.", pod, svc)
	}

	return aksCheckDNS(np, svc)
}

func aksCheckDNS(np map[string]any, svc netip.Prefix) error {
	v := strOf(np, "dnsServiceIP")
	if v == "" {
		return nil
	}

	ip, err := netip.ParseAddr(v)
	if err != nil || !ip.Is4() {
		return aksBad("networkProfile.dnsServiceIP '%s' is not a valid IPv4 address.", v)
	}

	if !svc.IsValid() {
		return nil
	}

	first := svc.Addr().Next()

	switch {
	case !svc.Contains(ip):
		return aksBad("networkProfile.dnsServiceIP '%s' must be within the serviceCidr '%s'.", v, svc)
	case ip == svc.Addr() || ip == first:
		return aksBad("networkProfile.dnsServiceIP '%s' cannot be the network address or the first IP address of "+
			"the serviceCidr '%s'.", v, svc)
	}

	return nil
}

// aksSubnetRef resolves a subnet resource ID against the network provider. Unknown virtual networks are accepted (the
// subnet may live outside this emulator); a known network without that subnet is an error.
func (p *ContainerServiceProvider) aksSubnetRef(field, subnetID string) ([]netip.Prefix, error) {
	rid, err := ParseGenericResourcePath(subnetID)
	if err != nil || len(rid.Types) != 2 || !strings.EqualFold(rid.Types[0], netVNets) ||
		!strings.EqualFold(rid.Types[1], netSubnets) {
		return nil, aksBad("%s '%s' is not a valid subnet resource ID.", field, subnetID)
	}

	if p.net == nil {
		return nil, nil
	}

	p.net.mu.RLock("aksSubnetRef")
	defer p.net.mu.RUnlock()

	_, sn, found := p.net.subnetByID(subnetID)
	if !found {
		if _, exists := p.net.top(netVNets, rid.ResourceGroup, rid.Names[0]); exists {
			return nil, aksBad("%s '%s': the subnet was not found in its virtual network.", field, subnetID)
		}

		return nil, nil
	}

	var out []netip.Prefix

	if s := strOf(sn.Props, "addressPrefix"); s != "" {
		if pf, perr := netip.ParsePrefix(s); perr == nil {
			out = append(out, pf)
		}
	}

	for _, s := range strList(sn.Props["addressPrefixes"]) {
		if pf, perr := netip.ParsePrefix(s); perr == nil {
			out = append(out, pf)
		}
	}

	return out, nil
}

// aksCheckSubnetOverlap rejects service and pod ranges that overlap the node subnets (Learn: concepts-network-ip-
// address-planning).
func aksCheckSubnetOverlap(np map[string]any, subnets map[string][]netip.Prefix) error {
	ranges := map[string]string{
		aksFieldSvcCidr: strOf(np, aksFieldSvcCidr),
		aksFieldPodCidr: strOf(np, aksFieldPodCidr),
	}

	for _, id := range sortedKeys(subnets) {
		for _, name := range []string{aksFieldSvcCidr, aksFieldPodCidr} {
			cidr, err := netip.ParsePrefix(ranges[name])
			if err != nil {
				continue
			}

			for _, sp := range subnets[id] {
				if sp.Overlaps(cidr) {
					return aksBad("networkProfile.%s '%s' overlaps the subnet '%s' (%s).", name, ranges[name], id, sp)
				}
			}
		}
	}

	return nil
}
