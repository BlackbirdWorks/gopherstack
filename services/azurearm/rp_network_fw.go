package azurearm

import (
	"maps"
	"net/http"
	"net/netip"
	"strings"

	"github.com/google/uuid"
)

const (
	netFWs          = "azureFirewalls"
	netIPGroups     = "ipGroups"
	fwTypePrefix    = "Microsoft.Network/azureFirewalls/"
	fwIPConfigs     = "ipConfigurations"
	fwMgmtConfig    = "managementIpConfiguration"
	fwAppRCs        = "applicationRuleCollections"
	fwNatRCs        = "natRuleCollections"
	fwNetRCs        = "networkRuleCollections"
	fwSubnetName    = "AzureFirewallSubnet"
	fwMgmtSubnet    = "AzureFirewallManagementSubnet"
	fwMinSubnetBits = 26
	fwMaxName       = 50
	fwMinPrio       = 100
	fwMaxPrio       = 65000
	fwSKUVNet       = "AZFW_VNet"
	fwSKUHub        = "AZFW_Hub"
	fwTierStd       = "Standard"
	fwTierPrem      = "Premium"
	fwTierBasic     = "Basic"
	fwPropPolicy    = "firewallPolicy"
	fwPropTI        = "threatIntelMode"
	fwTIOff         = "Off"
	fwAny           = "Any"
	fwPropIPs       = "ipAddresses"
	fwMaxPort       = 64000
)

//nolint:gochecknoglobals // read-only tables
var (
	fwKidTypes  = []string{fwIPConfigs, fwMgmtConfig, fwAppRCs, fwNatRCs, fwNetRCs}
	fwTiers     = []string{fwTierStd, fwTierPrem, fwTierBasic}
	fwTIModes   = []string{"Alert", ruleDeny, fwTIOff}
	fwNetProtos = []string{"TCP", "UDP", "ICMP", fwAny}
	fwNatProtos = []string{"TCP", "UDP", fwAny}
	fwAppProtos = []string{"Http", "Https", "Mssql"}
)

func fwSegment(kt string) string {
	if kt == fwIPConfigs || kt == fwMgmtConfig {
		return "azureFirewallIpConfigurations"
	}

	return kt
}

// --- IP groups ---

func validIPGroupAddress(s string) bool {
	if _, err := netip.ParsePrefix(s); err == nil {
		return true
	}

	if _, err := netip.ParseAddr(s); err == nil {
		return true
	}

	lo, hi, found := strings.Cut(s, "-")
	if !found {
		return false
	}

	a, err1 := netip.ParseAddr(lo)
	b, err2 := netip.ParseAddr(hi)

	return err1 == nil && err2 == nil && a.Is4() == b.Is4() && a.Compare(b) <= 0
}

func (p *NetworkProvider) putIPGroup(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)
	addrs := make([]any, 0, len(anyList(props[fwPropIPs])))
	seen := map[string]bool{}

	for _, a := range strList(props[fwPropIPs]) {
		if !validIPGroupAddress(a) {
			return nil, false, netErr(http.StatusBadRequest, "InvalidIpAddressInIpGroup",
				"IP group %s contains the invalid entry %s; use an IP address, a CIDR range or an address range.",
				id.Names[0], a)
		}

		if !seen[a] {
			seen[a] = true

			addrs = append(addrs, a)
		}
	}

	it, obj, created := p.upsertTop(netIPGroups, id, body)
	obj.Props = map[string]any{fwPropIPs: addrs}

	return p.bodyIPGroup(id, it), created, nil
}

func (p *NetworkProvider) bodyIPGroup(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	fws, pols := p.ipGroupUsers(it.armID())
	props["firewalls"] = idRefs(fws)
	props["firewallPolicies"] = idRefs(pols)
	props[fieldProvisioningState] = netStatusSucceeded

	return p.envelope(id, netIPGroups, &it.Obj, props)
}

// ipGroupRefs lists the IP group ids referenced anywhere inside v.
func ipGroupRefs(v any) []string {
	var out []string

	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			if k == "sourceIpGroups" || k == "destinationIpGroups" {
				out = append(out, strList(e)...)

				continue
			}

			out = append(out, ipGroupRefs(e)...)
		}
	case []any:
		for _, e := range t {
			out = append(out, ipGroupRefs(e)...)
		}
	}

	return out
}

func (p *NetworkProvider) ipGroupUsers(groupID string) ([]string, []string) {
	var fws, pols []string

	uses := func(kids map[string][]*netObj) bool {
		for _, list := range kids {
			for _, k := range list {
				for _, r := range ipGroupRefs(k.Props) {
					if strings.EqualFold(r, groupID) {
						return true
					}
				}
			}
		}

		return false
	}

	for _, fw := range p.ofType(netFWs) {
		if uses(fw.Kids) {
			fws = append(fws, fw.armID())
		}
	}

	for _, pol := range p.ofType(netFWPols) {
		if uses(pol.Kids) {
			pols = append(pols, pol.armID())
		}
	}

	return fws, pols
}

func (p *NetworkProvider) checkIPGroupRefs(v any) error {
	for _, r := range ipGroupRefs(v) {
		if _, ok := p.netRef(r, netIPGroups); !ok {
			return refMissing(r)
		}
	}

	return nil
}

// --- Azure Firewalls ---

type fwCtx struct {
	taken    map[string]map[string]string
	prevAddr map[string]string
	pips     map[string]string
	id, loc  string
	tier     string
	hasMgmt  bool
	hasPol   bool
}

func (p *NetworkProvider) putFW(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)

	if len(id.Names[0]) > fwMaxName {
		return nil, false, netErr(http.StatusBadRequest, netCodeName,
			"Azure Firewall name %s is too long; names are limited to %d characters.", id.Names[0], fwMaxName)
	}

	skuName, tier, err := parseFWSKU(props)
	if err != nil {
		return nil, false, err
	}

	ti, ok := canonEnum(firstNonEmpty(strOf(props, fwPropTI), "Alert"), fwTIModes)
	if !ok {
		return nil, false, netBad("threatIntelMode must be Alert, Deny or Off.")
	}

	old, existed := p.top(netFWs, id.ResourceGroup, id.Names[0])
	loc := normLocation(strOf(body, fieldLocation))

	if loc == "" && existed {
		loc = old.Obj.Location
	}

	c := &fwCtx{
		id:       id.ARMID(),
		loc:      loc,
		tier:     tier,
		taken:    p.usedAddrsSkip("", id.ARMID()),
		prevAddr: map[string]string{},
		pips:     map[string]string{},
	}
	c.hasMgmt = props[fwMgmtConfig] != nil

	if old != nil {
		for _, k := range old.Kids[fwIPConfigs] {
			c.prevAddr[strings.ToLower(k.Name)] = strOf(k.Props, propPrivateIP)
		}
	}

	pol, err := p.fwPolicyRef(c, props)
	if err != nil {
		return nil, false, err
	}

	kids, err := p.buildFWKids(c, props)
	if err != nil {
		return nil, false, err
	}

	if err = p.checkFW(c, kids); err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netFWs, id, body)
	drop := append([]string{
		fieldProvisioningState, "hubIPAddresses", "ipGroups", agwPropSKU, fwPropPolicy, "virtualHub",
	}, fwKidTypes...)
	obj.Props = takeProps(body, drop...)
	obj.Props[fwPropTI] = ti

	if pol != "" {
		obj.Props[fwPropPolicy] = idRef(pol)
	}

	obj.Props[agwPropSKU] = map[string]any{fieldName: skuName, netFieldTier: tier}
	obj.Extra = map[string]any{}

	if z, has := body[netFieldZones]; has {
		obj.Extra[netFieldZones] = z
	}

	it.Kids = kids

	return p.bodyFW(id, it), created, nil
}

func (p *NetworkProvider) fwPolicyRef(c *fwCtx, props map[string]any) (string, error) {
	pid := subID(props, fwPropPolicy)
	if pid == "" {
		return "", nil
	}

	pol, ok := p.netRef(pid, netFWPols)
	if !ok {
		return "", refMissing(pid)
	}

	pt := policyTier(pol)

	if (pt == fwTierPrem && c.tier != fwTierPrem) || (pt == fwTierBasic && c.tier != fwTierBasic) ||
		(pt == fwTierStd && c.tier == fwTierBasic) {
		return "", netErr(http.StatusBadRequest, "AzureFirewallPolicyTierMismatch",
			"A %s firewall policy cannot be associated with a %s Azure Firewall.", pt, c.tier)
	}

	c.hasPol = true

	return pid, nil
}

func policyTier(pol *netParent) string {
	sku, _ := pol.Obj.Props[agwPropSKU].(map[string]any)

	return firstNonEmpty(strOf(sku, netFieldTier), fwTierStd)
}

func (p *NetworkProvider) buildFWKids(c *fwCtx, props map[string]any) (map[string][]*netObj, error) {
	kids := map[string][]*netObj{}

	cfgs := anyList(props[fwIPConfigs])
	if len(cfgs) == 0 {
		return nil, netBad("Azure Firewall %s requires at least one IP configuration.", c.id)
	}

	subnets := 0

	for _, e := range cfgs {
		o, err := p.normFWIPConfig(c, fwIPConfigs, e, false)
		if err != nil {
			return nil, err
		}

		if hasObj(kids[fwIPConfigs], o.Name) {
			return nil, netBad("IP configuration name %s is used more than once.", o.Name)
		}

		if subID(o.Props, netFieldSubnet) != "" {
			subnets++
		}

		if !c.hasMgmt && subID(o.Props, "publicIPAddress") == "" {
			return nil, netBad("IP configuration %s requires a public IP address.", o.Name)
		}

		kids[fwIPConfigs] = append(kids[fwIPConfigs], o)
	}

	if subnets != 1 {
		return nil, netBad(
			"Exactly one IP configuration of an Azure Firewall must reference the %s subnet.",
			fwSubnetName,
		)
	}

	if m, ok := props[fwMgmtConfig].(map[string]any); ok {
		o, err := p.normFWIPConfig(c, fwMgmtConfig, m, true)
		if err != nil {
			return nil, err
		}

		kids[fwMgmtConfig] = []*netObj{o}
	}

	if c.tier == fwTierBasic && !c.hasMgmt {
		return nil, netBad("Azure Firewall Basic requires managementIpConfiguration.")
	}

	if err := p.buildFWClassic(props, kids); err != nil {
		return nil, err
	}

	return kids, nil
}

func (p *NetworkProvider) normFWIPConfig(c *fwCtx, kt string, raw any, mgmt bool) (*netObj, error) {
	m, _ := raw.(map[string]any)
	name := strOf(m, fieldName)

	if err := validNetName(name, netMaxName); err != nil {
		return nil, err
	}

	cp := propsOf(m)
	out := map[string]any{}
	self := c.id + "/" + fwSegment(kt) + "/" + name

	if pid := subID(cp, "publicIPAddress"); pid != "" {
		if err := p.bindFWPublicIP(c, self, pid, out); err != nil {
			return nil, err
		}
	} else if mgmt {
		return nil, netBad("The management IP configuration %s requires a public IP address.", name)
	}

	if sid := subID(cp, netFieldSubnet); sid != "" {
		if err := p.bindFWSubnet(c, self, name, sid, mgmt, out); err != nil {
			return nil, err
		}
	} else if mgmt {
		return nil, netBad("The management IP configuration %s requires the %s subnet.", name, fwMgmtSubnet)
	}

	return &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: out}, nil
}

func (p *NetworkProvider) bindFWSubnet(c *fwCtx, self, name, sid string, mgmt bool, out map[string]any) error {
	vnet, sn, found := p.subnetByID(sid)
	if !found || (c.loc != "" && vnet.Obj.Location != c.loc) {
		return refMissing(sid)
	}

	want := fwSubnetName
	if mgmt {
		want = fwMgmtSubnet
	}

	if !strings.EqualFold(sn.Name, want) {
		return netErr(http.StatusBadRequest, "AzureFirewallSubnetNameInvalid",
			"The subnet of IP configuration %s must be named %s (found %s).", name, want, sn.Name)
	}

	for _, pf := range subnetPrefixes(sn) {
		if pf.Bits() > fwMinSubnetBits {
			return netErr(http.StatusBadRequest, "AzureFirewallSubnetTooSmall",
				"Subnet %s must be at least a /%d.", sn.Name, fwMinSubnetBits)
		}
	}

	if others := append(p.subnetForeignUsers(sid), p.agwSubnetUsers(sid)...); len(others) > 0 {
		return netErr(http.StatusBadRequest, "AzureFirewallSubnetNotDedicated",
			"Subnet %s contains %s; the Azure Firewall subnet must be dedicated to the firewall.", sn.Name, others[0])
	}

	key := strings.ToLower(sid)
	if c.taken[key] == nil {
		c.taken[key] = map[string]string{}
	}

	addr, err := assignIP(self, methodDynamic, "", subnetPrefixes(sn), c.taken[key], c.prevAddr[strings.ToLower(name)])
	if err != nil {
		return err
	}

	c.taken[key][addr] = self
	out[netFieldSubnet] = idRef(sid)
	out[propPrivateIP] = addr

	return nil
}

func (p *NetworkProvider) checkFW(c *fwCtx, kids map[string][]*netObj) error {
	for _, kt := range []string{fwAppRCs, fwNatRCs, fwNetRCs} {
		if c.hasPol && len(kids[kt]) > 0 {
			return netBad("Classic rule collections cannot be used with an associated firewall policy.")
		}
	}

	for _, list := range kids {
		for _, k := range list {
			if err := p.checkIPGroupRefs(k.Props); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *NetworkProvider) bodyFW(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	for _, kt := range fwKidTypes {
		list := make([]any, 0, len(it.Kids[kt]))

		for _, k := range it.Kids[kt] {
			list = append(list, fwKidBody(it, kt, k))
		}

		if kt == fwMgmtConfig {
			if len(list) > 0 {
				props[kt] = list[0]
			}

			continue
		}

		props[kt] = list
	}

	props[fieldProvisioningState] = netStatusSucceeded

	return p.envelope(id, netFWs, &it.Obj, props)
}

func fwKidBody(fw *netParent, kt string, k *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, k.Props)

	props[fieldProvisioningState] = netStatusSucceeded

	return map[string]any{
		netFieldID: fw.armID() + "/" + fwSegment(kt) + "/" + k.Name, fieldName: k.Name,
		fieldType: fwTypePrefix + fwSegment(kt), netFieldEtag: k.Etag, fieldProperties: props,
	}
}

// fwConfigUsers lists firewall IP configurations whose property key references target.
func (p *NetworkProvider) fwConfigUsers(key, target string) []string {
	var out []string

	for _, fw := range p.ofType(netFWs) {
		for _, kt := range []string{fwIPConfigs, fwMgmtConfig} {
			for _, k := range fw.Kids[kt] {
				if strings.EqualFold(subID(k.Props, key), target) {
					out = append(out, fw.armID()+"/"+fwSegment(kt)+"/"+k.Name)
				}
			}
		}
	}

	return out
}

func (p *NetworkProvider) fwSubnetUsers(subnetID string) []string {
	return p.fwConfigUsers(netFieldSubnet, subnetID)
}

func (p *NetworkProvider) putGatewayTop(top string, id ResourceID, body map[string]any) (map[string]any, bool, error) {
	switch top {
	case netAGWs:
		return p.putAGW(id, body)
	case netWAFs:
		return p.putWAF(id, body)
	case netFWs:
		return p.putFW(id, body)
	case netFWPols:
		return p.putFWPol(id, body)
	default:
		return p.putIPGroup(id, body)
	}
}

func (p *NetworkProvider) bodyGatewayTop(id ResourceID, it *netParent) map[string]any {
	switch it.Type {
	case netAGWs:
		return p.bodyAGW(id, it)
	case netWAFs:
		return p.bodyWAF(id, it)
	case netFWs:
		return p.bodyFW(id, it)
	case netFWPols:
		return p.bodyFWPol(id, it)
	default:
		return p.bodyIPGroup(id, it)
	}
}

func parseFWSKU(props map[string]any) (string, string, error) {
	sku, _ := props[agwPropSKU].(map[string]any)

	name, ok := canonEnum(firstNonEmpty(strOf(sku, fieldName), fwSKUVNet), []string{fwSKUVNet, fwSKUHub})
	if !ok {
		return "", "", netBad("Azure Firewall SKU name must be AZFW_VNet or AZFW_Hub.")
	}

	if name == fwSKUHub || props["virtualHub"] != nil {
		return "", "", netBad("Azure Firewalls in virtual hubs (AZFW_Hub) are not supported by this emulator.")
	}

	tier, ok := canonEnum(firstNonEmpty(strOf(sku, netFieldTier), fwTierStd), fwTiers)
	if !ok {
		return "", "", netBad("Azure Firewall SKU tier must be Standard, Premium or Basic.")
	}

	return name, tier, nil
}

// bindFWPublicIP checks a firewall public IP (Standard, Static, one holder) and records it in out.
func (p *NetworkProvider) bindFWPublicIP(c *fwCtx, self, pid string, out map[string]any) error {
	pip, ok := p.netRef(pid, netPIPs)
	if !ok || (c.loc != "" && pip.Obj.Location != c.loc) {
		return refMissing(pid)
	}

	if sk, _ := pip.Obj.Extra[netFieldSKU].(map[string]any); strOf(sk, fieldName) != skuStandard ||
		strOf(pip.Obj.Props, "publicIPAllocationMethod") != methodStatic {
		return netErr(http.StatusBadRequest, "AzureFirewallPublicIpNotStandardStatic",
			"Public IP %s must use the Standard SKU with Static allocation to be used by an Azure Firewall.", pid)
	}

	if user := p.pipHolder(pip.armID()); user != "" && !strings.EqualFold(user, self) {
		return netErr(http.StatusBadRequest, "PublicIPAddressInUse",
			"Public IP address %s is already allocated to resource %s.", pid, user)
	}

	if other, dup := c.pips[strings.ToLower(pid)]; dup {
		return netErr(http.StatusBadRequest, "PublicIPAddressInUse",
			"Public IP address %s is already allocated to %s.", pid, other)
	}

	c.pips[strings.ToLower(pid)] = self
	out["publicIPAddress"] = idRef(pid)

	return nil
}
