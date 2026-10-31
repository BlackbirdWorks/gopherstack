package azurearm

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	auxPENIC          = "nic"
	auxPERec          = "rec:"
	peConnTypePath    = "Microsoft.Network/privateEndpoints/privateLinkServiceConnections"
	peZoneGroupPath   = "Microsoft.Network/privateEndpoints/privateDnsZoneGroups"
	propPEAuto        = "privateLinkServiceConnections"
	propPEManual      = "manualPrivateLinkServiceConnections"
	propPEIPConfigs   = "ipConfigurations"
	propPENICName     = "customNetworkInterfaceName"
	propPEIPVersion   = "ipVersionType"
	delegatedSubnetCD = "PrivateEndpointCreationNotAllowedAsSubnetIsDelegated"
)

// SetLookup gives the provider a way to resolve private-link targets in other namespaces.
func (p *NetworkProvider) SetLookup(f func(context.Context, ResourceID) (map[string]any, error)) {
	p.mu.Lock("SetLookup")
	p.lookup = f
	p.mu.Unlock()
}

// peLink is a validated private link service connection.
type peLink struct {
	raw    map[string]any
	group  string
	name   string
	tid    string
	pub    string
	fqdns  []string
	auto   bool
	sameSu bool
}

func (p *NetworkProvider) buildPELinks(sub, loc, key string, raw any, auto bool) ([]peLink, error) {
	var out []peLink

	for _, e := range anyList(raw) {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if err := validNetName(name, netMaxName); err != nil {
			return nil, err
		}

		cp := propsOf(m)
		tid := strOf(cp, "privateLinkServiceId")

		rid, err := ParseGenericResourcePath(tid)
		if err != nil || len(rid.Types) != 1 || len(rid.Names) != 1 {
			return nil, netErr(http.StatusBadRequest, "LinkedInvalidPropertyId",
				"Property id '%s' at path 'properties.%s[%s].properties.privateLinkServiceId' is invalid. Expect "+
					"fully qualified resource Id that start with '/subscriptions/{subscriptionId}'.", tid, key, name)
		}

		t, ok := peTargets[peTargetKey(rid)]
		if !ok {
			return nil, netBad("Private endpoint connection %s: resource type %s/%s does not support private "+
				"endpoints in this emulator.", name, rid.Namespace, rid.Types[0])
		}

		groups := strList(cp[propGroupIDs])
		if len(groups) != 1 {
			return nil, netErr(http.StatusBadRequest, "PrivateEndpointGroupIdsInvalid",
				"Private endpoint connection %s must name exactly one group id; valid values for %s are %s.", name,
				rid.Types[0], strings.Join(groupNames(t), ", "))
		}

		gkey, g, ok := t.group(groups[0])
		if !ok {
			return nil, netErr(http.StatusBadRequest, "PrivateEndpointGroupIdsInvalid",
				"Group id %s is not valid for %s; valid values are %s.", groups[0], rid.Types[0],
				strings.Join(groupNames(t), ", "))
		}

		tloc, err := p.targetLocation(tid, loc)
		if err != nil {
			return nil, err
		}

		norm := map[string]any{
			fieldName: name,
			fieldProperties: map[string]any{
				"privateLinkServiceId": tid, propGroupIDs: []any{gkey}, "requestMessage": strOf(cp, "requestMessage"),
			},
		}

		out = append(out, peLink{
			raw: norm, pub: g.pub, group: gkey, name: name, tid: tid, auto: auto,
			fqdns: g.fqdns(rid.Names[0], tloc), sameSu: strings.EqualFold(rid.SubscriptionID, sub),
		})
	}

	return out, nil
}

func groupNames(t peTarget) []string {
	out := make([]string, 0, len(t.groups))
	for k := range t.groups {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

// targetLocation checks the target exists and returns its location (the endpoint's when unknown).
func (p *NetworkProvider) targetLocation(tid, fallback string) (string, error) {
	if p.lookup == nil {
		return fallback, nil
	}

	rid, _ := ParseGenericResourcePath(tid)

	body, err := p.lookup(context.Background(), rid)
	if err != nil {
		return "", refMissing(tid)
	}

	if loc := normLocation(strOf(body, fieldLocation)); loc != "" {
		return loc, nil
	}

	return fallback, nil
}

func (p *NetworkProvider) putPE(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)

	sid := subID(props, netFieldSubnet)
	if sid == "" {
		return nil, false, netBad("Private endpoint %s requires properties.subnet.id.", id.Names[0])
	}

	vnet, sn, found := p.subnetByID(sid)
	if !found {
		return nil, false, refMissing(sid)
	}

	old, existed := p.top(netPEs, id.ResourceGroup, id.Names[0])
	loc := normLocation(strOf(body, fieldLocation))

	if loc == "" && existed {
		loc = old.Obj.Location
	}

	if vnet.Obj.Location != loc {
		return nil, false, netErr(http.StatusBadRequest, "PrivateEndpointAndVnetLocationMismatch",
			"Private endpoint %s must be in the same region (%s) as virtual network %s.", id.Names[0],
			vnet.Obj.Location, vnet.Obj.Name)
	}

	if len(anyList(sn.Props["delegations"])) > 0 {
		return nil, false, netErr(http.StatusBadRequest, delegatedSubnetCD,
			"Private endpoint cannot be created in a subnet that is delegated to a service: %s.", sid)
	}

	links, err := p.collectPELinks(id, loc, props)
	if err != nil {
		return nil, false, err
	}

	asgs, err := p.peASGs(loc, props)
	if err != nil {
		return nil, false, err
	}

	cfgs, nicName, err := p.planPENIC(id, old, sid, sn, props, links)
	if err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netPEs, id, body)
	obj.Props = takeProps(body, propPEAuto, propPEManual, "networkInterfaces", "customDnsConfigs",
		fieldProvisioningState, propPEIPConfigs, propNICASGs)
	obj.Props[netFieldSubnet] = idRef(sid)
	obj.Props[propPEIPVersion] = firstNonEmpty(strOf(props, propPEIPVersion), versionV4)

	if len(asgs) > 0 {
		obj.Props[propNICASGs] = asgs
	}

	if reqCfgs := anyList(props[propPEIPConfigs]); len(reqCfgs) > 0 {
		obj.Props[propPEIPConfigs] = reqCfgs
	}

	p.applyPELinks(it, links)
	p.installPENIC(it, id, loc, nicName, cfgs)
	p.refreshZoneGroups(it)

	return p.bodyPE(id, it), created, nil
}

func (p *NetworkProvider) collectPELinks(id ResourceID, loc string, props map[string]any) ([]peLink, error) {
	auto, err := p.buildPELinks(id.SubscriptionID, loc, propPEAuto, props[propPEAuto], true)
	if err != nil {
		return nil, err
	}

	manual, err := p.buildPELinks(id.SubscriptionID, loc, propPEManual, props[propPEManual], false)
	if err != nil {
		return nil, err
	}

	links := slices.Concat(auto, manual)
	if len(links) == 0 {
		return nil, netBad("Private endpoint %s requires at least one private link service connection.", id.Names[0])
	}

	seen := map[string]bool{}

	for _, l := range links {
		if seen[strings.ToLower(l.name)] {
			return nil, netBad("Private link service connection name %s is used more than once.", l.name)
		}

		seen[strings.ToLower(l.name)] = true
	}

	return links, nil
}

func (p *NetworkProvider) peASGs(loc string, props map[string]any) ([]any, error) {
	var out []any

	for _, a := range anyList(props[propNICASGs]) {
		aid := refIDOf(a)

		asg, ok := p.netRef(aid, netASGs)
		if !ok || asg.Obj.Location != loc {
			return nil, refMissing(aid)
		}

		out = append(out, idRef(aid))
	}

	return out, nil
}

// applyPELinks stores the connections on the endpoint and settles their approval state.
func (p *NetworkProvider) applyPELinks(it *netParent, links []peLink) {
	prev := map[string]string{}

	for k, v := range it.Obj.Aux {
		for _, pre := range peConnAuxPrefixes {
			if strings.HasPrefix(k, pre) {
				prev[k] = v
				delete(it.Obj.Aux, k)
			}
		}
	}

	var auto, manual []any

	for _, l := range links {
		settleLink(it, l, prev)

		if l.auto {
			auto = append(auto, l.raw)
		} else {
			manual = append(manual, l.raw)
		}
	}

	if len(auto) > 0 {
		it.Obj.Props[propPEAuto] = auto
	}

	if len(manual) > 0 {
		it.Obj.Props[propPEManual] = manual
	}
}

//nolint:gochecknoglobals // read-only table
var peConnAuxPrefixes = []string{"st:", "desc:", "guid:", "gone:", "tid:"}

// settleLink keeps the state of a connection that is unchanged and starts a new one otherwise.
func settleLink(it *netParent, l peLink, prev map[string]string) {
	n := l.name

	if prev["tid:"+n] == strings.ToLower(l.tid) && prev["st:"+n] != "" {
		for _, k := range []string{"st:", "desc:", "guid:", "gone:"} {
			if v, ok := prev[k+n]; ok {
				it.Obj.Aux[k+n] = v
			}
		}
	} else {
		it.Obj.Aux["guid:"+n] = uuid.NewString()
		it.Obj.Aux["st:"+n], it.Obj.Aux["desc:"+n] = peStatusWait, strOf(propsOf(l.raw), "requestMessage")

		if l.auto && l.sameSu {
			it.Obj.Aux["st:"+n], it.Obj.Aux["desc:"+n] = peStatusOK, peAutoMessage
		}
	}

	it.Obj.Aux["tid:"+n] = strings.ToLower(l.tid)
}

// pePlannedCfg is one NIC IP configuration planned for a private endpoint.
type pePlannedCfg struct {
	cfg    map[string]any
	recKey string
	ip     string
}

// peNICName picks the managed interface name: the requested one, the current one, or a new one.
func (p *NetworkProvider) peNICName(id ResourceID, old *netParent, props map[string]any) (string, string, error) {
	oldNIC := ""
	if old != nil {
		oldNIC = old.Obj.Aux[auxPENIC]
	}

	name := firstNonEmpty(strOf(props, propPENICName), oldNIC)
	if name == "" {
		name = id.Names[0] + ".nic." + uuid.NewString()
	}

	if ex, clash := p.top(netNICs, id.ResourceGroup, name); clash && !strings.EqualFold(name, oldNIC) {
		return "", "", netErr(http.StatusConflict, "NetworkInterfaceAlreadyExists",
			"Network interface %s already exists.", ex.armID())
	}

	return name, oldNIC, nil
}

// prevPEConfigs maps a member name to the address and IP configuration name it had on the current interface.
func (p *NetworkProvider) prevPEConfigs(old *netParent, oldNIC, sid string) (map[string]string, map[string]string) {
	prevIP, prevName := map[string]string{}, map[string]string{}

	if old == nil {
		return prevIP, prevName
	}

	nic, ok := p.top(netNICs, old.RG, oldNIC)
	if !ok || !strings.EqualFold(subID(firstCfg(nic), netFieldSubnet), sid) {
		return prevIP, prevName
	}

	for _, c := range anyList(nic.Obj.Props[netIPConfigs]) {
		m, _ := c.(map[string]any)
		member := strOf(propsMap(propsOf(m)["privateLinkConnectionProperties"]), "requiredMemberName")
		prevIP[member], prevName[member] = strOf(propsOf(m), propPrivateIP), strOf(m, fieldName)
	}

	return prevIP, prevName
}

func staticPEAddrs(props map[string]any) map[string]string {
	static := map[string]string{}

	for _, c := range anyList(props[propPEIPConfigs]) {
		cm, _ := c.(map[string]any)
		cp := propsOf(cm)

		if a := strOf(cp, propPrivateIP); a != "" {
			static[firstNonEmpty(strOf(cp, "memberName"), strOf(cp, "groupId"))] = a
		}
	}

	return static
}

// planPENIC allocates the private addresses (one per FQDN) without touching state.
func (p *NetworkProvider) planPENIC(
	id ResourceID, old *netParent, sid string, sn *netObj, props map[string]any, links []peLink,
) ([]pePlannedCfg, string, error) {
	nicName, oldNIC, err := p.peNICName(id, old, props)
	if err != nil {
		return nil, "", err
	}

	nicID := ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: namespaceMicrosoftNetwork,
		Types: []string{netNICs}, Names: []string{nicName},
	}.ARMID()

	taken := p.usedAddrsSkip(nicID, "")[strings.ToLower(sid)]
	if taken == nil {
		taken = map[string]string{}
	}

	prevIP, prevName := p.prevPEConfigs(old, oldNIC, sid)
	static := staticPEAddrs(props)

	var out []pePlannedCfg

	for _, l := range links {
		for i, fq := range l.fqdns {
			member := l.group
			if i > 0 {
				member = l.group + "_" + strconv.Itoa(i)
			}

			cfgName := firstNonEmpty(prevName[member], "privateEndpointIpConfig."+uuid.NewString())
			cfgID := nicID + "/" + netIPConfigs + "/" + cfgName
			method, want := methodDynamic, static[member]

			if want != "" {
				method = methodStatic
			}

			addr, aerr := assignIP(cfgID, method, want, subnetPrefixes(sn), taken, prevIP[member])
			if aerr != nil {
				return nil, "", aerr
			}

			taken[addr] = cfgID

			out = append(out, pePlannedCfg{ip: addr, recKey: recordLabel(l, fq), cfg: peIPConfig(cfgName, sid, addr,
				l.group, member, fq, len(out) == 0)})
		}
	}

	return out, nicName, nil
}

func peIPConfig(name, sid, addr, group, member, fqdn string, primary bool) map[string]any {
	return map[string]any{
		fieldName: name,
		fieldProperties: map[string]any{
			netFieldSubnet: idRef(sid), propPrivateIP: addr, "privateIPAddressVersion": versionV4,
			"privateIPAllocationMethod": methodDynamic, "primary": primary,
			"privateLinkConnectionProperties": map[string]any{
				"groupId": group, "requiredMemberName": member, "fqdns": []any{fqdn},
			},
		},
	}
}

func firstCfg(nic *netParent) map[string]any {
	l := anyList(nic.Obj.Props[netIPConfigs])
	if len(l) == 0 {
		return nil
	}

	m, _ := l[0].(map[string]any)

	return propsOf(m)
}

// recordLabel is the DNS record name an FQDN gets in its private zone: the FQDN minus the public suffix.
func recordLabel(l peLink, fqdn string) string {
	if pub := l.pub; strings.HasSuffix(fqdn, "."+pub) {
		return strings.TrimSuffix(fqdn, "."+pub)
	}

	return fqdn[:strings.Index(fqdn+".", ".")]
}

// installPENIC creates or replaces the managed network interface of the endpoint.
func (p *NetworkProvider) installPENIC(
	it *netParent, id ResourceID, loc, nicName string, cfgs []pePlannedCfg,
) {
	if oldName := it.Obj.Aux[auxPENIC]; oldName != "" && !strings.EqualFold(oldName, nicName) {
		delete(p.items, netKey(netNICs, id.ResourceGroup, oldName))
	}

	for k := range it.Obj.Aux {
		if strings.HasPrefix(k, auxPERec) {
			delete(it.Obj.Aux, k)
		}
	}

	list := make([]any, 0, len(cfgs))

	for i, c := range cfgs {
		list = append(list, c.cfg)
		it.Obj.Aux[auxPERec+strconv.Itoa(i)] = c.recKey + "|" + c.ip
	}

	key := netKey(netNICs, id.ResourceGroup, nicName)
	guid := uuid.NewString()

	if ex, ok := p.items[key]; ok {
		guid = ex.Obj.GUID
	}

	nic := &netParent{
		Type: netNICs, RG: id.ResourceGroup, Sub: id.SubscriptionID, Kids: map[string][]*netObj{},
		Obj: netObj{
			Name: nicName, Location: loc, Etag: newEtag(), GUID: guid,
			Props: map[string]any{netIPConfigs: list, "privateEndpoint": idRef(it.armID())},
			Aux: map[string]string{
				"mac": macFor(guid), "suffix": strings.ReplaceAll(guid, "-", "")[:8] + ".bx.internal.cloudapp.net",
				"pe": it.armID(),
			},
		},
	}

	p.items[key] = nic
	it.Obj.Aux[auxPENIC] = nicName
}

func (p *NetworkProvider) peNIC(pe *netParent) *netParent {
	nic, ok := p.top(netNICs, pe.RG, pe.Obj.Aux[auxPENIC])
	if !ok {
		return nil
	}

	return nic
}

func (p *NetworkProvider) bodyPE(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	peID := it.armID()

	for _, key := range []string{propPEAuto, propPEManual} {
		var list []any

		for _, c := range anyList(it.Obj.Props[key]) {
			m, _ := c.(map[string]any)
			list = append(list, p.peConnBody(it, peID, m))
		}

		if list != nil {
			props[key] = list
		} else {
			props[key] = []any{}
		}
	}

	nics := []any{}
	dns := []any{}

	if nic := p.peNIC(it); nic != nil {
		nics = append(nics, idRef(nic.armID()))

		for _, c := range anyList(nic.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			cp := propsOf(m)
			fq := strList(propsMap(cp["privateLinkConnectionProperties"])["fqdns"])

			if len(fq) > 0 {
				dns = append(dns, map[string]any{propFQDN: fq[0], "ipAddresses": []any{strOf(cp, propPrivateIP)}})
			}
		}
	}

	props["networkInterfaces"] = nics
	props["customDnsConfigs"] = dns
	props[fieldProvisioningState] = netStatusSucceeded

	if _, ok := props[propNICASGs]; !ok {
		props[propNICASGs] = []any{}
	}

	return p.envelope(id, netPEs, &it.Obj, props)
}

func (p *NetworkProvider) peConnBody(pe *netParent, peID string, m map[string]any) map[string]any {
	name := strOf(m, fieldName)
	cp := propsOf(m)
	status, desc := connState(pe, name)

	return map[string]any{
		netFieldID: peID + "/privateLinkServiceConnections/" + name, fieldName: name, fieldType: peConnTypePath,
		netFieldEtag: pe.Obj.Etag,
		fieldProperties: map[string]any{
			"privateLinkServiceId": strOf(cp, "privateLinkServiceId"), propGroupIDs: cp[propGroupIDs],
			"requestMessage": strOf(cp, "requestMessage"), fieldProvisioningState: netStatusSucceeded,
			"privateLinkServiceConnectionState": map[string]any{
				netPropStatus: status, netPropDescription: desc, propActionsRequired: netNone,
			},
		},
	}
}

// --- private DNS zone groups ---

type zoneRec struct{ name, ip string }

func (p *NetworkProvider) peRecords(pe *netParent) []zoneRec {
	var out []zoneRec

	for k, v := range pe.Obj.Aux {
		if !strings.HasPrefix(k, auxPERec) {
			continue
		}

		if name, ip, ok := strings.Cut(v, "|"); ok {
			out = append(out, zoneRec{name: strings.ToLower(name), ip: ip})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].name+out[i].ip < out[j].name+out[j].ip })

	return out
}

func zoneGroupID(pe *netParent, name string) string {
	return pe.armID() + "/" + peZoneGroups + "/" + name
}

func zoneConfigs(zg *netObj) []map[string]any {
	raw := anyList(zg.Props["privateDnsZoneConfigs"])
	out := make([]map[string]any, 0, len(raw))

	for _, c := range raw {
		m, _ := c.(map[string]any)
		out = append(out, m)
	}

	return out
}

// addZoneRecords writes the endpoint's A records into the zones of one zone group.
func (p *NetworkProvider) addZoneRecords(pe *netParent, zg *netObj) {
	zgID := zoneGroupID(pe, zg.Name)

	for _, cfg := range zoneConfigs(zg) {
		zone, ok := p.netRef(strOf(propsOf(cfg), "privateDnsZoneId"), netZones)
		if !ok {
			continue
		}

		for _, r := range p.peRecords(pe) {
			k, idx := zone.kid("A", r.name)
			if idx < 0 {
				k = &netObj{
					Name: r.name, Etag: uuid.NewString(), GUID: uuid.NewString(),
					Props: map[string]any{propTTL: peDNSTTL, "aRecords": []any{}}, Aux: map[string]string{},
				}
				zone.Kids["A"] = append(zone.Kids["A"], k)
			}

			if k.Aux == nil {
				k.Aux = map[string]string{}
			}

			recs := anyList(k.Props["aRecords"])
			if !hasA(recs, r.ip) {
				k.Props["aRecords"] = append(recs, map[string]any{"ipv4Address": r.ip})
			}

			k.Aux["ip:"+r.ip] = zgID
			k.Etag = uuid.NewString()
			p.touch(zone)
		}
	}
}

func hasA(recs []any, ip string) bool {
	for _, r := range recs {
		m, _ := r.(map[string]any)
		if strOf(m, "ipv4Address") == ip {
			return true
		}
	}

	return false
}

// removeZoneRecords deletes the addresses a zone group put into its zones.
func (p *NetworkProvider) removeZoneRecords(pe *netParent, zg *netObj) {
	zgID := zoneGroupID(pe, zg.Name)

	for _, cfg := range zoneConfigs(zg) {
		zone, ok := p.netRef(strOf(propsOf(cfg), "privateDnsZoneId"), netZones)
		if !ok {
			continue
		}

		kept := zone.Kids["A"][:0]

		for _, k := range zone.Kids["A"] {
			var left []any

			for _, r := range anyList(k.Props["aRecords"]) {
				m, _ := r.(map[string]any)
				ip := strOf(m, "ipv4Address")

				if k.Aux["ip:"+ip] == zgID {
					delete(k.Aux, "ip:"+ip)

					continue
				}

				left = append(left, r)
			}

			if len(left) == 0 && len(anyList(k.Props["aRecords"])) > 0 {
				continue
			}

			k.Props["aRecords"] = left
			kept = append(kept, k)
		}

		zone.Kids["A"] = kept
		p.touch(zone)
	}
}

func (p *NetworkProvider) refreshZoneGroups(pe *netParent) {
	for _, zg := range pe.Kids[peZoneGroups] {
		p.removeZoneRecords(pe, zg)
		p.addZoneRecords(pe, zg)
	}
}

func (p *NetworkProvider) writeZoneGroup(pe *netParent, name string, body map[string]any) (bool, error) {
	cfgs := anyList(propsOf(body)["privateDnsZoneConfigs"])
	if len(cfgs) == 0 {
		return false, netBad("Private DNS zone group %s requires at least one privateDnsZoneConfig.", name)
	}

	norm := make([]any, 0, len(cfgs))
	seen := map[string]bool{}

	for _, c := range cfgs {
		m, _ := c.(map[string]any)
		cn := strOf(m, fieldName)

		if err := validNetName(cn, netMaxName); err != nil {
			return false, err
		}

		if seen[strings.ToLower(cn)] {
			return false, netBad("Private DNS zone config name %s is used more than once.", cn)
		}

		seen[strings.ToLower(cn)] = true

		zid := strOf(propsOf(m), "privateDnsZoneId")
		if _, ok := p.netRef(zid, netZones); !ok {
			return false, refMissing(zid)
		}

		norm = append(norm, map[string]any{fieldName: cn, fieldProperties: map[string]any{"privateDnsZoneId": zid}})
	}

	prev, idx := pe.kid(peZoneGroups, name)
	if prev != nil {
		p.removeZoneRecords(pe, prev)
	}

	o := lbObj(name, map[string]any{"privateDnsZoneConfigs": norm})
	if prev != nil {
		o.GUID = prev.GUID
	}

	if idx >= 0 {
		pe.Kids[peZoneGroups][idx] = o
	} else {
		pe.Kids[peZoneGroups] = append(pe.Kids[peZoneGroups], o)
	}

	p.addZoneRecords(pe, o)
	p.touch(pe)

	return idx < 0, nil
}

func (p *NetworkProvider) bodyZoneGroup(pe *netParent, id ResourceID, zg *netObj) map[string]any {
	zcs := zoneConfigs(zg)
	cfgs := make([]any, 0, len(zcs))

	for _, cfg := range zcs {
		zid := strOf(propsOf(cfg), "privateDnsZoneId")
		zoneName := zid[strings.LastIndex(zid, "/")+1:]
		grouped := map[string][]any{}

		var order []string

		for _, r := range p.peRecords(pe) {
			if _, seen := grouped[r.name]; !seen {
				order = append(order, r.name)
			}

			grouped[r.name] = append(grouped[r.name], r.ip)
		}

		sets := make([]any, 0, len(order))
		for _, n := range order {
			sets = append(sets, map[string]any{
				"recordSetName": n, "recordType": "A", propFQDN: n + "." + zoneName + ".", propTTL: peDNSTTL,
				fieldProvisioningState: netStatusSucceeded, "ipAddresses": grouped[n],
			})
		}

		cfgs = append(cfgs, map[string]any{
			fieldName: strOf(cfg, fieldName),
			fieldProperties: map[string]any{
				"privateDnsZoneId": zid, "recordSets": sets,
			},
		})
	}

	return map[string]any{
		netFieldID: id.ARMID(), fieldName: zg.Name, fieldType: peZoneGroupPath, netFieldEtag: zg.Etag,
		fieldProperties: map[string]any{"privateDnsZoneConfigs": cfgs, fieldProvisioningState: netStatusSucceeded},
	}
}

// afterPERemoved cleans up what a deleted endpoint owned: its zone group records and its network interface.
func (p *NetworkProvider) afterPERemoved(pe *netParent) {
	for _, zg := range pe.Kids[peZoneGroups] {
		p.removeZoneRecords(pe, zg)
	}

	delete(p.items, netKey(netNICs, pe.RG, pe.Obj.Aux[auxPENIC]))
}

func (p *NetworkProvider) peOnSubnet(subnetID string) []string {
	var out []string

	for _, pe := range p.ofType(netPEs) {
		if strings.EqualFold(subID(pe.Obj.Props, netFieldSubnet), subnetID) {
			out = append(out, pe.armID())
		}
	}

	return out
}
