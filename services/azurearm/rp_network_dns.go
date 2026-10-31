package azurearm

import (
	"maps"
	"net/http"
	"net/netip"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"
)

const (
	dnsAllRecords    = "ALL"
	dnsSOA           = "SOA"
	dnsCNAME         = "CNAME"
	dnsApex          = "@"
	dnsMaxLabel      = 63
	dnsMaxName       = 253
	dnsMinZoneLabels = 2
	dnsMaxRecordSets = 25000
	dnsMaxLinks      = 1000
	dnsMaxRegLinks   = 100
	dnsMaxTTL        = 2147483647
	linkTypePath     = "Microsoft.Network/privateDnsZones/virtualNetworkLinks"
	dnsZoneTypePath  = "Microsoft.Network/privateDnsZones"
	dnsCodeBad       = "BadRequest"
	dnsAAAA          = "AAAA"
	dnsPTR           = "PTR"
	dnsSRV           = "SRV"
	dnsTXT           = "TXT"
	soaExpire        = 2419200
	soaMinTTL        = 10
	soaDefaultTTL    = 3600
	soaRetry         = 300
)

//nolint:gochecknoglobals // read-only tables
var (
	dnsRecordTypes = []string{"A", dnsAAAA, dnsCNAME, "MX", dnsPTR, dnsSOA, dnsSRV, dnsTXT}
	dnsLabelOK     = regexp.MustCompile(`^[A-Za-z0-9_*]([A-Za-z0-9_*-]*[A-Za-z0-9_*])?$`)
	dnsZoneLabelOK = regexp.MustCompile(`^[A-Za-z0-9_]([A-Za-z0-9_-]*[A-Za-z0-9_])?$`)
)

func validZoneName(name string) error {
	labels := strings.Split(name, ".")
	ok := len(name) <= dnsMaxName && len(labels) >= dnsMinZoneLabels

	for _, l := range labels {
		ok = ok && len(l) <= dnsMaxLabel && dnsZoneLabelOK.MatchString(l)
	}

	if !ok {
		return netErr(http.StatusBadRequest, netCodeName,
			"The zone name %s is invalid. A private DNS zone name needs at least two labels of letters, digits, "+
				"'-' and '_', each up to 63 characters.", name)
	}

	return nil
}

func validRecordName(name string) bool {
	if name == dnsApex {
		return true
	}

	if len(name) > dnsMaxName {
		return false
	}

	for l := range strings.SplitSeq(name, ".") {
		if len(l) > dnsMaxLabel || !dnsLabelOK.MatchString(l) {
			return false
		}
	}

	return true
}

func defaultSOA() map[string]any {
	return map[string]any{
		"email": "azureprivatedns-host.microsoft.com", "expireTime": soaExpire, "host": "azureprivatedns.net",
		"minimumTtl": soaMinTTL, "refreshTime": soaDefaultTTL, "retryTime": soaRetry, "serialNumber": 1,
	}
}

func (p *NetworkProvider) putZone(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	it, obj, created := p.upsertTop(netZones, id, body)
	obj.Location = netGlobalLocation
	obj.Etag = uuid.NewString()
	obj.Props = map[string]any{}

	if created {
		soa := &netObj{
			Name: dnsApex, Etag: uuid.NewString(), GUID: uuid.NewString(),
			Props: map[string]any{"ttl": soaDefaultTTL, "soaRecord": defaultSOA()},
		}
		it.Kids[dnsSOA] = []*netObj{soa}
	}

	return p.bodyZone(id, it), created, nil
}

func (p *NetworkProvider) bodyZone(id ResourceID, it *netParent) map[string]any {
	sets, links, reg := 0, len(it.Kids[netLinks]), 0

	for _, rt := range dnsRecordTypes {
		sets += len(it.Kids[rt])
	}

	for _, l := range it.Kids[netLinks] {
		if b, _ := l.Props["registrationEnabled"].(bool); b {
			reg++
		}
	}

	props := map[string]any{
		"maxNumberOfRecordSets": dnsMaxRecordSets, "numberOfRecordSets": sets,
		"maxNumberOfVirtualNetworkLinks": dnsMaxLinks, "numberOfVirtualNetworkLinks": links,
		"maxNumberOfVirtualNetworkLinksWithRegistration": dnsMaxRegLinks,
		"numberOfVirtualNetworkLinksWithRegistration":    reg,
		fieldProvisioningState:                           netStatusSucceeded,
	}

	return p.envelope(id, netZones, &it.Obj, props)
}

func recordFQDN(zone, name string) string {
	if name == dnsApex {
		return zone + "."
	}

	return name + "." + zone + "."
}

func normalizeRecord(rt, name string, props map[string]any, prev *netObj) (*netObj, error) {
	if !validRecordName(name) {
		return nil, netErr(http.StatusBadRequest, netCodeName, "The record set name %s is invalid.", name)
	}

	ttl, ok := props["ttl"].(float64)
	if i, isInt := props["ttl"].(int); isInt {
		ttl, ok = float64(i), true
	}

	if !ok || ttl < 0 || ttl > dnsMaxTTL {
		return nil, netErr(http.StatusBadRequest, dnsCodeBad, "The record set %s requires a ttl between 0 and %d.",
			name, dnsMaxTTL)
	}

	out := map[string]any{"ttl": int(ttl)}

	if md, has := props["metadata"]; has {
		out["metadata"] = md
	}

	if err := recordData(rt, name, props, out); err != nil {
		return nil, err
	}

	o := &netObj{Name: name, Etag: uuid.NewString(), GUID: uuid.NewString(), Props: out}
	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

func recordData(rt, name string, props, out map[string]any) error {
	bad := func(format string, a ...any) error { return netErr(http.StatusBadRequest, dnsCodeBad, format, a...) }

	field := map[string]string{
		"A": "aRecords", dnsAAAA: "aaaaRecords", "MX": "mxRecords", dnsPTR: "ptrRecords", dnsSRV: "srvRecords",
		dnsTXT: "txtRecords",
	}[rt]

	switch rt {
	case dnsCNAME:
		c, _ := props["cnameRecord"].(map[string]any)
		if strOf(c, "cname") == "" {
			return bad("The CNAME record set %s requires cnameRecord.cname.", name)
		}

		if name == dnsApex {
			return bad("A CNAME record set cannot be created at the zone apex.")
		}

		out["cnameRecord"] = map[string]any{"cname": strOf(c, "cname")}

		return nil
	case dnsSOA:
		if name != dnsApex {
			return bad("An SOA record set must be named @.")
		}

		soa := defaultSOA()

		maps.Copy(soa, propsMap(props["soaRecord"]))

		out["soaRecord"] = soa

		return nil
	}

	recs := anyList(props[field])
	if len(recs) == 0 {
		return bad("The %s record set %s requires at least one record in %s.", rt, name, field)
	}

	for _, r := range recs {
		if err := checkRecord(rt, propsMap(r)); err != nil {
			return err
		}
	}

	out[field] = recs

	return nil
}

func propsMap(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

func checkRecord(rt string, r map[string]any) error {
	bad := func(what string) error {
		return netErr(http.StatusBadRequest, dnsCodeBad, "Invalid %s record: %s.", rt, what)
	}

	switch rt {
	case "A":
		if a, err := netip.ParseAddr(strOf(r, "ipv4Address")); err != nil || !a.Is4() {
			return bad("ipv4Address must be an IPv4 address")
		}
	case dnsAAAA:
		if a, err := netip.ParseAddr(strOf(r, "ipv6Address")); err != nil || !a.Is6() {
			return bad("ipv6Address must be an IPv6 address")
		}
	case "MX":
		if strOf(r, "exchange") == "" {
			return bad("exchange is required")
		}
	case dnsPTR:
		if strOf(r, "ptrdname") == "" {
			return bad("ptrdname is required")
		}
	case dnsSRV:
		if strOf(r, "target") == "" {
			return bad("target is required")
		}
	case dnsTXT:
		if len(strList(r["value"])) == 0 {
			return bad("value is required")
		}
	}

	return nil
}

func (p *NetworkProvider) writeRecord(zone *netParent, rt, name string, body map[string]any) (bool, error) {
	prev, idx := zone.kid(rt, name)

	o, err := normalizeRecord(rt, name, propsOf(body), prev)
	if err != nil {
		return false, err
	}

	for _, other := range dnsRecordTypes {
		if other == rt {
			continue
		}

		if _, at := zone.kid(other, name); at >= 0 && (rt == dnsCNAME || other == dnsCNAME) && name != dnsApex {
			return false, netErr(http.StatusConflict, "Conflict",
				"The record set %s of type %s conflicts with an existing record set of type %s.", name, rt, other)
		}
	}

	if idx >= 0 {
		zone.Kids[rt][idx] = o
	} else {
		zone.Kids[rt] = append(zone.Kids[rt], o)
	}

	p.touch(zone)

	return idx < 0, nil
}

func (p *NetworkProvider) bodyRecord(zone *netParent, rt string, id ResourceID, k *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, k.Props)

	props["fqdn"] = recordFQDN(zone.Obj.Name, k.Name)
	props["isAutoRegistered"] = false

	return map[string]any{
		netFieldID: id.ARMID(), fieldName: k.Name, fieldType: dnsZoneTypePath + "/" + rt,
		netFieldEtag: k.Etag, fieldProperties: props,
	}
}

func (p *NetworkProvider) listAllRecords(zone *netParent, id ResourceID) []map[string]any {
	out := []map[string]any{}

	for _, rt := range dnsRecordTypes {
		kids := append([]*netObj{}, zone.Kids[rt]...)
		sort.Slice(kids, func(i, j int) bool { return kids[i].Name < kids[j].Name })

		for _, k := range kids {
			out = append(out, p.bodyRecord(zone, rt, kidID(id, rt, k.Name), k))
		}
	}

	return out
}

func (p *NetworkProvider) writeLink(zone *netParent, name string, body map[string]any) (bool, error) {
	props := propsOf(body)

	vid := subID(props, "virtualNetwork")
	if vid == "" {
		return false, netBad("Virtual network link %s requires properties.virtualNetwork.id.", name)
	}

	if _, ok := p.netRef(vid, netVNets); !ok {
		return false, refMissing(vid)
	}

	prev, idx := zone.kid(netLinks, name)
	out := takeProps(body, "virtualNetworkLinkState", fieldProvisioningState)
	out["virtualNetwork"] = map[string]any{netFieldID: vid}

	if _, set := out["registrationEnabled"].(bool); !set {
		out["registrationEnabled"] = false
	}

	o := &netObj{
		Name: name, Etag: uuid.NewString(), GUID: uuid.NewString(), Props: out, Tags: stringTags(body[fieldTags]),
		Location: netGlobalLocation,
	}
	if prev != nil {
		o.GUID = prev.GUID
	}

	if idx >= 0 {
		zone.Kids[netLinks][idx] = o
	} else {
		zone.Kids[netLinks] = append(zone.Kids[netLinks], o)
	}

	p.touch(zone)

	return idx < 0, nil
}

func (p *NetworkProvider) bodyLink(_ *netParent, id ResourceID, k *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, k.Props)

	props["virtualNetworkLinkState"] = "Completed"
	props[fieldProvisioningState] = netStatusSucceeded

	out := map[string]any{
		netFieldID: id.ARMID(), fieldName: k.Name, fieldType: linkTypePath, netFieldEtag: k.Etag,
		fieldLocation: netGlobalLocation, fieldProperties: props,
	}

	if k.Tags != nil {
		out[fieldTags] = k.Tags
	}

	return out
}
