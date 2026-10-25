package gcpdns

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/miekg/dns"
	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxTTL         = 2147483647
	typeSOA        = "SOA"
	typeNS         = "NS"
	typeCNAME      = "CNAME"
	rrsetKind      = "dns#resourceRecordSet"
	rrsetPathCh    = "entity.change"
	rrsetPathRRSet = "entity.resourceRecordSet"
)

var nameTargetTypes = map[string]bool{ //nolint:gochecknoglobals // immutable catalog
	"CNAME": true, "NS": true, "PTR": true, "MX": true, "SRV": true, "DNAME": true,
}

func rrKey(name, typ string) string { return canon(name) + "|" + strings.ToUpper(typ) }

// normRRSet copies rr with a canonical name, upper-case type and kind.
func normRRSet(rr *dnsapi.ResourceRecordSet) *dnsapi.ResourceRecordSet {
	out := clone(rr)
	out.Kind = rrsetKind
	out.Name = canon(out.Name)
	out.Type = strings.ToUpper(out.Type)

	return out
}

func sameData(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)

	return slices.Equal(x, y)
}

func sameRRSet(a, b *dnsapi.ResourceRecordSet) bool {
	if a.Ttl != b.Ttl || !sameData(a.Rrdatas, b.Rrdatas) || !sameData(a.SignatureRrdatas, b.SignatureRrdatas) {
		return false
	}

	ar, _ := json.Marshal(a.RoutingPolicy)
	br, _ := json.Marshal(b.RoutingPolicy)

	return string(ar) == string(br)
}

func validateRData(name, typ string, ttl int64, data []string, path string) error {
	for i, d := range data {
		p := fmt.Sprintf("%s.rrdatas[%d]", path, i)

		if strings.TrimSpace(d) == "" {
			return errInvalidValue(p, d)
		}

		if (typ == "TXT" || typ == "SPF") && !strings.HasPrefix(strings.TrimSpace(d), `"`) {
			return errInvalid("Invalid value for '%s': '%s' (text data must be quoted)", p, d)
		}

		if nameTargetTypes[typ] {
			fields := strings.Fields(d)
			if len(fields) == 0 || !strings.HasSuffix(fields[len(fields)-1], ".") {
				return errInvalid("Invalid value for '%s': '%s' (domain names must be fully qualified)", p, d)
			}
		}

		rr, err := dns.NewRR(fmt.Sprintf("%s %d IN %s %s", name, ttl, typ, d))
		if err != nil || rr == nil || rr.Header().Rrtype != dns.StringToType[typ] {
			return errInvalidValue(p, d)
		}
	}

	return nil
}

func routingKinds(rp *dnsapi.RRSetRoutingPolicy) int {
	n := 0

	for _, set := range []bool{rp.Wrr != nil, rp.Geo != nil, rp.PrimaryBackup != nil} {
		if set {
			n++
		}
	}

	return n
}

func wrrData(w *dnsapi.RRSetRoutingPolicyWrrPolicy, path string) ([][]string, error) {
	w.Kind = "dns#rRSetRoutingPolicyWrrPolicy"

	if len(w.Items) > maxRouting {
		return nil, errQuota("Quota 'ITEMS_PER_ROUTING_POLICY' exceeded.")
	}

	sets := make([][]string, 0, len(w.Items))

	for _, it := range w.Items {
		it.Kind = "dns#rRSetRoutingPolicyWrrPolicyWrrPolicyItem"
		if it.Weight < 0 {
			return nil, errInvalidValue(
				path+".routingPolicy.wrr.items.weight",
				strconv.FormatFloat(it.Weight, 'f', -1, 64),
			)
		}

		sets = append(sets, it.Rrdatas)
	}

	return sets, nil
}

func geoData(g *dnsapi.RRSetRoutingPolicyGeoPolicy, path string) ([][]string, error) {
	g.Kind = "dns#rRSetRoutingPolicyGeoPolicy"

	if len(g.Items) > maxRouting {
		return nil, errQuota("Quota 'ITEMS_PER_ROUTING_POLICY' exceeded.")
	}

	sets := make([][]string, 0, len(g.Items))

	for _, it := range g.Items {
		it.Kind = "dns#rRSetRoutingPolicyGeoPolicyGeoPolicyItem"
		if it.Location == "" {
			return nil, errRequired(path + ".routingPolicy.geo.items.location")
		}

		sets = append(sets, it.Rrdatas)
	}

	return sets, nil
}

func validateRouting(rp *dnsapi.RRSetRoutingPolicy, name, typ string, ttl int64, path string) error {
	rp.Kind = "dns#rRSetRoutingPolicy"

	if routingKinds(rp) != 1 {
		return errInvalid("Invalid value for '%s.routingPolicy': exactly one of wrr, geo or primaryBackup", path)
	}

	var sets [][]string

	if rp.Wrr != nil {
		w, err := wrrData(rp.Wrr, path)
		if err != nil {
			return err
		}

		sets = append(sets, w...)
	}

	if rp.Geo != nil {
		g, err := geoData(rp.Geo, path)
		if err != nil {
			return err
		}

		sets = append(sets, g...)
	}

	for _, s := range sets {
		if err := validateRData(name, typ, ttl, s, path+".routingPolicy"); err != nil {
			return err
		}
	}

	return nil
}

func validateRRSetHeader(zone *dnsapi.ManagedZone, rr *dnsapi.ResourceRecordSet, path string) error {
	if rr.Name == "" || rr.Name == "." {
		return errRequired(path + ".name")
	}

	if !isDomain(rr.Name) || !dns.IsSubDomain(zone.DnsName, rr.Name) {
		return errInvalidValue(path+".name", rr.Name)
	}

	if rr.Type == "" {
		return errRequired(path + ".type")
	}

	if _, ok := dns.StringToType[rr.Type]; !ok || rr.Type == "ANY" || rr.Type == "AXFR" {
		return errInvalidValue(path+".type", rr.Type)
	}

	if rr.Ttl < 0 || rr.Ttl > maxTTL {
		return errInvalidValue(path+".ttl", strconv.FormatInt(rr.Ttl, 10))
	}

	return nil
}

// validateRRSet checks rr (already normalised) against zone and fills defaults.
func validateRRSet(zone *dnsapi.ManagedZone, rr *dnsapi.ResourceRecordSet, path string) error {
	if err := validateRRSetHeader(zone, rr, path); err != nil {
		return err
	}

	if rr.Ttl == 0 {
		rr.Ttl = defaultTTL
	}

	if len(rr.Rrdatas) == 0 && rr.RoutingPolicy == nil {
		return errRequired(path + ".rrdata")
	}

	if len(rr.Rrdatas) > 0 && rr.RoutingPolicy != nil {
		return errInvalid("Invalid value for '%s.rrdatas': not allowed together with routingPolicy", path)
	}

	if len(rr.Rrdatas) > maxRRDatas {
		return errQuota("Quota 'RESOURCE_RECORDS_PER_RRSET' exceeded.")
	}

	apex := rr.Name == zone.DnsName

	switch {
	case rr.Type == typeCNAME && (apex || len(rr.Rrdatas) > 1):
		return errInvalid("Invalid value for '%s': CNAME needs exactly one rrdata and cannot be at the apex", path)
	case rr.Type == typeSOA && (!apex || len(rr.Rrdatas) != 1):
		return errInvalid("Invalid value for '%s': SOA is a single record at the zone apex", path)
	}

	if rr.RoutingPolicy != nil {
		return validateRouting(rr.RoutingPolicy, rr.Name, rr.Type, rr.Ttl, path)
	}

	return validateRData(rr.Name, rr.Type, rr.Ttl, rr.Rrdatas, path)
}

// canonicalLess orders names by reversed labels, then by type.
func rrOrder(a, b *dnsapi.ResourceRecordSet) int {
	if c := strings.Compare(reverseLabels(a.Name), reverseLabels(b.Name)); c != 0 {
		return c
	}

	return strings.Compare(a.Type, b.Type)
}

func reverseLabels(name string) string {
	labels := dns.SplitDomainName(name)
	slices.Reverse(labels)

	return strings.Join(labels, ".")
}

// GetRRSet returns one record set.
func (b *InMemoryBackend) GetRRSet(project, zone, name, typ string) (*dnsapi.ResourceRecordSet, error) {
	b.mu.RLock("GetRRSet")
	defer b.mu.RUnlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return nil, err
	}

	rr, ok := rec.RRSets[rrKey(name, typ)]
	if !ok {
		return nil, errNotFound("parameters.name", canon(name)+" ("+strings.ToUpper(typ)+")")
	}

	return clone(rr), nil
}

// ListRRSets returns a page of record sets in canonical order, optionally filtered by name and type.
func (b *InMemoryBackend) ListRRSets(
	project, zone, name, typ, token string, limit int,
) (page.Page[*dnsapi.ResourceRecordSet], error) {
	var none page.Page[*dnsapi.ResourceRecordSet]

	if typ != "" && name == "" {
		return none, errInvalid("Invalid value for 'type': name must be specified when type is specified")
	}

	if err := page.ValidateToken(token); err != nil {
		return none, errInvalidValue("pageToken", token)
	}

	b.mu.RLock("ListRRSets")
	defer b.mu.RUnlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return none, err
	}

	var all []*dnsapi.ResourceRecordSet

	for _, rr := range rec.RRSets {
		if name != "" && rr.Name != canon(name) || typ != "" && rr.Type != strings.ToUpper(typ) {
			continue
		}

		all = append(all, clone(rr))
	}

	slices.SortFunc(all, rrOrder)

	return page.New(all, token, limit, pageSize), nil
}

// CreateRRSet adds a record set as a one-addition change.
func (b *InMemoryBackend) CreateRRSet(
	project, zone string, in *dnsapi.ResourceRecordSet,
) (*dnsapi.ResourceRecordSet, error) {
	b.mu.Lock("CreateRRSet")
	defer b.mu.Unlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return nil, err
	}

	ch, err := b.applyChange(rec, &dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{in}}, rrsetPathRRSet)
	if err != nil {
		return nil, err
	}

	return clone(ch.Additions[0]), nil
}

// PatchRRSet merges patch into an existing record set.
func (b *InMemoryBackend) PatchRRSet(
	project, zone, name, typ string, patch map[string]any,
) (*dnsapi.ResourceRecordSet, error) {
	b.mu.Lock("PatchRRSet")
	defer b.mu.Unlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return nil, err
	}

	cur, ok := rec.RRSets[rrKey(name, typ)]
	if !ok {
		return nil, errNotFound("parameters.name", canon(name)+" ("+strings.ToUpper(typ)+")")
	}

	merged, err := patched(cur, patch)
	if err != nil {
		return nil, err
	}

	merged.Name, merged.Type = cur.Name, cur.Type

	if _, hasRouting := patch["routingPolicy"]; hasRouting && len(merged.Rrdatas) > 0 {
		if _, hasData := patch["rrdatas"]; !hasData {
			merged.Rrdatas = nil
		}
	}

	ch, err := b.applyChange(rec, &dnsapi.Change{
		Deletions: []*dnsapi.ResourceRecordSet{cur}, Additions: []*dnsapi.ResourceRecordSet{merged},
	}, rrsetPathRRSet)
	if err != nil {
		return nil, err
	}

	for _, a := range ch.Additions {
		if rrKey(a.Name, a.Type) == rrKey(name, typ) {
			return clone(a), nil
		}
	}

	return clone(merged), nil
}

// DeleteRRSet removes a record set as a one-deletion change.
func (b *InMemoryBackend) DeleteRRSet(project, zone, name, typ string) error {
	b.mu.Lock("DeleteRRSet")
	defer b.mu.Unlock()

	rec, err := b.zoneLocked(project, zone)
	if err != nil {
		return err
	}

	cur, ok := rec.RRSets[rrKey(name, typ)]
	if !ok {
		return errNotFound("parameters.name", canon(name)+" ("+strings.ToUpper(typ)+")")
	}

	_, err = b.applyChange(rec, &dnsapi.Change{Deletions: []*dnsapi.ResourceRecordSet{cur}}, rrsetPathRRSet)

	return err
}
