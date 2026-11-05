package ocicore

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"slices"
	"time"
)

const (
	relContains       = "CONTAINS"
	relAssociatedWith = "ASSOCIATED_WITH"
	relRoutesTo       = "ROUTES_TO"

	topoVcn        = "VCN"
	topoSubnet     = "SUBNET"
	topoNetworking = "NETWORKING"
	etagLen        = 32
)

type topoAssociation struct {
	Via []string `json:"via,omitempty"`
}

type topoRoute struct {
	DestinationType string `json:"destinationType"`
	Destination     string `json:"destination"`
	RouteTableID    string `json:"routeTableId"`
	RouteType       string `json:"routeType,omitempty"`
}

type topoRel struct {
	Association *topoAssociation `json:"associatedWithDetails,omitempty"`
	Route       *topoRoute       `json:"routeRuleDetails,omitempty"`
	ID1         string           `json:"id1"`
	ID2         string           `json:"id2"`
	Type        string           `json:"type"`
}

// TopologyOut is the VcnTopology, SubnetTopology and NetworkingTopology response.
type TopologyOut struct {
	TimeCreated     time.Time        `json:"timeCreated"`
	Type            string           `json:"type"`
	VcnID           string           `json:"vcnId,omitempty"`
	SubnetID        string           `json:"subnetId,omitempty"`
	Entities        []map[string]any `json:"entities"`
	Relationships   []topoRel        `json:"relationships"`
	LimitedEntities []string         `json:"limitedEntities"`
}

type topoBuilder struct {
	b        *InMemoryBackend
	seen     map[string]bool
	relSeen  map[string]bool
	entities []map[string]any
	rels     []topoRel
}

func (t *topoBuilder) entity(r resource) {
	id := r.base().ID
	if t.seen[id] {
		return
	}

	t.seen[id] = true

	raw, err := json.Marshal(r.view())
	if err != nil {
		return
	}

	m := map[string]any{}
	if json.Unmarshal(raw, &m) != nil {
		return
	}

	m["entityType"] = ocidKind(id)
	t.entities = append(t.entities, m)
}

func (t *topoBuilder) rel(r topoRel) {
	key := r.Type + "|" + r.ID1 + "|" + r.ID2
	if r.Route != nil {
		key += "|" + r.Route.Destination
	}

	if t.relSeen[key] {
		return
	}

	t.relSeen[key] = true
	t.rels = append(t.rels, r)
}

func (t *topoBuilder) contains(parent, child resource) {
	t.entity(child)
	t.rel(topoRel{Type: relContains, ID1: parent.base().ID, ID2: child.base().ID})
}

func (t *topoBuilder) associate(from, to resource, via ...string) {
	t.entity(to)

	r := topoRel{Type: relAssociatedWith, ID1: from.base().ID, ID2: to.base().ID}
	if len(via) > 0 {
		r.Association = &topoAssociation{Via: via}
	}

	t.rel(r)
}

// routeTarget resolves the entity a route rule sends traffic to.
func (t *topoBuilder) routeTarget(id string) resource {
	if r, ok := t.b.res[id]; ok && live(r) {
		if p, isIP := r.(*PrivateIP); isIP {
			if v, found := t.b.res[p.VnicID]; found {
				return v
			}
		}

		return r
	}

	return nil
}

func (t *topoBuilder) routes(rt *RouteTable) {
	for _, rule := range rt.RouteRules {
		target := t.routeTarget(rule.NetworkEntityID)
		if target == nil {
			continue
		}

		t.entity(target)
		t.rel(topoRel{Type: relRoutesTo, ID1: rt.ID, ID2: target.base().ID, Route: &topoRoute{
			DestinationType: rule.DestinationType, Destination: rule.Destination, RouteTableID: rt.ID,
			RouteType: rule.RouteType,
		}})
	}
}

// subnetParts adds a subnet, what it uses and what runs in it.
func (t *topoBuilder) subnetParts(v *Vcn, s *Subnet) {
	t.contains(v, s)

	if rt, ok := t.b.res[s.RouteTableID].(*RouteTable); ok && live(rt) {
		t.associate(s, rt)
		t.routes(rt)
	}

	if d, ok := t.b.res[s.DhcpOptionsID].(*DhcpOptions); ok && live(d) {
		t.associate(s, d)
	}

	for _, id := range s.SecurityListIDs {
		if sl, ok := t.b.res[id].(*SecurityList); ok && live(sl) {
			t.associate(s, sl)
		}
	}

	for _, vn := range liveOf(t.b, func(x *Vnic) bool { return x.SubnetID == s.ID }) {
		t.contains(s, vn)
		t.vnicParts(vn)
	}
}

// vnicParts adds the NSGs a VNIC is in and the instance it is attached to.
func (t *topoBuilder) vnicParts(vn *Vnic) {
	for _, id := range vn.NsgIDs {
		if n, ok := t.b.res[id].(*NetworkSecurityGroup); ok && live(n) {
			t.associate(vn, n)
		}
	}

	for _, a := range liveOf(t.b, func(x *VnicAttachment) bool { return x.VnicID == vn.ID }) {
		if inst, ok := t.b.res[a.InstanceID].(*Instance); ok && live(inst) {
			t.entity(inst)
			t.rel(topoRel{Type: relContains, ID1: inst.ID, ID2: vn.ID})
		}
	}
}

// vcnParts adds a VCN with everything inside it and the DRG it is attached to.
func (t *topoBuilder) vcnParts(v *Vcn) {
	t.entity(v)

	subnets := liveOf(t.b, inVcn[*Subnet](v.ID))
	sortRes(subnets, ListOpts{Order: orderAsc})

	for _, s := range subnets {
		t.subnetParts(v, s)
	}

	t.vcnChildren(v)

	if a := t.b.vcnDrgAttachment(v.ID); a != nil {
		if drg, ok := t.b.res[a.DrgID].(*Drg); ok && live(drg) {
			t.entity(a)
			t.associate(v, drg, a.ID)
		}
	}
}

func (t *topoBuilder) vcnChildren(v *Vcn) {
	var kids []resource

	for _, r := range t.b.res {
		if r.vcnRef() != v.ID || !live(r) {
			continue
		}

		switch r.(type) {
		case *RouteTable, *SecurityList, *NetworkSecurityGroup, *DhcpOptions, *InternetGateway, *NatGateway,
			*ServiceGateway, *LocalPeeringGateway:
			kids = append(kids, r)
		}
	}

	sortByCreation(kids)

	for _, k := range kids {
		t.contains(v, k)

		if rt, ok := k.(*RouteTable); ok {
			t.routes(rt)
		}
	}
}

func sortByCreation(rs []resource) {
	slices.SortFunc(rs, func(a, c resource) int { return cmp.Compare(a.base().Internal.Seq, c.base().Internal.Seq) })
}

func (b *InMemoryBackend) topoOf(kind string, build func(*topoBuilder)) TopologyOut {
	t := &topoBuilder{b: b, seen: map[string]bool{}, relSeen: map[string]bool{}}
	build(t)

	if t.entities == nil {
		t.entities = []map[string]any{}
	}

	if t.rels == nil {
		t.rels = []topoRel{}
	}

	return TopologyOut{
		TimeCreated: b.nowUTC(), Type: kind, Entities: t.entities, Relationships: t.rels, LimitedEntities: []string{},
	}
}

// GetVcnTopology describes a VCN, its contents and its DRG attachment.
func (b *InMemoryBackend) GetVcnTopology(compartment, vcnID string) (TopologyOut, *APIError) {
	b.mu.RLock("GetVcnTopology")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(compartment); aerr != nil {
		return TopologyOut{}, aerr
	}

	v, aerr := get[*Vcn](b, vcnID)
	if aerr != nil {
		return TopologyOut{}, aerr
	}

	out := b.topoOf(topoVcn, func(t *topoBuilder) { t.vcnParts(v) })
	out.VcnID = v.ID

	return out, nil
}

// GetSubnetTopology describes a subnet, its VCN, what it uses and what runs in it.
func (b *InMemoryBackend) GetSubnetTopology(compartment, subnetID string) (TopologyOut, *APIError) {
	b.mu.RLock("GetSubnetTopology")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(compartment); aerr != nil {
		return TopologyOut{}, aerr
	}

	s, aerr := get[*Subnet](b, subnetID)
	if aerr != nil {
		return TopologyOut{}, aerr
	}

	v, aerr := get[*Vcn](b, s.VcnID)
	if aerr != nil {
		return TopologyOut{}, aerr
	}

	out := b.topoOf(topoSubnet, func(t *topoBuilder) {
		t.entity(v)
		t.subnetParts(v, s)
	})
	out.SubnetID = s.ID

	return out, nil
}

// GetNetworkingTopology describes every VCN of a compartment.
func (b *InMemoryBackend) GetNetworkingTopology(compartment string) (TopologyOut, *APIError) {
	b.mu.RLock("GetNetworkingTopology")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(compartment); aerr != nil {
		return TopologyOut{}, aerr
	}

	vcns := liveOf(b, func(v *Vcn) bool { return v.CompartmentID == compartment })
	sortRes(vcns, ListOpts{Order: orderAsc})

	return b.topoOf(topoNetworking, func(t *topoBuilder) {
		for _, v := range vcns {
			t.vcnParts(v)
		}
	}), nil
}

// etag identifies the topology content independent of when it was read.
func (o TopologyOut) etag() string {
	raw, err := json.Marshal([]any{o.Type, o.VcnID, o.SubnetID, o.Entities, o.Relationships})
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:])[:etagLen]
}

func topologyEntries() []entry {
	return []entry{
		{topologyOp(func(h *Handler, q *req) (TopologyOut, *APIError) {
			return h.Backend.GetVcnTopology(q.query("compartmentId"), q.query("vcnId"))
		}), http.MethodGet, "/vcnTopology", "GetVcnTopology"},
		{topologyOp(func(h *Handler, q *req) (TopologyOut, *APIError) {
			return h.Backend.GetSubnetTopology(q.query("compartmentId"), q.query("subnetId"))
		}), http.MethodGet, "/subnetTopology", "GetSubnetTopology"},
		{topologyOp(func(h *Handler, q *req) (TopologyOut, *APIError) {
			return h.Backend.GetNetworkingTopology(q.query("compartmentId"))
		}), http.MethodGet, "/networkingTopology", "GetNetworkingTopology"},
	}
}

func topologyOp(fn func(*Handler, *req) (TopologyOut, *APIError)) opFunc {
	return func(h *Handler, q *req) error {
		switch q.query("accessLevel") {
		case "", "ANY", "RESTRICTED":
		default:
			return errParam("accessLevel must be ANY or RESTRICTED")
		}

		out, aerr := fn(h, q)
		if aerr != nil {
			return aerr
		}

		etag := out.etag()
		if match := q.r.Header.Get("If-None-Match"); match != "" && match == etag {
			q.header("etag", etag)
			q.w.WriteHeader(http.StatusNotModified)

			return nil
		}

		return q.json(http.StatusOK, out, etag)
	}
}
