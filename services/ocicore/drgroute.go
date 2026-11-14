package ocicore

import (
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	routeStatic  = "STATIC"
	routeDynamic = "DYNAMIC"
	provVcn      = "VCN"
	provStatic   = "STATIC"
	ruleIDBytes  = 26
)

// CreateDrgRouteTableIn is the CreateDrgRouteTable request body.
type CreateDrgRouteTableIn struct {
	TagsIn
	IsEcmpEnabled                *bool  `json:"isEcmpEnabled"`
	DrgID                        string `json:"drgId"`
	DisplayName                  string `json:"displayName"`
	ImportDrgRouteDistributionID string `json:"importDrgRouteDistributionId"`
}

func (b *InMemoryBackend) importDistribution(id, drgID string) *APIError {
	d, aerr := getLive[*DrgRouteDistribution](b, id)
	if aerr != nil {
		return aerr
	}

	if d.DrgID != drgID || d.DistributionType != distImport {
		return errParam("importDrgRouteDistributionId must name an import distribution of the same DRG")
	}

	return nil
}

// CreateDrgRouteTable creates a DRG route table.
func (b *InMemoryBackend) CreateDrgRouteTable(in CreateDrgRouteTableIn) (Resource, string, *APIError) {
	if in.DrgID == "" {
		return nil, "", errMissing("drgId is required")
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	drg, aerr := getLive[*Drg](b, in.DrgID)
	if aerr != nil {
		return nil, "", aerr
	}

	if in.ImportDrgRouteDistributionID != "" {
		if aerr = b.importDistribution(in.ImportDrgRouteDistributionID, drg.ID); aerr != nil {
			return nil, "", aerr
		}
	}

	t := &DrgRouteTable{
		Base: b.newBase(kindDrgRouteTable, drg.CompartmentID, in.DisplayName, in.TagsIn), DrgID: drg.ID,
		ImportDrgRouteDistributionID: in.ImportDrgRouteDistributionID, IsEcmpEnabled: flag(in.IsEcmpEnabled),
	}
	b.add(t)

	return t.view(), t.Internal.Etag, nil
}

// UpdateDrgRouteTableIn is the UpdateDrgRouteTable request body.
type UpdateDrgRouteTableIn struct {
	ImportDrgRouteDistributionID *string `json:"importDrgRouteDistributionId"`
	IsEcmpEnabled                *bool   `json:"isEcmpEnabled"`
	UpdateIn
}

// UpdateDrgRouteTable changes a DRG route table.
func (b *InMemoryBackend) UpdateDrgRouteTable(
	id, ifMatch string,
	in UpdateDrgRouteTableIn,
) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(t *DrgRouteTable) *APIError {
		if in.ImportDrgRouteDistributionID != nil && *in.ImportDrgRouteDistributionID != "" {
			if aerr := b.importDistribution(*in.ImportDrgRouteDistributionID, t.DrgID); aerr != nil {
				return aerr
			}

			t.ImportDrgRouteDistributionID = *in.ImportDrgRouteDistributionID
		}

		if in.IsEcmpEnabled != nil {
			t.IsEcmpEnabled = *in.IsEcmpEnabled
		}

		return nil
	})
}

// RemoveImportDrgRouteDistribution clears the import distribution of a DRG route table.
func (b *InMemoryBackend) RemoveImportDrgRouteDistribution(id, ifMatch string) (Resource, string, *APIError) {
	b.mu.Lock("RemoveImportDrgRouteDistribution")
	defer b.mu.Unlock()

	t, aerr := getLive[*DrgRouteTable](b, id)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(t, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	t.ImportDrgRouteDistributionID = ""
	b.bump(t)

	return t.view(), t.Internal.Etag, nil
}

// RemoveExportDrgRouteDistribution stops an attachment from advertising routes.
func (b *InMemoryBackend) RemoveExportDrgRouteDistribution(id, ifMatch string) (Resource, string, *APIError) {
	b.mu.Lock("RemoveExportDrgRouteDistribution")
	defer b.mu.Unlock()

	a, aerr := getLive[*DrgAttachment](b, id)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(a, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	a.ExportDrgRouteDistributionID = ""
	b.bump(a)

	return a.view(), a.Internal.Etag, nil
}

// CreateDrgRouteDistributionIn is the CreateDrgRouteDistribution request body.
type CreateDrgRouteDistributionIn struct {
	TagsIn
	DrgID            string `json:"drgId"`
	DisplayName      string `json:"displayName"`
	DistributionType string `json:"distributionType"`
}

// CreateDrgRouteDistribution creates an import route distribution; the export one is created with the DRG.
func (b *InMemoryBackend) CreateDrgRouteDistribution(in CreateDrgRouteDistributionIn) (Resource, string, *APIError) {
	if in.DrgID == "" {
		return nil, "", errMissing("drgId is required")
	}

	if in.DistributionType != distImport {
		return nil, "", errParam("distributionType must be IMPORT: the export distribution is created with the DRG")
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	drg, aerr := getLive[*Drg](b, in.DrgID)
	if aerr != nil {
		return nil, "", aerr
	}

	d := &DrgRouteDistribution{
		Base: b.newBase(kindDrgDist, drg.CompartmentID, in.DisplayName, in.TagsIn), DrgID: drg.ID,
		DistributionType: distImport,
	}
	b.add(d)

	return d.view(), d.Internal.Etag, nil
}

// UpdateDrgRouteDistribution changes a route distribution's name and tags.
func (b *InMemoryBackend) UpdateDrgRouteDistribution(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*DrgRouteDistribution) *APIError { return nil })
}

func (b *InMemoryBackend) checkMatchCriteria(drgID string, mc []DrgMatchCriteria) *APIError {
	for _, c := range mc {
		switch c.MatchType {
		case matchAll:
		case matchType:
			if !slices.Contains([]string{attVcn, attVC, attRPC, attIpsec}, c.AttachmentType) {
				return errParam(
					"attachmentType must be VCN, VIRTUAL_CIRCUIT, REMOTE_PEERING_CONNECTION or IPSEC_TUNNEL",
				)
			}
		case matchID:
			a, aerr := getLive[*DrgAttachment](b, c.DrgAttachmentID)
			if aerr != nil {
				return aerr
			}

			if a.DrgID != drgID {
				return errParam("The DRG attachment " + a.ID + " belongs to a different DRG")
			}
		default:
			return errParam("matchType must be MATCH_ALL, DRG_ATTACHMENT_TYPE or DRG_ATTACHMENT_ID")
		}
	}

	return nil
}

func checkPriority(p int, taken []DrgStatement, selfID string) *APIError {
	if p < 0 || p > maxStatementPri {
		return errParam("priority must be between 0 and 65535")
	}

	if slices.ContainsFunc(taken, func(s DrgStatement) bool { return s.Priority == p && s.ID != selfID }) {
		return errConflict("The priority " + strconv.Itoa(p) + " is already used in this route distribution")
	}

	return nil
}

// DrgStatementIn is a statement of AddDrgRouteDistributionStatements.
type DrgStatementIn struct {
	Priority      *int               `json:"priority"`
	Action        string             `json:"action"`
	MatchCriteria []DrgMatchCriteria `json:"matchCriteria"`
}

// AddDrgStatementsIn is the AddDrgRouteDistributionStatements request body.
type AddDrgStatementsIn struct {
	Statements []DrgStatementIn `json:"statements"`
}

// AddDrgRouteDistributionStatements adds statements to a distribution, all or nothing.
func (b *InMemoryBackend) AddDrgRouteDistributionStatements(
	id string,
	in AddDrgStatementsIn,
) ([]DrgStatement, *APIError) {
	b.mu.Lock("AddDrgRouteDistributionStatements")
	defer b.mu.Unlock()

	d, aerr := getLive[*DrgRouteDistribution](b, id)
	if aerr != nil {
		return nil, aerr
	}

	if len(in.Statements) == 0 {
		return nil, errMissing("statements is required")
	}

	next := slices.Clone(d.Statements)
	added := make([]DrgStatement, 0, len(in.Statements))

	for _, s := range in.Statements {
		if s.Priority == nil {
			return nil, errMissing("priority is required for every statement")
		}

		if s.Action != actionAccept {
			return nil, errParam("action must be ACCEPT")
		}

		if aerr = checkPriority(*s.Priority, next, ""); aerr != nil {
			return nil, aerr
		}

		if aerr = b.checkMatchCriteria(d.DrgID, s.MatchCriteria); aerr != nil {
			return nil, aerr
		}

		st := DrgStatement{
			ID: newOCID(kindDrgStatement), Action: s.Action, Priority: *s.Priority,
			MatchCriteria: nonNilCriteria(s.MatchCriteria),
		}
		next, added = append(next, st), append(added, st)
	}

	d.Statements = next
	b.bump(d)

	return added, nil
}

func nonNilCriteria(c []DrgMatchCriteria) []DrgMatchCriteria {
	if c == nil {
		return []DrgMatchCriteria{}
	}

	return c
}

// UpdateDrgStatementIn is a statement of UpdateDrgRouteDistributionStatements.
type UpdateDrgStatementIn struct {
	Priority      *int                `json:"priority"`
	MatchCriteria *[]DrgMatchCriteria `json:"matchCriteria"`
	ID            string              `json:"id"`
}

// UpdateDrgStatementsIn is the UpdateDrgRouteDistributionStatements request body.
type UpdateDrgStatementsIn struct {
	Statements []UpdateDrgStatementIn `json:"statements"`
}

// UpdateDrgRouteDistributionStatements changes statements by id, all or nothing.
func (b *InMemoryBackend) UpdateDrgRouteDistributionStatements(
	id string, in UpdateDrgStatementsIn,
) ([]DrgStatement, *APIError) {
	b.mu.Lock("UpdateDrgRouteDistributionStatements")
	defer b.mu.Unlock()

	d, aerr := getLive[*DrgRouteDistribution](b, id)
	if aerr != nil {
		return nil, aerr
	}

	next := slices.Clone(d.Statements)
	changed := make([]DrgStatement, 0, len(in.Statements))

	for _, u := range in.Statements {
		i := slices.IndexFunc(next, func(s DrgStatement) bool { return s.ID == u.ID })
		if i < 0 {
			return nil, errNotFound(u.ID)
		}

		if u.Priority != nil {
			if aerr = checkPriority(*u.Priority, next, u.ID); aerr != nil {
				return nil, aerr
			}

			next[i].Priority = *u.Priority
		}

		if u.MatchCriteria != nil {
			if aerr = b.checkMatchCriteria(d.DrgID, *u.MatchCriteria); aerr != nil {
				return nil, aerr
			}

			next[i].MatchCriteria = nonNilCriteria(*u.MatchCriteria)
		}

		changed = append(changed, next[i])
	}

	d.Statements = next
	b.bump(d)

	return changed, nil
}

// RemoveDrgRouteDistributionStatements removes statements by id, all or nothing.
func (b *InMemoryBackend) RemoveDrgRouteDistributionStatements(id string, ids []string) *APIError {
	b.mu.Lock("RemoveDrgRouteDistributionStatements")
	defer b.mu.Unlock()

	d, aerr := getLive[*DrgRouteDistribution](b, id)
	if aerr != nil {
		return aerr
	}

	for _, sid := range ids {
		if !slices.ContainsFunc(d.Statements, func(s DrgStatement) bool { return s.ID == sid }) {
			return errNotFound(sid)
		}
	}

	d.Statements = slices.DeleteFunc(slices.Clone(d.Statements), func(s DrgStatement) bool {
		return slices.Contains(ids, s.ID)
	})
	b.bump(d)

	return nil
}

// ListDrgRouteDistributionStatements lists statements by priority.
func (b *InMemoryBackend) ListDrgRouteDistributionStatements(
	id, order, token string, limit int,
) (page.Page[DrgStatement], *APIError) {
	b.mu.RLock("ListDrgRouteDistributionStatements")
	defer b.mu.RUnlock()

	d, aerr := get[*DrgRouteDistribution](b, id)
	if aerr != nil {
		return page.Page[DrgStatement]{}, aerr
	}

	all := slices.Clone(d.Statements)
	slices.SortFunc(all, func(a, c DrgStatement) int { return a.Priority - c.Priority })

	if strings.EqualFold(order, "DESC") {
		slices.Reverse(all)
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// DrgRuleWire is a DRG route table rule as returned by the API.
type DrgRuleWire struct {
	Destination            string `json:"destination"`
	DestinationType        string `json:"destinationType"`
	NextHopDrgAttachmentID string `json:"nextHopDrgAttachmentId"`
	ID                     string `json:"id"`
	RouteProvenance        string `json:"routeProvenance"`
	RouteType              string `json:"routeType"`
	IsConflict             bool   `json:"isConflict"`
	IsBlackhole            bool   `json:"isBlackhole"`
}

func staticWire(r DrgStaticRule) DrgRuleWire {
	return DrgRuleWire{
		Destination:            r.Destination,
		DestinationType:        r.DestinationType,
		NextHopDrgAttachmentID: r.NextHopDrgAttachmentID,
		ID:                     r.ID,
		RouteProvenance:        provStatic,
		RouteType:              routeStatic,
	}
}

func (s DrgStatement) matches(a *DrgAttachment) bool {
	for _, c := range s.MatchCriteria {
		switch c.MatchType {
		case matchAll:
		case matchType:
			if attachmentType(a) != c.AttachmentType {
				return false
			}
		case matchID:
			if a.ID != c.DrgAttachmentID {
				return false
			}
		default:
			return false
		}
	}

	return true
}

// imports reports whether the distribution accepts routes from the attachment: the first matching statement decides.
func (d *DrgRouteDistribution) imports(a *DrgAttachment) bool {
	sts := slices.Clone(d.Statements)
	slices.SortFunc(sts, func(x, y DrgStatement) int { return x.Priority - y.Priority })

	for _, s := range sts {
		if s.matches(a) {
			return s.Action == actionAccept
		}
	}

	return false
}

// attachmentCidrs lists what a VCN attachment advertises: the VCN CIDR blocks or the subnet CIDR blocks.
func (b *InMemoryBackend) attachmentCidrs(a *DrgAttachment) []string {
	vcn, ok := b.res[a.VcnID].(*Vcn)
	if !ok || !live(vcn) {
		return nil
	}

	if a.NetworkDetails != nil && a.NetworkDetails.VcnRouteType == vcnRouteSubnets {
		var out []string

		for _, s := range liveOf(b, inVcn[*Subnet](vcn.ID)) {
			out = append(out, s.Ipv4CidrBlocks...)
			out = append(out, s.Ipv6CidrBlocks...)
		}

		slices.Sort(out)

		return out
	}

	return slices.Concat(vcn.CidrBlocks, vcn.Ipv6CidrBlocks, vcn.Ipv6PrivateCidrs)
}

func dynamicRuleID(tableID, attID, cidr string) string {
	sum := sha256.Sum256([]byte(tableID + "|" + attID + "|" + cidr))

	return "ocid1." + kindDrgRule + ".oc1." + RegionKey + "." + hex.EncodeToString(sum[:ruleIDBytes])
}

// drgRules returns the static rules and the rules learned from the import distribution of a table.
func (b *InMemoryBackend) drgRules(t *DrgRouteTable) []DrgRuleWire {
	out := make([]DrgRuleWire, 0, len(t.Rules))
	for _, r := range t.Rules {
		out = append(out, staticWire(r))
	}

	dist, ok := b.res[t.ImportDrgRouteDistributionID].(*DrgRouteDistribution)
	if !ok || !live(dist) {
		return out
	}

	var dyn []DrgRuleWire

	for _, a := range b.drgAttachments(t.DrgID) {
		if attachmentType(a) != attVcn || !dist.imports(a) {
			continue
		}

		for _, c := range b.attachmentCidrs(a) {
			dyn = append(dyn, DrgRuleWire{
				Destination: c, DestinationType: ctCIDR, NextHopDrgAttachmentID: a.ID, ID: dynamicRuleID(t.ID, a.ID, c),
				RouteProvenance: provVcn, RouteType: routeDynamic,
			})
		}
	}

	if !t.IsEcmpEnabled {
		for i := range dyn {
			dyn[i].IsConflict = slices.ContainsFunc(dyn, func(o DrgRuleWire) bool {
				return o.Destination == dyn[i].Destination && o.NextHopDrgAttachmentID != dyn[i].NextHopDrgAttachmentID
			})
		}
	}

	slices.SortStableFunc(dyn, func(x, y DrgRuleWire) int { return strings.Compare(x.Destination, y.Destination) })

	return append(out, dyn...)
}

// DrgRuleIn is a rule of AddDrgRouteRules.
type DrgRuleIn struct {
	Destination            string `json:"destination"`
	DestinationType        string `json:"destinationType"`
	NextHopDrgAttachmentID string `json:"nextHopDrgAttachmentId"`
}

// AddDrgRulesIn is the AddDrgRouteRules request body.
type AddDrgRulesIn struct {
	RouteRules []DrgRuleIn `json:"routeRules"`
}

func (b *InMemoryBackend) checkDrgRule(t *DrgRouteTable, r DrgRuleIn, others []DrgStaticRule, selfID string) *APIError {
	if r.DestinationType != ctCIDR {
		return errParam("destinationType must be CIDR_BLOCK")
	}

	if _, err := netip.ParsePrefix(r.Destination); err != nil {
		return errParam("destination " + r.Destination + " is not a valid CIDR block")
	}

	a, aerr := getLive[*DrgAttachment](b, r.NextHopDrgAttachmentID)
	if aerr != nil {
		return aerr
	}

	if a.DrgID != t.DrgID {
		return errParam("The next hop attachment " + a.ID + " belongs to a different DRG")
	}

	if slices.ContainsFunc(
		others,
		func(o DrgStaticRule) bool { return o.Destination == r.Destination && o.ID != selfID },
	) {
		return errConflict("A static route for " + r.Destination + " already exists in this DRG route table")
	}

	return nil
}

// AddDrgRouteRules adds static rules, all or nothing.
func (b *InMemoryBackend) AddDrgRouteRules(id string, in AddDrgRulesIn) ([]DrgRuleWire, *APIError) {
	b.mu.Lock("AddDrgRouteRules")
	defer b.mu.Unlock()

	t, aerr := getLive[*DrgRouteTable](b, id)
	if aerr != nil {
		return nil, aerr
	}

	next := slices.Clone(t.Rules)
	added := make([]DrgRuleWire, 0, len(in.RouteRules))

	for _, r := range in.RouteRules {
		if aerr = b.checkDrgRule(t, r, next, ""); aerr != nil {
			return nil, aerr
		}

		rule := DrgStaticRule{
			ID: newOCID(kindDrgRule), Destination: r.Destination, DestinationType: r.DestinationType,
			NextHopDrgAttachmentID: r.NextHopDrgAttachmentID,
		}
		next, added = append(next, rule), append(added, staticWire(rule))
	}

	t.Rules = next
	b.bump(t)

	return added, nil
}

// UpdateDrgRuleIn is a rule of UpdateDrgRouteRules.
type UpdateDrgRuleIn struct {
	Destination            *string `json:"destination"`
	DestinationType        *string `json:"destinationType"`
	NextHopDrgAttachmentID *string `json:"nextHopDrgAttachmentId"`
	ID                     string  `json:"id"`
}

// UpdateDrgRulesIn is the UpdateDrgRouteRules request body.
type UpdateDrgRulesIn struct {
	RouteRules []UpdateDrgRuleIn `json:"routeRules"`
}

// UpdateDrgRouteRules changes static rules by id, all or nothing.
func (b *InMemoryBackend) UpdateDrgRouteRules(id string, in UpdateDrgRulesIn) ([]DrgRuleWire, *APIError) {
	b.mu.Lock("UpdateDrgRouteRules")
	defer b.mu.Unlock()

	t, aerr := getLive[*DrgRouteTable](b, id)
	if aerr != nil {
		return nil, aerr
	}

	next := slices.Clone(t.Rules)
	changed := make([]DrgRuleWire, 0, len(in.RouteRules))

	for _, u := range in.RouteRules {
		i := slices.IndexFunc(next, func(r DrgStaticRule) bool { return r.ID == u.ID })
		if i < 0 {
			return nil, errNotFound(u.ID)
		}

		cand := DrgRuleIn{
			Destination:            cmpOr(u.Destination, next[i].Destination),
			DestinationType:        cmpOr(u.DestinationType, next[i].DestinationType),
			NextHopDrgAttachmentID: cmpOr(u.NextHopDrgAttachmentID, next[i].NextHopDrgAttachmentID),
		}

		if aerr = b.checkDrgRule(t, cand, next, u.ID); aerr != nil {
			return nil, aerr
		}

		next[i].Destination, next[i].DestinationType = cand.Destination, cand.DestinationType
		next[i].NextHopDrgAttachmentID = cand.NextHopDrgAttachmentID
		changed = append(changed, staticWire(next[i]))
	}

	t.Rules = next
	b.bump(t)

	return changed, nil
}

// RemoveDrgRouteRules removes static rules by id, all or nothing.
func (b *InMemoryBackend) RemoveDrgRouteRules(id string, ids []string) *APIError {
	b.mu.Lock("RemoveDrgRouteRules")
	defer b.mu.Unlock()

	t, aerr := getLive[*DrgRouteTable](b, id)
	if aerr != nil {
		return aerr
	}

	for _, rid := range ids {
		if !slices.ContainsFunc(t.Rules, func(r DrgStaticRule) bool { return r.ID == rid }) {
			return errNotFound(rid)
		}
	}

	t.Rules = slices.DeleteFunc(slices.Clone(t.Rules), func(r DrgStaticRule) bool { return slices.Contains(ids, r.ID) })
	b.bump(t)

	return nil
}

// ListDrgRouteRules lists the static and learned rules of a DRG route table.
func (b *InMemoryBackend) ListDrgRouteRules(
	id, routeType, token string, limit int,
) (page.Page[DrgRuleWire], *APIError) {
	b.mu.RLock("ListDrgRouteRules")
	defer b.mu.RUnlock()

	t, aerr := get[*DrgRouteTable](b, id)
	if aerr != nil {
		return page.Page[DrgRuleWire]{}, aerr
	}

	if routeType != "" && routeType != routeStatic && routeType != routeDynamic {
		return page.Page[DrgRuleWire]{}, errParam("routeType must be STATIC or DYNAMIC")
	}

	all := b.drgRules(t)
	if routeType != "" {
		all = slices.DeleteFunc(all, func(r DrgRuleWire) bool { return r.RouteType != routeType })
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// GetAllDrgAttachments lists the live attachments of a DRG, including those of other tenancies.
func (b *InMemoryBackend) GetAllDrgAttachments(
	id, attType, token string, limit int, crossTenancy *bool,
) (page.Page[AttachmentInfo], *APIError) {
	b.mu.RLock("GetAllDrgAttachments")
	defer b.mu.RUnlock()

	if _, aerr := get[*Drg](b, id); aerr != nil {
		return page.Page[AttachmentInfo]{}, aerr
	}

	atts := b.drgAttachments(id)
	sortRes(atts, ListOpts{Order: orderAsc})

	var all []AttachmentInfo

	for _, a := range atts {
		if (attType == "" || attType == "ALL" || attachmentType(a) == attType) &&
			(crossTenancy == nil || a.IsCrossTenancy == *crossTenancy) {
			all = append(all, AttachmentInfo{ID: a.ID})
		}
	}

	return page.New(all, token, limit, defaultListLimit), nil
}
