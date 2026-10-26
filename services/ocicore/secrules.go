package ocicore

import (
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxSLRules   = 200
	maxNSGRules  = 120
	maxNSGs      = 1000
	maxProto     = 255
	maxPort      = 65535
	dirIngress   = "INGRESS"
	dirEgress    = "EGRESS"
	protoAll     = "all"
	protoICMP    = "1"
	protoTCP     = "6"
	protoUDP     = "17"
	protoICMPv6  = "58"
	maxIcmpField = 255
)

func validProtocol(p string) bool {
	if p == protoAll {
		return true
	}

	n, err := strconv.Atoi(p)

	return err == nil && n >= 0 && n <= maxProto
}

func validPorts(o *PortOptions) *APIError {
	for _, r := range []*PortRange{o.DestinationPortRange, o.SourcePortRange} {
		if r != nil && (r.Min < 1 || r.Max > maxPort || r.Min > r.Max) {
			return errParam("port ranges must satisfy 1 <= min <= max <= 65535")
		}
	}

	return nil
}

// ruleOptions validates the protocol and the protocol specific options shared by every rule shape.
func ruleOptions(proto string, icmp *IcmpOptions, tcp, udp *PortOptions) *APIError {
	if !validProtocol(proto) {
		return errParam("protocol " + proto + " must be all or an IANA protocol number")
	}

	if (tcp != nil && proto != protoTCP) || (udp != nil && proto != protoUDP) ||
		(icmp != nil && proto != protoICMP && proto != protoICMPv6) {
		return errParam("Protocol options do not match protocol " + proto)
	}

	for _, o := range []*PortOptions{tcp, udp} {
		if o != nil {
			if aerr := validPorts(o); aerr != nil {
				return aerr
			}
		}
	}

	if icmp != nil && (icmp.Type < 0 || icmp.Type > maxIcmpField ||
		(icmp.Code != nil && (*icmp.Code < 0 || *icmp.Code > maxIcmpField))) {
		return errParam("ICMP type and code must be between 0 and 255")
	}

	return nil
}

// endpoint validates a rule source or destination; vcnID is empty for security lists.
func (b *InMemoryBackend) endpoint(value, typ, vcnID string) (string, *APIError) {
	if value == "" {
		return "", errMissing("A rule source or destination is required")
	}

	switch typ {
	case "", ctCIDR:
		if _, ok := parseAnyCidr(value); !ok {
			return "", errParam(value + " is not a valid CIDR block")
		}

		return ctCIDR, nil
	case ctService:
		if _, ok := serviceByCidr(value); !ok {
			return "", errParam(value + " is not a known service CIDR label")
		}

		return ctService, nil
	case ctNSG:
		if vcnID == "" {
			return "", errParam("NETWORK_SECURITY_GROUP is only valid in network security group rules")
		}

		if aerr := refInVcn[*NetworkSecurityGroup](b, value, vcnID, "network security group"); aerr != nil {
			return "", aerr
		}

		return ctNSG, nil
	}

	return "", errParam("Unknown source or destination type " + typ)
}

func (b *InMemoryBackend) validateIngress(rules []IngressRule) ([]IngressRule, *APIError) {
	out := make([]IngressRule, 0, len(rules))

	for _, r := range rules {
		if aerr := ruleOptions(r.Protocol, r.IcmpOptions, r.TCPOptions, r.UDPOptions); aerr != nil {
			return nil, aerr
		}

		typ, aerr := b.endpoint(r.Source, r.SourceType, "")
		if aerr != nil {
			return nil, aerr
		}

		r.SourceType = typ
		if r.IsStateless == nil {
			f := false
			r.IsStateless = &f
		}

		out = append(out, r)
	}

	return out, nil
}

func (b *InMemoryBackend) validateEgress(rules []EgressRule) ([]EgressRule, *APIError) {
	out := make([]EgressRule, 0, len(rules))

	for _, r := range rules {
		if aerr := ruleOptions(r.Protocol, r.IcmpOptions, r.TCPOptions, r.UDPOptions); aerr != nil {
			return nil, aerr
		}

		typ, aerr := b.endpoint(r.Destination, r.DestinationType, "")
		if aerr != nil {
			return nil, aerr
		}

		r.DestinationType = typ
		if r.IsStateless == nil {
			f := false
			r.IsStateless = &f
		}

		out = append(out, r)
	}

	return out, nil
}

func (b *InMemoryBackend) validateLists(in []IngressRule, eg []EgressRule) ([]IngressRule, []EgressRule, *APIError) {
	if len(in) > maxSLRules || len(eg) > maxSLRules {
		return nil, nil, errLimit("A security list can have at most 200 ingress and 200 egress rules")
	}

	ing, aerr := b.validateIngress(in)
	if aerr != nil {
		return nil, nil, aerr
	}

	egr, aerr := b.validateEgress(eg)

	return ing, egr, aerr
}

// CreateSecurityListIn is the CreateSecurityList request body.
type CreateSecurityListIn struct {
	CompartmentID string        `json:"compartmentId"`
	VcnID         string        `json:"vcnId"`
	DisplayName   string        `json:"displayName"`
	IngressRules  []IngressRule `json:"ingressSecurityRules"`
	EgressRules   []EgressRule  `json:"egressSecurityRules"`
	TagsIn
}

// CreateSecurityList creates a security list.
func (b *InMemoryBackend) CreateSecurityList(in CreateSecurityListIn) (resource, string, *APIError) {
	b.mu.Lock("CreateSecurityList")
	defer b.mu.Unlock()

	v, aerr := b.gatewayVcn(in.CompartmentID, in.VcnID, in.DisplayName, "")
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = gatewayLimit[*SecurityList](b, v.ID, "security lists", maxCollection); aerr != nil {
		return nil, "", aerr
	}

	ing, egr, aerr := b.validateLists(in.IngressRules, in.EgressRules)
	if aerr != nil {
		return nil, "", aerr
	}

	sl := &SecurityList{
		Base: b.newBase(kindSL, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID,
		IngressRules: ing, EgressRules: egr,
	}
	b.add(sl)

	return sl.view(), sl.Internal.Etag, nil
}

// UpdateSecurityListIn is the UpdateSecurityList request body.
type UpdateSecurityListIn struct {
	IngressRules *[]IngressRule `json:"ingressSecurityRules"`
	EgressRules  *[]EgressRule  `json:"egressSecurityRules"`
	UpdateIn
}

// UpdateSecurityList changes a security list; rule lists replace the existing ones.
func (b *InMemoryBackend) UpdateSecurityList(id, ifMatch string, in UpdateSecurityListIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(sl *SecurityList) *APIError {
		ing, egr := sl.IngressRules, sl.EgressRules

		if in.IngressRules != nil {
			ing = *in.IngressRules
		}

		if in.EgressRules != nil {
			egr = *in.EgressRules
		}

		ing, egr, aerr := b.validateLists(ing, egr)
		if aerr != nil {
			return aerr
		}

		sl.IngressRules, sl.EgressRules = ing, egr

		return nil
	})
}

// CreateNsgIn is the CreateNetworkSecurityGroup request body.
type CreateNsgIn struct {
	CompartmentID string `json:"compartmentId"`
	VcnID         string `json:"vcnId"`
	DisplayName   string `json:"displayName"`
	TagsIn
}

// CreateNetworkSecurityGroup creates an NSG.
func (b *InMemoryBackend) CreateNetworkSecurityGroup(in CreateNsgIn) (resource, string, *APIError) {
	b.mu.Lock("CreateNetworkSecurityGroup")
	defer b.mu.Unlock()

	v, aerr := b.gatewayVcn(in.CompartmentID, in.VcnID, in.DisplayName, "")
	if aerr == nil {
		aerr = gatewayLimit[*NetworkSecurityGroup](b, v.ID, "network security groups", maxNSGs)
	}

	if aerr != nil {
		return nil, "", aerr
	}

	n := &NetworkSecurityGroup{Base: b.newBase(kindNSG, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID}
	b.add(n)

	return n.view(), n.Internal.Etag, nil
}

// UpdateNetworkSecurityGroup changes an NSG's name and tags.
func (b *InMemoryBackend) UpdateNetworkSecurityGroup(id, ifMatch string, in UpdateIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*NetworkSecurityGroup) *APIError { return nil })
}

// SecurityRuleIn is a rule in an add or update call.
type SecurityRuleIn struct {
	IcmpOptions     *IcmpOptions `json:"icmpOptions"`
	TCPOptions      *PortOptions `json:"tcpOptions"`
	UDPOptions      *PortOptions `json:"udpOptions"`
	IsStateless     *bool        `json:"isStateless"`
	ID              string       `json:"id"`
	Direction       string       `json:"direction"`
	Protocol        string       `json:"protocol"`
	Source          string       `json:"source"`
	SourceType      string       `json:"sourceType"`
	Destination     string       `json:"destination"`
	DestinationType string       `json:"destinationType"`
	Description     string       `json:"description"`
}

func (b *InMemoryBackend) buildRule(n *NetworkSecurityGroup, in SecurityRuleIn) (SecurityRule, *APIError) {
	rule := SecurityRule{
		Direction: in.Direction, Protocol: in.Protocol, Description: in.Description, IsStateless: in.IsStateless,
		IcmpOptions: in.IcmpOptions, TCPOptions: in.TCPOptions, UDPOptions: in.UDPOptions, IsValid: true,
	}

	if aerr := ruleOptions(in.Protocol, in.IcmpOptions, in.TCPOptions, in.UDPOptions); aerr != nil {
		return rule, aerr
	}

	if rule.IsStateless == nil {
		f := false
		rule.IsStateless = &f
	}

	var aerr *APIError

	switch in.Direction {
	case dirIngress:
		rule.Source = in.Source
		rule.SourceType, aerr = b.endpoint(in.Source, in.SourceType, n.VcnID)
	case dirEgress:
		rule.Destination = in.Destination
		rule.DestinationType, aerr = b.endpoint(in.Destination, in.DestinationType, n.VcnID)
	default:
		aerr = errParam("direction must be INGRESS or EGRESS")
	}

	return rule, aerr
}

func (b *InMemoryBackend) nsgRuleList(nsgID string) []*NSGRule {
	var out []*NSGRule

	for _, r := range b.nsgRules {
		if r.NsgID == nsgID {
			out = append(out, r)
		}
	}

	slices.SortFunc(out, func(a, c *NSGRule) int { return int(a.Seq - c.Seq) })

	return out
}

// SecurityRulesIn is the body of the add and update security rule calls.
type SecurityRulesIn struct {
	SecurityRules []SecurityRuleIn `json:"securityRules"`
}

// SecurityRulesOut is the result of the add and update security rule calls.
type SecurityRulesOut struct {
	SecurityRules []SecurityRule `json:"securityRules"`
}

// AddSecurityRules adds rules to an NSG; either all are added or none.
func (b *InMemoryBackend) AddSecurityRules(id string, in SecurityRulesIn) (SecurityRulesOut, *APIError) {
	b.mu.Lock("AddSecurityRules")
	defer b.mu.Unlock()

	n, aerr := getLive[*NetworkSecurityGroup](b, id)
	if aerr != nil {
		return SecurityRulesOut{}, aerr
	}

	if len(b.nsgRuleList(id))+len(in.SecurityRules) > maxNSGRules {
		return SecurityRulesOut{}, errLimit("A network security group can have at most 120 security rules")
	}

	now := b.nowUTC()
	built := make([]*NSGRule, 0, len(in.SecurityRules))

	for _, r := range in.SecurityRules {
		rule, aerr := b.buildRule(n, r)
		if aerr != nil {
			return SecurityRulesOut{}, aerr
		}

		rule.ID, rule.TimeCreated = strings.ToUpper(uuid.NewString()), now
		built = append(built, &NSGRule{NsgID: id, Seq: b.nextSeq(), SecurityRule: rule})
	}

	out := SecurityRulesOut{SecurityRules: make([]SecurityRule, 0, len(built))}

	for _, r := range built {
		b.nsgRules[r.ID] = r
		out.SecurityRules = append(out.SecurityRules, r.SecurityRule)
	}

	b.bump(n)

	return out, nil
}

// UpdateSecurityRules replaces existing NSG rules by id; either all are updated or none.
func (b *InMemoryBackend) UpdateSecurityRules(id string, in SecurityRulesIn) (SecurityRulesOut, *APIError) {
	b.mu.Lock("UpdateSecurityRules")
	defer b.mu.Unlock()

	n, aerr := getLive[*NetworkSecurityGroup](b, id)
	if aerr != nil {
		return SecurityRulesOut{}, aerr
	}

	type change struct {
		old  *NSGRule
		rule SecurityRule
	}

	changes := make([]change, 0, len(in.SecurityRules))

	for _, r := range in.SecurityRules {
		old, ok := b.nsgRules[r.ID]
		if !ok || old.NsgID != id {
			return SecurityRulesOut{}, errNotFound(r.ID)
		}

		rule, aerr := b.buildRule(n, r)
		if aerr != nil {
			return SecurityRulesOut{}, aerr
		}

		rule.ID, rule.TimeCreated = old.ID, old.TimeCreated
		changes = append(changes, change{old, rule})
	}

	out := SecurityRulesOut{SecurityRules: make([]SecurityRule, 0, len(changes))}

	for _, c := range changes {
		c.old.SecurityRule = c.rule
		out.SecurityRules = append(out.SecurityRules, c.rule)
	}

	b.bump(n)

	return out, nil
}

// RemoveSecurityRules removes NSG rules by id; either all are removed or none.
func (b *InMemoryBackend) RemoveSecurityRules(id string, ids []string) *APIError {
	b.mu.Lock("RemoveSecurityRules")
	defer b.mu.Unlock()

	n, aerr := getLive[*NetworkSecurityGroup](b, id)
	if aerr != nil {
		return aerr
	}

	for _, rid := range ids {
		if r, ok := b.nsgRules[rid]; !ok || r.NsgID != id {
			return errNotFound(rid)
		}
	}

	for _, rid := range ids {
		delete(b.nsgRules, rid)
	}

	b.bump(n)

	return nil
}

// RuleListOpts carries the ListNetworkSecurityGroupSecurityRules query.
type RuleListOpts struct {
	Direction string
	SortBy    string
	Order     string
	Page      string
	Limit     int
}

// ListSecurityRules lists the rules of an NSG.
func (b *InMemoryBackend) ListSecurityRules(id string, o RuleListOpts) (page.Page[SecurityRule], *APIError) {
	if o.Direction != "" && o.Direction != dirIngress && o.Direction != dirEgress {
		return page.Page[SecurityRule]{}, errParam("direction must be INGRESS or EGRESS")
	}

	if aerr := validListOpts(ListOpts{SortBy: o.SortBy, Order: o.Order}); aerr != nil {
		return page.Page[SecurityRule]{}, aerr
	}

	b.mu.RLock("ListSecurityRules")
	defer b.mu.RUnlock()

	if _, aerr := get[*NetworkSecurityGroup](b, id); aerr != nil {
		return page.Page[SecurityRule]{}, aerr
	}

	rules := b.nsgRuleList(id)
	if o.Direction != "" {
		rules = slices.DeleteFunc(rules, func(r *NSGRule) bool { return r.Direction != o.Direction })
	}

	if o.Order == "" || strings.EqualFold(o.Order, "DESC") {
		slices.Reverse(rules)
	}

	pg := page.New(rules, o.Page, o.Limit, defaultListLimit)
	out := make([]SecurityRule, 0, len(pg.Data))

	for _, r := range pg.Data {
		out = append(out, r.SecurityRule)
	}

	return page.Page[SecurityRule]{Data: out, Next: pg.Next}, nil
}

// ListNsgVnics lists the VNICs in an NSG; this emulator has no compute, so it is always empty.
func (b *InMemoryBackend) ListNsgVnics(id string) *APIError {
	b.mu.RLock("ListNsgVnics")
	defer b.mu.RUnlock()

	_, aerr := get[*NetworkSecurityGroup](b, id)

	return aerr
}
