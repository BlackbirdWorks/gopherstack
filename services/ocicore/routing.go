package ocicore

import "slices"

const maxRouteRules = 200

// CreateRouteTableIn is the CreateRouteTable request body.
type CreateRouteTableIn struct {
	TagsIn
	CompartmentID string      `json:"compartmentId"`
	VcnID         string      `json:"vcnId"`
	DisplayName   string      `json:"displayName"`
	RouteRules    []RouteRule `json:"routeRules"`
}

// checkTarget validates a route rule target and returns the attached service for service gateways.
func (b *InMemoryBackend) checkTarget(rule RouteRule, vcnID string) *APIError {
	id := rule.NetworkEntityID

	var aerr *APIError

	switch ocidKind(id) {
	case kindIGW:
		aerr = refInVcn[*InternetGateway](b, id, vcnID, "internet gateway")
	case kindNAT:
		aerr = refInVcn[*NatGateway](b, id, vcnID, "NAT gateway")
	case kindLPG:
		aerr = refInVcn[*LocalPeeringGateway](b, id, vcnID, "local peering gateway")
	case kindSGW:
		aerr = b.checkSgwTarget(rule, vcnID)
	case kindDRG, "privateip":
		// DRGs and private IPs belong to services this emulator does not model; the OCID shape is all we can check.
	default:
		return errParam("networkEntityId " + id + " is not a valid route target")
	}

	return aerr
}

func (b *InMemoryBackend) checkSgwTarget(rule RouteRule, vcnID string) *APIError {
	if aerr := refInVcn[*ServiceGateway](b, rule.NetworkEntityID, vcnID, "service gateway"); aerr != nil {
		return aerr
	}

	sgw, _ := get[*ServiceGateway](b, rule.NetworkEntityID)
	svc, ok := serviceByCidr(rule.Destination)

	if !ok || !slices.ContainsFunc(sgw.Services, func(r ServiceRef) bool { return r.ServiceID == svc.ID }) {
		return errParam("The service " + rule.Destination + " is not enabled on service gateway " + sgw.ID)
	}

	return nil
}

func normalizeRule(rule RouteRule) (RouteRule, *APIError) {
	if rule.NetworkEntityID == "" {
		return rule, errMissing("networkEntityId is required for every route rule")
	}

	if rule.Destination == "" {
		rule.Destination = rule.CidrBlock
	}

	if rule.Destination == "" {
		return rule, errMissing("destination is required for every route rule")
	}

	switch rule.DestinationType {
	case "":
		rule.DestinationType = ctCIDR
	case ctCIDR, ctService:
	default:
		return rule, errParam("destinationType must be CIDR_BLOCK or SERVICE_CIDR_BLOCK")
	}

	isSgw := ocidKind(rule.NetworkEntityID) == kindSGW
	if isSgw != (rule.DestinationType == ctService) {
		return rule, errParam("SERVICE_CIDR_BLOCK destinations require a service gateway target and vice versa")
	}

	if rule.DestinationType == ctCIDR {
		if _, ok := parseAnyCidr(rule.Destination); !ok {
			return rule, errParam("destination " + rule.Destination + " is not a valid CIDR block")
		}

		rule.CidrBlock = rule.Destination
	} else {
		rule.CidrBlock = ""
	}

	rule.RouteType = "STATIC"

	return rule, nil
}

func (b *InMemoryBackend) validateRules(vcnID string, rules []RouteRule) ([]RouteRule, *APIError) {
	if len(rules) > maxRouteRules {
		return nil, errLimit("A route table can have at most 200 route rules")
	}

	out := make([]RouteRule, 0, len(rules))

	for _, in := range rules {
		rule, aerr := normalizeRule(in)
		if aerr != nil {
			return nil, aerr
		}

		if slices.ContainsFunc(out, func(x RouteRule) bool { return x.Destination == rule.Destination }) {
			return nil, errParam("Duplicate route rule destination " + rule.Destination)
		}

		if aerr = b.checkTarget(rule, vcnID); aerr != nil {
			return nil, aerr
		}

		out = append(out, rule)
	}

	return out, nil
}

// CreateRouteTable creates a route table.
func (b *InMemoryBackend) CreateRouteTable(in CreateRouteTableIn) (resource, string, *APIError) {
	b.mu.Lock("CreateRouteTable")
	defer b.mu.Unlock()

	v, aerr := b.gatewayVcn(in.CompartmentID, in.VcnID, in.DisplayName, "")
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = gatewayLimit[*RouteTable](b, v.ID, "route tables", maxCollection); aerr != nil {
		return nil, "", aerr
	}

	rules, aerr := b.validateRules(v.ID, in.RouteRules)
	if aerr != nil {
		return nil, "", aerr
	}

	rt := &RouteTable{
		Base: b.newBase(kindRT, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID, RouteRules: rules,
	}
	b.add(rt)

	return rt.view(), rt.Internal.Etag, nil
}

// UpdateRouteTableIn is the UpdateRouteTable request body.
type UpdateRouteTableIn struct {
	RouteRules *[]RouteRule `json:"routeRules"`
	UpdateIn
}

// UpdateRouteTable changes a route table; routeRules replaces the whole list.
func (b *InMemoryBackend) UpdateRouteTable(id, ifMatch string, in UpdateRouteTableIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(rt *RouteTable) *APIError {
		if in.RouteRules == nil {
			return nil
		}

		rules, aerr := b.validateRules(rt.VcnID, *in.RouteRules)
		if aerr != nil {
			return aerr
		}

		rt.RouteRules = rules

		return nil
	})
}

// CreateDhcpOptionsIn is the CreateDhcpOptions request body.
type CreateDhcpOptionsIn struct {
	TagsIn
	CompartmentID  string       `json:"compartmentId"`
	VcnID          string       `json:"vcnId"`
	DisplayName    string       `json:"displayName"`
	DomainNameType string       `json:"domainNameType"`
	Options        []DhcpOption `json:"options"`
}

const maxCollection = 300

func validateDhcp(opts []DhcpOption, domainType string) *APIError {
	if domainType != "" && !slices.Contains([]string{"SUBNET_DOMAIN", "VCN_DOMAIN", domainCustom}, domainType) {
		return errParam("domainNameType must be one of SUBNET_DOMAIN, VCN_DOMAIN, CUSTOM_DOMAIN")
	}

	seen := map[string]bool{}

	for _, o := range opts {
		if seen[o.Type] {
			return errParam("At most one " + o.Type + " option is allowed")
		}

		seen[o.Type] = true

		if aerr := validateDhcpOption(o); aerr != nil {
			return aerr
		}
	}

	return nil
}

func validateDNSOption(o DhcpOption) *APIError {
	const maxDNS = 3

	switch o.ServerType {
	case "VcnLocal", "VcnLocalPlusInternet":
		if len(o.CustomDNSServers) > 0 {
			return errParam("customDnsServers is only valid with serverType CustomDnsServer")
		}
	case "CustomDnsServer":
		if len(o.CustomDNSServers) == 0 || len(o.CustomDNSServers) > maxDNS {
			return errParam("customDnsServers must list between 1 and 3 IPv4 addresses")
		}

		for _, s := range o.CustomDNSServers {
			if a, ok := parseAddr(s); !ok || !a.Is4() {
				return errParam("customDnsServers entry " + s + " is not a valid IPv4 address")
			}
		}
	default:
		return errParam("serverType must be VcnLocal, VcnLocalPlusInternet or CustomDnsServer")
	}

	return nil
}

func validateDhcpOption(o DhcpOption) *APIError {
	switch o.Type {
	case "DomainNameServer":
		return validateDNSOption(o)
	case "SearchDomain":
		if len(o.SearchDomainNames) != 1 || o.SearchDomainNames[0] == "" {
			return errParam("searchDomainNames must contain exactly one domain name")
		}

		return nil
	}

	return errParam("option type must be DomainNameServer or SearchDomain")
}

// CreateDhcpOptions creates a DHCP options set.
func (b *InMemoryBackend) CreateDhcpOptions(in CreateDhcpOptionsIn) (resource, string, *APIError) {
	b.mu.Lock("CreateDhcpOptions")
	defer b.mu.Unlock()

	v, aerr := b.gatewayVcn(in.CompartmentID, in.VcnID, in.DisplayName, "")
	if aerr != nil {
		return nil, "", aerr
	}

	if in.Options == nil {
		return nil, "", errMissing("options is required")
	}

	if aerr = validateDhcp(in.Options, in.DomainNameType); aerr != nil {
		return nil, "", aerr
	}

	if aerr = gatewayLimit[*DhcpOptions](b, v.ID, "DHCP options sets", maxCollection); aerr != nil {
		return nil, "", aerr
	}

	if in.DomainNameType == "" {
		in.DomainNameType = domainCustom
	}

	d := &DhcpOptions{
		Base: b.newBase(kindDHCP, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID,
		Options: in.Options, DomainNameType: in.DomainNameType,
	}
	b.add(d)

	return d.view(), d.Internal.Etag, nil
}

// UpdateDhcpOptionsIn is the UpdateDhcpOptions request body.
type UpdateDhcpOptionsIn struct {
	Options        *[]DhcpOption `json:"options"`
	DomainNameType *string       `json:"domainNameType"`
	UpdateIn
}

// UpdateDhcpOptions changes a DHCP options set.
func (b *InMemoryBackend) UpdateDhcpOptions(id, ifMatch string, in UpdateDhcpOptionsIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(d *DhcpOptions) *APIError {
		opts, dt := d.Options, d.DomainNameType

		if in.Options != nil {
			opts = *in.Options
		}

		if in.DomainNameType != nil {
			dt = *in.DomainNameType
		}

		if aerr := validateDhcp(opts, dt); aerr != nil {
			return aerr
		}

		d.Options, d.DomainNameType = opts, dt

		return nil
	})
}
