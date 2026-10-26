package ocicore

import (
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	entityNAT   = "NAT_GATEWAY"
	peerRevoked = "REVOKED"

	allServicesCidr = "all-iad-services-in-oracle-services-network"
	ipPerOctet      = 254
)

//nolint:gochecknoglobals // immutable service catalogue
var services = []Service{
	{
		ID:          "ocid1.service.oc1.iad.aaaaaaaam4zfmy2rjue6fmglumm3czgisxzrnvrwqeodtztg7hwa272mlfna",
		Name:        "All IAD Services In Oracle Services Network",
		Description: "All IAD Services In Oracle Services Network",
		CidrBlock:   allServicesCidr,
	},
	{
		ID:          "ocid1.service.oc1.iad.aaaaaaaaoyfwkeqkcdg6e7ctfh2v4qjwdmqcphdp33ijinw6bfxe4fpfhlzq",
		Name:        "OCI IAD Object Storage",
		Description: "OCI IAD Object Storage",
		CidrBlock:   "oci-iad-objectstorage",
	},
}

func serviceByID(id string) (Service, bool) {
	i := slices.IndexFunc(services, func(s Service) bool { return s.ID == id })
	if i < 0 {
		return Service{}, false
	}

	return services[i], true
}

func serviceByCidr(label string) (Service, bool) {
	i := slices.IndexFunc(services, func(s Service) bool { return s.CidrBlock == label })
	if i < 0 {
		return Service{}, false
	}

	return services[i], true
}

// ListServices lists the Oracle services available to service gateways.
func (b *InMemoryBackend) ListServices(pageToken string, limit int) (page.Page[Service], *APIError) {
	return page.New(slices.Clone(services), pageToken, limit, defaultListLimit), nil
}

// GetService returns one service.
func (b *InMemoryBackend) GetService(id string) (Service, *APIError) {
	s, ok := serviceByID(id)
	if !ok {
		return Service{}, errNotFound(id)
	}

	return s, nil
}

func (b *InMemoryBackend) allocIP() string {
	b.ipSeq++

	return fmt.Sprintf("129.213.%d.%d", (b.ipSeq-1)/ipPerOctet%ipPerOctet, (b.ipSeq-1)%ipPerOctet+1)
}

// optRouteTable validates an optional gateway route table association.
func (b *InMemoryBackend) optRouteTable(id, vcnID string) *APIError {
	if id == "" {
		return nil
	}

	return refInVcn[*RouteTable](b, id, vcnID, "route table")
}

// gatewayBase runs the checks every gateway create shares and enforces the one-per-VCN limits.
func gatewayLimit[T resource](b *InMemoryBackend, vcnID, what string, limit int) *APIError {
	n := len(b.liveRefs(func(r resource) bool { _, ok := r.(T); return ok && r.vcnRef() == vcnID }))
	if n >= limit {
		return errLimit("A VCN can have at most " + fmt.Sprint(limit) + " " + what)
	}

	return nil
}

func (b *InMemoryBackend) gatewayVcn(compartment, vcnID, name, rt string) (*Vcn, *APIError) {
	if aerr := b.checkCompartment(compartment); aerr != nil {
		return nil, aerr
	}

	if vcnID == "" {
		return nil, errMissing("vcnId is required")
	}

	if aerr := validName(name); aerr != nil {
		return nil, aerr
	}

	v, aerr := getLive[*Vcn](b, vcnID)
	if aerr != nil {
		return nil, aerr
	}

	return v, b.optRouteTable(rt, vcnID)
}

// CreateInternetGatewayIn is the CreateInternetGateway request body.
type CreateInternetGatewayIn struct {
	IsEnabled     *bool  `json:"isEnabled"`
	CompartmentID string `json:"compartmentId"`
	VcnID         string `json:"vcnId"`
	DisplayName   string `json:"displayName"`
	RouteTableID  string `json:"routeTableId"`
	TagsIn
}

// CreateInternetGateway creates an internet gateway.
func (b *InMemoryBackend) CreateInternetGateway(in CreateInternetGatewayIn) (resource, string, *APIError) {
	b.mu.Lock("CreateInternetGateway")
	defer b.mu.Unlock()

	if in.IsEnabled == nil {
		return nil, "", errMissing("isEnabled is required")
	}

	v, aerr := b.gatewayVcn(in.CompartmentID, in.VcnID, in.DisplayName, in.RouteTableID)
	if aerr == nil {
		aerr = gatewayLimit[*InternetGateway](b, v.ID, "internet gateway", 1)
	}

	if aerr != nil {
		return nil, "", aerr
	}

	g := &InternetGateway{
		Base: b.newBase(kindIGW, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID,
		IsEnabled: *in.IsEnabled, RouteTableID: in.RouteTableID,
	}
	b.add(g)

	return g.view(), g.Internal.Etag, nil
}

// UpdateInternetGatewayIn is the UpdateInternetGateway request body.
type UpdateInternetGatewayIn struct {
	IsEnabled    *bool   `json:"isEnabled"`
	RouteTableID *string `json:"routeTableId"`
	UpdateIn
}

// UpdateInternetGateway changes an internet gateway.
func (b *InMemoryBackend) UpdateInternetGateway(id, ifMatch string, in UpdateInternetGatewayIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(g *InternetGateway) *APIError {
		if in.RouteTableID != nil {
			if aerr := b.optRouteTable(*in.RouteTableID, g.VcnID); aerr != nil {
				return aerr
			}

			g.RouteTableID = *in.RouteTableID
		}

		if in.IsEnabled != nil {
			g.IsEnabled = *in.IsEnabled
		}

		return nil
	})
}

// CreateNatGatewayIn is the CreateNatGateway request body.
type CreateNatGatewayIn struct {
	BlockTraffic  *bool  `json:"blockTraffic"`
	CompartmentID string `json:"compartmentId"`
	VcnID         string `json:"vcnId"`
	DisplayName   string `json:"displayName"`
	PublicIPID    string `json:"publicIpId"`
	RouteTableID  string `json:"routeTableId"`
	TagsIn
}

// CreateNatGateway creates a NAT gateway backed by a reserved or newly allocated public IP.
func (b *InMemoryBackend) CreateNatGateway(in CreateNatGatewayIn) (resource, string, *APIError) {
	b.mu.Lock("CreateNatGateway")
	defer b.mu.Unlock()

	v, aerr := b.gatewayVcn(in.CompartmentID, in.VcnID, in.DisplayName, in.RouteTableID)
	if aerr == nil {
		aerr = gatewayLimit[*NatGateway](b, v.ID, "NAT gateway", 1)
	}

	if aerr != nil {
		return nil, "", aerr
	}

	n := &NatGateway{
		Base: b.newBase(kindNAT, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID,
		RouteTableID: in.RouteTableID, BlockTraffic: in.BlockTraffic != nil && *in.BlockTraffic,
	}

	var ip *PublicIP

	if in.PublicIPID != "" {
		if ip, aerr = getLive[*PublicIP](b, in.PublicIPID); aerr != nil {
			return nil, "", aerr
		}

		if ip.Lifetime != lifetimeReserved || ip.AssignedEntityID != "" {
			return nil, "", errConflict("The public IP " + ip.ID + " is not an unassigned reserved public IP")
		}
	} else {
		ip = &PublicIP{
			Base: b.newBase(kindPublicIP, in.CompartmentID, "", TagsIn{}), IPAddress: b.allocIP(),
			Lifetime: lifetimeEphemeral, Scope: scopeRegion,
		}
		b.add(ip)
	}

	ip.AssignedEntityID, ip.AssignedEntityType = n.ID, entityNAT
	ip.LifecycleState, ip.Internal.TransitionAt = statAssigned, nil
	b.bump(ip)

	n.NatIP, n.PublicIPID = ip.IPAddress, ip.ID
	b.add(n)

	return n.view(), n.Internal.Etag, nil
}

func (b *InMemoryBackend) releaseNatIP(n *NatGateway) {
	ip, ok := b.res[n.PublicIPID].(*PublicIP)
	if !ok {
		return
	}

	if ip.Lifetime == lifetimeEphemeral {
		b.markTerminating(ip)

		return
	}

	ip.AssignedEntityID, ip.AssignedEntityType, ip.LifecycleState = "", "", statAvailable
	b.bump(ip)
}

// UpdateNatGatewayIn is the UpdateNatGateway request body.
type UpdateNatGatewayIn struct {
	BlockTraffic *bool   `json:"blockTraffic"`
	RouteTableID *string `json:"routeTableId"`
	UpdateIn
}

// UpdateNatGateway changes a NAT gateway.
func (b *InMemoryBackend) UpdateNatGateway(id, ifMatch string, in UpdateNatGatewayIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(g *NatGateway) *APIError {
		if in.RouteTableID != nil {
			if aerr := b.optRouteTable(*in.RouteTableID, g.VcnID); aerr != nil {
				return aerr
			}

			g.RouteTableID = *in.RouteTableID
		}

		if in.BlockTraffic != nil {
			g.BlockTraffic = *in.BlockTraffic
		}

		return nil
	})
}

// ServiceIDIn names a service in a request body.
type ServiceIDIn struct {
	ServiceID string `json:"serviceId"`
}

func serviceRefs(ins []ServiceIDIn) ([]ServiceRef, *APIError) {
	out := []ServiceRef{}

	for _, in := range ins {
		svc, ok := serviceByID(in.ServiceID)
		if !ok {
			return nil, errNotFound(in.ServiceID)
		}

		if !slices.ContainsFunc(out, func(r ServiceRef) bool { return r.ServiceID == svc.ID }) {
			out = append(out, ServiceRef{ServiceID: svc.ID, ServiceName: svc.Name})
		}
	}

	return out, checkServiceMix(out)
}

func checkServiceMix(refs []ServiceRef) *APIError {
	all, _ := serviceByCidr(allServicesCidr)

	if len(refs) > 1 && slices.ContainsFunc(refs, func(r ServiceRef) bool { return r.ServiceID == all.ID }) {
		return errParam("The all-services entry cannot be combined with other services")
	}

	return nil
}

// CreateServiceGatewayIn is the CreateServiceGateway request body.
type CreateServiceGatewayIn struct {
	CompartmentID string        `json:"compartmentId"`
	VcnID         string        `json:"vcnId"`
	DisplayName   string        `json:"displayName"`
	RouteTableID  string        `json:"routeTableId"`
	Services      []ServiceIDIn `json:"services"`
	TagsIn
}

// CreateServiceGateway creates a service gateway.
func (b *InMemoryBackend) CreateServiceGateway(in CreateServiceGatewayIn) (resource, string, *APIError) {
	b.mu.Lock("CreateServiceGateway")
	defer b.mu.Unlock()

	if len(in.Services) == 0 {
		return nil, "", errMissing("services is required")
	}

	v, aerr := b.gatewayVcn(in.CompartmentID, in.VcnID, in.DisplayName, in.RouteTableID)
	if aerr == nil {
		aerr = gatewayLimit[*ServiceGateway](b, v.ID, "service gateway", 1)
	}

	if aerr != nil {
		return nil, "", aerr
	}

	refs, aerr := serviceRefs(in.Services)
	if aerr != nil {
		return nil, "", aerr
	}

	g := &ServiceGateway{
		Base: b.newBase(kindSGW, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID,
		RouteTableID: in.RouteTableID, Services: refs,
	}
	b.add(g)

	return g.view(), g.Internal.Etag, nil
}

// UpdateServiceGatewayIn is the UpdateServiceGateway request body.
type UpdateServiceGatewayIn struct {
	BlockTraffic *bool          `json:"blockTraffic"`
	RouteTableID *string        `json:"routeTableId"`
	Services     *[]ServiceIDIn `json:"services"`
	UpdateIn
}

// UpdateServiceGateway changes a service gateway; services replaces the attached list.
func (b *InMemoryBackend) UpdateServiceGateway(id, ifMatch string, in UpdateServiceGatewayIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(g *ServiceGateway) *APIError {
		if in.RouteTableID != nil {
			if aerr := b.optRouteTable(*in.RouteTableID, g.VcnID); aerr != nil {
				return aerr
			}
		}

		var refs []ServiceRef

		if in.Services != nil {
			var aerr *APIError
			if refs, aerr = serviceRefs(*in.Services); aerr != nil {
				return aerr
			}

			if aerr = b.sgwRulesStillValid(g, refs); aerr != nil {
				return aerr
			}
		}

		if in.RouteTableID != nil {
			g.RouteTableID = *in.RouteTableID
		}

		if in.BlockTraffic != nil {
			g.BlockTraffic = *in.BlockTraffic
		}

		if in.Services != nil {
			g.Services = refs
		}

		return nil
	})
}

// sgwRulesStillValid refuses to drop a service that a route rule still targets through this gateway.
func (b *InMemoryBackend) sgwRulesStillValid(g *ServiceGateway, refs []ServiceRef) *APIError {
	for _, rt := range b.liveRefs(routeTarget(g.ID)) {
		for _, rule := range rt.(*RouteTable).RouteRules { //nolint:forcetypeassert // filtered by routeTarget
			if rule.NetworkEntityID != g.ID {
				continue
			}

			svc, _ := serviceByCidr(rule.Destination)
			if !slices.ContainsFunc(refs, func(r ServiceRef) bool { return r.ServiceID == svc.ID }) {
				return errConflict("The service " + rule.Destination + " is still used by a route rule of " + rt.base().ID)
			}
		}
	}

	return nil
}

// AttachService adds a service to a service gateway.
func (b *InMemoryBackend) AttachService(id, ifMatch string, in ServiceIDIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, UpdateIn{}, func(g *ServiceGateway) *APIError {
		svc, ok := serviceByID(in.ServiceID)
		if !ok {
			return errNotFound(in.ServiceID)
		}

		if slices.ContainsFunc(g.Services, func(r ServiceRef) bool { return r.ServiceID == svc.ID }) {
			return errConflict("The service " + svc.ID + " is already attached to " + g.ID)
		}

		refs := append(slices.Clone(g.Services), ServiceRef{ServiceID: svc.ID, ServiceName: svc.Name})
		if aerr := checkServiceMix(refs); aerr != nil {
			return aerr
		}

		g.Services = refs

		return nil
	})
}

// DetachService removes a service from a service gateway.
func (b *InMemoryBackend) DetachService(id, ifMatch string, in ServiceIDIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, UpdateIn{}, func(g *ServiceGateway) *APIError {
		i := slices.IndexFunc(g.Services, func(r ServiceRef) bool { return r.ServiceID == in.ServiceID })
		if i < 0 {
			return errNotFound(in.ServiceID)
		}

		refs := slices.Delete(slices.Clone(g.Services), i, i+1)
		if aerr := b.sgwRulesStillValid(g, refs); aerr != nil {
			return aerr
		}

		g.Services = refs

		return nil
	})
}
