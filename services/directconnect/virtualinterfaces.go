package directconnect

import (
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// vifCreateParams collects the fields shared by every Create*/Allocate*
// VirtualInterface variant (private/public/transit), letting
// createVirtualInterfaceLocked do the real validation/construction work
// once instead of six times.
type vifCreateParams struct {
	vifType      string
	connectionID string
	// ownerAccount is empty for same-account Create* flows (defaults to
	// b.accountID) and set for cross-account Allocate* flows.
	ownerAccount           string
	name                   string
	addressFamily          string
	amazonAddress          string
	customerAddress        string
	authKey                string
	directConnectGatewayID string
	virtualGatewayID       string
	rateLimit              string
	asnLong                *int64
	mtu                    *int32
	enableSiteLink         *bool
	routeFilterPrefixes    []routeFilterPrefixWire
	tags                   []tagWire
	vlan                   int32
	asn                    int32
}

// resolveGatewayBindingLocked validates a private/transit VIF's optional
// DirectConnectGatewayId/VirtualGatewayId pair: at most one may be set
// (PARITY.md's exactly-one convention), and if set it must resolve to a
// real resource -- DirectConnectGatewayId against this backend's own
// gateways table, VirtualGatewayId against EC2's VpnGateway list when an
// EC2GatewayResolver is wired in (see store.go), accepted unchecked
// otherwise.
func (b *InMemoryBackend) resolveGatewayBindingLocked(directConnectGatewayID, virtualGatewayID string) error {
	if directConnectGatewayID != "" && virtualGatewayID != "" {
		return clientError("cannot specify both directConnectGatewayId and virtualGatewayId")
	}

	if directConnectGatewayID != "" {
		if _, ok := b.gateways.Get(directConnectGatewayID); !ok {
			return notFoundError(resourceGateway, directConnectGatewayID)
		}
	}

	if virtualGatewayID != "" && b.ec2Resolver != nil {
		if !b.ec2Resolver.ResolveVpnGateway(virtualGatewayID) {
			return notFoundError("virtual private gateway", virtualGatewayID)
		}
	}

	return nil
}

// validateVifCreateLocked runs every check createVirtualInterfaceLocked
// needs before it may construct a VirtualInterface: the connection exists,
// name/vlan are present where required, VLAN is unique on the connection,
// the gateway binding (private/transit only) resolves, and Mtu (if given)
// is one of the two supported values. Split out from
// createVirtualInterfaceLocked purely to decompose it rather than suppress
// its cognitive-complexity lint finding. Callers must hold b.mu.
func (b *InMemoryBackend) validateVifCreateLocked(p vifCreateParams) error {
	if _, ok := b.connections.Get(p.connectionID); !ok {
		return notFoundError(resourceConnection, p.connectionID)
	}

	if p.vifType != VifTypeTransit && (p.name == "" || p.vlan == 0) {
		return clientError("virtualInterfaceName and vlan are required")
	}

	if p.vlan != 0 {
		for _, existing := range b.vifsByConnection.Get(p.connectionID) {
			if existing.Vlan == p.vlan {
				return clientError("VLAN " + p.name + " is already in use on this connection")
			}
		}
	}

	if p.vifType != VifTypePublic {
		if err := b.resolveGatewayBindingLocked(p.directConnectGatewayID, p.virtualGatewayID); err != nil {
			return err
		}
	}

	if p.mtu != nil && *p.mtu != defaultMtu && *p.mtu != jumboMtu {
		return clientError("mtu must be 1500 or 8500")
	}

	return nil
}

// initialVifState returns the VirtualInterfaceState a newly created VIF
// starts in: "confirming" for a cross-account Allocate* flow (awaits
// ConfirmXVirtualInterface regardless of type), "verifying" for a
// same-account public VIF (its own pre-provisioning validation step), or
// "pending" for a same-account private/transit VIF.
func initialVifState(p vifCreateParams) string {
	switch {
	case p.ownerAccount != "":
		return VifStateConfirming
	case p.vifType == VifTypePublic:
		return VifStateVerifying
	default:
		return VifStatePending
	}
}

// createVirtualInterfaceLocked validates and stores a new VirtualInterface.
// Callers must hold b.mu and have already validated Tags via
// validateNewTags.
func (b *InMemoryBackend) createVirtualInterfaceLocked(p vifCreateParams) (*VirtualInterface, error) {
	if err := b.validateVifCreateLocked(p); err != nil {
		return nil, err
	}

	owner := p.ownerAccount
	if owner == "" {
		owner = b.accountID
	}

	name := p.name
	if name == "" {
		name = p.vifType + "-vif-" + newVifID()
	}

	mtu := int32(defaultMtu)
	if p.mtu != nil {
		mtu = *p.mtu
	}

	asnValue, asnProvided := resolveAsnInput(p.asn, p.asnLong)

	id := newVifID()
	t := tags.New("directconnect.vif." + id + ".tags")
	t.Merge(tagWireToMap(p.tags))

	vif := &VirtualInterface{
		VirtualInterfaceID:     id,
		VirtualInterfaceName:   name,
		VirtualInterfaceType:   p.vifType,
		VirtualInterfaceState:  initialVifState(p),
		ConnectionID:           p.connectionID,
		OwnerAccount:           owner,
		Region:                 b.region,
		Location:               b.locationOfConnectionLocked(p.connectionID),
		AddressFamily:          p.addressFamily,
		AmazonAddress:          p.amazonAddress,
		CustomerAddress:        p.customerAddress,
		AuthKey:                p.authKey,
		DirectConnectGatewayID: p.directConnectGatewayID,
		VirtualGatewayID:       p.virtualGatewayID,
		RateLimit:              p.rateLimit,
		RouteFilterPrefixes:    fromRouteFilterPrefixWire(p.routeFilterPrefixes),
		Vlan:                   p.vlan,
		Mtu:                    mtu,
		AsnValue:               asnValue,
		AsnProvided:            asnProvided,
		SiteLinkEnabled:        p.enableSiteLink != nil && *p.enableSiteLink,
		Tags:                   t,
	}

	b.virtualInterfaces.Put(vif)

	if p.ownerAccount == "" {
		b.scheduleVifToAvailableLocked(vif)
	}

	return vif.clone(), nil
}

// scheduleVifToAvailableLocked schedules the create-time auto-progression
// to "available" for a same-account VIF -- verifying->pending->available
// for public, pending->available for private/transit. Callers must hold
// b.mu.
func (b *InMemoryBackend) scheduleVifToAvailableLocked(vif *VirtualInterface) {
	steps := []string{VifStateAvailable}
	if vif.VirtualInterfaceType == VifTypePublic {
		steps = []string{VifStatePending, VifStateAvailable}
	}

	b.scheduleTransition("vif:"+vif.VirtualInterfaceID, steps, &vif.VirtualInterfaceState)
}

func (b *InMemoryBackend) locationOfConnectionLocked(connectionID string) string {
	if c, ok := b.connections.Get(connectionID); ok {
		return c.Location
	}

	if l, ok := b.lags.Get(connectionID); ok {
		return l.Location
	}

	return ""
}

// CreatePrivateVirtualInterface creates a same-account private VIF.
func (b *InMemoryBackend) CreatePrivateVirtualInterface(
	connectionID string, n *newPrivateVifWire,
) (*VirtualInterface, error) {
	if n == nil {
		return nil, clientError("newPrivateVirtualInterface is required")
	}

	if err := validateNewTags(tagWireKeys(n.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePrivateVirtualInterface")
	defer b.mu.Unlock()

	return b.createVirtualInterfaceLocked(vifCreateParams{
		vifType:                VifTypePrivate,
		connectionID:           connectionID,
		name:                   n.VirtualInterfaceName,
		vlan:                   n.Vlan,
		addressFamily:          n.AddressFamily,
		amazonAddress:          n.AmazonAddress,
		asn:                    n.Asn,
		asnLong:                n.AsnLong,
		authKey:                n.AuthKey,
		customerAddress:        n.CustomerAddress,
		directConnectGatewayID: n.DirectConnectGatewayID,
		virtualGatewayID:       n.VirtualGatewayID,
		mtu:                    n.Mtu,
		rateLimit:              n.RateLimit,
		enableSiteLink:         n.EnableSiteLink,
		tags:                   n.Tags,
	})
}

// AllocatePrivateVirtualInterface allocates a cross-account private VIF.
func (b *InMemoryBackend) AllocatePrivateVirtualInterface(
	connectionID, ownerAccount string, n *newPrivateVifAllocationWire,
) (*VirtualInterface, error) {
	if n == nil || ownerAccount == "" {
		return nil, clientError("newPrivateVirtualInterfaceAllocation and ownerAccount are required")
	}

	if err := validateNewTags(tagWireKeys(n.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("AllocatePrivateVirtualInterface")
	defer b.mu.Unlock()

	return b.createVirtualInterfaceLocked(vifCreateParams{
		vifType:         VifTypePrivate,
		connectionID:    connectionID,
		ownerAccount:    ownerAccount,
		name:            n.VirtualInterfaceName,
		vlan:            n.Vlan,
		addressFamily:   n.AddressFamily,
		amazonAddress:   n.AmazonAddress,
		asn:             n.Asn,
		asnLong:         n.AsnLong,
		authKey:         n.AuthKey,
		customerAddress: n.CustomerAddress,
		mtu:             n.Mtu,
		rateLimit:       n.RateLimit,
		tags:            n.Tags,
	})
}

// CreatePublicVirtualInterface creates a same-account public VIF.
func (b *InMemoryBackend) CreatePublicVirtualInterface(
	connectionID string, n *newPublicVifWire,
) (*VirtualInterface, error) {
	if n == nil {
		return nil, clientError("newPublicVirtualInterface is required")
	}

	if err := validateNewTags(tagWireKeys(n.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePublicVirtualInterface")
	defer b.mu.Unlock()

	return b.createVirtualInterfaceLocked(vifCreateParams{
		vifType:             VifTypePublic,
		connectionID:        connectionID,
		name:                n.VirtualInterfaceName,
		vlan:                n.Vlan,
		addressFamily:       n.AddressFamily,
		amazonAddress:       n.AmazonAddress,
		asn:                 n.Asn,
		asnLong:             n.AsnLong,
		authKey:             n.AuthKey,
		customerAddress:     n.CustomerAddress,
		rateLimit:           n.RateLimit,
		routeFilterPrefixes: n.RouteFilterPrefixes,
		tags:                n.Tags,
	})
}

// AllocatePublicVirtualInterface allocates a cross-account public VIF.
func (b *InMemoryBackend) AllocatePublicVirtualInterface(
	connectionID, ownerAccount string, n *newPublicVifAllocationWire,
) (*VirtualInterface, error) {
	if n == nil || ownerAccount == "" {
		return nil, clientError("newPublicVirtualInterfaceAllocation and ownerAccount are required")
	}

	if err := validateNewTags(tagWireKeys(n.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("AllocatePublicVirtualInterface")
	defer b.mu.Unlock()

	return b.createVirtualInterfaceLocked(vifCreateParams{
		vifType:             VifTypePublic,
		connectionID:        connectionID,
		ownerAccount:        ownerAccount,
		name:                n.VirtualInterfaceName,
		vlan:                n.Vlan,
		addressFamily:       n.AddressFamily,
		amazonAddress:       n.AmazonAddress,
		asn:                 n.Asn,
		asnLong:             n.AsnLong,
		authKey:             n.AuthKey,
		customerAddress:     n.CustomerAddress,
		rateLimit:           n.RateLimit,
		routeFilterPrefixes: n.RouteFilterPrefixes,
		tags:                n.Tags,
	})
}

// CreateTransitVirtualInterface creates a same-account transit VIF.
func (b *InMemoryBackend) CreateTransitVirtualInterface(
	connectionID string, n *newTransitVifWire,
) (*VirtualInterface, error) {
	if n == nil {
		return nil, clientError("newTransitVirtualInterface is required")
	}

	if err := validateNewTags(tagWireKeys(n.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateTransitVirtualInterface")
	defer b.mu.Unlock()

	return b.createVirtualInterfaceLocked(vifCreateParams{
		vifType:                VifTypeTransit,
		connectionID:           connectionID,
		name:                   n.VirtualInterfaceName,
		vlan:                   n.Vlan,
		addressFamily:          n.AddressFamily,
		amazonAddress:          n.AmazonAddress,
		asn:                    n.Asn,
		asnLong:                n.AsnLong,
		authKey:                n.AuthKey,
		customerAddress:        n.CustomerAddress,
		directConnectGatewayID: n.DirectConnectGatewayID,
		mtu:                    n.Mtu,
		rateLimit:              n.RateLimit,
		enableSiteLink:         n.EnableSiteLink,
		tags:                   n.Tags,
	})
}

// AllocateTransitVirtualInterface allocates a cross-account transit VIF.
func (b *InMemoryBackend) AllocateTransitVirtualInterface(
	connectionID, ownerAccount string, n *newTransitVifAllocationWire,
) (*VirtualInterface, error) {
	if n == nil || ownerAccount == "" {
		return nil, clientError("newTransitVirtualInterfaceAllocation and ownerAccount are required")
	}

	if err := validateNewTags(tagWireKeys(n.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("AllocateTransitVirtualInterface")
	defer b.mu.Unlock()

	return b.createVirtualInterfaceLocked(vifCreateParams{
		vifType:         VifTypeTransit,
		connectionID:    connectionID,
		ownerAccount:    ownerAccount,
		name:            n.VirtualInterfaceName,
		vlan:            n.Vlan,
		addressFamily:   n.AddressFamily,
		amazonAddress:   n.AmazonAddress,
		asn:             n.Asn,
		asnLong:         n.AsnLong,
		authKey:         n.AuthKey,
		customerAddress: n.CustomerAddress,
		mtu:             n.Mtu,
		rateLimit:       n.RateLimit,
		tags:            n.Tags,
	})
}

// ConfirmPrivateVirtualInterface confirms a cross-account private VIF.
func (b *InMemoryBackend) ConfirmPrivateVirtualInterface(
	vifID, directConnectGatewayID, virtualGatewayID string,
) (string, error) {
	b.mu.Lock("ConfirmPrivateVirtualInterface")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(vifID)
	if !ok {
		return "", notFoundError(resourceVif, vifID)
	}

	if v.VirtualInterfaceType != VifTypePrivate || v.VirtualInterfaceState != VifStateConfirming {
		return "", clientError("virtual interface " + vifID + " is not awaiting private confirmation")
	}

	if err := b.resolveGatewayBindingLocked(directConnectGatewayID, virtualGatewayID); err != nil {
		return "", err
	}

	if directConnectGatewayID != "" {
		v.DirectConnectGatewayID = directConnectGatewayID
	}

	if virtualGatewayID != "" {
		v.VirtualGatewayID = virtualGatewayID
	}

	v.VirtualInterfaceState = VifStatePending
	b.scheduleVifToAvailableLocked(v)

	return v.VirtualInterfaceState, nil
}

// ConfirmPublicVirtualInterface confirms a cross-account public VIF.
func (b *InMemoryBackend) ConfirmPublicVirtualInterface(vifID string) (string, error) {
	b.mu.Lock("ConfirmPublicVirtualInterface")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(vifID)
	if !ok {
		return "", notFoundError(resourceVif, vifID)
	}

	if v.VirtualInterfaceType != VifTypePublic || v.VirtualInterfaceState != VifStateConfirming {
		return "", clientError("virtual interface " + vifID + " is not awaiting public confirmation")
	}

	v.VirtualInterfaceState = VifStatePending
	b.scheduleTransition("vif:"+vifID, []string{VifStateAvailable}, &v.VirtualInterfaceState)

	return v.VirtualInterfaceState, nil
}

// ConfirmTransitVirtualInterface confirms a cross-account transit VIF,
// binding it to directConnectGatewayID (required at confirm time even
// though optional at create time -- PARITY.md).
func (b *InMemoryBackend) ConfirmTransitVirtualInterface(vifID, directConnectGatewayID string) (string, error) {
	if directConnectGatewayID == "" {
		return "", clientError("directConnectGatewayId is required")
	}

	b.mu.Lock("ConfirmTransitVirtualInterface")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(vifID)
	if !ok {
		return "", notFoundError(resourceVif, vifID)
	}

	if v.VirtualInterfaceType != VifTypeTransit || v.VirtualInterfaceState != VifStateConfirming {
		return "", clientError("virtual interface " + vifID + " is not awaiting transit confirmation")
	}

	if _, gwOK := b.gateways.Get(directConnectGatewayID); !gwOK {
		return "", notFoundError(resourceGateway, directConnectGatewayID)
	}

	v.DirectConnectGatewayID = directConnectGatewayID
	v.VirtualInterfaceState = VifStatePending
	b.scheduleTransition("vif:"+vifID, []string{VifStateAvailable}, &v.VirtualInterfaceState)

	return v.VirtualInterfaceState, nil
}

// AssociateVirtualInterface moves an already-provisioned VIF onto a
// different connection or LAG.
func (b *InMemoryBackend) AssociateVirtualInterface(vifID, targetID string) (*VirtualInterface, error) {
	b.mu.Lock("AssociateVirtualInterface")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(vifID)
	if !ok {
		return nil, notFoundError(resourceVif, vifID)
	}

	if _, targetOK := b.resolveHostLocked(targetID); !targetOK {
		return nil, notFoundError(resourceConnection, targetID)
	}

	v.ConnectionID = targetID

	return v.clone(), nil
}

// UpdateVirtualInterfaceAttributes applies a partial update.
func (b *InMemoryBackend) UpdateVirtualInterfaceAttributes(req *updateVifAttributesRequest) (*VirtualInterface, error) {
	b.mu.Lock("UpdateVirtualInterfaceAttributes")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(req.VirtualInterfaceID)
	if !ok {
		return nil, notFoundError(resourceVif, req.VirtualInterfaceID)
	}

	if req.Mtu != nil {
		if *req.Mtu != defaultMtu && *req.Mtu != jumboMtu {
			return nil, clientError("mtu must be 1500 or 8500")
		}

		v.Mtu = *req.Mtu
		v.JumboFrameCapable = *req.Mtu == jumboMtu
	}

	if req.EnableSiteLink != nil {
		v.SiteLinkEnabled = *req.EnableSiteLink
	}

	if req.RateLimit != "" {
		v.RateLimit = req.RateLimit
	}

	if req.VirtualInterfaceName != "" {
		v.VirtualInterfaceName = req.VirtualInterfaceName
	}

	return v.clone(), nil
}

// DeleteVirtualInterface transitions a VIF to "deleting" then "deleted",
// or straight to the terminal "rejected" state if the owner declines it
// while still awaiting cross-account confirmation (PARITY.md).
func (b *InMemoryBackend) DeleteVirtualInterface(vifID string) (string, error) {
	b.mu.Lock("DeleteVirtualInterface")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(vifID)
	if !ok {
		return "", notFoundError(resourceVif, vifID)
	}

	if v.VirtualInterfaceState == VifStateConfirming {
		v.VirtualInterfaceState = VifStateRejected

		return v.VirtualInterfaceState, nil
	}

	v.VirtualInterfaceState = VifStateDeleting
	b.scheduleTransition("vif:"+vifID, []string{VifStateDeleted}, &v.VirtualInterfaceState)

	return v.VirtualInterfaceState, nil
}

// DescribeVirtualInterfaces returns VIFs, filtered independently by
// connectionID and/or vifID when non-empty (PARITY.md: both filters are
// independently optional, any combination).
func (b *InMemoryBackend) DescribeVirtualInterfaces(connectionID, vifID string) []*VirtualInterface {
	b.mu.RLock("DescribeVirtualInterfaces")
	defer b.mu.RUnlock()

	if vifID != "" {
		v, ok := b.virtualInterfaces.Get(vifID)
		if !ok || (connectionID != "" && v.ConnectionID != connectionID) {
			return nil
		}

		return []*VirtualInterface{v.clone()}
	}

	var all []*VirtualInterface
	if connectionID != "" {
		all = b.vifsByConnection.Get(connectionID)
	} else {
		all = b.virtualInterfaces.Snapshot()
	}

	out := make([]*VirtualInterface, 0, len(all))
	for _, v := range all {
		out = append(out, v.clone())
	}

	return out
}

// GatewayAmazonSideAsn returns the AmazonSideAsn of the given
// DirectConnectGateway, or nil if it does not exist -- used to populate
// VirtualInterface.AmazonSideAsn at wire-build time (see models.go's
// VirtualInterface doc comment).
func (b *InMemoryBackend) GatewayAmazonSideAsn(gatewayID string) *int64 {
	if gatewayID == "" {
		return nil
	}

	b.mu.RLock("GatewayAmazonSideAsn")
	defer b.mu.RUnlock()

	g, ok := b.gateways.Get(gatewayID)
	if !ok || g.AmazonSideAsn == 0 {
		return nil
	}

	v := g.AmazonSideAsn

	return &v
}
