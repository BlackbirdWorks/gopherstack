package networkmanager

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// This file implements PARITY.md families G-J: four Global-Networks
// association kinds binding a Device/Link to something else --
// CustomerGatewayAssociation (an EC2 CustomerGateway ARN), TransitGateway
// Registration (an EC2 TransitGateway ARN, scoped to the GlobalNetwork
// rather than a Device/Link), TransitGatewayConnectPeerAssociation (an EC2
// TransitGatewayConnectPeer ARN), and ConnectPeerAssociation (a Cloud WAN
// ConnectPeer -- the one real structural bridge between this service's two
// product halves, PARITY.md family J).
//
// Scope decision (loudly documented, not silent): this backend does NOT
// validate CustomerGatewayArn/TransitGatewayArn/TransitGatewayConnectPeerArn
// against services/ec2's real state. Doing so honestly would require a
// live cross-service backend reference wired through cli.go at
// Provider.Init time (this package cannot reach into another service's
// package-private state on its own), which was judged out of scope for
// this implementation pass given the size of the rest of this service --
// see the top-level report. These three ARN fields are therefore accepted
// as opaque, non-empty strings, same treatment as DirectConnectGatewayArn
// (family Q4, which PARITY.md itself flags as unvalidated since
// services/directconnect has no reachable backend reference either).
// ConnectPeerId, by contrast, names a resource this OWN package creates
// (family K) and IS validated below.

// ---- Customer Gateway Association ----

func (b *InMemoryBackend) AssociateCustomerGateway(
	globalNetworkID, customerGatewayArn, deviceID, linkID string,
) (*CustomerGatewayAssociation, error) {
	b.mu.Lock("AssociateCustomerGateway")
	defer b.mu.Unlock()

	if d, ok := b.devices.Get(deviceID); !ok || d.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceDevice, deviceID)
	}

	if customerGatewayArn == "" {
		return nil, validationError("CustomerGatewayArn is required")
	}

	a := &CustomerGatewayAssociation{
		CustomerGatewayArn: customerGatewayArn, DeviceID: deviceID, GlobalNetworkID: globalNetworkID, LinkID: linkID,
		State: assocStatePending,
	}
	b.customerGatewayAssociations.Put(a)

	key := customerGatewayAssociationKey(globalNetworkID, customerGatewayArn)
	scheduleAdvance(b, "CustomerGatewayAssociationAvailable", b.customerGatewayAssociations, key,
		func(v *CustomerGatewayAssociation) *string { return &v.State }, assocStatePending, assocStateAvailable)

	return a.clone(), nil
}

func (b *InMemoryBackend) DisassociateCustomerGateway(
	globalNetworkID, customerGatewayArn string,
) (*CustomerGatewayAssociation, error) {
	b.mu.Lock("DisassociateCustomerGateway")
	defer b.mu.Unlock()

	key := customerGatewayAssociationKey(globalNetworkID, customerGatewayArn)

	a, ok := b.customerGatewayAssociations.Get(key)
	if !ok {
		return nil, notFoundError(resourceCustomerGwAssn, key)
	}

	a.State = stateDeleting
	scheduleRemoval(b, "CustomerGatewayAssociationDeleted", b.customerGatewayAssociations, key)

	return a.clone(), nil
}

func (b *InMemoryBackend) GetCustomerGatewayAssociations(
	globalNetworkID string, arns []string, token string, limit int,
) (page.Page[*CustomerGatewayAssociation], error) {
	b.mu.RLock("GetCustomerGatewayAssociations")
	defer b.mu.RUnlock()

	all := filterByGlobalNetwork(
		b.customerGatewayAssociations.Snapshot(), globalNetworkID,
		func(a *CustomerGatewayAssociation) string { return a.GlobalNetworkID },
	)
	if len(arns) > 0 {
		all = filterByIDs(all, arns, func(a *CustomerGatewayAssociation) string { return a.CustomerGatewayArn })
	}

	p := sortAndPage(
		all, func(a *CustomerGatewayAssociation) string { return a.CustomerGatewayArn },
		(*CustomerGatewayAssociation).clone, token, limit,
	)

	return p, nil
}

// ---- Transit Gateway Registration ----

func (b *InMemoryBackend) RegisterTransitGateway(
	globalNetworkID, transitGatewayArn string,
) (*TransitGatewayRegistration, error) {
	b.mu.Lock("RegisterTransitGateway")
	defer b.mu.Unlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return nil, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	if transitGatewayArn == "" {
		return nil, validationError("TransitGatewayArn is required")
	}

	r := &TransitGatewayRegistration{
		GlobalNetworkID: globalNetworkID, TransitGatewayArn: transitGatewayArn,
		State: &TransitGatewayRegistrationStateReason{Code: tgwRegStatePending},
	}
	b.transitGatewayRegistrations.Put(r)

	key := transitGatewayRegistrationKey(globalNetworkID, transitGatewayArn)
	b.work.After("TransitGatewayRegistrationAvailable", asyncTransitionDelay, func() {
		b.mu.Lock("TransitGatewayRegistrationAvailable-async")
		defer b.mu.Unlock()

		if v, ok := b.transitGatewayRegistrations.Get(key); ok && v.State != nil && v.State.Code == tgwRegStatePending {
			v.State.Code = tgwRegStateAvailable
		}
	})

	return r.clone(), nil
}

func (b *InMemoryBackend) DeregisterTransitGateway(
	globalNetworkID, transitGatewayArn string,
) (*TransitGatewayRegistration, error) {
	b.mu.Lock("DeregisterTransitGateway")
	defer b.mu.Unlock()

	key := transitGatewayRegistrationKey(globalNetworkID, transitGatewayArn)

	r, ok := b.transitGatewayRegistrations.Get(key)
	if !ok {
		return nil, notFoundError(resourceTgwReg, key)
	}

	if r.State == nil {
		r.State = &TransitGatewayRegistrationStateReason{}
	}

	r.State.Code = tgwRegStateDeleting
	scheduleRemoval(b, "TransitGatewayRegistrationDeleted", b.transitGatewayRegistrations, key)

	return r.clone(), nil
}

func (b *InMemoryBackend) GetTransitGatewayRegistrations(
	globalNetworkID string, arns []string, token string, limit int,
) (page.Page[*TransitGatewayRegistration], error) {
	b.mu.RLock("GetTransitGatewayRegistrations")
	defer b.mu.RUnlock()

	all := filterByGlobalNetwork(
		b.transitGatewayRegistrations.Snapshot(), globalNetworkID,
		func(r *TransitGatewayRegistration) string { return r.GlobalNetworkID },
	)
	if len(arns) > 0 {
		all = filterByIDs(all, arns, func(r *TransitGatewayRegistration) string { return r.TransitGatewayArn })
	}

	p := sortAndPage(
		all, func(r *TransitGatewayRegistration) string { return r.TransitGatewayArn },
		(*TransitGatewayRegistration).clone, token, limit,
	)

	return p, nil
}

// ---- Transit Gateway Connect Peer Association ----

func (b *InMemoryBackend) AssociateTransitGatewayConnectPeer(
	globalNetworkID, deviceID, transitGatewayConnectPeerArn, linkID string,
) (*TransitGatewayConnectPeerAssociation, error) {
	b.mu.Lock("AssociateTransitGatewayConnectPeer")
	defer b.mu.Unlock()

	if d, ok := b.devices.Get(deviceID); !ok || d.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceDevice, deviceID)
	}

	if transitGatewayConnectPeerArn == "" {
		return nil, validationError("TransitGatewayConnectPeerArn is required")
	}

	a := &TransitGatewayConnectPeerAssociation{
		DeviceID: deviceID, GlobalNetworkID: globalNetworkID, LinkID: linkID,
		TransitGatewayConnectPeerArn: transitGatewayConnectPeerArn, State: assocStatePending,
	}
	b.transitGatewayConnectPeerAssociations.Put(a)

	key := transitGatewayConnectPeerAssociationKey(globalNetworkID, transitGatewayConnectPeerArn)
	scheduleAdvance(
		b,
		"TransitGatewayConnectPeerAssociationAvailable",
		b.transitGatewayConnectPeerAssociations,
		key,
		func(v *TransitGatewayConnectPeerAssociation) *string { return &v.State },
		assocStatePending,
		assocStateAvailable,
	)

	return a.clone(), nil
}

func (b *InMemoryBackend) DisassociateTransitGatewayConnectPeer(
	globalNetworkID, transitGatewayConnectPeerArn string,
) (*TransitGatewayConnectPeerAssociation, error) {
	b.mu.Lock("DisassociateTransitGatewayConnectPeer")
	defer b.mu.Unlock()

	key := transitGatewayConnectPeerAssociationKey(globalNetworkID, transitGatewayConnectPeerArn)

	a, ok := b.transitGatewayConnectPeerAssociations.Get(key)
	if !ok {
		return nil, notFoundError(resourceTgwCpAssn, key)
	}

	a.State = stateDeleting
	scheduleRemoval(b, "TransitGatewayConnectPeerAssociationDeleted", b.transitGatewayConnectPeerAssociations, key)

	return a.clone(), nil
}

func (b *InMemoryBackend) GetTransitGatewayConnectPeerAssociations(
	globalNetworkID string, arns []string, token string, limit int,
) (page.Page[*TransitGatewayConnectPeerAssociation], error) {
	b.mu.RLock("GetTransitGatewayConnectPeerAssociations")
	defer b.mu.RUnlock()

	all := filterByGlobalNetwork(
		b.transitGatewayConnectPeerAssociations.Snapshot(), globalNetworkID,
		func(a *TransitGatewayConnectPeerAssociation) string { return a.GlobalNetworkID },
	)
	if len(arns) > 0 {
		all = filterByIDs(
			all, arns, func(a *TransitGatewayConnectPeerAssociation) string { return a.TransitGatewayConnectPeerArn },
		)
	}

	p := sortAndPage(
		all, func(a *TransitGatewayConnectPeerAssociation) string { return a.TransitGatewayConnectPeerArn },
		(*TransitGatewayConnectPeerAssociation).clone, token, limit,
	)

	return p, nil
}

// ---- Connect Peer <-> Global Network association (the concrete bridge) ----

func (b *InMemoryBackend) AssociateConnectPeer(
	globalNetworkID, connectPeerID, deviceID, linkID string,
) (*ConnectPeerAssociation, error) {
	b.mu.Lock("AssociateConnectPeer")
	defer b.mu.Unlock()

	if d, ok := b.devices.Get(deviceID); !ok || d.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceDevice, deviceID)
	}

	if _, ok := b.connectPeers.Get(connectPeerID); !ok {
		return nil, notFoundError(resourceConnectPeer, connectPeerID)
	}

	a := &ConnectPeerAssociation{
		ConnectPeerID: connectPeerID, DeviceID: deviceID, GlobalNetworkID: globalNetworkID, LinkID: linkID,
		State: assocStatePending,
	}
	b.connectPeerAssociations.Put(a)

	key := connectPeerAssociationKey(globalNetworkID, connectPeerID)
	scheduleAdvance(b, "ConnectPeerAssociationAvailable", b.connectPeerAssociations, key,
		func(v *ConnectPeerAssociation) *string { return &v.State }, assocStatePending, assocStateAvailable)

	return a.clone(), nil
}

func (b *InMemoryBackend) DisassociateConnectPeer(
	globalNetworkID, connectPeerID string,
) (*ConnectPeerAssociation, error) {
	b.mu.Lock("DisassociateConnectPeer")
	defer b.mu.Unlock()

	key := connectPeerAssociationKey(globalNetworkID, connectPeerID)

	a, ok := b.connectPeerAssociations.Get(key)
	if !ok {
		return nil, notFoundError(resourceConnectPeerAsn, key)
	}

	a.State = stateDeleting
	scheduleRemoval(b, "ConnectPeerAssociationDeleted", b.connectPeerAssociations, key)

	return a.clone(), nil
}

func (b *InMemoryBackend) GetConnectPeerAssociations(
	globalNetworkID string, ids []string, token string, limit int,
) (page.Page[*ConnectPeerAssociation], error) {
	b.mu.RLock("GetConnectPeerAssociations")
	defer b.mu.RUnlock()

	all := filterByGlobalNetwork(
		b.connectPeerAssociations.Snapshot(), globalNetworkID,
		func(a *ConnectPeerAssociation) string { return a.GlobalNetworkID },
	)
	if len(ids) > 0 {
		all = filterByIDs(all, ids, func(a *ConnectPeerAssociation) string { return a.ConnectPeerID })
	}

	p := sortAndPage(
		all, func(a *ConnectPeerAssociation) string { return a.ConnectPeerID }, (*ConnectPeerAssociation).clone,
		token, limit,
	)

	return p, nil
}
