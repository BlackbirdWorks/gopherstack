package ec2

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Errors for batch4 operations.
var (
	ErrManagedPrefixListNotFound         = errors.New("InvalidPrefixListID.NotFound")
	ErrClientVpnEndpointNotFound         = errors.New("InvalidClientVpnEndpointId.NotFound")
	ErrTransitGatewayConnectNotFound     = errors.New("InvalidTransitGatewayAttachmentID.NotFound")
	ErrTransitGatewayConnectPeerNotFound = errors.New("InvalidTransitGatewayConnectPeerId.NotFound")
	ErrTGWPrefixListRefNotFound          = errors.New("InvalidTransitGatewayPrefixListReferenceId.NotFound")
	ErrVerifiedAccessEndpointNotFound    = errors.New("InvalidVerifiedAccessEndpointId.NotFound")
	ErrVerifiedAccessGroupNotFound       = errors.New("InvalidVerifiedAccessGroupId.NotFound")
	ErrVerifiedAccessInstanceNotFound    = errors.New("InvalidVerifiedAccessInstanceId.NotFound")
	ErrVerifiedAccessTrustProviderNF     = errors.New("InvalidVerifiedAccessTrustProviderId.NotFound")
)

// ManagedPrefixList represents a managed prefix list.
type ManagedPrefixList struct {
	PrefixListID   string            `json:"prefixListId,omitempty"`
	PrefixListName string            `json:"prefixListName,omitempty"`
	PrefixListArn  string            `json:"prefixListArn,omitempty"`
	AddressFamily  string            `json:"addressFamily,omitempty"`
	State          string            `json:"state,omitempty"`
	OwnerID        string            `json:"ownerId,omitempty"`
	Entries        []PrefixListEntry `json:"entries,omitempty"`
	Version        int64             `json:"version"`
	MaxEntries     int               `json:"maxEntries,omitempty"`
}

// PrefixListEntry holds a single CIDR entry in a managed prefix list.
type PrefixListEntry struct {
	Cidr        string `json:"cidr,omitempty"`
	Description string `json:"description,omitempty"`
}

// ClientVpnTargetNetwork represents an associated target network for a Client VPN endpoint.
type ClientVpnTargetNetwork struct {
	AssociationID       string   `json:"associationId,omitempty"`
	SubnetID            string   `json:"subnetId,omitempty"`
	VPCID               string   `json:"vpcId,omitempty"`
	ClientVpnEndpointID string   `json:"clientVpnEndpointId,omitempty"`
	Status              string   `json:"status,omitempty"`
	SecurityGroups      []string `json:"securityGroups,omitempty"`
}

// ClientVpnEndpoint represents an EC2 Client VPN endpoint.
type ClientVpnEndpoint struct {
	ServerCertificateArn      string                    `json:"serverCertificateArn,omitempty"`
	DNSName                   string                    `json:"dnsName,omitempty"`
	Status                    string                    `json:"status,omitempty"`
	Description               string                    `json:"description,omitempty"`
	ClientCidrBlock           string                    `json:"clientCidrBlock,omitempty"`
	ClientVpnEndpointID       string                    `json:"clientVpnEndpointId,omitempty"`
	VpnProtocol               string                    `json:"vpnProtocol,omitempty"`
	TransportProtocol         string                    `json:"transportProtocol,omitempty"`
	VPCID                     string                    `json:"vpcId,omitempty"`
	CertificateRevocationList string                    `json:"certificateRevocationList,omitempty"`
	CreationTime              string                    `json:"creationTime,omitempty"`
	SelfServicePortalURL      string                    `json:"selfServicePortalUrl,omitempty"`
	DNSServers                []string                  `json:"dnsServers,omitempty"`
	SecurityGroupIDs          []string                  `json:"securityGroupIds,omitempty"`
	TargetNetworks            []*ClientVpnTargetNetwork `json:"targetNetworks,omitempty"`
	Routes                    []ClientVpnRoute          `json:"routes,omitempty"`
	AuthRules                 []ClientVpnAuthRule       `json:"authRules,omitempty"`
	SessionTimeoutHours       int32                     `json:"sessionTimeoutHours,omitempty"`
	VpnPort                   int32                     `json:"vpnPort,omitempty"`
	SplitTunnel               bool                      `json:"splitTunnel,omitempty"`
}

// ClientVpnRoute holds a single route for a Client VPN endpoint.
type ClientVpnRoute struct {
	DestinationCidr string `json:"destinationCidr,omitempty"`
	Status          string `json:"status,omitempty"`
	Description     string `json:"description,omitempty"`
	// Origin indicates how the route was added: "add-route" for routes created
	// via CreateClientVpnRoute, "associate" for routes auto-added when a target
	// network is associated.
	Origin string `json:"origin,omitempty"`
	// TargetSubnet is the ID of the subnet through which traffic is routed.
	TargetSubnet string `json:"targetSubnet,omitempty"`
}

// ClientVpnAuthRule holds a single authorization rule for a Client VPN endpoint.
type ClientVpnAuthRule struct {
	Cidr        string `json:"cidr,omitempty"`
	Status      string `json:"status,omitempty"`
	Description string `json:"description,omitempty"`
	// GroupID is the Active Directory group ID the rule grants access to, if any.
	GroupID string `json:"groupId,omitempty"`
	// AccessAll indicates the rule grants access to all clients (true when
	// GroupID is empty).
	AccessAll bool `json:"accessAll,omitempty"`
}

// ClientVpnConnection represents an active client connection to a Client VPN
// endpoint. No API in this backend creates connections (a real client would
// need to actually establish an OpenVPN session), so DescribeClientVpnConnections
// always returns an empty set — that is the correct AWS shape when no clients
// are connected.
type ClientVpnConnection struct {
	ConnectionID              string `json:"connectionId,omitempty"`
	ClientVpnEndpointID       string `json:"clientVpnEndpointId,omitempty"`
	Username                  string `json:"username,omitempty"`
	ClientIP                  string `json:"clientIp,omitempty"`
	CommonName                string `json:"commonName,omitempty"`
	ConnectionEstablishedTime string `json:"connectionEstablishedTime,omitempty"`
	Status                    string `json:"status,omitempty"`
}

// TransitGatewayConnect represents a TGW connect attachment.
type TransitGatewayConnect struct {
	TransitGatewayAttachmentID          string `json:"transitGatewayAttachmentId,omitempty"`
	TransportTransitGatewayAttachmentID string `json:"transportTransitGatewayAttachmentId,omitempty"`
	TransitGatewayID                    string `json:"transitGatewayId,omitempty"`
	State                               string `json:"state,omitempty"`
}

// TransitGatewayConnectPeer represents a TGW connect peer.
type TransitGatewayConnectPeer struct {
	TransitGatewayConnectPeerID string   `json:"transitGatewayConnectPeerId,omitempty"`
	TransitGatewayAttachmentID  string   `json:"transitGatewayAttachmentId,omitempty"`
	PeerAddress                 string   `json:"peerAddress,omitempty"`
	State                       string   `json:"state,omitempty"`
	InsideCidrBlocks            []string `json:"insideCidrBlocks,omitempty"`
}

// TransitGatewayPrefixListReference represents a TGW prefix list reference.
type TransitGatewayPrefixListReference struct {
	PrefixListID               string `json:"prefixListId,omitempty"`
	TransitGatewayRouteTableID string `json:"transitGatewayRouteTableId,omitempty"`
	State                      string `json:"state,omitempty"`
	TransitGatewayAttachmentID string `json:"transitGatewayAttachmentId,omitempty"`
	Blackhole                  bool   `json:"blackhole,omitempty"`
}

// VerifiedAccessEndpoint represents a Verified Access endpoint.
type VerifiedAccessEndpoint struct {
	VerifiedAccessEndpointID string `json:"verifiedAccessEndpointId,omitempty"`
	VerifiedAccessGroupID    string `json:"verifiedAccessGroupId,omitempty"`
	Status                   string `json:"status,omitempty"`
	Description              string `json:"description,omitempty"`
	EndpointType             string `json:"endpointType,omitempty"`
}

// VerifiedAccessGroup represents a Verified Access group.
type VerifiedAccessGroup struct {
	VerifiedAccessGroupID    string `json:"verifiedAccessGroupId,omitempty"`
	VerifiedAccessInstanceID string `json:"verifiedAccessInstanceId,omitempty"`
	Status                   string `json:"status,omitempty"`
	Description              string `json:"description,omitempty"`
}

// VerifiedAccessInstance represents a Verified Access instance.
type VerifiedAccessInstance struct {
	VerifiedAccessInstanceID string   `json:"verifiedAccessInstanceId,omitempty"`
	Status                   string   `json:"status,omitempty"`
	Description              string   `json:"description,omitempty"`
	AttachedTrustProviderIDs []string `json:"attachedTrustProviderIds,omitempty"`
}

// VerifiedAccessTrustProvider represents a Verified Access trust provider.
type VerifiedAccessTrustProvider struct {
	VerifiedAccessTrustProviderID string `json:"verifiedAccessTrustProviderId,omitempty"`
	TrustProviderType             string `json:"trustProviderType,omitempty"`
	Status                        string `json:"status,omitempty"`
	Description                   string `json:"description,omitempty"`
}

// ---- ManagedPrefixList ----

// CreateManagedPrefixList creates a new managed prefix list.
func (b *InMemoryBackend) CreateManagedPrefixList(
	name, addressFamily string,
	maxEntries int,
) (*ManagedPrefixList, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: PrefixListName is required", ErrInvalidParameter)
	}
	if addressFamily == "" {
		addressFamily = "IPv4"
	}

	b.mu.Lock("CreateManagedPrefixList")
	defer b.mu.Unlock()

	id := "pl-" + uuid.New().String()[:8]
	pl := &ManagedPrefixList{
		PrefixListID:   id,
		PrefixListName: name,
		PrefixListArn:  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":prefix-list/" + id,
		AddressFamily:  addressFamily,
		State:          "create-complete",
		MaxEntries:     maxEntries,
		Version:        1,
		OwnerID:        b.AccountID,
	}
	b.managedPrefixLists[id] = pl

	return pl, nil
}

// DeleteManagedPrefixList removes a managed prefix list.
func (b *InMemoryBackend) DeleteManagedPrefixList(id string) error {
	if id == "" {
		return fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteManagedPrefixList")
	defer b.mu.Unlock()

	if _, ok := b.managedPrefixLists[id]; !ok {
		return fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}
	delete(b.managedPrefixLists, id)

	return nil
}

// DescribeManagedPrefixLists returns managed prefix lists, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeManagedPrefixLists(ids []string) []*ManagedPrefixList {
	b.mu.RLock("DescribeManagedPrefixLists")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*ManagedPrefixList
	for _, pl := range b.managedPrefixLists {
		if len(filter) > 0 && !filter[pl.PrefixListID] {
			continue
		}
		cp := *pl
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PrefixListID < out[j].PrefixListID })

	return out
}

// GetManagedPrefixListEntries returns the entries for a prefix list.
func (b *InMemoryBackend) GetManagedPrefixListEntries(id string) ([]PrefixListEntry, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetManagedPrefixListEntries")
	defer b.mu.RUnlock()

	pl, ok := b.managedPrefixLists[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}

	out := make([]PrefixListEntry, len(pl.Entries))
	copy(out, pl.Entries)

	return out, nil
}

// ModifyManagedPrefixList modifies a managed prefix list.
func (b *InMemoryBackend) ModifyManagedPrefixList(
	id string,
	addEntries, removeEntries []PrefixListEntry,
) error {
	if id == "" {
		return fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyManagedPrefixList")
	defer b.mu.Unlock()

	pl, ok := b.managedPrefixLists[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}

	// Remove entries
	if len(removeEntries) > 0 {
		removeCIDRs := make(map[string]bool, len(removeEntries))
		for _, e := range removeEntries {
			removeCIDRs[e.Cidr] = true
		}
		var kept []PrefixListEntry
		for _, e := range pl.Entries {
			if !removeCIDRs[e.Cidr] {
				kept = append(kept, e)
			}
		}
		pl.Entries = kept
	}

	// Add entries
	pl.Entries = append(pl.Entries, addEntries...)
	pl.Version++
	pl.State = "modify-complete"

	return nil
}

// RestoreManagedPrefixListVersion restores a previous version of a prefix list.
func (b *InMemoryBackend) RestoreManagedPrefixListVersion(id string, version int64) error {
	if id == "" {
		return fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreManagedPrefixListVersion")
	defer b.mu.Unlock()

	pl, ok := b.managedPrefixLists[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}
	pl.Version = version
	pl.State = "restore-complete"

	return nil
}

// ---- ClientVpnEndpoint ----

// ClientVpnEndpointOptions holds the optional advanced Client VPN endpoint
// fields available via CreateClientVpnEndpointWithOptions and
// ModifyClientVpnEndpointWithOptions.
type ClientVpnEndpointOptions struct {
	SplitTunnel          *bool
	ServerCertificateArn string
	TransportProtocol    string
	VpcID                string
	SelfServicePortalURL string
	SecurityGroupIDs     []string
	VpnPort              int32
	SessionTimeoutHours  int32
}

// CreateClientVpnEndpoint creates a new Client VPN endpoint.
func (b *InMemoryBackend) CreateClientVpnEndpoint(
	clientCidrBlock, description string,
	dnsServers []string,
) (*ClientVpnEndpoint, error) {
	return b.CreateClientVpnEndpointWithOptions(clientCidrBlock, description, dnsServers, ClientVpnEndpointOptions{})
}

// CreateClientVpnEndpointWithOptions creates a new Client VPN endpoint with
// the full set of AWS-accepted advanced options.
func (b *InMemoryBackend) CreateClientVpnEndpointWithOptions(
	clientCidrBlock, description string,
	dnsServers []string,
	opts ClientVpnEndpointOptions,
) (*ClientVpnEndpoint, error) {
	if clientCidrBlock == "" {
		return nil, fmt.Errorf("%w: ClientCidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateClientVpnEndpoint")
	defer b.mu.Unlock()

	transportProtocol := opts.TransportProtocol
	if transportProtocol == "" {
		transportProtocol = "udp"
	}

	vpnPort := opts.VpnPort
	if vpnPort == 0 {
		vpnPort = 443
	}

	sessionTimeoutHours := opts.SessionTimeoutHours
	if sessionTimeoutHours == 0 {
		sessionTimeoutHours = 24
	}

	id := "cvpn-endpoint-" + uuid.New().String()[:8]
	ep := &ClientVpnEndpoint{
		ClientVpnEndpointID:  id,
		DNSName:              id + ".prod.clientvpn." + b.Region + ".amazonaws.com",
		Status:               stateAvailable,
		Description:          description,
		ClientCidrBlock:      clientCidrBlock,
		DNSServers:           dnsServers,
		VpnProtocol:          "openvpn",
		TransportProtocol:    transportProtocol,
		VPCID:                opts.VpcID,
		VpnPort:              vpnPort,
		SplitTunnel:          opts.SplitTunnel != nil && *opts.SplitTunnel,
		SecurityGroupIDs:     opts.SecurityGroupIDs,
		ServerCertificateArn: opts.ServerCertificateArn,
		SessionTimeoutHours:  sessionTimeoutHours,
		SelfServicePortalURL: opts.SelfServicePortalURL,
		CreationTime:         time.Now().UTC().Format(time.RFC3339),
	}
	b.clientVpnEndpoints[id] = ep

	return ep, nil
}

// DeleteClientVpnEndpoint removes a Client VPN endpoint.
func (b *InMemoryBackend) DeleteClientVpnEndpoint(id string) error {
	if id == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteClientVpnEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.clientVpnEndpoints[id]; !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, id)
	}
	delete(b.clientVpnEndpoints, id)

	return nil
}

// DescribeClientVpnEndpoints returns Client VPN endpoints, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeClientVpnEndpoints(ids []string) []*ClientVpnEndpoint {
	b.mu.RLock("DescribeClientVpnEndpoints")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*ClientVpnEndpoint
	for _, ep := range b.clientVpnEndpoints {
		if len(filter) > 0 && !filter[ep.ClientVpnEndpointID] {
			continue
		}
		cp := *ep
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientVpnEndpointID < out[j].ClientVpnEndpointID })

	return out
}

// AssociateClientVpnTargetNetwork associates a subnet with a Client VPN endpoint.
// Returns the generated association ID.
func (b *InMemoryBackend) AssociateClientVpnTargetNetwork(
	endpointID, subnetID string,
) (string, error) {
	if endpointID == "" || subnetID == "" {
		return "", fmt.Errorf("%w: ClientVpnEndpointId and SubnetId are required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateClientVpnTargetNetwork")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	for _, tn := range ep.TargetNetworks {
		if tn.SubnetID == subnetID {
			return tn.AssociationID, nil // already associated
		}
	}

	assocID := "cvpn-assoc-" + uuid.New().String()[:17]
	tn := &ClientVpnTargetNetwork{
		AssociationID:       assocID,
		SubnetID:            subnetID,
		ClientVpnEndpointID: endpointID,
		Status:              stateAssociated,
		SecurityGroups:      ep.SecurityGroupIDs,
	}
	if subnet, subnetFound := b.subnets[subnetID]; subnetFound {
		tn.VPCID = subnet.VPCID
		if ep.VPCID == "" {
			ep.VPCID = subnet.VPCID
		}
	}
	ep.TargetNetworks = append(ep.TargetNetworks, tn)

	return assocID, nil
}

// DisassociateClientVpnTargetNetwork disassociates a target network by association ID.
func (b *InMemoryBackend) DisassociateClientVpnTargetNetwork(
	endpointID, assocID string,
) error {
	if endpointID == "" || assocID == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId and AssociationId are required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateClientVpnTargetNetwork")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	var kept []*ClientVpnTargetNetwork
	found := false

	for _, tn := range ep.TargetNetworks {
		if tn.AssociationID == assocID {
			found = true

			continue
		}

		kept = append(kept, tn)
	}

	if !found {
		return fmt.Errorf("%w: association %s not found", ErrInvalidParameter, assocID)
	}

	ep.TargetNetworks = kept

	return nil
}

// DescribeClientVpnTargetNetworks returns associated target networks for an endpoint.
func (b *InMemoryBackend) DescribeClientVpnTargetNetworks(endpointID string) ([]*ClientVpnTargetNetwork, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeClientVpnTargetNetworks")
	defer b.mu.RUnlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	out := make([]*ClientVpnTargetNetwork, len(ep.TargetNetworks))
	for i, tn := range ep.TargetNetworks {
		cp := *tn
		out[i] = &cp
	}

	return out, nil
}

// CreateClientVpnRoute creates a route for a Client VPN endpoint.
func (b *InMemoryBackend) CreateClientVpnRoute(
	endpointID, destinationCidr, description string,
) error {
	if endpointID == "" || destinationCidr == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId and DestinationCidrBlock are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateClientVpnRoute")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	ep.Routes = append(ep.Routes, ClientVpnRoute{
		DestinationCidr: destinationCidr,
		Status:          stateActive,
		Description:     description,
		Origin:          "add-route",
	})

	return nil
}

// DeleteClientVpnRoute removes a route from a Client VPN endpoint.
func (b *InMemoryBackend) DeleteClientVpnRoute(endpointID, destinationCidr string) error {
	if endpointID == "" || destinationCidr == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId and DestinationCidrBlock are required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteClientVpnRoute")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	var kept []ClientVpnRoute
	for _, r := range ep.Routes {
		if r.DestinationCidr != destinationCidr {
			kept = append(kept, r)
		}
	}
	ep.Routes = kept

	return nil
}

// DescribeClientVpnRoutes returns routes for a Client VPN endpoint.
func (b *InMemoryBackend) DescribeClientVpnRoutes(endpointID string) ([]ClientVpnRoute, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeClientVpnRoutes")
	defer b.mu.RUnlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	out := make([]ClientVpnRoute, len(ep.Routes))
	copy(out, ep.Routes)

	return out, nil
}

// AuthorizeClientVpnIngress creates an authorization rule for a Client VPN endpoint.
func (b *InMemoryBackend) AuthorizeClientVpnIngress(
	endpointID, cidr, description string,
) error {
	if endpointID == "" || cidr == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId and AuthorizeAllGroups/Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeClientVpnIngress")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	ep.AuthRules = append(ep.AuthRules, ClientVpnAuthRule{
		Cidr:        cidr,
		Status:      stateActive,
		Description: description,
		AccessAll:   true, // no ActiveDirectory group id plumbed through this signature
	})

	return nil
}

// RevokeClientVpnIngress removes an authorization rule from a Client VPN endpoint.
func (b *InMemoryBackend) RevokeClientVpnIngress(endpointID, cidr string) error {
	if endpointID == "" || cidr == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("RevokeClientVpnIngress")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	var kept []ClientVpnAuthRule
	for _, r := range ep.AuthRules {
		if r.Cidr != cidr {
			kept = append(kept, r)
		}
	}
	ep.AuthRules = kept

	return nil
}

// DescribeClientVpnAuthorizationRules returns authorization rules for a Client VPN endpoint.
func (b *InMemoryBackend) DescribeClientVpnAuthorizationRules(
	endpointID string,
) ([]ClientVpnAuthRule, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeClientVpnAuthorizationRules")
	defer b.mu.RUnlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	out := make([]ClientVpnAuthRule, len(ep.AuthRules))
	copy(out, ep.AuthRules)

	return out, nil
}

// ModifyClientVpnEndpoint modifies a Client VPN endpoint.
func (b *InMemoryBackend) ModifyClientVpnEndpoint(
	endpointID, description string,
	dnsServers []string,
) error {
	return b.ModifyClientVpnEndpointWithOptions(endpointID, description, dnsServers, ClientVpnEndpointOptions{})
}

// ModifyClientVpnEndpointWithOptions modifies a Client VPN endpoint,
// including the advanced fields AWS accepts on ModifyClientVpnEndpoint.
func (b *InMemoryBackend) ModifyClientVpnEndpointWithOptions(
	endpointID, description string,
	dnsServers []string,
	opts ClientVpnEndpointOptions,
) error {
	if endpointID == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClientVpnEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}
	if description != "" {
		ep.Description = description
	}
	if len(dnsServers) > 0 {
		ep.DNSServers = dnsServers
	}
	if opts.ServerCertificateArn != "" {
		ep.ServerCertificateArn = opts.ServerCertificateArn
	}
	if opts.VpcID != "" {
		ep.VPCID = opts.VpcID
	}
	if opts.VpnPort != 0 {
		ep.VpnPort = opts.VpnPort
	}
	if opts.SessionTimeoutHours != 0 {
		ep.SessionTimeoutHours = opts.SessionTimeoutHours
	}
	if opts.SelfServicePortalURL != "" {
		ep.SelfServicePortalURL = opts.SelfServicePortalURL
	}
	if len(opts.SecurityGroupIDs) > 0 {
		ep.SecurityGroupIDs = opts.SecurityGroupIDs
	}
	if opts.SplitTunnel != nil {
		ep.SplitTunnel = *opts.SplitTunnel
	}

	return nil
}

// ApplySecurityGroupsToClientVpnTargetNetwork applies security groups to all
// target networks associated with a Client VPN endpoint's VPC.
func (b *InMemoryBackend) ApplySecurityGroupsToClientVpnTargetNetwork(endpointID string, sgIDs []string) error {
	if endpointID == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ApplySecurityGroupsToClientVpnTargetNetwork")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	ep.SecurityGroupIDs = sgIDs
	for _, tn := range ep.TargetNetworks {
		tn.SecurityGroups = sgIDs
	}

	return nil
}

// DescribeClientVpnConnections returns client VPN connections (empty list in mock).
func (b *InMemoryBackend) DescribeClientVpnConnections(endpointID string) ([]string, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeClientVpnConnections")
	defer b.mu.RUnlock()

	if _, ok := b.clientVpnEndpoints[endpointID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	return nil, nil
}

// TerminateClientVpnConnections terminates client VPN connections (no-op in mock).
func (b *InMemoryBackend) TerminateClientVpnConnections(endpointID string) error {
	if endpointID == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("TerminateClientVpnConnections")
	defer b.mu.RUnlock()

	if _, ok := b.clientVpnEndpoints[endpointID]; !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	return nil
}

// ExportClientVpnClientConfiguration returns an OpenVPN client configuration
// file for the given Client VPN endpoint.
func (b *InMemoryBackend) ExportClientVpnClientConfiguration(endpointID string) (string, error) {
	if endpointID == "" {
		return "", fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("ExportClientVpnClientConfiguration")
	defer b.mu.RUnlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	config := fmt.Sprintf(
		"client\ndev tun\nproto %s\nremote %s %d\nremote-random-hostname\n"+
			"resolv-retry infinite\nnobind\nremote-cert-tls server\ncipher AES-256-GCM\n"+
			"verb 3\n<ca>\n</ca>\nreneg-sec 0\n",
		ep.TransportProtocol, ep.DNSName, ep.VpnPort,
	)

	return config, nil
}

// ExportClientVpnClientCertificateRevocationList returns the certificate
// revocation list currently configured on the Client VPN endpoint.
func (b *InMemoryBackend) ExportClientVpnClientCertificateRevocationList(endpointID string) (string, error) {
	if endpointID == "" {
		return "", fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("ExportClientVpnClientCertificateRevocationList")
	defer b.mu.RUnlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	if ep.CertificateRevocationList != "" {
		return ep.CertificateRevocationList, nil
	}

	// AWS always returns a well-formed (possibly empty) CRL PEM block, even
	// when no client certificates have been revoked.
	return "-----BEGIN X509 CRL-----\n-----END X509 CRL-----\n", nil
}

// ImportClientVpnClientCertificateRevocationList uploads a certificate
// revocation list to the Client VPN endpoint.
func (b *InMemoryBackend) ImportClientVpnClientCertificateRevocationList(endpointID, crl string) error {
	if endpointID == "" || crl == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId and CertificateRevocationList are required", ErrInvalidParameter)
	}

	b.mu.Lock("ImportClientVpnClientCertificateRevocationList")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	ep.CertificateRevocationList = crl

	return nil
}

// ---- TransitGatewayPeeringAttachment ----

// CreateTransitGatewayPeeringAttachment creates a TGW peering attachment.
func (b *InMemoryBackend) CreateTransitGatewayPeeringAttachment(
	transitGatewayID, peerTransitGatewayID, _ string,
) (*TransitGatewayPeeringAttachment, error) {
	if transitGatewayID == "" || peerTransitGatewayID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayId and PeerTransitGatewayId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayPeeringAttachment")
	defer b.mu.Unlock()

	id := "tgw-attach-" + uuid.New().String()[:8]
	att := &TransitGatewayPeeringAttachment{
		TransitGatewayAttachmentID: id,
		RequesterTransitGatewayID:  transitGatewayID,
		AccepterTransitGatewayID:   peerTransitGatewayID,
		State:                      "pendingAcceptance",
	}
	b.tgwPeeringAttachments[id] = att

	return att, nil
}

// DeleteTransitGatewayPeeringAttachment removes a TGW peering attachment.
func (b *InMemoryBackend) DeleteTransitGatewayPeeringAttachment(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayPeeringAttachment")
	defer b.mu.Unlock()

	if _, ok := b.tgwPeeringAttachments[id]; !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, id)
	}
	delete(b.tgwPeeringAttachments, id)

	return nil
}

// DescribeTransitGatewayPeeringAttachments returns TGW peering attachments.
func (b *InMemoryBackend) DescribeTransitGatewayPeeringAttachments(
	ids []string,
) []*TransitGatewayPeeringAttachment {
	b.mu.RLock("DescribeTransitGatewayPeeringAttachments")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*TransitGatewayPeeringAttachment
	for _, att := range b.tgwPeeringAttachments {
		if len(filter) > 0 && !filter[att.TransitGatewayAttachmentID] {
			continue
		}
		cp := *att
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out
}

// ---- TransitGatewayConnect ----

// CreateTransitGatewayConnect creates a TGW connect attachment.
func (b *InMemoryBackend) CreateTransitGatewayConnect(
	transportAttachmentID, transitGatewayID string,
) (*TransitGatewayConnect, error) {
	if transportAttachmentID == "" || transitGatewayID == "" {
		return nil, fmt.Errorf(
			"%w: TransportTransitGatewayAttachmentId and TransitGatewayId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayConnect")
	defer b.mu.Unlock()

	id := "tgw-attach-" + uuid.New().String()[:8]
	conn := &TransitGatewayConnect{
		TransitGatewayAttachmentID:          id,
		TransportTransitGatewayAttachmentID: transportAttachmentID,
		TransitGatewayID:                    transitGatewayID,
		State:                               stateAvailable,
	}
	b.tgwConnects[id] = conn

	return conn, nil
}

// DeleteTransitGatewayConnect removes a TGW connect attachment.
func (b *InMemoryBackend) DeleteTransitGatewayConnect(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayConnect")
	defer b.mu.Unlock()

	if _, ok := b.tgwConnects[id]; !ok {
		return fmt.Errorf("%w: %s", ErrTransitGatewayConnectNotFound, id)
	}
	delete(b.tgwConnects, id)

	return nil
}

// DescribeTransitGatewayConnects returns TGW connect attachments.
func (b *InMemoryBackend) DescribeTransitGatewayConnects(ids []string) []*TransitGatewayConnect {
	b.mu.RLock("DescribeTransitGatewayConnects")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*TransitGatewayConnect
	for _, conn := range b.tgwConnects {
		if len(filter) > 0 && !filter[conn.TransitGatewayAttachmentID] {
			continue
		}
		cp := *conn
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out
}

// CreateTransitGatewayConnectPeer creates a TGW connect peer.
func (b *InMemoryBackend) CreateTransitGatewayConnectPeer(
	connectAttachmentID, peerAddress string,
	insideCidrBlocks []string,
) (*TransitGatewayConnectPeer, error) {
	if connectAttachmentID == "" || peerAddress == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayAttachmentId and PeerAddress are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayConnectPeer")
	defer b.mu.Unlock()

	if _, ok := b.tgwConnects[connectAttachmentID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayConnectNotFound, connectAttachmentID)
	}

	id := "tgw-connect-peer-" + uuid.New().String()[:8]
	peer := &TransitGatewayConnectPeer{
		TransitGatewayConnectPeerID: id,
		TransitGatewayAttachmentID:  connectAttachmentID,
		State:                       stateAvailable,
		InsideCidrBlocks:            insideCidrBlocks,
		PeerAddress:                 peerAddress,
	}
	b.tgwConnectPeers[id] = peer

	return peer, nil
}

// DeleteTransitGatewayConnectPeer removes a TGW connect peer.
func (b *InMemoryBackend) DeleteTransitGatewayConnectPeer(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayConnectPeerId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayConnectPeer")
	defer b.mu.Unlock()

	if _, ok := b.tgwConnectPeers[id]; !ok {
		return fmt.Errorf("%w: %s", ErrTransitGatewayConnectPeerNotFound, id)
	}
	delete(b.tgwConnectPeers, id)

	return nil
}

// DescribeTransitGatewayConnectPeers returns TGW connect peers.
func (b *InMemoryBackend) DescribeTransitGatewayConnectPeers(
	ids []string,
) []*TransitGatewayConnectPeer {
	b.mu.RLock("DescribeTransitGatewayConnectPeers")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*TransitGatewayConnectPeer
	for _, peer := range b.tgwConnectPeers {
		if len(filter) > 0 && !filter[peer.TransitGatewayConnectPeerID] {
			continue
		}
		cp := *peer
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayConnectPeerID < out[j].TransitGatewayConnectPeerID
	})

	return out
}

// ---- TransitGatewayPrefixListReference ----

// CreateTransitGatewayPrefixListReference creates a TGW prefix list reference.
func (b *InMemoryBackend) CreateTransitGatewayPrefixListReference(
	routeTableID, prefixListID string,
	blackhole bool,
) (*TransitGatewayPrefixListReference, error) {
	if routeTableID == "" || prefixListID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayRouteTableId and PrefixListId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayPrefixListReference")
	defer b.mu.Unlock()

	key := routeTableID + "/" + prefixListID
	ref := &TransitGatewayPrefixListReference{
		PrefixListID:               prefixListID,
		TransitGatewayRouteTableID: routeTableID,
		State:                      stateAvailable,
		Blackhole:                  blackhole,
	}
	b.tgwPrefixListRefs[key] = ref

	return ref, nil
}

// DeleteTransitGatewayPrefixListReference removes a TGW prefix list reference.
func (b *InMemoryBackend) DeleteTransitGatewayPrefixListReference(
	routeTableID, prefixListID string,
) error {
	if routeTableID == "" || prefixListID == "" {
		return fmt.Errorf(
			"%w: TransitGatewayRouteTableId and PrefixListId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteTransitGatewayPrefixListReference")
	defer b.mu.Unlock()

	key := routeTableID + "/" + prefixListID
	if _, ok := b.tgwPrefixListRefs[key]; !ok {
		return fmt.Errorf("%w: %s/%s", ErrTGWPrefixListRefNotFound, routeTableID, prefixListID)
	}
	delete(b.tgwPrefixListRefs, key)

	return nil
}

// GetTransitGatewayPrefixListReferences returns TGW prefix list references for a route table.
func (b *InMemoryBackend) GetTransitGatewayPrefixListReferences(
	routeTableID string,
) ([]*TransitGatewayPrefixListReference, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetTransitGatewayPrefixListReferences")
	defer b.mu.RUnlock()

	var out []*TransitGatewayPrefixListReference
	for _, ref := range b.tgwPrefixListRefs {
		if ref.TransitGatewayRouteTableID == routeTableID {
			cp := *ref
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PrefixListID < out[j].PrefixListID })

	return out, nil
}

// ModifyTransitGatewayPrefixListReference updates the blackhole flag and/or
// target attachment of an existing TGW prefix list reference.
func (b *InMemoryBackend) ModifyTransitGatewayPrefixListReference(
	routeTableID, prefixListID, attachmentID string,
	blackhole bool,
) (*TransitGatewayPrefixListReference, error) {
	if routeTableID == "" || prefixListID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayRouteTableId and PrefixListId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyTransitGatewayPrefixListReference")
	defer b.mu.Unlock()

	key := routeTableID + "/" + prefixListID

	ref, ok := b.tgwPrefixListRefs[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrTGWPrefixListRefNotFound, routeTableID, prefixListID)
	}

	ref.Blackhole = blackhole
	if attachmentID != "" {
		ref.TransitGatewayAttachmentID = attachmentID
	}

	cp := *ref

	return &cp, nil
}

// ---- VerifiedAccess ----

// CreateVerifiedAccessEndpoint creates a Verified Access endpoint.
func (b *InMemoryBackend) CreateVerifiedAccessEndpoint(
	groupID, endpointType, description string,
) (*VerifiedAccessEndpoint, error) {
	if groupID == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessGroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVerifiedAccessEndpoint")
	defer b.mu.Unlock()

	id := "vae-" + uuid.New().String()[:8]
	ep := &VerifiedAccessEndpoint{
		VerifiedAccessEndpointID: id,
		VerifiedAccessGroupID:    groupID,
		Status:                   stateActive,
		Description:              description,
		EndpointType:             endpointType,
	}
	b.verifiedAccessEndpoints[id] = ep

	return ep, nil
}

// DeleteVerifiedAccessEndpoint removes a Verified Access endpoint.
func (b *InMemoryBackend) DeleteVerifiedAccessEndpoint(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVerifiedAccessEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessEndpoints[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessEndpointNotFound, id)
	}
	delete(b.verifiedAccessEndpoints, id)

	return nil
}

// DescribeVerifiedAccessEndpoints returns Verified Access endpoints.
func (b *InMemoryBackend) DescribeVerifiedAccessEndpoints(ids []string) []*VerifiedAccessEndpoint {
	b.mu.RLock("DescribeVerifiedAccessEndpoints")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessEndpoint
	for _, ep := range b.verifiedAccessEndpoints {
		if len(filter) > 0 && !filter[ep.VerifiedAccessEndpointID] {
			continue
		}
		cp := *ep
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VerifiedAccessEndpointID < out[j].VerifiedAccessEndpointID
	})

	return out
}

// ModifyVerifiedAccessEndpoint modifies a Verified Access endpoint.
func (b *InMemoryBackend) ModifyVerifiedAccessEndpoint(id, description string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.verifiedAccessEndpoints[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessEndpointNotFound, id)
	}
	if description != "" {
		ep.Description = description
	}

	return nil
}

// CreateVerifiedAccessGroup creates a Verified Access group.
func (b *InMemoryBackend) CreateVerifiedAccessGroup(
	instanceID, description string,
) (*VerifiedAccessGroup, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVerifiedAccessGroup")
	defer b.mu.Unlock()

	id := "vagr-" + uuid.New().String()[:8]
	grp := &VerifiedAccessGroup{
		VerifiedAccessGroupID:    id,
		VerifiedAccessInstanceID: instanceID,
		Status:                   stateActive,
		Description:              description,
	}
	b.verifiedAccessGroups[id] = grp

	return grp, nil
}

// DeleteVerifiedAccessGroup removes a Verified Access group.
func (b *InMemoryBackend) DeleteVerifiedAccessGroup(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessGroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVerifiedAccessGroup")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessGroups[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessGroupNotFound, id)
	}
	delete(b.verifiedAccessGroups, id)

	return nil
}

// DescribeVerifiedAccessGroups returns Verified Access groups.
func (b *InMemoryBackend) DescribeVerifiedAccessGroups(ids []string) []*VerifiedAccessGroup {
	b.mu.RLock("DescribeVerifiedAccessGroups")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessGroup
	for _, grp := range b.verifiedAccessGroups {
		if len(filter) > 0 && !filter[grp.VerifiedAccessGroupID] {
			continue
		}
		cp := *grp
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VerifiedAccessGroupID < out[j].VerifiedAccessGroupID
	})

	return out
}

// CreateVerifiedAccessInstance creates a Verified Access instance.
func (b *InMemoryBackend) CreateVerifiedAccessInstance(description string) (*VerifiedAccessInstance, error) {
	b.mu.Lock("CreateVerifiedAccessInstance")
	defer b.mu.Unlock()

	id := "vai-" + uuid.New().String()[:8]
	inst := &VerifiedAccessInstance{
		VerifiedAccessInstanceID: id,
		Status:                   stateActive,
		Description:              description,
	}
	b.verifiedAccessInstances[id] = inst

	return inst, nil
}

// DeleteVerifiedAccessInstance removes a Verified Access instance.
func (b *InMemoryBackend) DeleteVerifiedAccessInstance(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVerifiedAccessInstance")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessInstances[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, id)
	}
	delete(b.verifiedAccessInstances, id)

	return nil
}

// DescribeVerifiedAccessInstances returns Verified Access instances.
func (b *InMemoryBackend) DescribeVerifiedAccessInstances(ids []string) []*VerifiedAccessInstance {
	b.mu.RLock("DescribeVerifiedAccessInstances")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessInstance
	for _, inst := range b.verifiedAccessInstances {
		if len(filter) > 0 && !filter[inst.VerifiedAccessInstanceID] {
			continue
		}
		cp := *inst
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VerifiedAccessInstanceID < out[j].VerifiedAccessInstanceID
	})

	return out
}

// CreateVerifiedAccessTrustProvider creates a Verified Access trust provider.
func (b *InMemoryBackend) CreateVerifiedAccessTrustProvider(
	trustProviderType, description string,
) (*VerifiedAccessTrustProvider, error) {
	if trustProviderType == "" {
		return nil, fmt.Errorf("%w: TrustProviderType is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	id := "vatp-" + uuid.New().String()[:8]
	tp := &VerifiedAccessTrustProvider{
		VerifiedAccessTrustProviderID: id,
		TrustProviderType:             trustProviderType,
		Status:                        stateActive,
		Description:                   description,
	}
	b.verifiedAccessTrustProviders[id] = tp

	return tp, nil
}

// DeleteVerifiedAccessTrustProvider removes a Verified Access trust provider.
func (b *InMemoryBackend) DeleteVerifiedAccessTrustProvider(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessTrustProviderId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessTrustProviders[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessTrustProviderNF, id)
	}
	delete(b.verifiedAccessTrustProviders, id)

	return nil
}

// DescribeVerifiedAccessTrustProviders returns Verified Access trust providers.
func (b *InMemoryBackend) DescribeVerifiedAccessTrustProviders(
	ids []string,
) []*VerifiedAccessTrustProvider {
	b.mu.RLock("DescribeVerifiedAccessTrustProviders")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessTrustProvider
	for _, tp := range b.verifiedAccessTrustProviders {
		if len(filter) > 0 && !filter[tp.VerifiedAccessTrustProviderID] {
			continue
		}
		cp := *tp
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VerifiedAccessTrustProviderID < out[j].VerifiedAccessTrustProviderID
	})

	return out
}

// AttachVerifiedAccessTrustProvider attaches a trust provider to a Verified Access instance.
func (b *InMemoryBackend) AttachVerifiedAccessTrustProvider(instanceID, trustProviderID string) error {
	if instanceID == "" || trustProviderID == "" {
		return fmt.Errorf(
			"%w: VerifiedAccessInstanceId and VerifiedAccessTrustProviderId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("AttachVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	inst, ok := b.verifiedAccessInstances[instanceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, instanceID)
	}
	if _, exists := b.verifiedAccessTrustProviders[trustProviderID]; !exists {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessTrustProviderNF, trustProviderID)
	}

	for _, id := range inst.AttachedTrustProviderIDs { //nolint:modernize // slices.Contains requires Go 1.21+
		if id == trustProviderID {
			return nil // already attached
		}
	}
	inst.AttachedTrustProviderIDs = append(inst.AttachedTrustProviderIDs, trustProviderID)

	return nil
}

// DetachVerifiedAccessTrustProvider detaches a trust provider from a Verified Access instance.
func (b *InMemoryBackend) DetachVerifiedAccessTrustProvider(instanceID, trustProviderID string) error {
	if instanceID == "" || trustProviderID == "" {
		return fmt.Errorf(
			"%w: VerifiedAccessInstanceId and VerifiedAccessTrustProviderId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DetachVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	inst, ok := b.verifiedAccessInstances[instanceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, instanceID)
	}

	var kept []string
	for _, id := range inst.AttachedTrustProviderIDs {
		if id != trustProviderID {
			kept = append(kept, id)
		}
	}
	inst.AttachedTrustProviderIDs = kept

	return nil
}
