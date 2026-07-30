package ec2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// TransitGatewayClientVpnAttachment represents an attachment between a Client
// VPN endpoint and a Transit Gateway, created implicitly by
// CreateClientVpnEndpointWithOptions when TransitGatewayConfiguration is
// supplied, and mutated by Accept/Reject/DeleteTransitGatewayClientVpnAttachment.
type TransitGatewayClientVpnAttachment struct {
	CreationTime               string `json:"creationTime,omitempty"`
	TransitGatewayAttachmentID string `json:"transitGatewayAttachmentId,omitempty"`
	TransitGatewayID           string `json:"transitGatewayId,omitempty"`
	ClientVpnEndpointID        string `json:"clientVpnEndpointId,omitempty"`
	ClientVpnOwnerID           string `json:"clientVpnOwnerId,omitempty"`
	State                      string `json:"state,omitempty"`
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

	if opts.TransitGatewayID != "" {
		if _, ok := b.transitGateways.Get(opts.TransitGatewayID); !ok {
			return nil, fmt.Errorf("%w: %s", ErrTransitGatewayNotFound, opts.TransitGatewayID)
		}

		b.tgwClientVpnAttachments.Put(&TransitGatewayClientVpnAttachment{
			TransitGatewayAttachmentID: newTransitGatewayAttachmentID(),
			TransitGatewayID:           opts.TransitGatewayID,
			ClientVpnEndpointID:        id,
			ClientVpnOwnerID:           b.AccountID,
			State:                      "pending-acceptance",
			CreationTime:               time.Now().UTC().Format(time.RFC3339),
		})
	}

	b.clientVpnEndpoints.Put(ep)

	return ep, nil
}

// ---- Transit Gateway Client VPN Attachments ----

// AcceptTransitGatewayClientVpnAttachment accepts a pending Transit Gateway
// Client VPN attachment, transitioning its state to "available".
func (b *InMemoryBackend) AcceptTransitGatewayClientVpnAttachment(
	attachmentID string,
) (*TransitGatewayClientVpnAttachment, error) {
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptTransitGatewayClientVpnAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwClientVpnAttachments.Get(attachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayAttachmentNotFound, attachmentID)
	}

	att.State = stateAvailable
	cp := *att

	return &cp, nil
}

// RejectTransitGatewayClientVpnAttachment rejects a pending Transit Gateway
// Client VPN attachment, transitioning its state to "rejected".
func (b *InMemoryBackend) RejectTransitGatewayClientVpnAttachment(
	attachmentID string,
) (*TransitGatewayClientVpnAttachment, error) {
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectTransitGatewayClientVpnAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwClientVpnAttachments.Get(attachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayAttachmentNotFound, attachmentID)
	}

	att.State = tgwAttachmentStateRejected
	cp := *att

	return &cp, nil
}

// DeleteTransitGatewayClientVpnAttachment removes a Transit Gateway Client
// VPN attachment, returning its final "deleted" state.
func (b *InMemoryBackend) DeleteTransitGatewayClientVpnAttachment(
	attachmentID string,
) (*TransitGatewayClientVpnAttachment, error) {
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayClientVpnAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwClientVpnAttachments.Get(attachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayAttachmentNotFound, attachmentID)
	}

	cp := *att
	cp.State = "deleted"
	b.tgwClientVpnAttachments.Delete(attachmentID)
	delete(b.tags, attachmentID)

	return &cp, nil
}

// DeleteClientVpnEndpoint removes a Client VPN endpoint.
func (b *InMemoryBackend) DeleteClientVpnEndpoint(id string) error {
	if id == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteClientVpnEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.clientVpnEndpoints.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, id)
	}
	b.clientVpnEndpoints.Delete(id)
	delete(b.tags, id)

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
	for _, ep := range b.clientVpnEndpoints.All() {
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	for _, tn := range ep.TargetNetworks {
		if tn.SubnetID == subnetID {
			return tn.AssociationID, nil // already associated
		}
	}

	assocID := newClientVPNAssociationID()
	tn := &ClientVpnTargetNetwork{
		AssociationID:       assocID,
		SubnetID:            subnetID,
		ClientVpnEndpointID: endpointID,
		Status:              stateAssociated,
		SecurityGroups:      ep.SecurityGroupIDs,
	}
	if subnet, subnetFound := b.subnets.Get(subnetID); subnetFound {
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	if _, ok := b.clientVpnEndpoints.Get(endpointID); !ok {
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

	if _, ok := b.clientVpnEndpoints.Get(endpointID); !ok {
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
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

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	ep.CertificateRevocationList = crl

	return nil
}

// ---- TransitGatewayPeeringAttachment ----
