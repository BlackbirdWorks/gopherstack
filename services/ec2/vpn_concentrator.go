package ec2

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ErrVpnConcentratorNotFound is returned when a VPN concentrator is not found.
var ErrVpnConcentratorNotFound = errors.New("InvalidVpnConcentratorID.NotFound")

// ---- constants ----

const (
	vpnConcentratorTypeIpsec1     = "ipsec.1"
	vpnConcentratorIDPrefixLength = 17
	vpnConcentratorStateDeleted   = "deleted"

	// activeVpnTunnelDefaultDHGroup is the Diffie-Hellman group number this mock
	// reports for both IKE phases; AWS's own default recommended configuration also
	// uses group 14 for both phases.
	activeVpnTunnelDefaultDHGroup = 14
)

// ---- models ----

// VpnConcentrator represents a VPN concentrator, an aggregation point for multiple
// Site-to-Site VPN connections attached to a transit gateway.
type VpnConcentrator struct {
	Tags                       map[string]string `json:"tags,omitempty"`
	VpnConcentratorID          string            `json:"vpnConcentratorId,omitempty"`
	Type                       string            `json:"type,omitempty"`
	State                      string            `json:"state,omitempty"`
	TransitGatewayID           string            `json:"transitGatewayId,omitempty"`
	TransitGatewayAttachmentID string            `json:"transitGatewayAttachmentId,omitempty"`
}

func cloneVpnConcentrator(vc *VpnConcentrator) *VpnConcentrator {
	cp := *vc
	cp.Tags = maps.Clone(vc.Tags)

	return &cp
}

// ---- CreateVpnConcentrator ----

// CreateVpnConcentrator creates a VPN concentrator, optionally attaching it to a
// transit gateway. If a transit gateway ID is supplied it must reference a real
// transit gateway; a synthetic attachment ID is generated to represent the resulting
// transit gateway attachment.
func (b *InMemoryBackend) CreateVpnConcentrator(
	vpnType, transitGatewayID string,
	tags map[string]string,
) (*VpnConcentrator, error) {
	if vpnType == "" {
		vpnType = vpnConcentratorTypeIpsec1
	}

	b.mu.Lock("CreateVpnConcentrator")
	defer b.mu.Unlock()

	var attachmentID string

	if transitGatewayID != "" {
		if _, ok := b.transitGateways.Get(transitGatewayID); !ok {
			return nil, fmt.Errorf("%w: %s", ErrTransitGatewayNotFound, transitGatewayID)
		}

		attachmentID = "tgw-attach-" + uuid.New().String()[:vpnConcentratorIDPrefixLength]
	}

	vc := &VpnConcentrator{
		VpnConcentratorID:          "vpnc-" + uuid.New().String()[:vpnConcentratorIDPrefixLength],
		Type:                       vpnType,
		State:                      stateAvailable,
		TransitGatewayID:           transitGatewayID,
		TransitGatewayAttachmentID: attachmentID,
		Tags:                       maps.Clone(tags),
	}
	b.vpnConcentrators.Put(vc)

	return cloneVpnConcentrator(vc), nil
}

// DeleteVpnConcentrator removes a VPN concentrator.
func (b *InMemoryBackend) DeleteVpnConcentrator(id string) (*VpnConcentrator, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VpnConcentratorId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpnConcentrator")
	defer b.mu.Unlock()

	vc, ok := b.vpnConcentrators.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnConcentratorNotFound, id)
	}

	vc.State = vpnConcentratorStateDeleted
	out := cloneVpnConcentrator(vc)
	b.vpnConcentrators.Delete(id)
	delete(b.tags, id)

	return out, nil
}

// DescribeVpnConcentrators returns VPN concentrators, optionally filtered by ID.
func (b *InMemoryBackend) DescribeVpnConcentrators(ids []string) []*VpnConcentrator {
	b.mu.RLock("DescribeVpnConcentrators")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*VpnConcentrator, 0, b.vpnConcentrators.Len())

	for _, vc := range b.vpnConcentrators.All() {
		if len(idSet) > 0 && !idSet[vc.VpnConcentratorID] {
			continue
		}

		out = append(out, cloneVpnConcentrator(vc))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VpnConcentratorID < out[j].VpnConcentratorID })

	return out
}

// ---- VPN tunnel extras ----

// GetActiveVpnTunnelStatus reports the current negotiated security configuration of a
// single tunnel of a VPN connection, identified by its outside IP address. It reads
// the real tunnel options already stored on the VPN connection.
func (b *InMemoryBackend) GetActiveVpnTunnelStatus(
	vpnConnectionID, outsideIPAddress string,
) (*ActiveVpnTunnelStatus, error) {
	if vpnConnectionID == "" || outsideIPAddress == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and VpnTunnelOutsideIpAddress are required", ErrInvalidParameter,
		)
	}

	b.mu.RLock("GetActiveVpnTunnelStatus")
	defer b.mu.RUnlock()

	conn, ok := b.vpnConnections.Get(vpnConnectionID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	idx := indexOfVpnTunnel(conn.Options.TunnelOptions, outsideIPAddress)
	if idx < 0 {
		return nil, fmt.Errorf(
			"%w: no tunnel with outside IP address %s on %s",
			ErrVpnTunnelNotFound, outsideIPAddress, vpnConnectionID,
		)
	}

	t := conn.Options.TunnelOptions[idx]
	ikeVersion := "ikev1"

	if len(t.IKEVersions) > 0 {
		ikeVersion = t.IKEVersions[len(t.IKEVersions)-1]
	}

	return &ActiveVpnTunnelStatus{
		Phase1EncryptionAlgorithm: "AES256",
		Phase1IntegrityAlgorithm:  "SHA2-256",
		Phase1DHGroup:             activeVpnTunnelDefaultDHGroup,
		Phase2EncryptionAlgorithm: "AES256",
		Phase2IntegrityAlgorithm:  "SHA2-256",
		Phase2DHGroup:             activeVpnTunnelDefaultDHGroup,
		IKEVersion:                ikeVersion,
		ProvisioningStatus:        "available",
	}, nil
}

// ActiveVpnTunnelStatus is the result of GetActiveVpnTunnelStatus.
type ActiveVpnTunnelStatus struct {
	IKEVersion                string
	Phase1EncryptionAlgorithm string
	Phase1IntegrityAlgorithm  string
	Phase2EncryptionAlgorithm string
	Phase2IntegrityAlgorithm  string
	ProvisioningStatus        string
	ProvisioningStatusReason  string
	Phase1DHGroup             int32
	Phase2DHGroup             int32
}

// ReplaceVpnTunnel rotates a single tunnel of a VPN connection: a new pre-shared key is
// generated and the tunnel's telemetry is stamped to reflect the replacement, mirroring
// AWS's tunnel endpoint replacement behavior.
func (b *InMemoryBackend) ReplaceVpnTunnel(vpnConnectionID, outsideIPAddress string) (bool, error) {
	if vpnConnectionID == "" || outsideIPAddress == "" {
		return false, fmt.Errorf(
			"%w: VpnConnectionId and VpnTunnelOutsideIpAddress are required", ErrInvalidParameter,
		)
	}

	b.mu.Lock("ReplaceVpnTunnel")
	defer b.mu.Unlock()

	conn, ok := b.vpnConnections.Get(vpnConnectionID)
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	idx := indexOfVpnTunnel(conn.Options.TunnelOptions, outsideIPAddress)
	if idx < 0 {
		return false, fmt.Errorf(
			"%w: no tunnel with outside IP address %s on %s",
			ErrVpnTunnelNotFound, outsideIPAddress, vpnConnectionID,
		)
	}

	conn.Options.TunnelOptions[idx].PreSharedKey = uuid.New().String()

	for i := range conn.VgwTelemetry {
		if conn.VgwTelemetry[i].OutsideIPAddress == outsideIPAddress {
			conn.VgwTelemetry[i].LastStatusChange = time.Now().UTC().Format(time.RFC3339)
			conn.VgwTelemetry[i].StatusMessage = "Tunnel replaced"
		}
	}

	return true, nil
}
