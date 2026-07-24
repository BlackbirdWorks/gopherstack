package ec2

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ModifyVpnConnection moves a VPN connection onto a different VPN Gateway. An empty
// vpnGatewayID leaves the connection's gateway attachment unchanged.
func (b *InMemoryBackend) ModifyVpnConnection(vpnConnectionID, vpnGatewayID string) error {
	if vpnConnectionID == "" {
		return fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpnConnection")
	defer b.mu.Unlock()

	conn, ok := b.vpnConnections.Get(vpnConnectionID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	if vpnGatewayID != "" {
		if _, exists := b.vpnGateways.Get(vpnGatewayID); !exists {
			return fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, vpnGatewayID)
		}

		conn.VpnGatewayID = vpnGatewayID
		conn.TransitGatewayID = ""
	}

	return nil
}

// VpnConnectionRoute represents a static route in a VPN connection.
type VpnConnectionRoute struct {
	VpnConnectionID string `json:"vpnConnectionId,omitempty"`
	DestinationCIDR string `json:"destinationCidrBlock,omitempty"`
	State           string `json:"state,omitempty"`
}

// CreateVpnConnectionRoute adds a static route to a VPN connection.
func (b *InMemoryBackend) CreateVpnConnectionRoute(
	vpnConnectionID, destinationCIDR string,
) (*VpnConnectionRoute, error) {
	if vpnConnectionID == "" || destinationCIDR == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and DestinationCidrBlock are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateVpnConnectionRoute")
	defer b.mu.Unlock()

	if _, ok := b.vpnConnections.Get(vpnConnectionID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, vpnConnectionID)
	}

	route := &VpnConnectionRoute{
		VpnConnectionID: vpnConnectionID,
		DestinationCIDR: destinationCIDR,
		State:           stateActive,
	}
	b.vpnConnectionRoutes.Put(route)

	return route, nil
}

// DeleteVpnConnectionRoute removes a static route from a VPN connection.
func (b *InMemoryBackend) DeleteVpnConnectionRoute(vpnConnectionID, destinationCIDR string) error {
	if vpnConnectionID == "" || destinationCIDR == "" {
		return fmt.Errorf(
			"%w: VpnConnectionId and DestinationCidrBlock are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteVpnConnectionRoute")
	defer b.mu.Unlock()

	key := vpnConnectionID + ":" + destinationCIDR
	if _, ok := b.vpnConnectionRoutes.Get(key); !ok {
		return fmt.Errorf("%w: route %s not found", ErrInvalidParameter, destinationCIDR)
	}
	b.vpnConnectionRoutes.Delete(key)

	return nil
}

// ---- VPN Connections ----

// CreateVpnConnection creates a new VPN connection between a customer gateway and VPN gateway.
func (b *InMemoryBackend) CreateVpnConnection(
	connType, customerGatewayID, vpnGatewayID string,
) (*VpnConnection, error) {
	if customerGatewayID == "" {
		return nil, fmt.Errorf("%w: CustomerGatewayId is required", ErrInvalidParameter)
	}

	if vpnGatewayID == "" {
		return nil, fmt.Errorf("%w: VpnGatewayId is required", ErrInvalidParameter)
	}

	if connType == "" {
		connType = vpnTypeIPSec
	}

	b.mu.Lock("CreateVpnConnection")
	defer b.mu.Unlock()

	if _, ok := b.customerGateways.Get(customerGatewayID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrCustomerGatewayNotFound, customerGatewayID)
	}

	if _, ok := b.vpnGateways.Get(vpnGatewayID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, vpnGatewayID)
	}

	conn := &VpnConnection{
		VpnConnectionID:   "vpn-" + uuid.New().String()[:8],
		State:             stateAvailable,
		CustomerGatewayID: customerGatewayID,
		VpnGatewayID:      vpnGatewayID,
		Type:              connType,
		Category:          "VPN",
	}
	conn.Options = VpnConnectionOptions{TunnelOptions: generateVpnTunnels(b.vpnConnections.Len())}
	conn.VgwTelemetry = vgwTelemetryFromTunnels(conn.Options.TunnelOptions)
	conn.CustomerGatewayConfiguration = buildCustomerGatewayConfiguration(conn)
	b.vpnConnections.Put(conn)

	return copyVpnConnection(conn), nil
}

// DescribeVpnConnections returns VPN connections, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpnConnections(ids []string) []*VpnConnection {
	b.mu.RLock("DescribeVpnConnections")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*VpnConnection, 0, b.vpnConnections.Len())

	for _, conn := range b.vpnConnections.All() {
		if len(idSet) > 0 && !idSet[conn.VpnConnectionID] {
			continue
		}

		out = append(out, copyVpnConnection(conn))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].VpnConnectionID < out[j].VpnConnectionID
	})

	return out
}

// DeleteVpnConnection removes a VPN connection along with any static routes registered against it.
func (b *InMemoryBackend) DeleteVpnConnection(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpnConnection")
	defer b.mu.Unlock()

	if _, ok := b.vpnConnections.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, id)
	}
	b.vpnConnections.Delete(id)
	delete(b.tags, id)

	prefix := id + ":"
	for _, route := range b.vpnConnectionRoutes.All() {
		key := vpnConnectionRoutesKeyFn(route)
		if strings.HasPrefix(key, prefix) {
			b.vpnConnectionRoutes.Delete(key)
		}
	}

	return nil
}

// GetVpnConnectionRoutes returns the static routes registered against a VPN connection.
func (b *InMemoryBackend) GetVpnConnectionRoutes(vpnConnectionID string) []*VpnConnectionRoute {
	b.mu.RLock("GetVpnConnectionRoutes")
	defer b.mu.RUnlock()

	prefix := vpnConnectionID + ":"
	out := make([]*VpnConnectionRoute, 0)

	for _, route := range b.vpnConnectionRoutes.All() {
		key := vpnConnectionRoutesKeyFn(route)
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		cp := *route
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DestinationCIDR < out[j].DestinationCIDR
	})

	return out
}

// copyVpnConnection returns a deep copy of a VPN connection, including its slice-valued fields,
// so callers cannot mutate backend state through the returned pointer.
func copyVpnConnection(conn *VpnConnection) *VpnConnection {
	cp := *conn

	cp.Options.TunnelOptions = make([]VpnTunnelOption, len(conn.Options.TunnelOptions))
	for i, t := range conn.Options.TunnelOptions {
		tc := t
		tc.IKEVersions = append([]string(nil), t.IKEVersions...)
		cp.Options.TunnelOptions[i] = tc
	}

	cp.VgwTelemetry = append([]VgwTelemetry(nil), conn.VgwTelemetry...)

	return &cp
}

// indexOfVpnTunnel returns the index of the tunnel matching outsideIPAddress, or -1 if none matches.
func indexOfVpnTunnel(tunnels []VpnTunnelOption, outsideIPAddress string) int {
	for i, t := range tunnels {
		if t.OutsideIPAddress == outsideIPAddress {
			return i
		}
	}

	return -1
}

// generateVpnTunnels synthesizes the two IPsec tunnels AWS always provisions for a new VPN
// connection. connIndex (the number of VPN connections that already exist) is used to vary the
// generated addressing across connections so tunnels don't collide.
func generateVpnTunnels(connIndex int) []VpnTunnelOption {
	octet1 := connIndex%vpnTunnelOctetRange + 1
	octet2 := (connIndex+1)%vpnTunnelOctetRange + 1
	insideBlock := (connIndex % vpnTunnelInsideCIDRRange) * vpnTunnelInsideCIDRStep

	return []VpnTunnelOption{
		newVpnTunnel(vpnTunnelOutsideIPBase1+strconv.Itoa(octet1), insideBlock),
		newVpnTunnel(vpnTunnelOutsideIPBase2+strconv.Itoa(octet2), insideBlock+vpnTunnelInsideCIDRStep),
	}
}

// newVpnTunnel builds a single tunnel's default configuration.
func newVpnTunnel(outsideIP string, insideBlock int) VpnTunnelOption {
	return VpnTunnelOption{
		OutsideIPAddress:       outsideIP,
		TunnelInsideCIDR:       fmt.Sprintf("169.254.%d.0/30", insideBlock),
		PreSharedKey:           uuid.New().String()[:vpnPreSharedKeyLength],
		Phase1LifetimeSeconds:  vpnPhase1LifetimeSeconds,
		Phase2LifetimeSeconds:  vpnPhase2LifetimeSeconds,
		RekeyMarginTimeSeconds: vpnRekeyMarginTimeSeconds,
		DPDTimeoutSeconds:      vpnDPDTimeoutSeconds,
		DPDTimeoutAction:       "clear",
		StartupAction:          "add",
		IKEVersions:            []string{"ikev1", "ikev2"},
	}
}

// vgwTelemetryFromTunnels builds the initial per-tunnel telemetry for a newly-created VPN
// connection. Tunnels start DOWN since no real customer gateway peer ever connects.
func vgwTelemetryFromTunnels(tunnels []VpnTunnelOption) []VgwTelemetry {
	now := time.Now().UTC().Format(time.RFC3339)
	out := make([]VgwTelemetry, 0, len(tunnels))

	for _, t := range tunnels {
		out = append(out, VgwTelemetry{
			OutsideIPAddress:   t.OutsideIPAddress,
			Status:             "DOWN",
			StatusMessage:      "IPSEC IS DOWN",
			AcceptedRouteCount: 0,
			LastStatusChange:   now,
		})
	}

	return out
}

// buildCustomerGatewayConfiguration renders the vendor-neutral sample XML configuration blob
// AWS returns in the customerGatewayConfiguration field of a VPN connection.
func buildCustomerGatewayConfiguration(conn *VpnConnection) string {
	var sb strings.Builder

	sb.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	fmt.Fprintf(&sb, "<vpn_connection id=\"%s\">\n", conn.VpnConnectionID)
	fmt.Fprintf(&sb, "  <customer_gateway_id>%s</customer_gateway_id>\n", conn.CustomerGatewayID)

	if conn.VpnGatewayID != "" {
		fmt.Fprintf(&sb, "  <vpn_gateway_id>%s</vpn_gateway_id>\n", conn.VpnGatewayID)
	}

	if conn.TransitGatewayID != "" {
		fmt.Fprintf(&sb, "  <transit_gateway_id>%s</transit_gateway_id>\n", conn.TransitGatewayID)
	}

	fmt.Fprintf(&sb, "  <vpn_connection_type>%s</vpn_connection_type>\n", conn.Type)

	for i, t := range conn.Options.TunnelOptions {
		fmt.Fprintf(&sb, "  <ipsec_tunnel index=\"%d\">\n", i+1)
		fmt.Fprintf(
			&sb,
			"    <customer_gateway><tunnel_outside_address><ip_address>%s</ip_address>"+
				"</tunnel_outside_address></customer_gateway>\n",
			t.OutsideIPAddress,
		)
		fmt.Fprintf(&sb, "    <ike><pre_shared_key>%s</pre_shared_key></ike>\n", t.PreSharedKey)
		sb.WriteString("  </ipsec_tunnel>\n")
	}

	sb.WriteString("</vpn_connection>\n")

	return sb.String()
}

// ModifyVpnConnectionOptions updates the negotiated local/remote IPv4 network CIDRs and the
// static-routes-only flag of a VPN connection. Empty strings and a nil staticRoutesOnly leave
// the corresponding field unchanged.
func (b *InMemoryBackend) ModifyVpnConnectionOptions(
	vpnConnectionID, localIPv4CIDR, remoteIPv4CIDR string, staticRoutesOnly *bool,
) (*VpnConnection, error) {
	if vpnConnectionID == "" {
		return nil, fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpnConnectionOptions")
	defer b.mu.Unlock()

	conn, ok := b.vpnConnections.Get(vpnConnectionID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	if localIPv4CIDR != "" {
		conn.Options.LocalIPv4NetworkCIDR = localIPv4CIDR
	}

	if remoteIPv4CIDR != "" {
		conn.Options.RemoteIPv4NetworkCIDR = remoteIPv4CIDR
	}

	if staticRoutesOnly != nil {
		conn.Options.StaticRoutesOnly = *staticRoutesOnly
	}

	return copyVpnConnection(conn), nil
}

// ModifyVpnTunnelOptions updates the configuration of a single tunnel of a VPN connection,
// identified by its outside IP address. Zero-valued fields in opts leave the corresponding
// tunnel field unchanged.
func (b *InMemoryBackend) ModifyVpnTunnelOptions(
	vpnConnectionID, outsideIPAddress string, opts VpnTunnelOptionsModify,
) (*VpnConnection, error) {
	if vpnConnectionID == "" || outsideIPAddress == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and VpnTunnelOutsideIpAddress are required", ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyVpnTunnelOptions")
	defer b.mu.Unlock()

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

	applyVpnTunnelOptionsModify(&conn.Options.TunnelOptions[idx], opts)

	return copyVpnConnection(conn), nil
}

// applyVpnTunnelOptionsModify merges non-zero fields of opts onto an existing tunnel.
func applyVpnTunnelOptionsModify(t *VpnTunnelOption, opts VpnTunnelOptionsModify) {
	if opts.TunnelInsideCIDR != "" {
		t.TunnelInsideCIDR = opts.TunnelInsideCIDR
	}

	if opts.PreSharedKey != "" {
		t.PreSharedKey = opts.PreSharedKey
	}

	if opts.Phase1LifetimeSeconds > 0 {
		t.Phase1LifetimeSeconds = opts.Phase1LifetimeSeconds
	}

	if opts.Phase2LifetimeSeconds > 0 {
		t.Phase2LifetimeSeconds = opts.Phase2LifetimeSeconds
	}

	if opts.RekeyMarginTimeSeconds > 0 {
		t.RekeyMarginTimeSeconds = opts.RekeyMarginTimeSeconds
	}

	if opts.DPDTimeoutSeconds > 0 {
		t.DPDTimeoutSeconds = opts.DPDTimeoutSeconds
	}

	if opts.DPDTimeoutAction != "" {
		t.DPDTimeoutAction = opts.DPDTimeoutAction
	}

	if opts.StartupAction != "" {
		t.StartupAction = opts.StartupAction
	}

	if len(opts.IKEVersions) > 0 {
		t.IKEVersions = append([]string(nil), opts.IKEVersions...)
	}
}

// ModifyVpnTunnelCertificate provisions a private-certificate-based authentication certificate
// for a single tunnel of a VPN connection, identified by its outside IP address.
func (b *InMemoryBackend) ModifyVpnTunnelCertificate(
	vpnConnectionID, outsideIPAddress string,
) (*VpnConnection, error) {
	if vpnConnectionID == "" || outsideIPAddress == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and VpnTunnelOutsideIpAddress are required", ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyVpnTunnelCertificate")
	defer b.mu.Unlock()

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

	certARN := fmt.Sprintf("arn:aws:acm:%s:%s:certificate/%s", b.Region, b.AccountID, uuid.New().String())
	conn.Options.TunnelOptions[idx].CertificateARN = certARN

	for i := range conn.VgwTelemetry {
		if conn.VgwTelemetry[i].OutsideIPAddress == outsideIPAddress {
			conn.VgwTelemetry[i].CertificateARN = certARN
		}
	}

	return copyVpnConnection(conn), nil
}

// GetVpnConnectionDeviceTypes returns the static catalog of customer gateway device
// vendor/platform/software combinations AWS publishes sample configurations for.
func (b *InMemoryBackend) GetVpnConnectionDeviceTypes() []VpnConnectionDeviceType {
	return []VpnConnectionDeviceType{
		{
			VpnConnectionDeviceTypeID: "cisco-systems-inc-cisco-ios-15",
			Vendor:                    "Cisco Systems, Inc.",
			Platform:                  "Cisco ISR Series Router",
			Software:                  "IOS 12.4",
		},
		{
			VpnConnectionDeviceTypeID: "cisco-systems-inc-cisco-asa-9",
			Vendor:                    "Cisco Systems, Inc.",
			Platform:                  "Cisco ASA Series Router",
			Software:                  "ASA 9.x",
		},
		{
			VpnConnectionDeviceTypeID: "juniper-networks-inc-junos-srx-12",
			Vendor:                    "Juniper Networks, Inc.",
			Platform:                  "J-Series Routers",
			Software:                  "JunOS 12.x",
		},
		{
			VpnConnectionDeviceTypeID: "fortinet-inc-fortigate-40-plus-series-5",
			Vendor:                    "Fortinet, Inc.",
			Platform:                  "Fortigate 40+ Series",
			Software:                  "FortiOS 5.x",
		},
		{
			VpnConnectionDeviceTypeID: "palo-alto-networks-inc-pan-os-8",
			Vendor:                    "Palo Alto Networks, Inc.",
			Platform:                  "PA Series Router",
			Software:                  "PAN-OS 8.x",
		},
		{
			VpnConnectionDeviceTypeID: "checkpoint-r80-10",
			Vendor:                    "Check Point Software Technologies Ltd.",
			Platform:                  "Security Gateway",
			Software:                  "R80.10",
		},
		{
			VpnConnectionDeviceTypeID: "generic-vendor-x-generic-platform-generic-version",
			Vendor:                    "Generic",
			Platform:                  "Generic",
			Software:                  "Vendor Agnostic",
		},
	}
}

// GetVpnConnectionDeviceSampleConfiguration generates a sample vendor configuration for a VPN
// connection's tunnels, targeting the given device type and IKE version.
func (b *InMemoryBackend) GetVpnConnectionDeviceSampleConfiguration(
	vpnConnectionID, deviceTypeID, ikeVersion string,
) (string, error) {
	if vpnConnectionID == "" {
		return "", fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetVpnConnectionDeviceSampleConfiguration")
	defer b.mu.RUnlock()

	conn, ok := b.vpnConnections.Get(vpnConnectionID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	if deviceTypeID == "" {
		deviceTypeID = "generic-vendor-x-generic-platform-generic-version"
	}

	if ikeVersion == "" {
		ikeVersion = "ikev2"
	}

	var sb strings.Builder

	fmt.Fprintf(&sb, "! Sample configuration for device type %s (IKE %s)\n", deviceTypeID, ikeVersion)
	fmt.Fprintf(&sb, "! Generated for VPN connection %s\n", conn.VpnConnectionID)

	for i, t := range conn.Options.TunnelOptions {
		fmt.Fprintf(&sb, "\n! --- Tunnel %d ---\n", i+1)
		fmt.Fprintf(&sb, "crypto vpn tunnel outside-address %s\n", t.OutsideIPAddress)
		fmt.Fprintf(&sb, "crypto vpn tunnel inside-cidr %s\n", t.TunnelInsideCIDR)
		fmt.Fprintf(&sb, "crypto vpn ike pre-shared-key %s\n", t.PreSharedKey)
		fmt.Fprintf(&sb, "crypto vpn ike lifetime %d\n", t.Phase1LifetimeSeconds)
		fmt.Fprintf(&sb, "crypto vpn ipsec lifetime %d\n", t.Phase2LifetimeSeconds)
	}

	return sb.String(), nil
}

// GetVpnTunnelReplacementStatus reports whether AWS-initiated tunnel endpoint maintenance is
// pending for a single tunnel of a VPN connection. This mock never schedules maintenance.
func (b *InMemoryBackend) GetVpnTunnelReplacementStatus(
	vpnConnectionID, outsideIPAddress string,
) (*VpnTunnelReplacementStatus, error) {
	if vpnConnectionID == "" || outsideIPAddress == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and VpnTunnelOutsideIpAddress are required", ErrInvalidParameter,
		)
	}

	b.mu.RLock("GetVpnTunnelReplacementStatus")
	defer b.mu.RUnlock()

	conn, ok := b.vpnConnections.Get(vpnConnectionID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	if indexOfVpnTunnel(conn.Options.TunnelOptions, outsideIPAddress) < 0 {
		return nil, fmt.Errorf(
			"%w: no tunnel with outside IP address %s on %s",
			ErrVpnTunnelNotFound, outsideIPAddress, vpnConnectionID,
		)
	}

	return &VpnTunnelReplacementStatus{
		VpnConnectionID:           conn.VpnConnectionID,
		TransitGatewayID:          conn.TransitGatewayID,
		VpnGatewayID:              conn.VpnGatewayID,
		CustomerGatewayID:         conn.CustomerGatewayID,
		VpnTunnelOutsideIPAddress: outsideIPAddress,
		MaintenanceDetails:        VpnTunnelMaintenanceDetails{PendingMaintenance: "false"},
	}, nil
}
