package cloudformation

import (
	"fmt"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	resTypeEC2VPNGateway    = "AWS::EC2::VPNGateway"
	resTypeEC2VPNConnection = "AWS::EC2::VPNConnection"
)

// createEC2VPNResource handles AWS::EC2::VPNGateway and
// AWS::EC2::VPNConnection creation. Returns handled=false otherwise.
func (rc *ResourceCreator) createEC2VPNResource(
	_, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2VPNGateway:
		id, err := rc.createEC2VPNGateway(props, params, physicalIDs)

		return id, true, err
	case resTypeEC2VPNConnection:
		id, err := rc.createEC2VPNConnection(props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteEC2VPNResource handles deletion for the types created above.
func (rc *ResourceCreator) deleteEC2VPNResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.EC2 == nil {
		switch resourceType {
		case resTypeEC2VPNGateway, resTypeEC2VPNConnection:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeEC2VPNGateway:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteVpnGateway(physicalID), ec2backend.ErrVpnGatewayNotFound,
		)
	case resTypeEC2VPNConnection:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteVpnConnection(physicalID), ec2backend.ErrVpnConnectionNotFound,
		)
	default:
		return false, nil
	}
}

// ---- AWS::EC2::VPNGateway ----
// Ref returns the ID of the VPN gateway (documented).

func (rc *ResourceCreator) createEC2VPNGateway(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "vgw-stub", nil
	}

	gatewayType := strProp(props, "Type", params, physicalIDs)
	asn := int64Prop(props, "AmazonSideAsn", params, physicalIDs)

	vgw, err := rc.backends.EC2.Backend.CreateVpnGateway(gatewayType, asn)
	if err != nil {
		return "", fmt.Errorf("create VPN gateway: %w", err)
	}

	return vgw.VpnGatewayID, nil
}

// ---- AWS::EC2::VPNConnection ----
// Ref returns the ID of the VPN connection (documented). Only the
// VpnGatewayId form is supported: the backend's CreateVpnConnection
// requires a VpnGatewayId, so a template using TransitGatewayId instead
// fails honestly rather than being silently accepted.

func (rc *ResourceCreator) createEC2VPNConnection(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "vpn-stub", nil
	}

	connType := strProp(props, "Type", params, physicalIDs)
	customerGatewayID := strProp(props, "CustomerGatewayId", params, physicalIDs)
	vpnGatewayID := strProp(props, "VpnGatewayId", params, physicalIDs)

	conn, err := rc.backends.EC2.Backend.CreateVpnConnection(connType, customerGatewayID, vpnGatewayID)
	if err != nil {
		return "", fmt.Errorf("create VPN connection: %w", err)
	}

	return conn.VpnConnectionID, nil
}
