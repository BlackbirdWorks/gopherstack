package cloudformation

import (
	"errors"
	"fmt"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	resTypeEC2DHCPOptions             = "AWS::EC2::DHCPOptions"
	resTypeEC2VPCDHCPOptionsAssoc     = "AWS::EC2::VPCDHCPOptionsAssociation"
	resTypeEC2EIPAssociation          = "AWS::EC2::EIPAssociation" //nolint:gosec // not a credential
	resTypeEC2EgressOnlyIGW           = "AWS::EC2::EgressOnlyInternetGateway"
	resTypeEC2CustomerGateway         = "AWS::EC2::CustomerGateway"
	resTypeEC2CarrierGateway          = "AWS::EC2::CarrierGateway"
	resTypeEC2InstanceConnectEndpoint = "AWS::EC2::InstanceConnectEndpoint"
	resTypeEC2ClientVpnEndpoint       = "AWS::EC2::ClientVpnEndpoint"
	resTypeEC2ClientVpnTargetNetAssoc = "AWS::EC2::ClientVpnTargetNetworkAssociation"
	resTypeEC2ClientVpnAuthRule       = "AWS::EC2::ClientVpnAuthorizationRule"
	resTypeEC2ClientVpnRoute          = "AWS::EC2::ClientVpnRoute"
	resTypeEC2IPAM                    = "AWS::EC2::IPAM"
	resTypeEC2IPAMScope               = "AWS::EC2::IPAMScope"
	resTypeEC2IPAMPool                = "AWS::EC2::IPAMPool"
	resTypeEC2IPAMPoolCidr            = "AWS::EC2::IPAMPoolCidr"
	resTypeEC2CapacityReservation     = "AWS::EC2::CapacityReservation"
	resTypeEC2Host                    = "AWS::EC2::Host"
)

// createEC2MoreResource handles the additional AWS::EC2::* types layered onto the
// TransitGateway family (split into networking/ClientVPN/IPAM/host sub-dispatchers
// to keep each switch's cyclomatic complexity down).
func (rc *ResourceCreator) createEC2MoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createEC2NetworkingResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createEC2ClientVpnResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createEC2IpamResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return rc.createEC2HostResource(logicalID, resourceType, props, params, physicalIDs)
}

// deleteEC2MoreResource handles deletion for the types createEC2MoreResource creates.
func (rc *ResourceCreator) deleteEC2MoreResource(resourceType, physicalID string) (bool, error) {
	if ok, err := rc.deleteEC2NetworkingResource(resourceType, physicalID); ok {
		return true, err
	}

	if ok, err := rc.deleteEC2ClientVpnResource(resourceType, physicalID); ok {
		return true, err
	}

	if ok, err := rc.deleteEC2IpamResource(resourceType, physicalID); ok {
		return true, err
	}

	return rc.deleteEC2HostResource(resourceType, physicalID)
}

// ---- Networking: DHCPOptions, EIPAssociation, EgressOnlyIGW, CustomerGateway,
// CarrierGateway, InstanceConnectEndpoint ----

func (rc *ResourceCreator) createEC2NetworkingResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2DHCPOptions:
		id, err := rc.createEC2DHCPOptions(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2VPCDHCPOptionsAssoc:
		id, err := rc.createEC2VPCDHCPOptionsAssociation(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2EIPAssociation:
		id, err := rc.createEC2EIPAssociation(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2EgressOnlyIGW:
		id, err := rc.createEC2EgressOnlyIGW(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2CustomerGateway:
		id, err := rc.createEC2CustomerGateway(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2CarrierGateway:
		id, err := rc.createEC2CarrierGateway(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2InstanceConnectEndpoint:
		id, err := rc.createEC2InstanceConnectEndpoint(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteEC2NetworkingResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeEC2DHCPOptions:

		return true, rc.deleteEC2DHCPOptions(physicalID)
	case resTypeEC2VPCDHCPOptionsAssoc:
		// Real AWS has no DisassociateDhcpOptions call; a VPC always has some
		// (default or explicit) DHCP options set, it's never "unassociated".

		return true, nil
	case resTypeEC2EIPAssociation:

		return true, rc.deleteEC2EIPAssociation(physicalID)
	case resTypeEC2EgressOnlyIGW:

		return true, rc.deleteEC2EgressOnlyIGW(physicalID)
	case resTypeEC2CustomerGateway:

		return true, rc.deleteEC2CustomerGateway(physicalID)
	case resTypeEC2CarrierGateway:

		return true, rc.deleteEC2CarrierGateway(physicalID)
	case resTypeEC2InstanceConnectEndpoint:

		return true, rc.deleteEC2InstanceConnectEndpoint(physicalID)
	default:

		return false, nil
	}
}

func dhcpConfigurationsFromProps(
	props map[string]any,
	params, physicalIDs map[string]string,
) []ec2backend.DhcpConfiguration {
	raw, ok := props["DhcpConfigurations"].([]any)
	if !ok {
		return nil
	}

	out := make([]ec2backend.DhcpConfiguration, 0, len(raw))

	for _, item := range raw {
		m, itemOK := item.(map[string]any)
		if !itemOK {
			continue
		}

		key := strProp(m, "Key", params, physicalIDs)
		if key == "" {
			continue
		}

		values := strSliceProp(m["Values"], params, physicalIDs)
		out = append(out, ec2backend.DhcpConfiguration{Key: key, Values: values})
	}

	return out
}

func (rc *ResourceCreator) createEC2DHCPOptions(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	configs := dhcpConfigurationsFromProps(props, params, physicalIDs)
	tags := tagListProp(props, params, physicalIDs)

	opts, err := rc.backends.EC2.Backend.CreateDhcpOptions(configs, tags)
	if err != nil {
		return "", fmt.Errorf("create DHCP options: %w", err)
	}

	return opts.DhcpOptionsID, nil
}

func (rc *ResourceCreator) deleteEC2DHCPOptions(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteDhcpOptions(id)
}

func (rc *ResourceCreator) createEC2VPCDHCPOptionsAssociation(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	dhcpID := strProp(props, "DhcpOptionsId", params, physicalIDs)
	vpcID := strProp(props, "VpcId", params, physicalIDs)

	if err := rc.backends.EC2.Backend.AssociateDhcpOptions(dhcpID, vpcID); err != nil {
		return "", fmt.Errorf("associate DHCP options %s with VPC %s: %w", dhcpID, vpcID, err)
	}

	return dhcpID + ":" + vpcID, nil
}

func (rc *ResourceCreator) createEC2EIPAssociation(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	allocationID := strProp(props, "AllocationId", params, physicalIDs)
	instanceID := strProp(props, "InstanceId", params, physicalIDs)

	assocID, err := rc.backends.EC2.Backend.AssociateAddress(allocationID, instanceID)
	if err != nil {
		return "", fmt.Errorf("associate EIP %s with instance %s: %w", allocationID, instanceID, err)
	}

	return assocID, nil
}

func (rc *ResourceCreator) deleteEC2EIPAssociation(assocID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DisassociateAddress(assocID)
}

func (rc *ResourceCreator) createEC2EgressOnlyIGW(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)

	igw, err := rc.backends.EC2.Backend.CreateEgressOnlyInternetGateway(vpcID)
	if err != nil {
		return "", fmt.Errorf("create egress-only internet gateway for VPC %s: %w", vpcID, err)
	}

	return igw.ID, nil
}

func (rc *ResourceCreator) deleteEC2EgressOnlyIGW(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteEgressOnlyInternetGateway(id)
}

func (rc *ResourceCreator) createEC2CustomerGateway(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	cgwType := strProp(props, "Type", params, physicalIDs)
	ipAddress := strProp(props, "IpAddress", params, physicalIDs)
	bgpAsn := strProp(props, "BgpAsn", params, physicalIDs)

	cgw, err := rc.backends.EC2.Backend.CreateCustomerGateway(cgwType, ipAddress, bgpAsn)
	if err != nil {
		return "", fmt.Errorf("create customer gateway: %w", err)
	}

	if tags := tagListProp(props, params, physicalIDs); len(tags) > 0 {
		if tagErr := rc.backends.EC2.Backend.CreateTags([]string{cgw.CustomerGatewayID}, tags); tagErr != nil {
			return "", fmt.Errorf("tag customer gateway %s: %w", cgw.CustomerGatewayID, tagErr)
		}
	}

	return cgw.CustomerGatewayID, nil
}

func (rc *ResourceCreator) deleteEC2CustomerGateway(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteCustomerGateway(id)
}

func (rc *ResourceCreator) createEC2CarrierGateway(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)

	gw, err := rc.backends.EC2.Backend.CreateCarrierGateway(vpcID)
	if err != nil {
		return "", fmt.Errorf("create carrier gateway for VPC %s: %w", vpcID, err)
	}

	if tags := tagListProp(props, params, physicalIDs); len(tags) > 0 {
		if tagErr := rc.backends.EC2.Backend.CreateTags([]string{gw.CarrierGatewayID}, tags); tagErr != nil {
			return "", fmt.Errorf("tag carrier gateway %s: %w", gw.CarrierGatewayID, tagErr)
		}
	}

	return gw.CarrierGatewayID, nil
}

func (rc *ResourceCreator) deleteEC2CarrierGateway(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	_, err := rc.backends.EC2.Backend.DeleteCarrierGateway(id)

	return err
}

func (rc *ResourceCreator) createEC2InstanceConnectEndpoint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	subnetID := strProp(props, "SubnetId", params, physicalIDs)
	sgIDs := strSliceProp(props["SecurityGroupIds"], params, physicalIDs)
	preserveClientIP := boolProp(props, "PreserveClientIp")

	ep, err := rc.backends.EC2.Backend.CreateInstanceConnectEndpoint(subnetID, sgIDs, preserveClientIP)
	if err != nil {
		return "", fmt.Errorf("create instance connect endpoint for subnet %s: %w", subnetID, err)
	}

	return ep.InstanceConnectEndpointID, nil
}

func (rc *ResourceCreator) deleteEC2InstanceConnectEndpoint(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	_, err := rc.backends.EC2.Backend.DeleteInstanceConnectEndpoint(id)
	if errors.Is(err, ec2backend.ErrInstanceConnectEndpointNotFound) {
		return nil
	}

	return err
}

// ---- Client VPN: Endpoint, TargetNetworkAssociation, AuthorizationRule, Route ----

func (rc *ResourceCreator) createEC2ClientVpnResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2ClientVpnEndpoint:
		id, err := rc.createEC2ClientVpnEndpoint(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2ClientVpnTargetNetAssoc:
		id, err := rc.createEC2ClientVpnTargetNetworkAssociation(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2ClientVpnAuthRule:
		id, err := rc.createEC2ClientVpnAuthRule(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2ClientVpnRoute:
		id, err := rc.createEC2ClientVpnRoute(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteEC2ClientVpnResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeEC2ClientVpnEndpoint:

		return true, rc.deleteEC2ClientVpnEndpoint(physicalID)
	case resTypeEC2ClientVpnTargetNetAssoc:

		return true, rc.deleteEC2ClientVpnTargetNetworkAssociation(physicalID)
	case resTypeEC2ClientVpnAuthRule:

		return true, rc.deleteEC2ClientVpnAuthRule(physicalID)
	case resTypeEC2ClientVpnRoute:

		return true, rc.deleteEC2ClientVpnRoute(physicalID)
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createEC2ClientVpnEndpoint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	cidr := strProp(props, "ClientCidrBlock", params, physicalIDs)
	desc := strProp(props, "Description", params, physicalIDs)
	dnsServers := strSliceProp(props["DnsServers"], params, physicalIDs)

	opts := ec2backend.ClientVpnEndpointOptions{
		TransportProtocol: strProp(props, "TransportProtocol", params, physicalIDs),
	}

	ep, err := rc.backends.EC2.Backend.CreateClientVpnEndpointWithOptions(cidr, desc, dnsServers, opts)
	if err != nil {
		return "", fmt.Errorf("create client VPN endpoint: %w", err)
	}

	return ep.ClientVpnEndpointID, nil
}

func (rc *ResourceCreator) deleteEC2ClientVpnEndpoint(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	err := rc.backends.EC2.Backend.DeleteClientVpnEndpoint(id)
	if errors.Is(err, ec2backend.ErrClientVpnEndpointNotFound) {
		return nil
	}

	return err
}

func (rc *ResourceCreator) createEC2ClientVpnTargetNetworkAssociation(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	endpointID := strProp(props, "ClientVpnEndpointId", params, physicalIDs)
	subnetID := strProp(props, "SubnetId", params, physicalIDs)

	assocID, err := rc.backends.EC2.Backend.AssociateClientVpnTargetNetwork(endpointID, subnetID)
	if err != nil {
		return "", fmt.Errorf("associate client VPN target network: %w", err)
	}

	return endpointID + ":" + assocID, nil
}

func (rc *ResourceCreator) deleteEC2ClientVpnTargetNetworkAssociation(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	endpointID, assocID := splitCompositeID(physicalID)
	if endpointID == "" || assocID == "" {
		return nil
	}

	err := rc.backends.EC2.Backend.DisassociateClientVpnTargetNetwork(endpointID, assocID)
	if errors.Is(err, ec2backend.ErrClientVpnEndpointNotFound) {
		return nil
	}

	return err
}

func (rc *ResourceCreator) createEC2ClientVpnAuthRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	endpointID := strProp(props, "ClientVpnEndpointId", params, physicalIDs)
	cidr := strProp(props, "TargetNetworkCidr", params, physicalIDs)
	desc := strProp(props, "Description", params, physicalIDs)

	if err := rc.backends.EC2.Backend.AuthorizeClientVpnIngress(endpointID, cidr, desc); err != nil {
		return "", fmt.Errorf("authorize client VPN ingress: %w", err)
	}

	return endpointID + ":" + cidr, nil
}

func (rc *ResourceCreator) deleteEC2ClientVpnAuthRule(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	endpointID, cidr := splitCompositeID(physicalID)
	if endpointID == "" || cidr == "" {
		return nil
	}

	err := rc.backends.EC2.Backend.RevokeClientVpnIngress(endpointID, cidr)
	if errors.Is(err, ec2backend.ErrClientVpnEndpointNotFound) {
		return nil
	}

	return err
}

func (rc *ResourceCreator) createEC2ClientVpnRoute(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	endpointID := strProp(props, "ClientVpnEndpointId", params, physicalIDs)
	cidr := strProp(props, "DestinationCidrBlock", params, physicalIDs)
	desc := strProp(props, "Description", params, physicalIDs)

	if err := rc.backends.EC2.Backend.CreateClientVpnRoute(endpointID, cidr, "", desc); err != nil {
		return "", fmt.Errorf("create client VPN route: %w", err)
	}

	return endpointID + ":" + cidr, nil
}

func (rc *ResourceCreator) deleteEC2ClientVpnRoute(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	endpointID, cidr := splitCompositeID(physicalID)
	if endpointID == "" || cidr == "" {
		return nil
	}

	err := rc.backends.EC2.Backend.DeleteClientVpnRoute(endpointID, cidr, "")
	if errors.Is(err, ec2backend.ErrClientVpnEndpointNotFound) {
		return nil
	}

	return err
}

// ---- IPAM: IPAM, IPAMScope, IPAMPool, IPAMPoolCidr ----

func (rc *ResourceCreator) createEC2IpamResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2IPAM:
		id, err := rc.createEC2Ipam(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2IPAMScope:
		id, err := rc.createEC2IpamScope(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2IPAMPool:
		id, err := rc.createEC2IpamPool(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2IPAMPoolCidr:
		id, err := rc.createEC2IpamPoolCidr(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteEC2IpamResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeEC2IPAM:

		return true, rc.deleteEC2Ipam(physicalID)
	case resTypeEC2IPAMScope:

		return true, rc.deleteEC2IpamScope(physicalID)
	case resTypeEC2IPAMPool:

		return true, rc.deleteEC2IpamPool(physicalID)
	case resTypeEC2IPAMPoolCidr:

		return true, rc.deleteEC2IpamPoolCidr(physicalID)
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createEC2Ipam(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	opts := ec2backend.IpamOptions{
		Description: strProp(props, "Description", params, physicalIDs),
		Tier:        strProp(props, "Tier", params, physicalIDs),
	}

	ipam, err := rc.backends.EC2.Backend.CreateIpam(opts)
	if err != nil {
		return "", fmt.Errorf("create IPAM: %w", err)
	}

	if tags := tagListProp(props, params, physicalIDs); len(tags) > 0 {
		if tagErr := rc.backends.EC2.Backend.CreateTags([]string{ipam.IpamID}, tags); tagErr != nil {
			return "", fmt.Errorf("tag IPAM %s: %w", ipam.IpamID, tagErr)
		}
	}

	return ipam.IpamID, nil
}

func (rc *ResourceCreator) deleteEC2Ipam(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	// Cascade: template-declared pools/scopes may not always be torn down
	// by the stack's delete ordering before the IPAM itself.
	err := rc.backends.EC2.Backend.DeleteIpam(id, true)
	if errors.Is(err, ec2backend.ErrIpamNotFound) {
		return nil
	}

	return err
}

func (rc *ResourceCreator) createEC2IpamScope(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	ipamID := strProp(props, "IpamId", params, physicalIDs)
	desc := strProp(props, "Description", params, physicalIDs)

	scope, err := rc.backends.EC2.Backend.CreateIpamScope(ipamID, desc)
	if err != nil {
		return "", fmt.Errorf("create IPAM scope for IPAM %s: %w", ipamID, err)
	}

	return scope.IpamScopeID, nil
}

func (rc *ResourceCreator) deleteEC2IpamScope(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	err := rc.backends.EC2.Backend.DeleteIpamScope(id)
	if errors.Is(err, ec2backend.ErrIpamScopeNotFound) {
		return nil
	}

	return err
}

// ipamPoolScopeAndIpamID resolves the owning IPAM ID for an IPAMPool
// resource: the CFN property is IpamScopeId, but CreateIpamPool needs the
// IPAM ID as its first argument, so the scope is looked up to find it.
func (rc *ResourceCreator) ipamPoolScopeAndIpamID(scopeID string) (string, error) {
	scopes := rc.backends.EC2.Backend.DescribeIpamScopes([]string{scopeID})
	if len(scopes) == 0 {
		return "", fmt.Errorf("%w: IPAM scope %s", ec2backend.ErrIpamScopeNotFound, scopeID)
	}

	return scopes[0].IpamID, nil
}

func (rc *ResourceCreator) createEC2IpamPool(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	scopeID := strProp(props, "IpamScopeId", params, physicalIDs)

	ipamID, err := rc.ipamPoolScopeAndIpamID(scopeID)
	if err != nil {
		return "", err
	}

	addressFamily := strProp(props, "AddressFamily", params, physicalIDs)
	locale := strProp(props, "Locale", params, physicalIDs)

	opts := ec2backend.IpamPoolOptions{
		IpamScopeID:          scopeID,
		Description:          strProp(props, "Description", params, physicalIDs),
		AutoImport:           boolProp(props, "AutoImport"),
		PubliclyAdvertisable: boolProp(props, "PubliclyAdvertisable"),
	}

	pool, err := rc.backends.EC2.Backend.CreateIpamPool(ipamID, addressFamily, locale, "", opts)
	if err != nil {
		return "", fmt.Errorf("create IPAM pool: %w", err)
	}

	if tags := tagListProp(props, params, physicalIDs); len(tags) > 0 {
		if tagErr := rc.backends.EC2.Backend.CreateTags([]string{pool.IpamPoolID}, tags); tagErr != nil {
			return "", fmt.Errorf("tag IPAM pool %s: %w", pool.IpamPoolID, tagErr)
		}
	}

	return pool.IpamPoolID, nil
}

func (rc *ResourceCreator) deleteEC2IpamPool(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	err := rc.backends.EC2.Backend.DeleteIpamPool(id)
	if errors.Is(err, ec2backend.ErrIpamPoolNotFound) {
		return nil
	}

	return err
}

func (rc *ResourceCreator) createEC2IpamPoolCidr(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	poolID := strProp(props, "IpamPoolId", params, physicalIDs)
	cidr := strProp(props, "Cidr", params, physicalIDs)

	if _, err := rc.backends.EC2.Backend.ProvisionIpamPoolCidr(poolID, cidr); err != nil {
		return "", fmt.Errorf("provision IPAM pool CIDR %s on pool %s: %w", cidr, poolID, err)
	}

	return poolID + ":" + cidr, nil
}

func (rc *ResourceCreator) deleteEC2IpamPoolCidr(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	poolID, cidr := splitCompositeID(physicalID)
	if poolID == "" || cidr == "" {
		return nil
	}

	_, err := rc.backends.EC2.Backend.DeprovisionIpamPoolCidr(poolID, cidr)
	if errors.Is(err, ec2backend.ErrIpamPoolNotFound) {
		return nil
	}

	return err
}

// ---- CapacityReservation, Host ----

func (rc *ResourceCreator) createEC2HostResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2CapacityReservation:
		id, err := rc.createEC2CapacityReservation(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2Host:
		id, err := rc.createEC2Host(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteEC2HostResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeEC2CapacityReservation:

		return true, rc.deleteEC2CapacityReservation(physicalID)
	case resTypeEC2Host:

		rc.deleteEC2Host(physicalID)

		return true, nil
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createEC2CapacityReservation(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	instanceType := strProp(props, "InstanceType", params, physicalIDs)
	az := strProp(props, "AvailabilityZone", params, physicalIDs)
	matchCriteria := strProp(props, "InstanceMatchCriteria", params, physicalIDs)
	tenancy := strProp(props, "Tenancy", params, physicalIDs)

	count := intProp(props, "InstanceCount")
	if count == 0 {
		count = 1
	}

	tags := tagListProp(props, params, physicalIDs)

	cr, err := rc.backends.EC2.Backend.CreateCapacityReservation(
		instanceType, az, matchCriteria, tenancy, count, tags,
	)
	if err != nil {
		return "", fmt.Errorf("create capacity reservation: %w", err)
	}

	if platform := strProp(props, "InstancePlatform", params, physicalIDs); platform != "" {
		rc.backends.EC2.Backend.SetCapacityReservationInstancePlatform(cr.CapacityReservationID, platform)
	}

	return cr.CapacityReservationID, nil
}

func (rc *ResourceCreator) deleteEC2CapacityReservation(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	err := rc.backends.EC2.Backend.CancelCapacityReservation(id)
	if errors.Is(err, ec2backend.ErrCapacityReservationNotFound) {
		return nil
	}

	return err
}

func (rc *ResourceCreator) createEC2Host(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	az := strProp(props, "AvailabilityZone", params, physicalIDs)
	instanceType := strProp(props, "InstanceType", params, physicalIDs)
	autoPlacement := strProp(props, "AutoPlacement", params, physicalIDs)
	hostRecovery := strProp(props, "HostRecovery", params, physicalIDs)

	hosts, err := rc.backends.EC2.Backend.AllocateHosts(az, instanceType, 1, autoPlacement, hostRecovery)
	if err != nil {
		return "", fmt.Errorf("allocate dedicated host: %w", err)
	}

	if len(hosts) == 0 {
		return "", errNoEC2Instances
	}

	return hosts[0].HostID, nil
}

func (rc *ResourceCreator) deleteEC2Host(id string) {
	if rc.backends.EC2 == nil {
		return
	}

	rc.backends.EC2.Backend.ReleaseHosts([]string{id})
}
