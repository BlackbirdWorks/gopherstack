package ec2

import (
	"fmt"
	"slices"
	"strings"
)

const (
	vpcEndpointTypeGateway             = "Gateway"
	vpcEndpointTypeGatewayLoadBalancer = "GatewayLoadBalancer"
	vpcEndpointTypeResource            = "Resource"
	vpcEndpointTypeServiceNetwork      = "ServiceNetwork"

	latticeResourceConfigurationPrefix = "resourceconfiguration/"
	latticeServiceNetworkPrefix        = "servicenetwork/"
	latticeARNFields                   = 6
)

// validateVpcEndpointTarget checks the type/target combination of CreateVpcEndpoint.
func validateVpcEndpointTarget(endpointType, serviceName string, o VpcEndpointCreateOptions) error {
	if !slices.Contains([]string{
		vpcEndpointTypeInterface, vpcEndpointTypeGateway, vpcEndpointTypeGatewayLoadBalancer,
		vpcEndpointTypeResource, vpcEndpointTypeServiceNetwork,
	}, endpointType) {
		return fmt.Errorf("%w: invalid VpcEndpointType %q", ErrInvalidParameter, endpointType)
	}

	switch endpointType {
	case vpcEndpointTypeResource:
		return validateLatticeEndpointTarget(
			"ResourceConfigurationArn", o.ResourceConfigurationArn, latticeResourceConfigurationPrefix,
			"ServiceNetworkArn", o.ServiceNetworkArn, serviceName)
	case vpcEndpointTypeServiceNetwork:
		return validateLatticeEndpointTarget(
			"ServiceNetworkArn", o.ServiceNetworkArn, latticeServiceNetworkPrefix,
			"ResourceConfigurationArn", o.ResourceConfigurationArn, serviceName)
	}

	if o.ResourceConfigurationArn != "" || o.ServiceNetworkArn != "" {
		return fmt.Errorf(
			"%w: ResourceConfigurationArn and ServiceNetworkArn apply only to Resource and ServiceNetwork endpoints",
			ErrInvalidParameter)
	}

	if serviceName == "" {
		return fmt.Errorf("%w: ServiceName is required", ErrInvalidParameter)
	}

	return nil
}

func validateLatticeEndpointTarget(
	field, value, resourcePrefix, otherField, other, serviceName string,
) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required for this endpoint type", ErrInvalidParameter, field)
	}

	if other != "" || serviceName != "" {
		return fmt.Errorf("%w: %s cannot be combined with %s or ServiceName", ErrInvalidParameter, field, otherField)
	}

	parts := strings.SplitN(value, ":", latticeARNFields)
	if len(parts) != latticeARNFields || parts[0] != "arn" || parts[2] != "vpc-lattice" ||
		!strings.HasPrefix(parts[5], resourcePrefix) || len(parts[5]) == len(resourcePrefix) {
		return fmt.Errorf("%w: %s %q is not a valid VPC Lattice ARN", ErrInvalidParameter, field, value)
	}

	return nil
}

// VpcEndpointsByServiceNetworkArn lists ServiceNetwork endpoints bound to the ARN.
func (b *InMemoryBackend) VpcEndpointsByServiceNetworkArn(serviceNetworkArn string) []*VpcEndpoint {
	return b.vpcEndpointsMatching(func(ep *VpcEndpoint) bool {
		return serviceNetworkArn != "" && ep.ServiceNetworkArn == serviceNetworkArn
	})
}

// VpcEndpointsByResourceConfigurationArn lists Resource endpoints bound to the ARN.
func (b *InMemoryBackend) VpcEndpointsByResourceConfigurationArn(resourceConfigurationArn string) []*VpcEndpoint {
	return b.vpcEndpointsMatching(func(ep *VpcEndpoint) bool {
		return resourceConfigurationArn != "" && ep.ResourceConfigurationArn == resourceConfigurationArn
	})
}

func (b *InMemoryBackend) vpcEndpointsMatching(match func(*VpcEndpoint) bool) []*VpcEndpoint {
	b.mu.RLock("VpcEndpointsMatching")
	defer b.mu.RUnlock()

	var out []*VpcEndpoint

	for _, ep := range b.vpcEndpoints.All() {
		if !match(ep) {
			continue
		}

		cp := *ep
		cp.SubnetIDs = slices.Clone(ep.SubnetIDs)
		cp.RouteTableIDs = slices.Clone(ep.RouteTableIDs)
		cp.SecurityGroupIDs = slices.Clone(ep.SecurityGroupIDs)
		out = append(out, &cp)
	}

	return out
}
