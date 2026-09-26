package dsql

import "fmt"

// GetVpcEndpointServiceName returns the VPC endpoint service name and
// cluster-specific VPC endpoint DNS name a client would use to reach the
// cluster privately. There is no real VPC/PrivateLink plane behind this
// emulator, so both values are wire-shaped but not backed by a functioning
// endpoint -- see PARITY.md.
func (b *InMemoryBackend) GetVpcEndpointServiceName(identifier, region string) (string, string, error) {
	b.mu.Lock("GetVpcEndpointServiceName")
	defer b.mu.Unlock()

	if _, err := b.resolveClusterLocked(identifier); err != nil {
		return "", "", err
	}

	serviceName := fmt.Sprintf("com.amazonaws.%s.dsql", region)
	clusterVpcEndpoint := fmt.Sprintf("%s.vpce.dsql.%s.on.aws", identifier, region)

	return serviceName, clusterVpcEndpoint, nil
}
