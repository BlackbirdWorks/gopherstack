package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// ---- VPC Endpoint Service Configurations ----

// CreateVpcEndpointServiceConfiguration creates a new VPC endpoint service configuration.
func (b *InMemoryBackend) CreateVpcEndpointServiceConfiguration(
	acceptanceRequired bool, nlbARNs []string,
) (*VpcEndpointServiceConfig, error) {
	b.mu.Lock("CreateVpcEndpointServiceConfiguration")
	defer b.mu.Unlock()

	svcID := "vpce-svc-" + uuid.New().String()[:8]
	svcName := "com.amazonaws.vpce." + b.Region + "." + svcID

	cfg := &VpcEndpointServiceConfig{
		ServiceID:               svcID,
		ServiceName:             svcName,
		ServiceType:             "Interface",
		AcceptanceRequired:      acceptanceRequired,
		NetworkLoadBalancerARNs: nlbARNs,
	}
	b.vpcEndpointServiceConfigs.Put(cfg)

	cp := *cfg
	cp.NetworkLoadBalancerARNs = append([]string(nil), cfg.NetworkLoadBalancerARNs...)

	return &cp, nil
}

// DescribeVpcEndpointServiceConfigurations returns endpoint service configs, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpcEndpointServiceConfigurations(
	ids []string,
) []*VpcEndpointServiceConfig {
	b.mu.RLock("DescribeVpcEndpointServiceConfigurations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*VpcEndpointServiceConfig, 0, b.vpcEndpointServiceConfigs.Len())

	for _, cfg := range b.vpcEndpointServiceConfigs.All() {
		if len(idSet) > 0 && !idSet[cfg.ServiceID] {
			continue
		}

		cp := *cfg
		cp.NetworkLoadBalancerARNs = append([]string(nil), cfg.NetworkLoadBalancerARNs...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ServiceID < out[j].ServiceID
	})

	return out
}

// DeleteVpcEndpointServiceConfigurations removes VPC endpoint service configurations by IDs.
func (b *InMemoryBackend) DeleteVpcEndpointServiceConfigurations(ids []string) error {
	b.mu.Lock("DeleteVpcEndpointServiceConfigurations")
	defer b.mu.Unlock()

	for _, id := range ids {
		if _, ok := b.vpcEndpointServiceConfigs.Get(id); !ok {
			return fmt.Errorf("%w: %s", ErrVpcEndpointServiceNotFound, id)
		}
	}

	for _, id := range ids {
		b.vpcEndpointServiceConfigs.Delete(id)
	}

	return nil
}

// ModifyVpcEndpointServiceConfiguration updates acceptance required for a service config.
func (b *InMemoryBackend) ModifyVpcEndpointServiceConfiguration(
	id string,
	acceptanceRequired bool,
) error {
	if id == "" {
		return fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcEndpointServiceConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.vpcEndpointServiceConfigs.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpcEndpointServiceNotFound, id)
	}

	cfg.AcceptanceRequired = acceptanceRequired

	return nil
}

// StartVpcEndpointServicePrivateDNSVerification initiates (and, for this
// in-memory backend, immediately completes) private DNS name verification for
// a VPC endpoint service configuration.
func (b *InMemoryBackend) StartVpcEndpointServicePrivateDNSVerification(id string) error {
	if id == "" {
		return fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("StartVpcEndpointServicePrivateDnsVerification")
	defer b.mu.Unlock()

	cfg, ok := b.vpcEndpointServiceConfigs.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpcEndpointServiceNotFound, id)
	}

	cfg.PrivateDNSNameState = "verified"

	return nil
}
