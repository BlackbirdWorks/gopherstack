package elb

import (
	"context"
	"fmt"
	"sort"
)

// ApplySecurityGroupsToLoadBalancer replaces the security groups for a VPC load balancer.
func (b *InMemoryBackend) ApplySecurityGroupsToLoadBalancer(
	ctx context.Context, name string, securityGroups []string,
) ([]string, error) {
	b.mu.Lock("ApplySecurityGroupsToLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	if !lb.IsVPC {
		return nil, fmt.Errorf(
			"%w: ApplySecurityGroupsToLoadBalancer is only available for VPC load balancers",
			ErrInvalidConfiguration,
		)
	}

	if b.ec2Resolver != nil {
		for _, sg := range securityGroups {
			if !b.ec2Resolver.SecurityGroupExists(sg) {
				return nil, fmt.Errorf("%w: %s", ErrInvalidSecurityGroup, sg)
			}
		}
	}

	cp := make([]string, len(securityGroups))
	copy(cp, securityGroups)
	sort.Strings(cp)
	lb.SecurityGroups = cp

	return cp, nil
}
