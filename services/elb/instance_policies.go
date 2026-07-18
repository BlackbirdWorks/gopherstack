package elb

import (
	"context"
	"fmt"
)

// SetLoadBalancerPoliciesForBackendServer sets the policies for a backend server instance port.
func (b *InMemoryBackend) SetLoadBalancerPoliciesForBackendServer(
	ctx context.Context,
	name string,
	instancePort int32,
	policyNames []string,
) error {
	b.mu.Lock("SetLoadBalancerPoliciesForBackendServer")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	lb, ok := b.lbs.Get(lbTableKey(region, name))
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// Validate each policy exists for this LB.
	for _, p := range policyNames {
		if !b.policies.Has(policyTableKey(region, name, p)) {
			return fmt.Errorf("%w: %q", ErrPolicyNotFound, p)
		}
	}

	cp := make([]string, len(policyNames))
	copy(cp, policyNames)

	if len(policyNames) == 0 {
		// Remove the BSD entry when policy list is empty.
		kept := lb.BackendServerDescriptions[:0]
		for _, bsd := range lb.BackendServerDescriptions {
			if bsd.InstancePort != instancePort {
				kept = append(kept, bsd)
			}
		}
		lb.BackendServerDescriptions = kept

		return nil
	}

	for i := range lb.BackendServerDescriptions {
		if lb.BackendServerDescriptions[i].InstancePort == instancePort {
			lb.BackendServerDescriptions[i].PolicyNames = cp

			return nil
		}
	}

	lb.BackendServerDescriptions = append(lb.BackendServerDescriptions, BackendServerDescription{
		InstancePort: instancePort,
		PolicyNames:  cp,
	})

	return nil
}
