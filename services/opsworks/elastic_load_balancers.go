package opsworks

import (
	"fmt"
	"slices"
)

// AttachElasticLoadBalancer attaches an ELB to a layer.
func (b *InMemoryBackend) AttachElasticLoadBalancer(elbName, layerID string) error {
	b.mu.Lock("AttachElasticLoadBalancer")
	defer b.mu.Unlock()

	l, ok := b.layers.Get(layerID)
	if !ok {
		return ErrLayerNotFound
	}

	b.elasticLBs.Put(&storedElasticLoadBalancer{
		ElasticLoadBalancerName: elbName,
		Region:                  b.region,
		DNSName:                 fmt.Sprintf("%s.%s.elb.amazonaws.com", elbName, b.region),
		StackID:                 l.StackID,
		LayerID:                 layerID,
	})

	return nil
}

// DetachElasticLoadBalancer detaches an ELB from a layer.
func (b *InMemoryBackend) DetachElasticLoadBalancer(elbName, _ string) error {
	b.mu.Lock("DetachElasticLoadBalancer")
	defer b.mu.Unlock()

	if !b.elasticLBs.Delete(elbName) {
		return ErrElasticLBNotFound
	}

	return nil
}

// DescribeElasticLoadBalancers returns ELBs optionally filtered by stack
// and/or layer. LayerIds is a real, plural DescribeElasticLoadBalancersInput
// filter member (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's
// api_op_DescribeElasticLoadBalancers.go) -- a previous version of this
// method discarded it entirely.
func (b *InMemoryBackend) DescribeElasticLoadBalancers(
	stackID string, layerIDs []string,
) ([]*ElasticLoadBalancer, error) {
	b.mu.RLock("DescribeElasticLoadBalancers")
	defer b.mu.RUnlock()

	result := make([]*ElasticLoadBalancer, 0)
	for _, e := range b.elasticLBs.All() {
		if stackID != "" && e.StackID != stackID {
			continue
		}
		if len(layerIDs) > 0 && !slices.Contains(layerIDs, e.LayerID) {
			continue
		}
		result = append(result, b.resolveElasticLoadBalancer(e))
	}

	return result, nil
}

func (b *InMemoryBackend) resolveElasticLoadBalancer(e *storedElasticLoadBalancer) *ElasticLoadBalancer {
	elb := e.toElasticLoadBalancer()

	stack, hasStack := b.stacks.Get(e.StackID)
	if hasStack {
		elb.VpcID = stack.VpcID
	}

	for _, i := range b.instancesByStack.Get(e.StackID) {
		if !slices.Contains(i.LayerIDs, e.LayerID) {
			continue
		}

		if i.SubnetID != "" && !slices.Contains(elb.SubnetIDs, i.SubnetID) {
			elb.SubnetIDs = append(elb.SubnetIDs, i.SubnetID)
		}

		az := ""
		if i.Extras != nil {
			az = i.Extras.AvailabilityZone
		}
		if az == "" && hasStack {
			az = stack.DefaultAvailabilityZone
		}
		if az != "" && !slices.Contains(elb.AvailabilityZones, az) {
			elb.AvailabilityZones = append(elb.AvailabilityZones, az)
		}
	}

	slices.Sort(elb.SubnetIDs)
	slices.Sort(elb.AvailabilityZones)

	return elb
}
