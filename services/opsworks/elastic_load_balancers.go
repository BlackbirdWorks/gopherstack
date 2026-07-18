package opsworks

import "fmt"

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

// DescribeElasticLoadBalancers returns ELBs optionally filtered by stack/layer.
func (b *InMemoryBackend) DescribeElasticLoadBalancers(stackID, _ string) ([]*ElasticLoadBalancer, error) {
	b.mu.RLock("DescribeElasticLoadBalancers")
	defer b.mu.RUnlock()

	result := make([]*ElasticLoadBalancer, 0)
	for _, e := range b.elasticLBs.All() {
		if stackID != "" && e.StackID != stackID {
			continue
		}
		result = append(result, e.toElasticLoadBalancer())
	}

	return result, nil
}
