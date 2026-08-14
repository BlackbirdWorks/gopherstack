package ec2

import (
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) CreateCarrierGateway(vpcID string) (*CarrierGateway, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCarrierGateway")
	defer b.mu.Unlock()

	id := "cagw-" + uuid.New().String()[:8]
	gw := &CarrierGateway{
		CarrierGatewayID: id,
		VpcID:            vpcID,
		State:            stateAvailableImg,
		OwnerID:          b.AccountID,
	}
	b.carrierGateways.Put(gw)

	cp := *gw

	return &cp, nil
}

func (b *InMemoryBackend) DeleteCarrierGateway(id string) (*CarrierGateway, error) {
	b.mu.Lock("DeleteCarrierGateway")
	defer b.mu.Unlock()

	gw, ok := b.carrierGateways.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCarrierGatewayNotFound, id)
	}
	cp := *gw
	b.carrierGateways.Delete(id)
	delete(b.tags, id)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeCarrierGateways(ids []string) []*CarrierGateway {
	b.mu.RLock("DescribeCarrierGateways")
	defer b.mu.RUnlock()

	var result []*CarrierGateway

	for _, gw := range b.carrierGateways.All() {
		if len(ids) > 0 && !slices.Contains(ids, gw.CarrierGatewayID) {
			continue
		}

		cp := *gw
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CarrierGatewayID < result[j].CarrierGatewayID
	})

	return result
}

// ---- Reserved Instances backend methods ----
