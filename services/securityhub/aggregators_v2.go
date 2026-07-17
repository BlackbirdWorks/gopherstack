package securityhub

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) aggregatorV2ARN(seq int) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("aggregator-v2/%d", seq))
}

func (b *InMemoryBackend) CreateAggregatorV2(regionLinkingMode string, regions []string) (*AggregatorV2, error) {
	b.mu.Lock("CreateAggregatorV2")
	defer b.mu.Unlock()

	b.aggregatorV2Seq++
	arn := b.aggregatorV2ARN(b.aggregatorV2Seq)
	now := time.Now().UTC().Format(time.RFC3339)

	agg := &AggregatorV2{
		AggregatorV2Arn:   arn,
		AggregationRegion: b.region,
		RegionLinkingMode: regionLinkingMode,
		Regions:           regions,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	b.aggregatorsV2.Put(agg)

	return agg, nil
}

func (b *InMemoryBackend) GetAggregatorV2(arn string) (*AggregatorV2, error) {
	b.mu.RLock("GetAggregatorV2")
	defer b.mu.RUnlock()

	agg, ok := b.aggregatorsV2.Get(arn)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *agg

	return &cp, nil
}

func (b *InMemoryBackend) ListAggregatorsV2(nextToken string, maxResults int) ([]*AggregatorV2, string) {
	b.mu.RLock("ListAggregatorsV2")
	defer b.mu.RUnlock()

	snap := b.aggregatorsV2.All()
	all := make([]*AggregatorV2, 0, len(snap))

	for _, agg := range snap {
		cp := *agg
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) UpdateAggregatorV2(arn, regionLinkingMode string, regions []string) (*AggregatorV2, error) {
	b.mu.Lock("UpdateAggregatorV2")
	defer b.mu.Unlock()

	agg, ok := b.aggregatorsV2.Get(arn)
	if !ok {
		return nil, ErrNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)
	agg.RegionLinkingMode = regionLinkingMode
	agg.Regions = regions
	agg.UpdatedAt = now

	cp := *agg

	return &cp, nil
}

func (b *InMemoryBackend) DeleteAggregatorV2(arn string) error {
	b.mu.Lock("DeleteAggregatorV2")
	defer b.mu.Unlock()

	if !b.aggregatorsV2.Delete(arn) {
		return ErrNotFound
	}

	return nil
}
