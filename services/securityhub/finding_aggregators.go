package securityhub

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) findingAggregatorARN(seq int) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("finding-aggregator/default-%d", seq))
}

func (b *InMemoryBackend) CreateFindingAggregator(
	regionLinkingMode string,
	regions []string,
) (*FindingAggregator, error) {
	b.mu.Lock("CreateFindingAggregator")
	defer b.mu.Unlock()

	b.findingAggregatorSeq++
	arn := b.findingAggregatorARN(b.findingAggregatorSeq)

	agg := &FindingAggregator{
		FindingAggregatorArn:     arn,
		FindingAggregationRegion: b.region,
		RegionLinkingMode:        regionLinkingMode,
		Regions:                  regions,
	}
	b.findingAggregators.Put(agg)
	cp := *agg

	return &cp, nil
}

func (b *InMemoryBackend) GetFindingAggregator(arn string) (*FindingAggregator, error) {
	b.mu.RLock("GetFindingAggregator")
	defer b.mu.RUnlock()

	agg, ok := b.findingAggregators.Get(arn)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *agg

	return &cp, nil
}

func (b *InMemoryBackend) ListFindingAggregators(nextToken string, maxResults int) ([]*FindingAggregator, string) {
	b.mu.RLock("ListFindingAggregators")
	defer b.mu.RUnlock()

	snap := b.findingAggregators.Snapshot()
	all := make([]*FindingAggregator, 0, len(snap))

	for _, agg := range snap {
		cp := *agg
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) UpdateFindingAggregator(
	arn, regionLinkingMode string,
	regions []string,
) (*FindingAggregator, error) {
	b.mu.Lock("UpdateFindingAggregator")
	defer b.mu.Unlock()

	agg, ok := b.findingAggregators.Get(arn)
	if !ok {
		return nil, ErrNotFound
	}

	agg.RegionLinkingMode = regionLinkingMode
	agg.Regions = regions

	cp := *agg

	return &cp, nil
}

func (b *InMemoryBackend) DeleteFindingAggregator(arn string) error {
	b.mu.Lock("DeleteFindingAggregator")
	defer b.mu.Unlock()

	if !b.findingAggregators.Delete(arn) {
		return ErrNotFound
	}

	return nil
}
