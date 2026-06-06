package securityhub

import (
	"fmt"
)

// FindingAggregator represents a Security Hub finding aggregator for cross-region aggregation.
type FindingAggregator struct {
	FindingAggregatorArn     string   `json:"FindingAggregatorArn"`
	FindingAggregationRegion string   `json:"FindingAggregationRegion"`
	RegionLinkingMode        string   `json:"RegionLinkingMode"`
	Regions                  []string `json:"Regions"`
}

func (b *InMemoryBackend) findingAggregatorARN(seq int) string {
	return fmt.Sprintf("arn:aws:securityhub:%s:%s:finding-aggregator/default-%d", b.region, b.accountID, seq)
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
	b.findingAggregators[arn] = agg

	return agg, nil
}

func (b *InMemoryBackend) GetFindingAggregator(arn string) (*FindingAggregator, error) {
	b.mu.RLock("GetFindingAggregator")
	defer b.mu.RUnlock()

	agg, ok := b.findingAggregators[arn]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *agg

	return &cp, nil
}

func (b *InMemoryBackend) ListFindingAggregators(nextToken string, maxResults int) ([]*FindingAggregator, string) {
	b.mu.RLock("ListFindingAggregators")
	defer b.mu.RUnlock()

	var all []*FindingAggregator

	for _, agg := range b.findingAggregators {
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

	agg, ok := b.findingAggregators[arn]
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

	if _, ok := b.findingAggregators[arn]; !ok {
		return ErrNotFound
	}

	delete(b.findingAggregators, arn)

	return nil
}
