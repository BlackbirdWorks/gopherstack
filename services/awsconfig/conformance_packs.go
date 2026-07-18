package awsconfig

import "fmt"

const conformancePackStateComplete = "COMPLETE"

// PutConformancePack creates or updates a conformance pack.
func (b *InMemoryBackend) PutConformancePack(name, deliveryS3Bucket, deliveryS3KeyPrefix string) error {
	if name == "" {
		return fmt.Errorf("%w: ConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("PutConformancePack")
	defer b.mu.Unlock()

	b.conformancePackCounter++
	packID := fmt.Sprintf("conformance-pack-%08d", b.conformancePackCounter)
	arn := fmt.Sprintf(
		"arn:aws:config:%s:%s:conformance-pack/%s/%s",
		b.region, b.accountID, name, packID,
	)

	b.conformancePacks.Put(&ConformancePack{
		ConformancePackName: name,
		ConformancePackArn:  arn,
		ConformancePackID:   packID,
		DeliveryS3Bucket:    deliveryS3Bucket,
		DeliveryS3KeyPrefix: deliveryS3KeyPrefix,
	})

	return nil
}

// DeleteConformancePack deletes a conformance pack by name.
func (b *InMemoryBackend) DeleteConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConformancePack")
	defer b.mu.Unlock()

	if !b.conformancePacks.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchConformancePack, name)
	}

	b.conformancePacks.Delete(name)

	return nil
}

// DescribeConformancePacks returns all conformance packs.
func (b *InMemoryBackend) DescribeConformancePacks() []ConformancePack {
	b.mu.RLock("DescribeConformancePacks")
	defer b.mu.RUnlock()

	all := b.conformancePacks.All()
	out := make([]ConformancePack, 0, len(all))

	for _, p := range all {
		out = append(out, *p)
	}

	return out
}

// DescribeAggregateComplianceByConformancePacks returns an empty list.
func (b *InMemoryBackend) DescribeAggregateComplianceByConformancePacks() []any {
	return []any{}
}

// DescribeConformancePackStatus returns conformance pack statuses.
// If names is empty, all packs are returned.
func (b *InMemoryBackend) DescribeConformancePackStatus(names []string) []ConformancePackStatus {
	b.mu.RLock("DescribeConformancePackStatus")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		all := b.conformancePacks.All()
		out := make([]ConformancePackStatus, 0, len(all))

		for _, p := range all {
			out = append(out, ConformancePackStatus{
				ConformancePackName:  p.ConformancePackName,
				ConformancePackState: conformancePackStateComplete,
				ConformancePackArn: fmt.Sprintf(
					"arn:aws:config:%s:%s:conformance-pack/%s",
					b.region, b.accountID, p.ConformancePackName,
				),
			})
		}

		return out
	}

	out := make([]ConformancePackStatus, 0, len(names))

	for _, name := range names {
		if p, ok := b.conformancePacks.Get(name); ok {
			out = append(out, ConformancePackStatus{
				ConformancePackName:  p.ConformancePackName,
				ConformancePackState: conformancePackStateComplete,
				ConformancePackArn: fmt.Sprintf(
					"arn:aws:config:%s:%s:conformance-pack/%s",
					b.region, b.accountID, p.ConformancePackName,
				),
			})
		}
	}

	return out
}

// DescribeConformancePackCompliance returns compliance items for a conformance pack.
// Returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) DescribeConformancePackCompliance(_ string) []ConformancePackComplianceItem {
	return []ConformancePackComplianceItem{}
}

// GetAggregateConformancePackComplianceSummary returns an empty summary (intentionally minimal stub).
func (b *InMemoryBackend) GetAggregateConformancePackComplianceSummary() []any { return []any{} }

// ListConformancePackComplianceScores returns an empty list.
func (b *InMemoryBackend) ListConformancePackComplianceScores() []any {
	return []any{}
}

// GetConformancePackComplianceDetails returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) GetConformancePackComplianceDetails() []any { return []any{} }

// GetConformancePackComplianceSummary returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) GetConformancePackComplianceSummary() []any { return []any{} }
