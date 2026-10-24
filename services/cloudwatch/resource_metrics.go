package cloudwatch

import (
	"fmt"
	"slices"
	"time"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
)

func validateResourceMetricsInput(resourceArn string, selections []ResourceMetricSelection) error {
	if _, err := awsarn.Parse(resourceArn); err != nil {
		return fmt.Errorf("%w: ResourceArn must be a valid ARN", ErrValidation)
	}

	for _, sel := range selections {
		if len(sel.IncludeMetrics) == 0 {
			return fmt.Errorf("%w: IncludeMetrics is required for every metric selection", ErrValidation)
		}
	}

	return nil
}

func cloneResourceMetrics(c *ResourceMetricsConfiguration) ResourceMetricsConfiguration {
	out := *c
	out.MetricSelections = cloneMetricSelections(c.MetricSelections)

	return out
}

func cloneMetricSelections(in []ResourceMetricSelection) []ResourceMetricSelection {
	if len(in) == 0 {
		return nil
	}

	out := make([]ResourceMetricSelection, len(in))
	for i, sel := range in {
		out[i] = ResourceMetricSelection{IncludeMetrics: slices.Clone(sel.IncludeMetrics)}
	}

	return out
}

// CreateResourceMetricsConfiguration enables detailed monitoring for a resource ARN.
func (b *InMemoryBackend) CreateResourceMetricsConfiguration(
	resourceArn string, selections []ResourceMetricSelection,
) (ResourceMetricsConfiguration, error) {
	if err := validateResourceMetricsInput(resourceArn, selections); err != nil {
		return ResourceMetricsConfiguration{}, err
	}

	b.mu.Lock("CreateResourceMetricsConfiguration")
	defer b.mu.Unlock()

	if b.resourceMetrics.Has(resourceArn) {
		return ResourceMetricsConfiguration{}, fmt.Errorf(
			"%w: a resource metrics configuration already exists for %s", ErrResourceMetricsConfigExists, resourceArn,
		)
	}

	now := time.Now().UTC()
	cfg := &ResourceMetricsConfiguration{
		ResourceArn: resourceArn, MetricSelections: cloneMetricSelections(selections), CreatedAt: now, UpdatedAt: now,
	}
	b.resourceMetrics.Put(cfg)

	return cloneResourceMetrics(cfg), nil
}

// GetResourceMetricsConfiguration returns the configuration for a resource ARN.
func (b *InMemoryBackend) GetResourceMetricsConfiguration(resourceArn string) (ResourceMetricsConfiguration, error) {
	if resourceArn == "" {
		return ResourceMetricsConfiguration{}, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.RLock("GetResourceMetricsConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.resourceMetrics.Get(resourceArn)
	if !ok {
		return ResourceMetricsConfiguration{}, resourceMetricsNotFound(resourceArn)
	}

	return cloneResourceMetrics(cfg), nil
}

// UpdateResourceMetricsConfiguration replaces (never merges) the metric selections.
func (b *InMemoryBackend) UpdateResourceMetricsConfiguration(
	resourceArn string, selections []ResourceMetricSelection,
) (ResourceMetricsConfiguration, error) {
	if err := validateResourceMetricsInput(resourceArn, selections); err != nil {
		return ResourceMetricsConfiguration{}, err
	}

	b.mu.Lock("UpdateResourceMetricsConfiguration")
	defer b.mu.Unlock()

	cur, ok := b.resourceMetrics.Get(resourceArn)
	if !ok {
		return ResourceMetricsConfiguration{}, resourceMetricsNotFound(resourceArn)
	}

	next := &ResourceMetricsConfiguration{
		ResourceArn: resourceArn, MetricSelections: cloneMetricSelections(selections),
		CreatedAt: cur.CreatedAt, UpdatedAt: time.Now().UTC(),
	}
	b.resourceMetrics.Put(next)

	return cloneResourceMetrics(next), nil
}

// DeleteResourceMetricsConfiguration removes the configuration for a resource ARN.
func (b *InMemoryBackend) DeleteResourceMetricsConfiguration(resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("DeleteResourceMetricsConfiguration")
	defer b.mu.Unlock()

	if !b.resourceMetrics.Delete(resourceArn) {
		return resourceMetricsNotFound(resourceArn)
	}

	return nil
}

func resourceMetricsNotFound(resourceArn string) error {
	return fmt.Errorf(
		"%w: no resource metrics configuration exists for %s",
		ErrResourceMetricsConfigNotFound,
		resourceArn,
	)
}
