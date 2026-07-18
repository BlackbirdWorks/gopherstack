package cloudwatch

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// metricFilterKey returns a stable map key for a metric filter.
func metricFilterKey(filterName, logGroupName string) string {
	return logGroupName + "/" + filterName
}

// PutMetricFilter creates or updates a metric filter.
func (b *InMemoryBackend) PutMetricFilter(filter *MetricFilter) error {
	if strings.TrimSpace(filter.FilterName) == "" {
		return fmt.Errorf("%w: FilterName parameter is required", ErrValidation)
	}
	if strings.TrimSpace(filter.LogGroupName) == "" {
		return fmt.Errorf("%w: LogGroupName parameter is required", ErrValidation)
	}

	b.mu.Lock("PutMetricFilter")
	defer b.mu.Unlock()

	cp := *filter
	if cp.CreationTime.IsZero() {
		cp.CreationTime = time.Now().UTC()
	}

	b.metricFilters.Put(&cp)

	return nil
}

// DescribeMetricFilters returns a paginated list of metric filters with optional filters.
func (b *InMemoryBackend) DescribeMetricFilters(
	filterNamePrefix, logGroupName, nextToken string,
	maxResults int,
) (page.Page[MetricFilter], error) {
	b.mu.RLock("DescribeMetricFilters")
	defer b.mu.RUnlock()

	var result []MetricFilter
	for _, f := range b.metricFilters.All() {
		if logGroupName != "" && f.LogGroupName != logGroupName {
			continue
		}
		if filterNamePrefix != "" && !strings.HasPrefix(f.FilterName, filterNamePrefix) {
			continue
		}
		result = append(result, *f)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].LogGroupName != result[j].LogGroupName {
			return result[i].LogGroupName < result[j].LogGroupName
		}

		return result[i].FilterName < result[j].FilterName
	})

	return page.New(result, nextToken, maxResults, cwDefaultDescribeMetricFiltersLimit), nil
}

// DeleteMetricFilter removes a metric filter by name and log group.
// Returns ErrMetricFilterNotFound if the filter does not exist.
func (b *InMemoryBackend) DeleteMetricFilter(filterName, logGroupName string) error {
	b.mu.Lock("DeleteMetricFilter")
	defer b.mu.Unlock()

	key := metricFilterKey(filterName, logGroupName)
	if !b.metricFilters.Has(key) {
		return fmt.Errorf("%w: %s/%s", ErrMetricFilterNotFound, logGroupName, filterName)
	}

	b.metricFilters.Delete(key)

	return nil
}
