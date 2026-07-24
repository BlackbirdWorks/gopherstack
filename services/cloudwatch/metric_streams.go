package cloudwatch

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// filterExcludesMetric returns true when an ExcludeFilters entry denies the metric.
func filterExcludesMetric(filters []MetricStreamFilter, namespace, metricName string) bool {
	for _, f := range filters {
		if f.Namespace != namespace {
			continue
		}
		if len(f.MetricNames) == 0 {
			return true
		}
		if filterNamesContain(f.MetricNames, metricName) {
			return true
		}
	}

	return false
}

// filterIncludesMetric returns true when at least one IncludeFilters entry allows the metric.
func filterIncludesMetric(filters []MetricStreamFilter, namespace, metricName string) bool {
	for _, f := range filters {
		if f.Namespace != namespace {
			continue
		}
		if len(f.MetricNames) == 0 {
			return true
		}
		if filterNamesContain(f.MetricNames, metricName) {
			return true
		}
	}

	return false
}

// filterNamesContain returns true when name is in the names list.
func filterNamesContain(names []string, name string) bool {
	return slices.Contains(names, name)
}

// streamAllowsMetric returns true when the given namespace/metricName passes the
// stream's IncludeFilters and ExcludeFilters. An empty IncludeFilters means "all
// namespaces allowed"; ExcludeFilters override IncludeFilters when both are set.
func streamAllowsMetric(s *MetricStream, namespace, metricName string) bool {
	if filterExcludesMetric(s.ExcludeFilters, namespace, metricName) {
		return false
	}
	if len(s.IncludeFilters) > 0 {
		return filterIncludesMetric(s.IncludeFilters, namespace, metricName)
	}

	return true
}

// matchingRunningStreamNames returns the names of running metric streams that
// allow at least one datum in data. Caller must hold b.mu (any lock).
// The caller updates LastUpdateDate in a separate, shorter lock acquisition to
// avoid holding the metrics write lock during the full stream-filter scan.
func (b *InMemoryBackend) matchingRunningStreamNames(
	namespace string,
	data []MetricDatum,
) []string {
	var names []string

	for _, s := range b.metricStreams.All() {
		if s.State != metricStreamStateRunning {
			continue
		}

		for _, d := range data {
			if streamAllowsMetric(s, namespace, d.MetricName) {
				names = append(names, s.Name)

				break
			}
		}
	}

	return names
}

// PutMetricStream creates or updates a metric stream by name.
func (b *InMemoryBackend) PutMetricStream(stream *MetricStream) error {
	if err := validateMetricStream(stream); err != nil {
		return err
	}

	b.PutMetricStreamInternal(stream)

	return nil
}

// GetMetricStream returns a metric stream by name.
func (b *InMemoryBackend) GetMetricStream(name string) (*MetricStream, error) {
	b.mu.RLock("GetMetricStream")
	defer b.mu.RUnlock()

	stream, ok := b.metricStreams.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMetricStreamNotFound, name)
	}

	cp := *stream

	return &cp, nil
}

// DeleteMetricStream removes a metric stream by name.
// Returns ErrMetricStreamNotFound if the stream does not exist.
func (b *InMemoryBackend) DeleteMetricStream(name string) error {
	b.mu.Lock("DeleteMetricStream")
	defer b.mu.Unlock()

	if !b.metricStreams.Has(name) {
		return fmt.Errorf("%w: %s", ErrMetricStreamNotFound, name)
	}

	b.metricStreams.Delete(name)

	return nil
}

// PutMetricStreamInternal creates or updates a metric stream (used for test seeding).
func (b *InMemoryBackend) PutMetricStreamInternal(stream *MetricStream) {
	b.mu.Lock("PutMetricStreamInternal")
	defer b.mu.Unlock()

	cp := *stream
	if cp.CreationDate.IsZero() {
		cp.CreationDate = time.Now().UTC()
	}

	cp.LastUpdateDate = time.Now().UTC()

	if cp.Arn == "" {
		cp.Arn = arn.Build("cloudwatch", b.region, b.accountID, "metric-stream/"+stream.Name)
	}

	// Preserve the existing state if not explicitly set so that Stop/Start calls are honoured.
	if existing, ok := b.metricStreams.Get(stream.Name); ok && cp.State == "" {
		cp.State = existing.State
	}

	if cp.State == "" {
		cp.State = metricStreamStateRunning
	}

	b.metricStreams.Put(&cp)
}

// ListMetricStreams returns a paginated list of all metric streams.
func (b *InMemoryBackend) ListMetricStreams(
	nextToken string,
	maxResults int,
) (page.Page[MetricStream], error) {
	b.mu.RLock("ListMetricStreams")
	defer b.mu.RUnlock()

	result := make([]MetricStream, 0, b.metricStreams.Len())
	for _, s := range b.metricStreams.All() {
		result = append(result, *s)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return page.New(result, nextToken, maxResults, cwDefaultListMetricStreamsLimit), nil
}

const (
	metricStreamStateRunning = "RUNNING"
	metricStreamStateStopped = "STOPPED"
)

// StartMetricStreams sets the State of the named streams to RUNNING.
// Names that do not exist are silently ignored (AWS behaviour).
func (b *InMemoryBackend) StartMetricStreams(names []string) error {
	b.mu.Lock("StartMetricStreams")
	defer b.mu.Unlock()

	for _, name := range names {
		if s, ok := b.metricStreams.Get(name); ok {
			s.State = metricStreamStateRunning
			s.LastUpdateDate = time.Now().UTC()
		}
	}

	return nil
}

// StopMetricStreams sets the State of the named streams to STOPPED.
// Names that do not exist are silently ignored (AWS behaviour).
func (b *InMemoryBackend) StopMetricStreams(names []string) error {
	b.mu.Lock("StopMetricStreams")
	defer b.mu.Unlock()

	for _, name := range names {
		if s, ok := b.metricStreams.Get(name); ok {
			s.State = metricStreamStateStopped
			s.LastUpdateDate = time.Now().UTC()
		}
	}

	return nil
}
