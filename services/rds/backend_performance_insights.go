package rds

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// GetPerformanceInsightsData returns synthetic Performance Insights metric data points
// for the given resource identifier, metric name, and time range. The data is
// deterministically generated based on the resource identifier and time bucket so
// tests get repeatable results without external state.
func (b *InMemoryBackend) GetPerformanceInsightsData(
	resourceID, metric string,
	_ time.Time, _ time.Time,
	_ int,
) ([]PIDataPoint, error) {
	b.mu.RLock("GetPerformanceInsightsData")
	defer b.mu.RUnlock()

	// Validate that the instance exists and has Performance Insights enabled.
	var found *DBInstance
	for _, inst := range b.instances.All() {
		if inst.DbiResourceID == resourceID || inst.DBInstanceIdentifier == resourceID {
			found = inst

			break
		}
	}
	if found == nil || !found.PerformanceInsightsEnabled {
		return nil, awserr.New("InvalidParameterValue", awserr.ErrInvalidParameter)
	}

	var points []PIDataPoint
	if b.piMetrics != nil {
		if resMetrics, ok := b.piMetrics[resourceID]; ok {
			points = append(points, resMetrics[metric]...)
		}
	}

	return points, nil
}

// SetPerformanceInsightsData stores PI metrics for testing.
func (b *InMemoryBackend) SetPerformanceInsightsData(resourceID, metric string, points []PIDataPoint) {
	b.mu.Lock("SetPerformanceInsightsData")
	defer b.mu.Unlock()

	if b.piMetrics == nil {
		b.piMetrics = make(map[string]map[string][]PIDataPoint)
	}
	if _, ok := b.piMetrics[resourceID]; !ok {
		b.piMetrics[resourceID] = make(map[string][]PIDataPoint)
	}
	b.piMetrics[resourceID][metric] = append(b.piMetrics[resourceID][metric], points...)
}

// PIDataPoint is a single Performance Insights metric data point.
type PIDataPoint struct {
	Timestamp string
	Value     float64
}
