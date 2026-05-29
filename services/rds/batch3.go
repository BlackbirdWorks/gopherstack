package rds

// batch3.go provides real stateful backend implementations for:
//   - DescribeCustomDBEngineVersions
//   - AddDBRecommendation (internal seeding helper)
//   - Performance Insights metric generation
//   - Recommendation status management (active/inactive/paused)

import (
	"fmt"
	"slices"
	"time"
)

const (
	recommendationStatusActive   = "active"
	recommendationStatusInactive = "inactive"
	recommendationStatusPaused   = "paused"

	engineLifecycleSupportOpenSource         = "open-source-rds-extended-support"
	engineLifecycleSupportOpenSourceDisabled = "open-source-rds-extended-support-disabled"

	storageTypeAuroraIOOptimized = "aurora-iopt1"
	storageTypeAurora            = "aurora"
	storageTypeIO1               = "io1"
	storageTypeGP2               = "gp2"
	storageTypeGP3               = "gp3"

	networkTypeIPV4 = "IPV4"
	networkTypeDual = "DUAL"
)

// DescribeCustomDBEngineVersions returns all custom engine versions, filtered by engine
// and/or engineVersion if non-empty.
func (b *InMemoryBackend) DescribeCustomDBEngineVersions(engine, engineVersion string) []CustomDBEngineVersion {
	b.mu.RLock("DescribeCustomDBEngineVersions")
	defer b.mu.RUnlock()

	result := make([]CustomDBEngineVersion, 0, len(b.customEngineVersions))

	for _, cev := range b.customEngineVersions {
		if engine != "" && cev.Engine != engine {
			continue
		}

		if engineVersion != "" && cev.EngineVersion != engineVersion {
			continue
		}

		result = append(result, *cev)
	}

	slices.SortFunc(result, func(a, b CustomDBEngineVersion) int {
		ka := a.Engine + "/" + a.EngineVersion
		kb := b.Engine + "/" + b.EngineVersion
		if ka < kb {
			return -1
		}
		if ka > kb {
			return 1
		}

		return 0
	})

	return result
}

// AddDBRecommendation adds a recommendation to the backend. Used by tests and internal
// workflows to seed recommendations so that ModifyDBRecommendation and
// DescribeDBRecommendations can exercise real state transitions.
func (b *InMemoryBackend) AddDBRecommendation(rec DBRecommendation) {
	b.mu.Lock("AddDBRecommendation")
	defer b.mu.Unlock()

	cp := rec
	b.recommendations[rec.RecommendationID] = &cp
}

// GetPerformanceInsightsData returns synthetic Performance Insights metric data points
// for the given resource identifier, metric name, and time range. The data is
// deterministically generated based on the resource identifier and time bucket so
// tests get repeatable results without external state.
func (b *InMemoryBackend) GetPerformanceInsightsData(
	resourceID, metric string,
	startTime, endTime time.Time,
	periodInSeconds int,
) []PIDataPoint {
	b.mu.RLock("GetPerformanceInsightsData")
	defer b.mu.RUnlock()

	if periodInSeconds <= 0 {
		periodInSeconds = 60
	}

	if startTime.IsZero() {
		startTime = endTime.Add(-time.Hour)
	}

	if endTime.IsZero() {
		endTime = time.Now().UTC()
	}

	bucketDur := time.Duration(periodInSeconds) * time.Second
	seed := piSeed(resourceID, metric)

	var points []PIDataPoint

	for t := startTime; !t.After(endTime); t = t.Add(bucketDur) {
		bucket := t.Unix() / int64(periodInSeconds)
		value := piValue(seed, bucket)
		points = append(points, PIDataPoint{
			Timestamp: t.UTC().Format(time.RFC3339),
			Value:     value,
		})
	}

	return points
}

// PIDataPoint is a single Performance Insights metric data point.
type PIDataPoint struct {
	Timestamp string
	Value     float64
}

// piSeed computes a stable hash seed from resourceID + metric.
func piSeed(resourceID, metric string) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range resourceID + "|" + metric {
		h ^= uint64(c)
		h *= 1099511628211
	}

	return h
}

// piValue derives a pseudo-random float in [0.0, 10.0) from seed and bucket index.
func piValue(seed uint64, bucket int64) float64 {
	x := seed ^ (uint64(bucket) * 2654435761)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33

	return float64(x%1000) / 100.0
}

// ValidateEngineLifecycleSupport returns an error if the value is not a recognized
// EngineLifecycleSupport option.
func ValidateEngineLifecycleSupport(val string) error {
	switch val {
	case "", engineLifecycleSupportOpenSource, engineLifecycleSupportOpenSourceDisabled:
		return nil
	default:
		return fmt.Errorf(
			"%w: EngineLifecycleSupport must be %q or %q, got %q",
			ErrInvalidParameter,
			engineLifecycleSupportOpenSource,
			engineLifecycleSupportOpenSourceDisabled,
			val,
		)
	}
}

// ValidateStorageTypeForCluster returns an error if the storage type is not valid for
// an Aurora cluster.
func ValidateStorageTypeForCluster(storageType string) error {
	switch storageType {
	case "", storageTypeAurora, storageTypeAuroraIOOptimized:
		return nil
	default:
		return fmt.Errorf(
			"%w: StorageType for Aurora cluster must be %q or %q, got %q",
			ErrInvalidParameter,
			storageTypeAurora,
			storageTypeAuroraIOOptimized,
			storageType,
		)
	}
}
