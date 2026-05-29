package rds

// batch3.go provides real stateful backend implementations for:
//   - DescribeCustomDBEngineVersions
//   - AddDBRecommendation (internal seeding helper)
//   - Performance Insights metric generation
//   - Recommendation status management (active/inactive/paused)

import (
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"time"
)

const (
	engineLifecycleSupportOpenSource         = "open-source-rds-extended-support"
	engineLifecycleSupportOpenSourceDisabled = "open-source-rds-extended-support-disabled"

	storageTypeAuroraIOOptimized = "aurora-iopt1"
	storageTypeAurora            = "aurora"
	storageTypeIO1               = "io1"
	storageTypeGP2               = "gp2"
	storageTypeGP3               = "gp3"

	piValueRange = 1000
	piValueScale = 100.0
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

// piSeed hashes resourceID and metric into a 64-bit seed.
func piSeed(resourceID, metric string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(resourceID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(metric))

	return h.Sum64()
}

// piValue returns a pseudo-random float in [0.0, 10.0) for the given seed and bucket.
// bucket and seed are both encoded as decimal strings to avoid any int/uint conversions.
func piValue(seed uint64, bucket int64) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strconv.FormatUint(seed, 10)))
	_, _ = h.Write([]byte(":"))
	_, _ = h.Write([]byte(strconv.FormatInt(bucket, 10)))

	return float64(h.Sum64()%piValueRange) / piValueScale
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
