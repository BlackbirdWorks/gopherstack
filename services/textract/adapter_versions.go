package textract

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// adapterVersionKey returns the storage key for an adapter version.
func adapterVersionKey(adapterID, version string) string {
	return adapterID + "#" + version
}

// buildAdapterVersionARN constructs an ARN for a Textract adapter version.
func buildAdapterVersionARN(region, accountID, adapterID, version string) string {
	return arn.Build("textract", region, accountID, fmt.Sprintf("adapter/%s/version/%s", adapterID, version))
}

// resolveARNToAdapterVersion finds an adapter version by ARN in the given region.
func resolveARNToAdapterVersion(
	adapterVersions *store.Table[AdapterVersion], region, resourceARN string,
) (*AdapterVersion, bool) {
	const versionPrefix = "/version/"

	idx := lastIndex(resourceARN, versionPrefix)
	if idx < 0 {
		return nil, false
	}

	adapterPart := resourceARN[:idx]
	version := resourceARN[idx+len(versionPrefix):]

	const adapterPrefix = "adapter/"

	adIdx := lastIndex(adapterPart, adapterPrefix)
	if adIdx < 0 {
		return nil, false
	}

	adapterID := adapterPart[adIdx+len(adapterPrefix):]

	return adapterVersions.Get(regionKey(region, adapterVersionKey(adapterID, version)))
}

// deterministic evaluation metrics for adapter versions.
const (
	evalF1Score   = 0.85
	evalPrecision = 0.88
	evalRecall    = 0.82

	evalBaselineF1Score   = 0.80
	evalBaselinePrecision = 0.83
	evalBaselineRecall    = 0.78
)

// buildEvaluationMetrics returns one AdapterVersionEvaluationMetric per
// FeatureType the adapter was created with, pairing a deterministic
// baseline score against a deterministic adapter-version score. Matches the
// real SDK's GetAdapterVersionOutput.EvaluationMetrics shape: a list scoped
// by FeatureType, not a single flat metrics struct.
func buildEvaluationMetrics(featureTypes []string) []AdapterVersionEvaluationMetric {
	metrics := make([]AdapterVersionEvaluationMetric, 0, len(featureTypes))
	for _, ft := range featureTypes {
		metrics = append(metrics, AdapterVersionEvaluationMetric{
			FeatureType: ft,
			Baseline: &EvaluationMetric{
				F1Score:   evalBaselineF1Score,
				Precision: evalBaselinePrecision,
				Recall:    evalBaselineRecall,
			},
			AdapterVersion: &EvaluationMetric{
				F1Score:   evalF1Score,
				Precision: evalPrecision,
				Recall:    evalRecall,
			},
		})
	}

	return metrics
}

// CreateAdapterVersion creates a new version for an existing adapter.
func (b *InMemoryBackend) CreateAdapterVersion(
	ctx context.Context, adapterID string, tags map[string]string,
) (*AdapterVersion, error) {
	return b.CreateAdapterVersionWithOptions(ctx, adapterID, tags, nil, nil, "", "")
}

// CreateAdapterVersionWithOptions creates an adapter version with full options.
func (b *InMemoryBackend) CreateAdapterVersionWithOptions(
	ctx context.Context,
	adapterID string,
	tags map[string]string,
	datasetConfig *DatasetConfig,
	outputConfig *OutputConfig,
	kmsKeyID, clientRequestToken string,
) (*AdapterVersion, error) {
	region := getRegion(ctx, b.region)

	var result *AdapterVersion
	var done bool
	var notFound bool
	var key, version string

	func() {
		b.mu.Lock("CreateAdapterVersion")
		defer b.mu.Unlock()

		adapter, ok := b.adapters.Get(regionKey(region, adapterID))
		if !ok {
			notFound = true

			return
		}

		version = uuid.NewString()
		av := &AdapterVersion{
			Region:         region,
			AdapterID:      adapterID,
			AdapterVersion: version,
			CreationTime:   time.Now(),
			FeatureTypes:   append([]string{}, adapter.FeatureTypes...),
			// Start in CREATION_IN_PROGRESS; goroutine transitions to ACTIVE.
			Status:             adapterVersionCreating,
			Tags:               cloneTags(tags),
			DatasetConfig:      datasetConfig,
			OutputConfig:       outputConfig,
			KMSKeyId:           kmsKeyID,
			ClientRequestToken: clientRequestToken,
			EvaluationMetrics:  buildEvaluationMetrics(adapter.FeatureTypes),
		}
		b.adapterVersions.Put(av)

		if b.asyncJobDelay == 0 {
			av.Status = adapterVersionActive
			result = cloneAdapterVersion(av)
			done = true

			return
		}

		key = adapterVersionTableKey(av)
	}()

	if notFound {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}
	if done {
		return result, nil
	}

	// Transition to ACTIVE after a short delay.
	b.runDelayed(b.asyncJobDelay, func() {
		b.mu.Lock("CreateAdapterVersion-complete")
		defer b.mu.Unlock()

		if stored, ok2 := b.adapterVersions.Get(key); ok2 {
			stored.Status = adapterVersionActive
		}
	})

	func() {
		b.mu.RLock("CreateAdapterVersion-read")
		defer b.mu.RUnlock()

		if stored, ok := b.adapterVersions.Get(key); ok {
			result = cloneAdapterVersion(stored)
		}
	}()

	if result == nil {
		return nil, fmt.Errorf("%w: adapter version %s/%s not found", ErrAdapterVersionNotFound, adapterID, version)
	}

	return result, nil
}

// GetAdapterVersion retrieves a specific adapter version.
func (b *InMemoryBackend) GetAdapterVersion(ctx context.Context, adapterID, version string) (*AdapterVersion, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetAdapterVersion")
	defer b.mu.RUnlock()

	av, ok := b.adapterVersions.Get(regionKey(region, adapterVersionKey(adapterID, version)))
	if !ok {
		return nil, fmt.Errorf("%w: adapter version %s/%s not found", ErrAdapterVersionNotFound, adapterID, version)
	}

	return cloneAdapterVersion(av), nil
}

// ListAdapterVersions returns versions for adapterID, or -- when adapterID is
// empty -- every version of every adapter in the region. Real AWS's
// ListAdapterVersionsInput marks AdapterId optional (a plain filter, not the
// sole identifier); omitting it lists across all adapters, which is the path
// exercised here. Sorted by AdapterID then AdapterVersion: the real SDK
// documents no ordering, so this is the deterministic choice that keeps
// pkgs/page's offset tokens stable across the merged, freshly-rebuilt set.
func (b *InMemoryBackend) ListAdapterVersions(ctx context.Context, adapterID string) ([]AdapterVersion, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAdapterVersions")
	defer b.mu.RUnlock()

	var out []AdapterVersion

	if adapterID == "" {
		out = b.allAdapterVersionsLocked(region)
	} else {
		if !b.adapters.Has(regionKey(region, adapterID)) {
			return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
		}

		out = cloneAdapterVersions(b.adapterVersionsByAdapter.Get(regionKey(region, adapterID)))
	}

	sortAdapterVersions(out)

	return out, nil
}

// allAdapterVersionsLocked returns every adapter version across every
// adapter in region, reusing adaptersByRegion and adapterVersionsByAdapter --
// the same two secondary indexes ListAdapters and the single-adapter path
// above already maintain. Caller must hold b.mu (read or write).
func (b *InMemoryBackend) allAdapterVersionsLocked(region string) []AdapterVersion {
	adapters := b.adaptersByRegion.Get(region)

	groups := make([][]*AdapterVersion, len(adapters))
	total := 0

	for i, a := range adapters {
		groups[i] = b.adapterVersionsByAdapter.Get(regionKey(region, a.AdapterID))
		total += len(groups[i])
	}

	out := make([]AdapterVersion, 0, total)
	for _, g := range groups {
		out = append(out, cloneAdapterVersions(g)...)
	}

	return out
}

// cloneAdapterVersions deep-copies a slice of *AdapterVersion (as owned by a
// store.Index group) into a slice of values safe to hold past the lock scope.
func cloneAdapterVersions(versions []*AdapterVersion) []AdapterVersion {
	out := make([]AdapterVersion, 0, len(versions))
	for _, av := range versions {
		out = append(out, *cloneAdapterVersion(av))
	}

	return out
}

// sortAdapterVersions orders by AdapterID then AdapterVersion.
func sortAdapterVersions(versions []AdapterVersion) {
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].AdapterID != versions[j].AdapterID {
			return versions[i].AdapterID < versions[j].AdapterID
		}

		return versions[i].AdapterVersion < versions[j].AdapterVersion
	})
}

// DeleteAdapterVersion removes a specific adapter version.
func (b *InMemoryBackend) DeleteAdapterVersion(ctx context.Context, adapterID, version string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAdapterVersion")
	defer b.mu.Unlock()

	key := regionKey(region, adapterVersionKey(adapterID, version))
	if !b.adapterVersions.Has(key) {
		return fmt.Errorf("%w: adapter version %s/%s not found", ErrAdapterVersionNotFound, adapterID, version)
	}

	b.adapterVersions.Delete(key)

	return nil
}

// cloneAdapterVersion returns a deep copy of an AdapterVersion.
func cloneAdapterVersion(av *AdapterVersion) *AdapterVersion {
	cp := *av
	cp.FeatureTypes = make([]string, len(av.FeatureTypes))
	copy(cp.FeatureTypes, av.FeatureTypes)
	cp.Tags = cloneTags(av.Tags)

	if av.EvaluationMetrics != nil {
		cp.EvaluationMetrics = make([]AdapterVersionEvaluationMetric, len(av.EvaluationMetrics))
		copy(cp.EvaluationMetrics, av.EvaluationMetrics)
	}

	return &cp
}

// BuildAdapterVersionARN returns the ARN for an adapter version (exported for handler use).
func (b *InMemoryBackend) BuildAdapterVersionARN(adapterID, version string) string {
	return buildAdapterVersionARN(b.region, b.accountID, adapterID, version)
}
