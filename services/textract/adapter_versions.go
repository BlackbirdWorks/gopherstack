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
)

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

	b.mu.Lock("CreateAdapterVersion")

	adapter, ok := b.adapters.Get(regionKey(region, adapterID))
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	version := uuid.NewString()
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
		EvaluationMetrics: &EvaluationMetrics{
			F1Score:   evalF1Score,
			Precision: evalPrecision,
			Recall:    evalRecall,
		},
	}
	b.adapterVersions.Put(av)

	if b.asyncJobDelay == 0 {
		av.Status = adapterVersionActive
		result := cloneAdapterVersion(av)
		b.mu.Unlock()

		return result, nil
	}

	b.mu.Unlock()

	key := adapterVersionTableKey(av)

	// Transition to ACTIVE after a short delay.
	b.runDelayed(b.asyncJobDelay, func() {
		b.mu.Lock("CreateAdapterVersion-complete")
		defer b.mu.Unlock()

		if stored, ok2 := b.adapterVersions.Get(key); ok2 {
			stored.Status = adapterVersionActive
		}
	})

	b.mu.RLock("CreateAdapterVersion-read")
	stored, _ := b.adapterVersions.Get(key)
	result := cloneAdapterVersion(stored)
	b.mu.RUnlock()

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

// ListAdapterVersions returns all versions for a given adapter, sorted by version string.
func (b *InMemoryBackend) ListAdapterVersions(ctx context.Context, adapterID string) ([]AdapterVersion, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAdapterVersions")
	defer b.mu.RUnlock()

	if !b.adapters.Has(regionKey(region, adapterID)) {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	versions := b.adapterVersionsByAdapter.Get(regionKey(region, adapterID))
	out := make([]AdapterVersion, 0, len(versions))

	for _, av := range versions {
		out = append(out, *cloneAdapterVersion(av))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AdapterVersion < out[j].AdapterVersion
	})

	return out, nil
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

	return &cp
}

// BuildAdapterVersionARN returns the ARN for an adapter version (exported for handler use).
func (b *InMemoryBackend) BuildAdapterVersionARN(adapterID, version string) string {
	return buildAdapterVersionARN(b.region, b.accountID, adapterID, version)
}
