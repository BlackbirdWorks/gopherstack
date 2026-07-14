package sagemaker

import "context"

// ---------------------------------------------------------------------------
// SageMaker Service Catalog portfolio (Enable/Disable/GetStatus)
//
// This is a single account/region-scoped toggle with no create operation of
// its own, mirroring how DescribeLineageGroup models the account's single
// auto-provisioned lineage group.
// ---------------------------------------------------------------------------

const (
	servicecatalogStatusEnabled  = "Enabled"
	servicecatalogStatusDisabled = "Disabled"
)

// EnableSagemakerServicecatalogPortfolio marks the Service Catalog portfolio enabled.
func (b *InMemoryBackend) EnableSagemakerServicecatalogPortfolio(ctx context.Context) {
	b.mu.Lock("EnableSagemakerServicecatalogPortfolio")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.servicecatalogPortfolioEnabled[region] = true
}

// DisableSagemakerServicecatalogPortfolio marks the Service Catalog portfolio disabled.
func (b *InMemoryBackend) DisableSagemakerServicecatalogPortfolio(ctx context.Context) {
	b.mu.Lock("DisableSagemakerServicecatalogPortfolio")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.servicecatalogPortfolioEnabled[region] = false
}

// GetSagemakerServicecatalogPortfolioStatus returns "Enabled" or "Disabled".
func (b *InMemoryBackend) GetSagemakerServicecatalogPortfolioStatus(ctx context.Context) string {
	b.mu.RLock("GetSagemakerServicecatalogPortfolioStatus")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	if b.servicecatalogPortfolioEnabled[region] {
		return servicecatalogStatusEnabled
	}

	return servicecatalogStatusDisabled
}

// ---------------------------------------------------------------------------
// ListResourceCatalogs
//
// ResourceCatalog is a read-only, AWS-managed resource with no Create API in
// the SageMaker SDK, so there is no state to model here. A fresh emulator
// account has none, matching the correct empty-list AWS shape.
// ---------------------------------------------------------------------------

// ListResourceCatalogs always returns an empty catalog list: there is no
// CreateResourceCatalog operation, so this backend never has any to report.
func (b *InMemoryBackend) ListResourceCatalogs() []string {
	return []string{}
}
