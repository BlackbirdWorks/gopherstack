package sagemaker

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

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

// ---------------------------------------------------------------------------
// TrialComponent association extras
// ---------------------------------------------------------------------------

// DisassociateTrialComponent removes the association between a trial and a
// trial component, if one exists. It is idempotent: disassociating a
// component that was never associated succeeds and simply returns the
// resources' ARNs (mirroring AssociateTrialComponent's non-strict existence
// checks).
func (b *InMemoryBackend) DisassociateTrialComponent(
	ctx context.Context,
	trialName, trialComponentName string,
) (string, string, error) {
	b.mu.Lock("DisassociateTrialComponent")
	defer b.mu.Unlock()

	if trialName == "" {
		return "", "", fmt.Errorf("%w: TrialName is required", ErrValidation)
	}

	if trialComponentName == "" {
		return "", "", fmt.Errorf("%w: TrialComponentName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	trialArn := arn.Build("sagemaker", region, b.accountID, "experiment-trial/"+trialName)
	trialComponentArn := arn.Build(
		"sagemaker", region, b.accountID, "experiment-trial-component/"+trialComponentName,
	)

	key := trialComponentKey(trialName, trialComponentName)
	b.trialComponentAssociationsStore(region).Delete(key)

	return trialArn, trialComponentArn, nil
}

// ListTrialComponents returns trial components, optionally filtered by the
// trial they're associated with or the experiment their trial belongs to.
func (b *InMemoryBackend) ListTrialComponents(
	ctx context.Context,
	experimentName, trialName, nextToken string,
) ([]*TrialComponent, string) {
	b.mu.RLock("ListTrialComponents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	allowed := b.trialComponentNameFilterLocked(region, experimentName, trialName)

	list := make([]*TrialComponent, 0, b.trialComponentsStore(region).Len())

	for _, tc := range b.trialComponentsStore(region).All() {
		if allowed != nil {
			if _, ok := allowed[tc.TrialComponentName]; !ok {
				continue
			}
		}

		list = append(list, cloneTrialComponent(tc))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].TrialComponentName < list[j].TrialComponentName })

	return paginateSlice(list, nextToken, 0)
}

// trialComponentNameFilterLocked returns the set of trial component names
// allowed by the given TrialName/ExperimentName filters, or nil if neither
// filter is set (meaning: no filtering). Callers must hold b.mu.
func (b *InMemoryBackend) trialComponentNameFilterLocked(
	region, experimentName, trialName string,
) map[string]bool {
	if trialName == "" && experimentName == "" {
		return nil
	}

	trialNames := map[string]bool{}

	if trialName != "" {
		trialNames[trialName] = true
	} else {
		for _, t := range b.trialsStore(region).All() {
			if t.ExperimentName == experimentName {
				trialNames[t.TrialName] = true
			}
		}
	}

	allowed := map[string]bool{}

	for _, assoc := range b.trialComponentAssociationsStore(region).All() {
		if trialNames[assoc.TrialName] {
			allowed[assoc.TrialComponentName] = true
		}
	}

	return allowed
}

// ---------------------------------------------------------------------------
// Image alias listing
// ---------------------------------------------------------------------------

// ListImageAliases returns the aliases attached to an image. If version is
// positive, only that version's aliases are considered; otherwise, aliases
// from every version of the image are aggregated.
func (b *InMemoryBackend) ListImageAliases(
	ctx context.Context,
	imageName string,
	version int32,
	nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListImageAliases")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.smImagesStore(region).Get(imageName); !ok {
		return nil, "", fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, imageName)
	}

	versions := b.imageVersionsStore(region)[imageName]

	var candidates []*ImageVersion

	if version > 0 {
		if iv, ok := versions[int(version)]; ok {
			candidates = append(candidates, iv)
		}
	} else {
		for _, iv := range versions {
			candidates = append(candidates, iv)
		}
	}

	aliases := dedupeAliases(candidates)
	sort.Strings(aliases)

	page, out := paginateSlice(aliases, nextToken, 0)

	return page, out, nil
}

// dedupeAliases flattens the Aliases of every given image version into a
// single de-duplicated slice.
func dedupeAliases(versions []*ImageVersion) []string {
	seen := map[string]bool{}
	aliases := make([]string, 0)

	for _, iv := range versions {
		for _, a := range iv.Aliases {
			if seen[a] {
				continue
			}

			seen[a] = true

			aliases = append(aliases, a)
		}
	}

	return aliases
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

// UpdateProject updates a project's description and merges in new tags.
func (b *InMemoryBackend) UpdateProject(
	ctx context.Context,
	name, description string,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.projectsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	if description != "" {
		p.ProjectDescription = description
	}

	if len(tags) > 0 {
		p.Tags = mergeTags(p.Tags, tags)
	}

	return cloneProject(p), nil
}
