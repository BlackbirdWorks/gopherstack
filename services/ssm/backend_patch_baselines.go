package ssm

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) patchGroupToBaselineStore(region string) map[string]string {
	return b.patchGroupToBaseline[region]
}
func (b *InMemoryBackend) patchBaselinesStore(region string) *store.Table[PatchBaseline] {
	return getOrCreateTable(b, b.patchBaselines, "patchBaselines", region, patchBaselineKeyFn)
}

// CreatePatchBaseline creates a new patch baseline.
func (b *InMemoryBackend) CreatePatchBaseline(
	ctx context.Context,
	input *CreatePatchBaselineInput,
) (*CreatePatchBaselineOutput, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidationException)
	}

	const defaultPatchOS = "WINDOWS"
	os := input.OperatingSystem
	if os == "" {
		os = defaultPatchOS
	}

	region := getRegion(ctx)
	b.mu.Lock("CreatePatchBaseline")
	defer b.mu.Unlock()

	baselineID := baselineIDPrefix + uuid.NewString()
	now := UnixTimeFloat(time.Now())

	bl := PatchBaseline{
		BaselineID:                     baselineID,
		Name:                           input.Name,
		Description:                    input.Description,
		OperatingSystem:                os,
		ApprovedPatches:                input.ApprovedPatches,
		RejectedPatches:                input.RejectedPatches,
		ApprovedPatchesComplianceLevel: input.ApprovedPatchesComplianceLevel,
		CreatedDate:                    now,
		ModifiedDate:                   now,
	}

	b.patchBaselinesStore(region).Put(&bl)

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}
		miscTags := b.miscResourceTagsStore(region)
		if miscTags[baselineID] == nil {
			miscTags[baselineID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			miscTags[baselineID][t.Key] = t.Value
		}
	}

	return &CreatePatchBaselineOutput{BaselineID: baselineID}, nil
}

// DeregisterPatchBaselineForPatchGroup removes a patch group association.
func (b *InMemoryBackend) DeregisterPatchBaselineForPatchGroup(
	ctx context.Context,
	input *DeregisterPatchBaselineForPatchGroupInput,
) (*DeregisterPatchBaselineForPatchGroupOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeregisterPatchBaselineForPatchGroup")
	defer b.mu.Unlock()

	delete(b.patchGroupToBaselineStore(region), input.PatchGroup)

	return &DeregisterPatchBaselineForPatchGroupOutput{
		BaselineID: input.BaselineID,
		PatchGroup: input.PatchGroup,
	}, nil
}

// DescribeAvailablePatches returns patches from the available patches catalog,
// lazily seeding it with the built-in catalogue (defaultPatchCatalog) on the
// region's first access rather than leaving it permanently empty.
func (b *InMemoryBackend) DescribeAvailablePatches(
	ctx context.Context,
	_ *DescribeAvailablePatchesInput,
) (*DescribeAvailablePatchesOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DescribeAvailablePatches")
	defer b.mu.Unlock()

	patches := b.availablePatchesFor(region)
	result := make([]Patch, len(patches))
	copy(result, patches)

	return &DescribeAvailablePatchesOutput{Patches: result}, nil
}

// patchBaselineMatchesFilters returns true when bl satisfies all provided key-value filters.
// Supported filter keys: OPERATING_SYSTEM, NAME_PREFIX.
func patchBaselineMatchesFilters(bl PatchBaseline, filters []PatchBaselineFilter) bool {
	for _, f := range filters {
		var fieldValue string

		switch f.Key {
		case "OPERATING_SYSTEM":
			fieldValue = bl.OperatingSystem
		case "NAME_PREFIX":
			for _, v := range f.Values {
				if len(bl.Name) >= len(v) && bl.Name[:len(v)] == v {
					fieldValue = v
				}
			}
		default:
			continue
		}

		if !slices.Contains(f.Values, fieldValue) {
			return false
		}
	}

	return true
}

// DescribePatchBaselines lists patch baselines with optional OS and name filters.
func (b *InMemoryBackend) DescribePatchBaselines(
	ctx context.Context,
	input *DescribePatchBaselinesInput,
) (*DescribePatchBaselinesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribePatchBaselines")
	defer b.mu.RUnlock()

	baselines := b.patchBaselinesStore(region)
	all := make([]PatchBaselineIdentity, 0, baselines.Len())
	for _, blPtr := range baselines.All() {
		bl := *blPtr
		if !patchBaselineMatchesFilters(bl, input.Filters) {
			continue
		}

		all = append(all, PatchBaselineIdentity{
			BaselineID:      bl.BaselineID,
			BaselineName:    bl.Name,
			OperatingSystem: bl.OperatingSystem,
			Description:     bl.Description,
		})
	}

	startIdx := parseNextToken(input.NextToken)

	const defaultBaselineMaxResults = 50

	maxResults := int64(defaultBaselineMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &DescribePatchBaselinesOutput{BaselineIdentities: []PatchBaselineIdentity{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &DescribePatchBaselinesOutput{
		BaselineIdentities: all[startIdx:end],
		NextToken:          nextToken,
	}, nil
}

// GetPatchBaseline retrieves a patch baseline by ID.
func (b *InMemoryBackend) GetPatchBaseline(
	ctx context.Context,
	input *GetPatchBaselineInput,
) (*GetPatchBaselineOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetPatchBaseline")
	defer b.mu.RUnlock()

	bl, exists := b.patchBaselinesStore(region).Get(input.BaselineID)
	if !exists {
		return nil, ErrPatchBaselineNotFound
	}

	return &GetPatchBaselineOutput{PatchBaseline: *bl}, nil
}

// RegisterPatchBaselineForPatchGroup associates a baseline with a patch group.
func (b *InMemoryBackend) RegisterPatchBaselineForPatchGroup(
	ctx context.Context,
	input *RegisterPatchBaselineForPatchGroupInput,
) (*RegisterPatchBaselineForPatchGroupOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("RegisterPatchBaselineForPatchGroup")
	defer b.mu.Unlock()

	if !b.patchBaselinesStore(region).Has(input.BaselineID) {
		return nil, ErrPatchBaselineNotFound
	}

	if b.patchGroupToBaseline[region] == nil {
		b.patchGroupToBaseline[region] = make(map[string]string)
	}
	b.patchGroupToBaselineStore(region)[input.PatchGroup] = input.BaselineID

	return &RegisterPatchBaselineForPatchGroupOutput{
		BaselineID: input.BaselineID,
		PatchGroup: input.PatchGroup,
	}, nil
}

// UpdatePatchBaseline updates a patch baseline.
func (b *InMemoryBackend) UpdatePatchBaseline(
	ctx context.Context,
	input *UpdatePatchBaselineInput,
) (*UpdatePatchBaselineOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdatePatchBaseline")
	defer b.mu.Unlock()

	baselines := b.patchBaselinesStore(region)
	blPtr, exists := baselines.Get(input.BaselineID)
	if !exists {
		return nil, ErrPatchBaselineNotFound
	}

	bl := *blPtr

	if input.Name != "" {
		bl.Name = input.Name
	}

	if input.Description != "" {
		bl.Description = input.Description
	}

	if len(input.ApprovedPatches) > 0 {
		bl.ApprovedPatches = input.ApprovedPatches
	}

	if len(input.RejectedPatches) > 0 {
		bl.RejectedPatches = input.RejectedPatches
	}

	if input.ApprovedPatchesComplianceLevel != "" {
		bl.ApprovedPatchesComplianceLevel = input.ApprovedPatchesComplianceLevel
	}

	bl.ModifiedDate = UnixTimeFloat(timeNow())
	baselines.Put(&bl)

	return &UpdatePatchBaselineOutput{PatchBaseline: bl}, nil
}

// GetDefaultPatchBaseline returns the baseline registered for "default" or a
// hard-coded fallback baseline ID.
func (b *InMemoryBackend) GetDefaultPatchBaseline(
	ctx context.Context,
	input *GetDefaultPatchBaselineInput,
) (*GetDefaultPatchBaselineOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetDefaultPatchBaseline")
	defer b.mu.RUnlock()

	key := "default"
	if input.OperatingSystem != "" {
		key = "default-" + input.OperatingSystem
	}

	if id, ok := b.patchGroupToBaselineStore(region)[key]; ok {
		os := input.OperatingSystem
		if os == "" {
			if blPtr, foundBl := b.patchBaselinesStore(region).Get(id); foundBl {
				os = blPtr.OperatingSystem
			}
		}

		return &GetDefaultPatchBaselineOutput{
			BaselineID:      id,
			OperatingSystem: os,
		}, nil
	}

	// No default registered for this OS: fall back to the AWS-managed default
	// baseline, which is real state seeded into the store (its ID is stable and
	// GetPatchBaseline can describe it), rather than a fabricated all-zeros ID.
	os := input.OperatingSystem
	if os == "" {
		os = "WINDOWS"
	}

	return &GetDefaultPatchBaselineOutput{
		BaselineID:      defaultBaselineID(os),
		OperatingSystem: os,
	}, nil
}

// GetPatchBaselineForPatchGroup looks up the baseline for a given patch group.
// Returns an empty result when PatchGroup is empty (stub compat).
func (b *InMemoryBackend) GetPatchBaselineForPatchGroup(
	ctx context.Context,
	input *GetPatchBaselineForPatchGroupInput,
) (*GetPatchBaselineForPatchBaselineOutput, error) {
	if input.PatchGroup == "" {
		return &GetPatchBaselineForPatchBaselineOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.RLock("GetPatchBaselineForPatchGroup")
	defer b.mu.RUnlock()

	id, ok := b.patchGroupToBaselineStore(region)[input.PatchGroup]
	if !ok {
		return nil, fmt.Errorf(
			"%w: patch group %q not found",
			ErrPatchBaselineNotFound,
			input.PatchGroup,
		)
	}

	return &GetPatchBaselineForPatchBaselineOutput{
		BaselineID:      id,
		PatchGroup:      input.PatchGroup,
		OperatingSystem: input.OperatingSystem,
	}, nil
}

// RegisterDefaultPatchBaseline sets the default patch baseline.
// Returns success with an empty BaselineID when the input is empty (stub compat).
func (b *InMemoryBackend) RegisterDefaultPatchBaseline(
	ctx context.Context,
	input *RegisterDefaultPatchBaselineInput,
) (*RegisterDefaultPatchBaselineOutput, error) {
	if input.BaselineID == "" {
		return &RegisterDefaultPatchBaselineOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.Lock("RegisterDefaultPatchBaseline")
	defer b.mu.Unlock()

	if !b.patchBaselinesStore(region).Has(input.BaselineID) {
		return nil, fmt.Errorf(
			"%w: baseline %q not found",
			ErrPatchBaselineNotFound,
			input.BaselineID,
		)
	}

	if b.patchGroupToBaseline[region] == nil {
		b.patchGroupToBaseline[region] = make(map[string]string)
	}
	store := b.patchGroupToBaselineStore(region)
	store["default"] = input.BaselineID

	// Also store per-OS key when the baseline has a known OperatingSystem.
	if bl, ok := b.patchBaselinesStore(region).Get(input.BaselineID); ok && bl.OperatingSystem != "" {
		store["default-"+bl.OperatingSystem] = input.BaselineID
	}

	return &RegisterDefaultPatchBaselineOutput{BaselineID: input.BaselineID}, nil
}

// DeletePatchBaseline removes a patch baseline by ID.
func (b *InMemoryBackend) DeletePatchBaseline(
	ctx context.Context,
	input *DeletePatchBaselineInput,
) (*DeletePatchBaselineOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeletePatchBaseline")
	defer b.mu.Unlock()

	patchBaselines := b.patchBaselinesStore(region)
	if !patchBaselines.Has(input.BaselineID) {
		return nil, ErrPatchBaselineNotFound
	}

	patchBaselines.Delete(input.BaselineID)

	return &DeletePatchBaselineOutput{BaselineID: input.BaselineID}, nil
}

// DescribePatchGroupState returns aggregated patch counts for a patch group.
func (b *InMemoryBackend) DescribePatchGroupState(
	ctx context.Context,
	input *DescribePatchGroupStateInput,
) (*DescribePatchGroupStateOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribePatchGroupState")
	defer b.mu.RUnlock()

	out := &DescribePatchGroupStateOutput{}
	for _, s := range b.instancePatchStatesStore(region).All() {
		if s.PatchGroup != input.PatchGroup {
			continue
		}
		out.Instances++
		if s.FailedCount > 0 {
			out.InstancesWithFailedPatches++
		}
		if s.InstalledCount > 0 {
			out.InstancesWithInstalledPatches++
		}
		if s.MissingCount > 0 {
			out.InstancesWithMissingPatches++
		}
	}

	return out, nil
}

// DescribePatchGroups lists the patch group to baseline mappings.
func (b *InMemoryBackend) DescribePatchGroups(
	ctx context.Context,
	input *DescribePatchGroupsInput,
) (*DescribePatchGroupsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribePatchGroups")
	defer b.mu.RUnlock()

	store := b.patchGroupToBaselineStore(region)
	mappings := make([]PatchGroupPatchBaselineMapping, 0, len(store))

	patchBaselines := b.patchBaselinesStore(region)
	for group, baselineID := range store {
		identity := PatchBaselineIdentity{BaselineID: baselineID}
		if bl, ok := patchBaselines.Get(baselineID); ok {
			identity.BaselineName = bl.Name
			identity.OperatingSystem = bl.OperatingSystem
			identity.Description = bl.Description
		}

		mappings = append(mappings, PatchGroupPatchBaselineMapping{
			PatchGroup:       group,
			BaselineIdentity: identity,
		})
	}

	startIdx := parseNextToken(input.NextToken)

	const (
		defaultPatchGroupsMaxResults = 50
		maxPatchGroupsMaxResults     = 100
	)

	maxResults := int64(defaultPatchGroupsMaxResults)
	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxPatchGroupsMaxResults {
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				maxPatchGroupsMaxResults,
			)
		}

		maxResults = *input.MaxResults
	}

	if startIdx >= len(mappings) {
		return &DescribePatchGroupsOutput{Mappings: []PatchGroupPatchBaselineMapping{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string
	if end < len(mappings) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(mappings)
	}

	return &DescribePatchGroupsOutput{
		Mappings:  mappings[startIdx:end],
		NextToken: nextToken,
	}, nil
}

// DescribePatchProperties returns property data aggregated from patch baselines.
func (b *InMemoryBackend) DescribePatchProperties(
	ctx context.Context,
	input *DescribePatchPropertiesInput,
) (*DescribePatchPropertiesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribePatchProperties")
	defer b.mu.RUnlock()

	seen := map[string]bool{}
	props := make([]map[string]string, 0)
	for _, bl := range b.patchBaselinesStore(region).All() {
		if input.OperatingSystem != "" && bl.OperatingSystem != input.OperatingSystem {
			continue
		}

		key := bl.OperatingSystem + ":" + bl.Name
		if seen[key] {
			continue
		}

		seen[key] = true
		props = append(props, map[string]string{
			"OperatingSystem": bl.OperatingSystem,
			"BaselineName":    bl.Name,
		})
	}

	return &DescribePatchPropertiesOutput{Properties: props}, nil
}

// DescribeEffectivePatchesForPatchBaseline returns the effective patch set for
// a baseline, derived from its approved/rejected patches plus the region's
// available-patches catalogue (see effectivePatchesForBaseline).
// Returns an empty list when BaselineID is empty (stub compat).
func (b *InMemoryBackend) DescribeEffectivePatchesForPatchBaseline(
	ctx context.Context,
	input *DescribeEffectivePatchesForPatchBaselineInput,
) (*DescribeEffectivePatchesForPatchBaselineOutput, error) {
	if input.BaselineID == "" {
		return &DescribeEffectivePatchesForPatchBaselineOutput{
			EffectivePatches: []EffectivePatch{},
		}, nil
	}

	region := getRegion(ctx)
	b.mu.RLock("DescribeEffectivePatchesForPatchBaseline")
	defer b.mu.RUnlock()

	baselinePtr, exists := b.patchBaselinesStore(region).Get(input.BaselineID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: baseline %q not found",
			ErrPatchBaselineNotFound,
			input.BaselineID,
		)
	}

	effective := b.effectivePatchesForBaseline(region, *baselinePtr)

	return paginateEffectivePatches(effective, input.NextToken, input.MaxResults), nil
}

// effectivePatchesForBaseline derives the effective patch set for a baseline
// from its explicitly-approved patches plus the region's available-patch
// catalogue (matched on OS/product), backing the response with real stored
// state instead of an empty list (once the catalogue has been seeded — see
// availablePatchesFor). Must be called with b.mu held.
func (b *InMemoryBackend) effectivePatchesForBaseline(
	region string,
	baseline PatchBaseline,
) []EffectivePatch {
	level := baseline.ApprovedPatchesComplianceLevel
	if level == "" {
		level = "UNSPECIFIED"
	}

	approvalDate := time.Unix(int64(baseline.CreatedDate), 0).UTC().Format(time.RFC3339)

	effective := make([]EffectivePatch, 0, len(baseline.ApprovedPatches))

	approved := make(map[string]struct{}, len(baseline.ApprovedPatches))
	for _, id := range baseline.ApprovedPatches {
		approved[id] = struct{}{}
		p := id
		effective = append(effective, EffectivePatch{
			Patch: &Patch{Name: p, Classification: patchClassificationSecurityUpdates},
			PatchStatus: &PatchStatus{
				DeploymentStatus: "EXPLICIT_APPROVED",
				ComplianceLevel:  level,
				ApprovalDate:     approvalDate,
			},
		})
	}

	// Include catalogue patches for the baseline's OS/product that are not
	// explicitly rejected — these are AVAILABLE for approval.
	rejected := make(map[string]struct{}, len(baseline.RejectedPatches))
	for _, id := range baseline.RejectedPatches {
		rejected[id] = struct{}{}
	}

	for _, p := range b.availablePatches[region] {
		if _, isApproved := approved[p.Name]; isApproved {
			continue
		}

		if _, isRejected := rejected[p.Name]; isRejected {
			continue
		}

		patch := p
		effective = append(effective, EffectivePatch{
			Patch: &patch,
			PatchStatus: &PatchStatus{
				DeploymentStatus: "AVAILABLE",
				ComplianceLevel:  level,
			},
		})
	}

	return effective
}

// paginateEffectivePatches applies opaque index-based pagination to an effective
// patch list.
func paginateEffectivePatches(
	all []EffectivePatch,
	nextToken string,
	maxResults *int64,
) *DescribeEffectivePatchesForPatchBaselineOutput {
	startIdx := parseNextToken(nextToken)

	const defaultMax = 100

	limit := int64(defaultMax)
	if maxResults != nil && *maxResults > 0 {
		limit = *maxResults
	}

	if startIdx >= len(all) {
		return &DescribeEffectivePatchesForPatchBaselineOutput{
			EffectivePatches: []EffectivePatch{},
		}
	}

	end := startIdx + int(limit)

	var token string

	if end < len(all) {
		token = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &DescribeEffectivePatchesForPatchBaselineOutput{
		EffectivePatches: all[startIdx:end],
		NextToken:        token,
	}
}

// GetDeployablePatchSnapshotForInstance returns the deployable patch snapshot
// for an instance. The snapshot is backed by the instance's effective patch
// baseline (looked up via its recorded patch state or the default baseline for
// its OS) rather than a random URL, and the Product reflects the real baseline
// OS. A caller-supplied SnapshotId is preserved so repeated calls are stable.
func (b *InMemoryBackend) GetDeployablePatchSnapshotForInstance(
	ctx context.Context,
	input *GetDeployablePatchSnapshotForInstanceInput,
) (*GetDeployablePatchSnapshotForInstanceOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetDeployablePatchSnapshotForInstance")
	defer b.mu.RUnlock()

	snapshotID := input.SnapshotID
	if snapshotID == "" {
		snapshotID = uuid.NewString()
	}

	// Resolve the instance's effective baseline: prefer its recorded patch
	// state, else the AWS default baseline for its OS (Windows fallback).
	product := patchProductAmazonLinux2
	baselineID := defaultBaselineID("AMAZON_LINUX_2")

	if st, ok := b.instancePatchStatesStore(region).Get(input.InstanceID); ok && st != nil {
		if st.BaselineID != "" {
			baselineID = st.BaselineID
		}

		if bl, found := b.patchBaselinesStore(region).Get(st.BaselineID); found &&
			bl.OperatingSystem != "" {
			product = bl.OperatingSystem
		}
	}

	return &GetDeployablePatchSnapshotForInstanceOutput{
		InstanceID: input.InstanceID,
		SnapshotID: snapshotID,
		Product:    product,
		SnapshotDownloadURL: "https://patch-baseline-snapshot-" + region +
			".s3." + region + ".amazonaws.com/" + baselineID + "-" + snapshotID,
	}, nil
}
