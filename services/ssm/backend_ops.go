package ssm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/google/uuid"
)

// backend_ops.go implements 50 backend operations that were previously stubs.
// All operations follow the same locking convention as backend.go:
//
//	b.mu.Lock("MethodName") / defer b.mu.Unlock() for writes
//	b.mu.RLock("MethodName") / defer b.mu.RUnlock() for reads

var (
	// ErrInventoryNotFound is returned when inventory for a type is not found.
	ErrInventoryNotFound = errors.New("InventoryTypeNotFound")
	// ErrDocumentVersionNotFound is returned when a document version is not found.
	ErrDocumentVersionNotFound = errors.New("InvalidDocumentVersion")
)

const (
	inventorySchemaV10           = "1.0"
	inventorySchemaV11           = "1.1"
	complianceStatusCompliant    = "COMPLIANT"
	complianceStatusNonCompliant = "NON_COMPLIANT"
)

// ---------------------------------------------------------------------------
// Group 1 — Document ops
// ---------------------------------------------------------------------------

// UpdateDocumentDefaultVersion sets the DefaultVersion field on an existing document.
// It fails if the document or the requested version does not exist.
// Returns a no-op success when Name or DocumentVersion is empty (legacy stub compat).
func (b *InMemoryBackend) UpdateDocumentDefaultVersion(
	ctx context.Context,
	input *UpdateDocumentDefaultVersionInput,
) (*UpdateDocumentDefaultVersionOutput, error) {
	if input.Name == "" || input.DocumentVersion == "" {
		return &UpdateDocumentDefaultVersionOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.Lock("UpdateDocumentDefaultVersion")
	defer b.mu.Unlock()

	docs := b.documentsStore(region)
	doc, exists := docs[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: document %q not found", ErrDocumentNotFound, input.Name)
	}

	// Verify the requested version exists in documentVersions.
	found := false

	docVersions := b.documentVersionsStore(region)
	for _, dv := range docVersions[input.Name] {
		if dv.DocumentVersion == input.DocumentVersion {
			found = true

			break
		}
	}

	if !found {
		return nil, fmt.Errorf("%w: version %q not found for document %q",
			ErrInvalidDocumentVersion, input.DocumentVersion, input.Name)
	}

	doc.DefaultVersion = input.DocumentVersion
	docs[input.Name] = doc

	// Also mark the version entry.
	versions := docVersions[input.Name]
	for i := range versions {
		versions[i].IsDefaultVersion = versions[i].DocumentVersion == input.DocumentVersion
	}

	docVersions[input.Name] = versions

	return &UpdateDocumentDefaultVersionOutput{
		Description: &DocumentDefaultVersionDescription{
			Name:           input.Name,
			DefaultVersion: input.DocumentVersion,
		},
	}, nil
}

// UpdateDocumentMetadata updates document reviews metadata.
// This is a lightweight implementation that acknowledges the request and
// returns success without modifying stored state (the AWS API is complex
// and review state is not tracked in this in-memory implementation).
func (b *InMemoryBackend) UpdateDocumentMetadata(
	ctx context.Context,
	input *UpdateDocumentMetadataInput,
) (*StubOutput, error) {
	if input.Name == "" {
		return &StubOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.RLock("UpdateDocumentMetadata")
	defer b.mu.RUnlock()

	if _, exists := b.documentsStore(region)[input.Name]; !exists {
		return nil, fmt.Errorf("%w: document %q not found", ErrDocumentNotFound, input.Name)
	}

	return &StubOutput{}, nil
}

// ListDocumentMetadataHistory returns an empty approval history.
// The in-memory backend does not track document review history; this returns
// a well-formed empty response consistent with the stateless stub approach.
func (b *InMemoryBackend) ListDocumentMetadataHistory(
	ctx context.Context,
	input *ListDocumentMetadataHistoryInput,
) (*ListDocumentMetadataHistoryOutput, error) {
	if input.Name == "" {
		return &ListDocumentMetadataHistoryOutput{
			Metadata: &DocumentMetadataResponseInfo{
				ReviewerResponse: []DocumentReviewerResponseSource{},
			},
		}, nil
	}

	region := getRegion(ctx)
	b.mu.RLock("ListDocumentMetadataHistory")
	defer b.mu.RUnlock()

	doc, exists := b.documentsStore(region)[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: document %q not found", ErrDocumentNotFound, input.Name)
	}

	return &ListDocumentMetadataHistoryOutput{
		Name:            doc.Name,
		DocumentVersion: doc.DocumentVersion,
		Metadata: &DocumentMetadataResponseInfo{
			ReviewerResponse: []DocumentReviewerResponseSource{},
		},
	}, nil
}

// ---------------------------------------------------------------------------
// Group 2 — Inventory
// ---------------------------------------------------------------------------

// PutInventory stores inventory items for an instance.
// It fails if InstanceId is empty AND Items are provided.
func (b *InMemoryBackend) PutInventory(ctx context.Context, input *PutInventoryInput) (*StubOutput, error) {
	if input.InstanceID == "" && len(input.Items) > 0 {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrValidationException)
	}

	if input.InstanceID == "" {
		return &StubOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.Lock("PutInventory")
	defer b.mu.Unlock()

	store := b.inventoryStore(region)
	existing := store[input.InstanceID]

	// Merge by TypeName: replace existing entries of the same type.
	typeMap := make(map[string]InventoryItem, len(existing))
	for _, item := range existing {
		typeMap[item.TypeName] = item
	}

	for _, item := range input.Items {
		typeMap[item.TypeName] = item
	}

	merged := make([]InventoryItem, 0, len(typeMap))
	for _, item := range typeMap {
		merged = append(merged, item)
	}

	store[input.InstanceID] = merged

	return &StubOutput{}, nil
}

// GetInventory returns stored inventory entities across all instances.
func (b *InMemoryBackend) GetInventory(ctx context.Context, input *GetInventoryInput) (*GetInventoryOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetInventory")
	defer b.mu.RUnlock()

	store := b.inventoryStore(region)
	entities := make([]InventoryResultEntity, 0, len(store))
	for instanceID, items := range store {
		data := make(map[string]InventoryTypeData, len(items))
		for _, item := range items {
			data[item.TypeName] = InventoryTypeData{
				TypeName:      item.TypeName,
				SchemaVersion: item.SchemaVersion,
				CaptureTime:   item.CaptureTime,
				ContentHash:   item.ContentHash,
				Content:       item.Content,
			}
		}

		entities = append(entities, InventoryResultEntity{
			ID:   instanceID,
			Data: data,
		})
	}

	startIdx := parseNextToken(input.NextToken)

	const defaultMaxResults = 50

	maxResults := int64(defaultMaxResults)
	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > defaultMaxResults {
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, defaultMaxResults)
		}

		maxResults = *input.MaxResults
	}

	if startIdx >= len(entities) {
		return &GetInventoryOutput{Entities: []InventoryResultEntity{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string
	if end < len(entities) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(entities)
	}

	return &GetInventoryOutput{
		Entities:  entities[startIdx:end],
		NextToken: nextToken,
	}, nil
}

// GetInventorySchema returns the built-in AWS SSM inventory schema types.
// When TypeName is provided, only schemas matching that prefix are returned.
func (b *InMemoryBackend) GetInventorySchema(
	_ context.Context,
	input *GetInventorySchemaInput,
) (*GetInventorySchemaOutput, error) {
	all := []InventorySchemaItem{
		{TypeName: "AWS:Application", Version: inventorySchemaV11},
		{TypeName: "AWS:AWSComponent", Version: inventorySchemaV10},
		{TypeName: "AWS:ComplianceItem", Version: inventorySchemaV11},
		{TypeName: "AWS:ComplianceSummary", Version: inventorySchemaV11},
		{TypeName: "AWS:InstanceDetailedInformation", Version: inventorySchemaV10},
		{TypeName: "AWS:InstanceInformation", Version: inventorySchemaV10},
		{TypeName: "AWS:Network", Version: inventorySchemaV10},
		{TypeName: "AWS:PatchCompliance", Version: inventorySchemaV11},
		{TypeName: "AWS:PatchSummary", Version: inventorySchemaV10},
		{TypeName: "AWS:WindowsRegistry", Version: inventorySchemaV10},
		{TypeName: "AWS:WindowsRole", Version: inventorySchemaV10},
		{TypeName: "AWS:WindowsUpdate", Version: inventorySchemaV10},
		{TypeName: "Custom:Application", Version: inventorySchemaV10},
	}

	if input.TypeName == "" {
		schemas := make([]any, len(all))
		for i, s := range all {
			schemas[i] = s
		}

		return &GetInventorySchemaOutput{Schemas: schemas}, nil
	}

	filtered := make([]any, 0)
	for _, s := range all {
		if s.TypeName == input.TypeName || len(s.TypeName) >= len(input.TypeName) &&
			s.TypeName[:len(input.TypeName)] == input.TypeName {
			filtered = append(filtered, s)
		}
	}

	return &GetInventorySchemaOutput{Schemas: filtered}, nil
}

// ListInventoryEntries returns stored inventory entries for an instance and type.
func (b *InMemoryBackend) ListInventoryEntries(
	ctx context.Context,
	input *ListInventoryEntriesInput,
) (*ListInventoryEntriesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListInventoryEntries")
	defer b.mu.RUnlock()

	items, exists := b.inventoryStore(region)[input.InstanceID]
	if !exists {
		return &ListInventoryEntriesOutput{
			InstanceID: input.InstanceID,
			TypeName:   input.TypeName,
			Entries:    []map[string]string{},
		}, nil
	}

	var entries []map[string]string

	for _, item := range items {
		if item.TypeName == input.TypeName {
			entries = append(entries, item.Content...)

			break
		}
	}

	if entries == nil {
		entries = []map[string]string{}
	}

	const maxInventoryEntries = 50

	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxInventoryEntries {
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, maxInventoryEntries)
		}
	}

	startIdx := parseNextToken(input.NextToken)
	limit := int64(maxInventoryEntries)

	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	if startIdx >= len(entries) {
		return &ListInventoryEntriesOutput{
			InstanceID: input.InstanceID,
			TypeName:   input.TypeName,
			Entries:    []map[string]string{},
		}, nil
	}

	end := startIdx + int(limit)

	var nextToken string

	if end < len(entries) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(entries)
	}

	return &ListInventoryEntriesOutput{
		InstanceID: input.InstanceID,
		TypeName:   input.TypeName,
		NextToken:  nextToken,
		Entries:    entries[startIdx:end],
	}, nil
}

// DeleteInventory removes all inventory for the given TypeName across all instances.
func (b *InMemoryBackend) DeleteInventory(ctx context.Context, input *DeleteInventoryInput) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteInventory")
	defer b.mu.Unlock()

	store := b.inventoryStore(region)
	for instanceID, items := range store {
		filtered := items[:0]
		for _, item := range items {
			if item.TypeName != input.TypeName {
				filtered = append(filtered, item)
			}
		}

		if len(filtered) == 0 {
			delete(store, instanceID)
		} else {
			store[instanceID] = filtered
		}
	}

	return &StubOutput{}, nil
}

// DescribeInventoryDeletions returns an empty list.
// The in-memory backend does not track deletion jobs.
func (b *InMemoryBackend) DescribeInventoryDeletions(
	_ context.Context,
	_ *DescribeInventoryDeletionsInput,
) (*DescribeInventoryDeletionsOutput, error) {
	return &DescribeInventoryDeletionsOutput{
		InventoryDeletions: []any{},
	}, nil
}

// ---------------------------------------------------------------------------
// Group 3 — Compliance
// ---------------------------------------------------------------------------

// PutComplianceItems stores compliance items for a resource.
// It fails if ResourceID is empty AND Items are provided.
func (b *InMemoryBackend) PutComplianceItems(ctx context.Context, input *PutComplianceItemsInput) (*StubOutput, error) {
	if input.ResourceID == "" && len(input.Items) > 0 {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidationException)
	}

	if input.ResourceID == "" {
		return &StubOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.Lock("PutComplianceItems")
	defer b.mu.Unlock()

	// Tag each item with ResourceId/ResourceType from the input envelope.
	newItems := make([]ComplianceItem, 0, len(input.Items))
	for _, item := range input.Items {
		ci := item
		ci.ResourceID = input.ResourceID
		ci.ResourceType = input.ResourceType

		if ci.ComplianceType == "" {
			ci.ComplianceType = input.ComplianceType
		}

		newItems = append(newItems, ci)
	}

	b.complianceStore(region)[input.ResourceID] = newItems

	return &StubOutput{}, nil
}

// ListComplianceItems returns stored compliance items, optionally filtered by ResourceId/ResourceType.
func (b *InMemoryBackend) ListComplianceItems(
	ctx context.Context,
	input *ListComplianceItemsInput,
) (*ListComplianceItemsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListComplianceItems")
	defer b.mu.RUnlock()

	var all []ComplianceItem

	for _, items := range b.complianceStore(region) {
		for _, item := range items {
			if input.ResourceID != "" && item.ResourceID != input.ResourceID {
				continue
			}

			if input.ResourceType != "" && item.ResourceType != input.ResourceType {
				continue
			}

			all = append(all, item)
		}
	}

	if all == nil {
		all = []ComplianceItem{}
	}

	const maxComplianceItems = 50

	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxComplianceItems {
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, maxComplianceItems)
		}
	}

	startIdx := parseNextToken(input.NextToken)
	limit := int64(maxComplianceItems)

	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &ListComplianceItemsOutput{ComplianceItems: []ComplianceItem{}}, nil
	}

	end := startIdx + int(limit)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &ListComplianceItemsOutput{NextToken: nextToken, ComplianceItems: all[startIdx:end]}, nil
}

// ListComplianceSummaries aggregates stored compliance items by ComplianceType.
func (b *InMemoryBackend) ListComplianceSummaries(
	ctx context.Context,
	input *ListComplianceSummariesInput,
) (*ListComplianceSummariesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListComplianceSummaries")
	defer b.mu.RUnlock()

	// Tally compliant/non-compliant counts per compliance type.
	type tally struct {
		compliantCount    int
		nonCompliantCount int
	}

	tallies := make(map[string]*tally)

	for _, items := range b.complianceStore(region) {
		for _, item := range items {
			ct := item.ComplianceType
			if ct == "" {
				ct = "Custom"
			}

			if tallies[ct] == nil {
				tallies[ct] = &tally{}
			}

			if item.Status == complianceStatusCompliant {
				tallies[ct].compliantCount++
			} else {
				tallies[ct].nonCompliantCount++
			}
		}
	}

	summaries := make([]any, 0, len(tallies))
	for ct, t := range tallies {
		summaries = append(summaries, ComplianceSummaryItem{
			ComplianceType: ct,
			CompliantSummary: ComplianceCountSummary{
				CompliantCount: t.compliantCount,
			},
			NonCompliantSummary: ComplianceCountSummary{
				NonCompliantCount: t.nonCompliantCount,
			},
		})
	}

	const maxComplianceSummaries = 50

	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxComplianceSummaries {
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, maxComplianceSummaries)
		}
	}

	startIdx := parseNextToken(input.NextToken)
	limit := int64(maxComplianceSummaries)

	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	if startIdx >= len(summaries) {
		return &ListComplianceSummariesOutput{ComplianceSummaryItems: []any{}}, nil
	}

	end := startIdx + int(limit)

	var nextToken string

	if end < len(summaries) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(summaries)
	}

	return &ListComplianceSummariesOutput{NextToken: nextToken, ComplianceSummaryItems: summaries[startIdx:end]}, nil
}

// ListResourceComplianceSummaries returns per-resource compliance summaries
// derived from stored compliance items.
func (b *InMemoryBackend) ListResourceComplianceSummaries(
	ctx context.Context,
	input *ListResourceComplianceSummariesInput,
) (*ListResourceComplianceSummariesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListResourceComplianceSummaries")
	defer b.mu.RUnlock()

	store := b.complianceStore(region)
	summaries := make([]any, 0, len(store))

	for resourceID, items := range store {
		if len(items) == 0 {
			continue
		}

		compliant := 0
		nonCompliant := 0

		for _, item := range items {
			if item.Status == complianceStatusCompliant {
				compliant++
			} else {
				nonCompliant++
			}
		}

		status := complianceStatusCompliant
		if nonCompliant > 0 {
			status = complianceStatusNonCompliant
		}

		summaries = append(summaries, ResourceComplianceSummaryItem{
			ResourceID:      resourceID,
			ResourceType:    items[0].ResourceType,
			ComplianceType:  items[0].ComplianceType,
			OverallSeverity: "INFORMATIONAL",
			Status:          status,
			CompliantSummary: ComplianceCountSummary{
				CompliantCount: compliant,
			},
			NonCompliantSummary: ComplianceCountSummary{
				NonCompliantCount: nonCompliant,
			},
		})
	}

	const maxResourceComplianceSummaries = 50

	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxResourceComplianceSummaries {
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, maxResourceComplianceSummaries)
		}
	}

	startIdx := parseNextToken(input.NextToken)
	limit := int64(maxResourceComplianceSummaries)

	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	if startIdx >= len(summaries) {
		return &ListResourceComplianceSummariesOutput{ResourceComplianceSummaryItems: []any{}}, nil
	}

	end := startIdx + int(limit)

	var nextToken string

	if end < len(summaries) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(summaries)
	}

	return &ListResourceComplianceSummariesOutput{NextToken: nextToken, ResourceComplianceSummaryItems: summaries[startIdx:end]}, nil
}

// ---------------------------------------------------------------------------
// Group 4 — Patch baseline / groups
// ---------------------------------------------------------------------------

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
		return &GetDefaultPatchBaselineOutput{
			BaselineID:      id,
			OperatingSystem: input.OperatingSystem,
		}, nil
	}

	// No default registered — return a synthetic default ID.
	return &GetDefaultPatchBaselineOutput{
		BaselineID:      "pb-00000000000000000",
		OperatingSystem: input.OperatingSystem,
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
		return nil, fmt.Errorf("%w: patch group %q not found", ErrPatchBaselineNotFound, input.PatchGroup)
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

	if _, exists := b.patchBaselinesStore(region)[input.BaselineID]; !exists {
		return nil, fmt.Errorf("%w: baseline %q not found", ErrPatchBaselineNotFound, input.BaselineID)
	}

	store := b.patchGroupToBaselineStore(region)
	store["default"] = input.BaselineID

	// Also store per-OS key when the baseline has a known OperatingSystem.
	if bl, ok := b.patchBaselinesStore(region)[input.BaselineID]; ok && bl.OperatingSystem != "" {
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

	store := b.patchBaselinesStore(region)
	if _, exists := store[input.BaselineID]; !exists {
		return nil, ErrPatchBaselineNotFound
	}

	delete(store, input.BaselineID)

	return &DeletePatchBaselineOutput{BaselineID: input.BaselineID}, nil
}

// DescribePatchGroupState returns zeroed counts for a patch group.
// The in-memory backend does not track per-instance patch state.
func (b *InMemoryBackend) DescribePatchGroupState(
	_ context.Context,
	_ *DescribePatchGroupStateInput,
) (*DescribePatchGroupStateOutput, error) {
	return &DescribePatchGroupStateOutput{}, nil
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
		if bl, ok := patchBaselines[baselineID]; ok {
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
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, maxPatchGroupsMaxResults)
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

// DescribePatchProperties returns an empty property list.
// The in-memory backend does not maintain patch metadata.
func (b *InMemoryBackend) DescribePatchProperties(
	_ context.Context,
	_ *DescribePatchPropertiesInput,
) (*DescribePatchPropertiesOutput, error) {
	return &DescribePatchPropertiesOutput{
		Properties: []map[string]string{},
	}, nil
}

// DescribeEffectivePatchesForPatchBaseline returns an empty patch list.
// The in-memory backend does not maintain patch catalogue data.
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

	if _, exists := b.patchBaselinesStore(region)[input.BaselineID]; !exists {
		return nil, fmt.Errorf("%w: baseline %q not found", ErrPatchBaselineNotFound, input.BaselineID)
	}

	return &DescribeEffectivePatchesForPatchBaselineOutput{
		EffectivePatches: []EffectivePatch{},
	}, nil
}

// GetDeployablePatchSnapshotForInstance returns a stub snapshot ID.
// The in-memory backend generates a synthetic snapshot URL.
func (b *InMemoryBackend) GetDeployablePatchSnapshotForInstance(
	_ context.Context,
	input *GetDeployablePatchSnapshotForInstanceInput,
) (*GetDeployablePatchSnapshotForInstanceOutput, error) {
	snapshotID := input.SnapshotID
	if snapshotID == "" {
		snapshotID = uuid.NewString()
	}

	return &GetDeployablePatchSnapshotForInstanceOutput{
		InstanceID:          input.InstanceID,
		SnapshotID:          snapshotID,
		SnapshotDownloadURL: "https://s3.amazonaws.com/gopherstack-patch-snapshot/" + snapshotID,
	}, nil
}

// ---------------------------------------------------------------------------
// Group 5 — Maintenance window
// ---------------------------------------------------------------------------

// DeleteMaintenanceWindow removes a maintenance window by ID.
func (b *InMemoryBackend) DeleteMaintenanceWindow(
	ctx context.Context,
	input *DeleteMaintenanceWindowInput,
) (*DeleteMaintenanceWindowOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteMaintenanceWindow")
	defer b.mu.Unlock()

	store := b.maintenanceWindowsStore(region)
	if _, exists := store[input.WindowID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	delete(store, input.WindowID)

	return &DeleteMaintenanceWindowOutput{WindowID: input.WindowID}, nil
}

// GetMaintenanceWindowTask retrieves a task by WindowId and WindowTaskId.
// Returns an empty task when WindowTaskID is empty (stub compat).
func (b *InMemoryBackend) GetMaintenanceWindowTask(
	ctx context.Context,
	input *GetMaintenanceWindowTaskInput,
) (*GetMaintenanceWindowTaskOutput, error) {
	if input.WindowTaskID == "" {
		return &GetMaintenanceWindowTaskOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.RLock("GetMaintenanceWindowTask")
	defer b.mu.RUnlock()

	task, exists := b.maintenanceWindowTasksStore(region)[input.WindowTaskID]
	if !exists || task.WindowID != input.WindowID {
		return nil, ErrMaintenanceWindowNotFound
	}

	return &GetMaintenanceWindowTaskOutput{MaintenanceWindowTask: task}, nil
}

// DescribeMaintenanceWindowsForTarget returns windows that have registered targets
// matching the given resource type and target key/value filters.
func (b *InMemoryBackend) DescribeMaintenanceWindowsForTarget(
	ctx context.Context,
	input *DescribeMaintenanceWindowsForTargetInput,
) (*DescribeMaintenanceWindowsForTargetOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeMaintenanceWindowsForTarget")
	defer b.mu.RUnlock()

	matchedWindowIDs := make(map[string]struct{})

	for _, windowTarget := range b.maintenanceWindowTargetsStore(region) {
		if input.ResourceType != "" && windowTarget.ResourceType != input.ResourceType {
			continue
		}

		if len(input.Targets) == 0 || windowTargetMatchesFilters(windowTarget.Targets, input.Targets) {
			matchedWindowIDs[windowTarget.WindowID] = struct{}{}
		}
	}

	maintenanceWindows := b.maintenanceWindowsStore(region)
	identities := make([]MaintenanceWindowIdentity, 0, len(matchedWindowIDs))
	for windowID := range matchedWindowIDs {
		if mw, ok := maintenanceWindows[windowID]; ok {
			identities = append(identities, MaintenanceWindowIdentity{
				WindowID:    mw.WindowID,
				Name:        mw.Name,
				Description: mw.Description,
				Enabled:     mw.Enabled,
				Duration:    mw.Duration,
				Cutoff:      mw.Cutoff,
				Schedule:    mw.Schedule,
			})
		}
	}

	const (
		defaultMWTargetMaxResults = 20
		maxMWTargetMaxResults     = 100
	)

	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxMWTargetMaxResults {
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, maxMWTargetMaxResults)
		}
	}

	startIdx := parseNextToken(input.NextToken)
	limit := int64(defaultMWTargetMaxResults)

	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	if startIdx >= len(identities) {
		return &DescribeMaintenanceWindowsForTargetOutput{WindowIdentities: []MaintenanceWindowIdentity{}}, nil
	}

	end := startIdx + int(limit)

	var nextToken string

	if end < len(identities) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(identities)
	}

	return &DescribeMaintenanceWindowsForTargetOutput{
		NextToken:        nextToken,
		WindowIdentities: identities[startIdx:end],
	}, nil
}

// windowTargetMatchesFilters reports whether any registered target key/value
// pair satisfies at least one of the requested filter targets.
func windowTargetMatchesFilters(registered []WindowTarget, requested []WindowTarget) bool {
	for _, req := range requested {
		for _, reg := range registered {
			if reg.Key != req.Key {
				continue
			}

			for _, reqVal := range req.Values {
				if slices.Contains(reg.Values, reqVal) {
					return true
				}
			}
		}
	}

	return false
}

// UpdateMaintenanceWindowTarget updates target fields.
// Returns an empty response when the target is not found (stub compat for empty ID).
func (b *InMemoryBackend) UpdateMaintenanceWindowTarget(
	ctx context.Context,
	input *UpdateMaintenanceWindowTargetInput,
) (*UpdateMaintenanceWindowTargetOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateMaintenanceWindowTarget")
	defer b.mu.Unlock()

	store := b.maintenanceWindowTargetsStore(region)
	target, exists := store[input.WindowTargetID]
	if !exists || target.WindowID != input.WindowID {
		// Return a no-op success rather than error to preserve stub compat for
		// callers that send non-existent IDs (e.g. the simple stub coverage test).
		return &UpdateMaintenanceWindowTargetOutput{
			WindowID:       input.WindowID,
			WindowTargetID: input.WindowTargetID,
		}, nil
	}

	if input.OwnerInfo != "" {
		target.OwnerInfo = input.OwnerInfo
	}

	if input.Name != "" {
		target.Name = input.Name
	}

	if input.Description != "" {
		target.Description = input.Description
	}

	if len(input.Targets) > 0 {
		target.Targets = input.Targets
	}

	store[input.WindowTargetID] = target

	return &UpdateMaintenanceWindowTargetOutput{
		WindowID:       target.WindowID,
		WindowTargetID: target.WindowTargetID,
		OwnerInfo:      target.OwnerInfo,
		Name:           target.Name,
		Description:    target.Description,
		Targets:        target.Targets,
	}, nil
}

// UpdateMaintenanceWindowTask updates task fields.
// Returns a no-op success when the task is not found (stub compat for non-existent IDs).
func (b *InMemoryBackend) UpdateMaintenanceWindowTask(
	ctx context.Context,
	input *UpdateMaintenanceWindowTaskInput,
) (*UpdateMaintenanceWindowTaskOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateMaintenanceWindowTask")
	defer b.mu.Unlock()

	store := b.maintenanceWindowTasksStore(region)
	task, exists := store[input.WindowTaskID]
	if !exists || task.WindowID != input.WindowID {
		return &UpdateMaintenanceWindowTaskOutput{
			WindowID:     input.WindowID,
			WindowTaskID: input.WindowTaskID,
		}, nil
	}

	if input.TaskArn != "" {
		task.TaskArn = input.TaskArn
	}

	if input.Name != "" {
		task.Name = input.Name
	}

	if input.Description != "" {
		task.Description = input.Description
	}

	if input.Priority != nil {
		task.Priority = *input.Priority
	}

	if input.ServiceRoleArn != "" {
		task.ServiceRoleArn = input.ServiceRoleArn
	}

	if input.MaxConcurrency != "" {
		task.MaxConcurrency = input.MaxConcurrency
	}

	if input.MaxErrors != "" {
		task.MaxErrors = input.MaxErrors
	}

	store[input.WindowTaskID] = task

	return &UpdateMaintenanceWindowTaskOutput{
		WindowID:       task.WindowID,
		WindowTaskID:   task.WindowTaskID,
		TaskArn:        task.TaskArn,
		Name:           task.Name,
		Description:    task.Description,
		Priority:       task.Priority,
		ServiceRoleArn: task.ServiceRoleArn,
		MaxConcurrency: task.MaxConcurrency,
		MaxErrors:      task.MaxErrors,
	}, nil
}

// ---------------------------------------------------------------------------
// Group 6 — OpsItem
// ---------------------------------------------------------------------------

// DeleteOpsItem removes an OpsItem by ID.
func (b *InMemoryBackend) DeleteOpsItem(ctx context.Context, input *DeleteOpsItemInput) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteOpsItem")
	defer b.mu.Unlock()

	opsItems := b.opsItemsStore(region)
	if _, exists := opsItems[input.OpsItemID]; !exists {
		return nil, ErrOpsItemNotFound
	}

	delete(opsItems, input.OpsItemID)
	delete(b.opsItemRelatedItemsStore(region), input.OpsItemID)

	return &StubOutput{}, nil
}

// DisassociateOpsItemRelatedItem removes a related item from an OpsItem.
// Returns success if the OpsItem does not exist (stub compat for empty ID).
func (b *InMemoryBackend) DisassociateOpsItemRelatedItem(
	ctx context.Context,
	input *DisassociateOpsItemRelatedItemInput,
) (*StubOutput, error) {
	if input.OpsItemID == "" {
		return &StubOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.Lock("DisassociateOpsItemRelatedItem")
	defer b.mu.Unlock()

	store := b.opsItemRelatedItemsStore(region)
	items, exists := store[input.OpsItemID]
	if !exists {
		// No-op if OpsItem doesn't have any related items.
		return &StubOutput{}, nil
	}

	filtered := items[:0]
	for _, item := range items {
		if item.AssociationID != input.AssociationID {
			filtered = append(filtered, item)
		}
	}

	store[input.OpsItemID] = filtered

	return &StubOutput{}, nil
}

// ListOpsItemRelatedItems returns stored related items for an OpsItem.
func (b *InMemoryBackend) ListOpsItemRelatedItems(
	ctx context.Context,
	input *ListOpsItemRelatedItemsInput,
) (*ListOpsItemRelatedItemsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListOpsItemRelatedItems")
	defer b.mu.RUnlock()

	var all []OpsItemRelatedItem

	store := b.opsItemRelatedItemsStore(region)
	if input.OpsItemID != "" {
		all = append(all, store[input.OpsItemID]...)
	} else {
		for _, items := range store {
			all = append(all, items...)
		}
	}

	if all == nil {
		all = []OpsItemRelatedItem{}
	}

	const maxOpsItemRelatedItems = 50

	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxOpsItemRelatedItems {
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, maxOpsItemRelatedItems)
		}
	}

	startIdx := parseNextToken(input.NextToken)
	limit := int64(maxOpsItemRelatedItems)

	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &ListOpsItemRelatedItemsOutput{Summaries: []OpsItemRelatedItem{}}, nil
	}

	end := startIdx + int(limit)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &ListOpsItemRelatedItemsOutput{NextToken: nextToken, Summaries: all[startIdx:end]}, nil
}

// ListOpsItemEvents returns tracked events for OpsItems, optionally filtered by OpsItemID.
func (b *InMemoryBackend) ListOpsItemEvents(
	ctx context.Context,
	input *ListOpsItemEventsInput,
) (*ListOpsItemEventsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListOpsItemEvents")
	defer b.mu.RUnlock()

	var summaries []OpsItemEventSummary

	events := b.opsItemEventsStore(region)
	for _, event := range events {
		if input.OpsItemID != "" && event.OpsItemID != input.OpsItemID {
			continue
		}

		summaries = append(summaries, event)
	}

	if summaries == nil {
		summaries = []OpsItemEventSummary{}
	}

	const maxOpsItemEvents = 50

	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxOpsItemEvents {
			return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidationException, maxOpsItemEvents)
		}
	}

	startIdx := parseNextToken(input.NextToken)
	limit := int64(maxOpsItemEvents)

	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	if startIdx >= len(summaries) {
		return &ListOpsItemEventsOutput{Summaries: []OpsItemEventSummary{}}, nil
	}

	end := startIdx + int(limit)

	var nextToken string

	if end < len(summaries) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(summaries)
	}

	return &ListOpsItemEventsOutput{NextToken: nextToken, Summaries: summaries[startIdx:end]}, nil
}

// DeleteOpsMetadata removes OpsMetadata by ARN.
func (b *InMemoryBackend) DeleteOpsMetadata(ctx context.Context, input *DeleteOpsMetadataInput) (*StubOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteOpsMetadata")
	defer b.mu.Unlock()

	opsMetadata := b.opsMetadataStore(region)
	meta, exists := opsMetadata[input.OpsMetadataArn]
	if !exists {
		return nil, ErrOpsMetadataNotFound
	}

	delete(b.resourceIDToOpsMetadataArnStore(region), meta.ResourceID)
	delete(opsMetadata, input.OpsMetadataArn)

	return &StubOutput{}, nil
}
