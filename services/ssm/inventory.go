package ssm

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) inventoryStore(region string) map[string][]InventoryItem {
	return b.inventory[region]
}
func (b *InMemoryBackend) inventoryDeletionsStore(region string) []InventoryDeletion {
	return b.inventoryDeletions[region]
}
func (b *InMemoryBackend) complianceStore(region string) map[string][]ComplianceItem {
	return b.compliance[region]
}

const (
	inventorySchemaV10           = "1.0"
	inventorySchemaV11           = "1.1"
	complianceStatusCompliant    = "COMPLIANT"
	complianceStatusNonCompliant = "NON_COMPLIANT"
)

// PutInventory stores inventory items for an instance.
// It fails if InstanceId is empty AND Items are provided.
func (b *InMemoryBackend) PutInventory(
	ctx context.Context,
	input *PutInventoryInput,
) (*PutInventoryOutput, error) {
	if input.InstanceID == "" && len(input.Items) > 0 {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrValidationException)
	}

	if input.InstanceID == "" {
		return &PutInventoryOutput{}, nil
	}

	region := getRegion(ctx)
	b.mu.Lock("PutInventory")
	defer b.mu.Unlock()

	if b.inventory[region] == nil {
		b.inventory[region] = make(map[string][]InventoryItem)
	}
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

	return &PutInventoryOutput{}, nil
}

// GetInventory returns stored inventory entities across all instances.
func (b *InMemoryBackend) GetInventory(
	ctx context.Context,
	input *GetInventoryInput,
) (*GetInventoryOutput, error) {
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
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				defaultMaxResults,
			)
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
	const maxInventoryEntries = 50

	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxInventoryEntries {
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				maxInventoryEntries,
			)
		}
	}

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

	var captureTime, schemaVersion string

	for _, item := range items {
		if item.TypeName == input.TypeName {
			entries = append(entries, item.Content...)
			captureTime = item.CaptureTime
			schemaVersion = item.SchemaVersion

			break
		}
	}

	if entries == nil {
		entries = []map[string]string{}
	}

	startIdx := parseNextToken(input.NextToken)
	limit := int64(maxInventoryEntries)

	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	if startIdx >= len(entries) {
		return &ListInventoryEntriesOutput{
			InstanceID:    input.InstanceID,
			TypeName:      input.TypeName,
			CaptureTime:   captureTime,
			SchemaVersion: schemaVersion,
			Entries:       []map[string]string{},
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
		InstanceID:    input.InstanceID,
		TypeName:      input.TypeName,
		NextToken:     nextToken,
		CaptureTime:   captureTime,
		SchemaVersion: schemaVersion,
		Entries:       entries[startIdx:end],
	}, nil
}

// DeleteInventory removes all inventory for the given TypeName across all
// instances.
//
// DryRun (real member, api_op_DeleteInventory.go: "view a summary of the
// deletion request without deleting any data") previously had no Go struct
// field at all, so a caller validating a delete before committing to it got
// a real, irreversible delete instead -- more permissive than AWS, which
// would delete nothing. Now honored: DryRun computes and returns the same
// summary without mutating the store or recording a deletion job.
func (b *InMemoryBackend) DeleteInventory(
	ctx context.Context,
	input *DeleteInventoryInput,
) (*DeleteInventoryOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteInventory")
	defer b.mu.Unlock()

	store := b.inventoryStore(region)

	removed := 0

	for instanceID, items := range store {
		for _, item := range items {
			if item.TypeName == input.TypeName {
				removed++
			}
		}

		if !input.DryRun {
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
	}

	summary := &InventoryDeletionSummary{
		TotalCount:     removed,
		RemainingCount: 0,
		SummaryItems:   []any{},
	}

	if input.DryRun {
		return &DeleteInventoryOutput{TypeName: input.TypeName, DeletionSummary: summary}, nil
	}

	cleanupEmptyInnerMap(b.inventory, region)

	// Record a real deletion job so DescribeInventoryDeletions can report it.
	deletionID := "deletion-" + uuid.NewString()
	deletion := InventoryDeletion{
		DeletionID:        deletionID,
		TypeName:          input.TypeName,
		LastStatus:        "Complete",
		LastStatusMessage: "The inventory deletion has completed.",
		DeletionStartTime: UnixTimeFloat(time.Now()),
		DeletionSummary:   summary,
	}
	b.inventoryDeletions[region] = append(b.inventoryDeletions[region], deletion)

	return &DeleteInventoryOutput{
		DeletionID:      deletionID,
		TypeName:        input.TypeName,
		DeletionSummary: deletion.DeletionSummary,
	}, nil
}

// DescribeInventoryDeletions returns the recorded DeleteInventory jobs,
// optionally filtered to a single DeletionId, backed by real stored state.
func (b *InMemoryBackend) DescribeInventoryDeletions(
	ctx context.Context,
	input *DescribeInventoryDeletionsInput,
) (*DescribeInventoryDeletionsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInventoryDeletions")
	defer b.mu.RUnlock()

	deletions := make([]any, 0, len(b.inventoryDeletionsStore(region)))
	for _, d := range b.inventoryDeletionsStore(region) {
		if input.DeletionID != "" && d.DeletionID != input.DeletionID {
			continue
		}

		deletions = append(deletions, d)
	}

	return &DescribeInventoryDeletionsOutput{
		InventoryDeletions: deletions,
	}, nil
}

// validatePutComplianceItemsInput enforces PutComplianceItems' required
// members (api_op_PutComplianceItems.go): ResourceId, ResourceType,
// ComplianceType, ExecutionSummary (with its own required ExecutionTime),
// and each item's Severity/Status (types.ComplianceItemEntry).
func validatePutComplianceItemsInput(input *PutComplianceItemsInput) error {
	if input.ResourceID == "" {
		return fmt.Errorf("%w: ResourceId is required", ErrValidationException)
	}

	if input.ResourceType == "" {
		return fmt.Errorf("%w: ResourceType is required", ErrValidationException)
	}

	if input.ComplianceType == "" {
		return fmt.Errorf("%w: ComplianceType is required", ErrValidationException)
	}

	if input.ExecutionSummary == nil {
		return fmt.Errorf("%w: ExecutionSummary is required", ErrValidationException)
	}

	if input.ExecutionSummary.ExecutionTime == 0 {
		return fmt.Errorf("%w: ExecutionSummary.ExecutionTime is required", ErrValidationException)
	}

	for _, item := range input.Items {
		if item.Severity == "" {
			return fmt.Errorf("%w: Items[].Severity is required", ErrValidationException)
		}

		if item.Status == "" {
			return fmt.Errorf("%w: Items[].Status is required", ErrValidationException)
		}
	}

	return nil
}

// PutComplianceItems stores compliance items for a resource.
//
// UploadType (COMPLETE/PARTIAL) is accepted but not evaluated: real AWS's
// PARTIAL mode only overwrites the items for one association (requiring
// SyncCompliance=MANUAL) while leaving other associations' compliance data
// for the same resource untouched. This backend always applies COMPLETE
// semantics (replaces every item for ResourceId), a real behavioral gap
// disclosed in PARITY.md rather than rushed -- it needs compliance storage
// reshaped to key by association, not just ResourceId.
func (b *InMemoryBackend) PutComplianceItems(
	ctx context.Context,
	input *PutComplianceItemsInput,
) (*PutComplianceItemsOutput, error) {
	if err := validatePutComplianceItemsInput(input); err != nil {
		return nil, err
	}

	region := getRegion(ctx)
	b.mu.Lock("PutComplianceItems")
	defer b.mu.Unlock()

	// Tag each item with ResourceId/ResourceType/ExecutionSummary from the
	// input envelope.
	newItems := make([]ComplianceItem, 0, len(input.Items))
	for _, item := range input.Items {
		ci := item
		ci.ResourceID = input.ResourceID
		ci.ResourceType = input.ResourceType
		ci.ExecutionSummary = input.ExecutionSummary

		if ci.ComplianceType == "" {
			ci.ComplianceType = input.ComplianceType
		}

		newItems = append(newItems, ci)
	}

	if b.compliance[region] == nil {
		b.compliance[region] = make(map[string][]ComplianceItem)
	}
	b.complianceStore(region)[input.ResourceID] = newItems

	return &PutComplianceItemsOutput{}, nil
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

	for _, items := range b.compliance[region] {
		for _, item := range items {
			if len(input.ResourceIDs) > 0 && !slices.Contains(input.ResourceIDs, item.ResourceID) {
				continue
			}

			if len(input.ResourceTypes) > 0 && !slices.Contains(input.ResourceTypes, item.ResourceType) {
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
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				maxComplianceItems,
			)
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

// buildComplianceTallies accumulates compliant/non-compliant item counts per ComplianceType.
func buildComplianceTallies(store map[string][]ComplianceItem) map[string]*complianceTally {
	tallies := make(map[string]*complianceTally)
	for _, items := range store {
		for _, item := range items {
			ct := item.ComplianceType
			if ct == "" {
				ct = "Custom"
			}
			if tallies[ct] == nil {
				tallies[ct] = &complianceTally{}
			}
			if item.Status == complianceStatusCompliant {
				tallies[ct].compliantCount++
			} else {
				tallies[ct].nonCompliantCount++
			}
		}
	}

	return tallies
}

// ListComplianceSummaries aggregates stored compliance items by ComplianceType.
func (b *InMemoryBackend) ListComplianceSummaries(
	ctx context.Context,
	input *ListComplianceSummariesInput,
) (*ListComplianceSummariesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListComplianceSummaries")
	defer b.mu.RUnlock()

	tallies := buildComplianceTallies(b.compliance[region])

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
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				maxComplianceSummaries,
			)
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

	return &ListComplianceSummariesOutput{
		NextToken:              nextToken,
		ComplianceSummaryItems: summaries[startIdx:end],
	}, nil
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

	store := b.compliance[region]
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
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				maxResourceComplianceSummaries,
			)
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

	return &ListResourceComplianceSummariesOutput{
		NextToken:                      nextToken,
		ResourceComplianceSummaryItems: summaries[startIdx:end],
	}, nil
}
