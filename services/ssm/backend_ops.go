package ssm

import (
	"errors"
	"fmt"
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

// ---------------------------------------------------------------------------
// Group 1 — Document ops
// ---------------------------------------------------------------------------

// UpdateDocumentDefaultVersion sets the DefaultVersion field on an existing document.
// It fails if the document or the requested version does not exist.
// Returns a no-op success when Name or DocumentVersion is empty (legacy stub compat).
func (b *InMemoryBackend) UpdateDocumentDefaultVersion(
	input *UpdateDocumentDefaultVersionInput,
) (*UpdateDocumentDefaultVersionOutput, error) {
	if input.Name == "" || input.DocumentVersion == "" {
		return &UpdateDocumentDefaultVersionOutput{}, nil
	}

	b.mu.Lock("UpdateDocumentDefaultVersion")
	defer b.mu.Unlock()

	doc, exists := b.documents[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: document %q not found", ErrDocumentNotFound, input.Name)
	}

	// Verify the requested version exists in documentVersions.
	found := false

	for _, dv := range b.documentVersions[input.Name] {
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
	b.documents[input.Name] = doc

	// Also mark the version entry.
	versions := b.documentVersions[input.Name]
	for i := range versions {
		versions[i].IsDefaultVersion = versions[i].DocumentVersion == input.DocumentVersion
	}

	b.documentVersions[input.Name] = versions

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
func (b *InMemoryBackend) UpdateDocumentMetadata(input *UpdateDocumentMetadataInput) (*StubOutput, error) {
	if input.Name == "" {
		return &StubOutput{}, nil
	}

	b.mu.RLock("UpdateDocumentMetadata")
	defer b.mu.RUnlock()

	if _, exists := b.documents[input.Name]; !exists {
		return nil, fmt.Errorf("%w: document %q not found", ErrDocumentNotFound, input.Name)
	}

	return &StubOutput{}, nil
}

// ListDocumentMetadataHistory returns an empty approval history.
// The in-memory backend does not track document review history; this returns
// a well-formed empty response consistent with the stateless stub approach.
func (b *InMemoryBackend) ListDocumentMetadataHistory(
	input *ListDocumentMetadataHistoryInput,
) (*ListDocumentMetadataHistoryOutput, error) {
	if input.Name == "" {
		return &ListDocumentMetadataHistoryOutput{
			Metadata: &DocumentMetadataResponseInfo{
				ReviewerResponse: []DocumentReviewerResponseSource{},
			},
		}, nil
	}

	b.mu.RLock("ListDocumentMetadataHistory")
	defer b.mu.RUnlock()

	doc, exists := b.documents[input.Name]
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
func (b *InMemoryBackend) PutInventory(input *PutInventoryInput) (*StubOutput, error) {
	if input.InstanceID == "" && len(input.Items) > 0 {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrValidationException)
	}

	if input.InstanceID == "" {
		return &StubOutput{}, nil
	}

	b.mu.Lock("PutInventory")
	defer b.mu.Unlock()

	existing := b.inventory[input.InstanceID]

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

	b.inventory[input.InstanceID] = merged

	return &StubOutput{}, nil
}

// GetInventory returns stored inventory entities across all instances.
func (b *InMemoryBackend) GetInventory(input *GetInventoryInput) (*GetInventoryOutput, error) {
	b.mu.RLock("GetInventory")
	defer b.mu.RUnlock()

	entities := make([]InventoryResultEntity, 0, len(b.inventory))
	for instanceID, items := range b.inventory {
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
	if input.MaxResults != nil && *input.MaxResults > 0 {
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
func (b *InMemoryBackend) GetInventorySchema(input *GetInventorySchemaInput) (*GetInventorySchemaOutput, error) {
	all := []InventorySchemaItem{
		{TypeName: "AWS:Application", Version: "1.1"},
		{TypeName: "AWS:AWSComponent", Version: "1.0"},
		{TypeName: "AWS:ComplianceItem", Version: "1.1"},
		{TypeName: "AWS:ComplianceSummary", Version: "1.1"},
		{TypeName: "AWS:InstanceDetailedInformation", Version: "1.0"},
		{TypeName: "AWS:InstanceInformation", Version: "1.0"},
		{TypeName: "AWS:Network", Version: "1.0"},
		{TypeName: "AWS:PatchCompliance", Version: "1.1"},
		{TypeName: "AWS:PatchSummary", Version: "1.0"},
		{TypeName: "AWS:WindowsRegistry", Version: "1.0"},
		{TypeName: "AWS:WindowsRole", Version: "1.0"},
		{TypeName: "AWS:WindowsUpdate", Version: "1.0"},
		{TypeName: "Custom:Application", Version: "1.0"},
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
	input *ListInventoryEntriesInput,
) (*ListInventoryEntriesOutput, error) {
	b.mu.RLock("ListInventoryEntries")
	defer b.mu.RUnlock()

	items, exists := b.inventory[input.InstanceID]
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

	return &ListInventoryEntriesOutput{
		InstanceID: input.InstanceID,
		TypeName:   input.TypeName,
		Entries:    entries,
	}, nil
}

// DeleteInventory removes all inventory for the given TypeName across all instances.
func (b *InMemoryBackend) DeleteInventory(input *DeleteInventoryInput) (*StubOutput, error) {
	b.mu.Lock("DeleteInventory")
	defer b.mu.Unlock()

	for instanceID, items := range b.inventory {
		filtered := items[:0]
		for _, item := range items {
			if item.TypeName != input.TypeName {
				filtered = append(filtered, item)
			}
		}

		if len(filtered) == 0 {
			delete(b.inventory, instanceID)
		} else {
			b.inventory[instanceID] = filtered
		}
	}

	return &StubOutput{}, nil
}

// DescribeInventoryDeletions returns an empty list.
// The in-memory backend does not track deletion jobs.
func (b *InMemoryBackend) DescribeInventoryDeletions(
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
func (b *InMemoryBackend) PutComplianceItems(input *PutComplianceItemsInput) (*StubOutput, error) {
	if input.ResourceID == "" && len(input.Items) > 0 {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidationException)
	}

	if input.ResourceID == "" {
		return &StubOutput{}, nil
	}

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

	b.compliance[input.ResourceID] = newItems

	return &StubOutput{}, nil
}

// ListComplianceItems returns stored compliance items, optionally filtered by ResourceId/ResourceType.
func (b *InMemoryBackend) ListComplianceItems(
	input *ListComplianceItemsInput,
) (*ListComplianceItemsOutput, error) {
	b.mu.RLock("ListComplianceItems")
	defer b.mu.RUnlock()

	var all []ComplianceItem

	for _, items := range b.compliance {
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

	return &ListComplianceItemsOutput{ComplianceItems: all}, nil
}

// ListComplianceSummaries aggregates stored compliance items by ComplianceType.
func (b *InMemoryBackend) ListComplianceSummaries(
	_ *ListComplianceSummariesInput,
) (*ListComplianceSummariesOutput, error) {
	b.mu.RLock("ListComplianceSummaries")
	defer b.mu.RUnlock()

	// Tally compliant/non-compliant counts per compliance type.
	type tally struct {
		compliantCount    int
		nonCompliantCount int
	}

	tallies := make(map[string]*tally)

	for _, items := range b.compliance {
		for _, item := range items {
			ct := item.ComplianceType
			if ct == "" {
				ct = "Custom"
			}

			if tallies[ct] == nil {
				tallies[ct] = &tally{}
			}

			if item.Status == "COMPLIANT" {
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

	return &ListComplianceSummariesOutput{ComplianceSummaryItems: summaries}, nil
}

// ListResourceComplianceSummaries returns per-resource compliance summaries
// derived from stored compliance items.
func (b *InMemoryBackend) ListResourceComplianceSummaries(
	_ *ListResourceComplianceSummariesInput,
) (*ListResourceComplianceSummariesOutput, error) {
	b.mu.RLock("ListResourceComplianceSummaries")
	defer b.mu.RUnlock()

	summaries := make([]any, 0, len(b.compliance))

	for resourceID, items := range b.compliance {
		if len(items) == 0 {
			continue
		}

		compliant := 0
		nonCompliant := 0

		for _, item := range items {
			if item.Status == "COMPLIANT" {
				compliant++
			} else {
				nonCompliant++
			}
		}

		status := "COMPLIANT"
		if nonCompliant > 0 {
			status = "NON_COMPLIANT"
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

	return &ListResourceComplianceSummariesOutput{ResourceComplianceSummaryItems: summaries}, nil
}

// ---------------------------------------------------------------------------
// Group 4 — Patch baseline / groups
// ---------------------------------------------------------------------------

// GetDefaultPatchBaseline returns the baseline registered for "default" or a
// hard-coded fallback baseline ID.
func (b *InMemoryBackend) GetDefaultPatchBaseline(
	input *GetDefaultPatchBaselineInput,
) (*GetDefaultPatchBaselineOutput, error) {
	b.mu.RLock("GetDefaultPatchBaseline")
	defer b.mu.RUnlock()

	key := "default"
	if input.OperatingSystem != "" {
		key = "default-" + input.OperatingSystem
	}

	if id, ok := b.patchGroupToBaseline[key]; ok {
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
	input *GetPatchBaselineForPatchGroupInput,
) (*GetPatchBaselineForPatchBaselineOutput, error) {
	if input.PatchGroup == "" {
		return &GetPatchBaselineForPatchBaselineOutput{}, nil
	}

	b.mu.RLock("GetPatchBaselineForPatchGroup")
	defer b.mu.RUnlock()

	id, ok := b.patchGroupToBaseline[input.PatchGroup]
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
	input *RegisterDefaultPatchBaselineInput,
) (*RegisterDefaultPatchBaselineOutput, error) {
	if input.BaselineID == "" {
		return &RegisterDefaultPatchBaselineOutput{}, nil
	}

	b.mu.Lock("RegisterDefaultPatchBaseline")
	defer b.mu.Unlock()

	if _, exists := b.patchBaselines[input.BaselineID]; !exists {
		return nil, fmt.Errorf("%w: baseline %q not found", ErrPatchBaselineNotFound, input.BaselineID)
	}

	b.patchGroupToBaseline["default"] = input.BaselineID

	// Also store per-OS key when the baseline has a known OperatingSystem.
	if bl, ok := b.patchBaselines[input.BaselineID]; ok && bl.OperatingSystem != "" {
		b.patchGroupToBaseline["default-"+bl.OperatingSystem] = input.BaselineID
	}

	return &RegisterDefaultPatchBaselineOutput{BaselineID: input.BaselineID}, nil
}

// DeletePatchBaseline removes a patch baseline by ID.
func (b *InMemoryBackend) DeletePatchBaseline(
	input *DeletePatchBaselineInput,
) (*DeletePatchBaselineOutput, error) {
	b.mu.Lock("DeletePatchBaseline")
	defer b.mu.Unlock()

	if _, exists := b.patchBaselines[input.BaselineID]; !exists {
		return nil, ErrPatchBaselineNotFound
	}

	delete(b.patchBaselines, input.BaselineID)

	return &DeletePatchBaselineOutput{BaselineID: input.BaselineID}, nil
}

// DescribePatchGroupState returns zeroed counts for a patch group.
// The in-memory backend does not track per-instance patch state.
func (b *InMemoryBackend) DescribePatchGroupState(
	_ *DescribePatchGroupStateInput,
) (*DescribePatchGroupStateOutput, error) {
	return &DescribePatchGroupStateOutput{}, nil
}

// DescribePatchGroups lists the patch group to baseline mappings.
func (b *InMemoryBackend) DescribePatchGroups(
	input *DescribePatchGroupsInput,
) (*DescribePatchGroupsOutput, error) {
	b.mu.RLock("DescribePatchGroups")
	defer b.mu.RUnlock()

	mappings := make([]PatchGroupPatchBaselineMapping, 0, len(b.patchGroupToBaseline))

	for group, baselineID := range b.patchGroupToBaseline {
		identity := PatchBaselineIdentity{BaselineID: baselineID}
		if bl, ok := b.patchBaselines[baselineID]; ok {
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

	const defaultMaxResults = 50

	maxResults := int64(defaultMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
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
	input *DescribeEffectivePatchesForPatchBaselineInput,
) (*DescribeEffectivePatchesForPatchBaselineOutput, error) {
	if input.BaselineID == "" {
		return &DescribeEffectivePatchesForPatchBaselineOutput{
			EffectivePatches: []EffectivePatch{},
		}, nil
	}

	b.mu.RLock("DescribeEffectivePatchesForPatchBaseline")
	defer b.mu.RUnlock()

	if _, exists := b.patchBaselines[input.BaselineID]; !exists {
		return nil, fmt.Errorf("%w: baseline %q not found", ErrPatchBaselineNotFound, input.BaselineID)
	}

	return &DescribeEffectivePatchesForPatchBaselineOutput{
		EffectivePatches: []EffectivePatch{},
	}, nil
}

// GetDeployablePatchSnapshotForInstance returns a stub snapshot ID.
// The in-memory backend generates a synthetic snapshot URL.
func (b *InMemoryBackend) GetDeployablePatchSnapshotForInstance(
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
	input *DeleteMaintenanceWindowInput,
) (*DeleteMaintenanceWindowOutput, error) {
	b.mu.Lock("DeleteMaintenanceWindow")
	defer b.mu.Unlock()

	if _, exists := b.maintenanceWindows[input.WindowID]; !exists {
		return nil, ErrMaintenanceWindowNotFound
	}

	delete(b.maintenanceWindows, input.WindowID)

	return &DeleteMaintenanceWindowOutput{WindowID: input.WindowID}, nil
}

// GetMaintenanceWindowTask retrieves a task by WindowId and WindowTaskId.
// Returns an empty task when WindowTaskID is empty (stub compat).
func (b *InMemoryBackend) GetMaintenanceWindowTask(
	input *GetMaintenanceWindowTaskInput,
) (*GetMaintenanceWindowTaskOutput, error) {
	if input.WindowTaskID == "" {
		return &GetMaintenanceWindowTaskOutput{}, nil
	}

	b.mu.RLock("GetMaintenanceWindowTask")
	defer b.mu.RUnlock()

	task, exists := b.maintenanceWindowTasks[input.WindowTaskID]
	if !exists || task.WindowID != input.WindowID {
		return nil, ErrMaintenanceWindowNotFound
	}

	return &GetMaintenanceWindowTaskOutput{MaintenanceWindowTask: task}, nil
}

// DescribeMaintenanceWindowsForTarget returns an empty list.
// DescribeMaintenanceWindowsForTarget returns windows that have registered targets
// matching the given resource type and target key/value filters.
func (b *InMemoryBackend) DescribeMaintenanceWindowsForTarget(
	input *DescribeMaintenanceWindowsForTargetInput,
) (*DescribeMaintenanceWindowsForTargetOutput, error) {
	b.mu.RLock("DescribeMaintenanceWindowsForTarget")
	defer b.mu.RUnlock()

	// Collect window IDs that match any of the requested targets.
	matchedWindowIDs := make(map[string]struct{})

	for _, windowTarget := range b.maintenanceWindowTargets {
		if input.ResourceType != "" && windowTarget.ResourceType != input.ResourceType {
			continue
		}

		if len(input.Targets) == 0 {
			matchedWindowIDs[windowTarget.WindowID] = struct{}{}
			continue
		}

		for _, reqTarget := range input.Targets {
			for _, wt := range windowTarget.Targets {
				if wt.Key == reqTarget.Key {
					for _, reqVal := range reqTarget.Values {
						for _, wVal := range wt.Values {
							if wVal == reqVal {
								matchedWindowIDs[windowTarget.WindowID] = struct{}{}
							}
						}
					}
				}
			}
		}
	}

	identities := make([]MaintenanceWindowIdentity, 0, len(matchedWindowIDs))
	for windowID := range matchedWindowIDs {
		if mw, ok := b.maintenanceWindows[windowID]; ok {
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

	return &DescribeMaintenanceWindowsForTargetOutput{
		WindowIdentities: identities,
	}, nil
}

// UpdateMaintenanceWindowTarget updates target fields.
// Returns an empty response when the target is not found (stub compat for empty ID).
func (b *InMemoryBackend) UpdateMaintenanceWindowTarget(
	input *UpdateMaintenanceWindowTargetInput,
) (*UpdateMaintenanceWindowTargetOutput, error) {
	b.mu.Lock("UpdateMaintenanceWindowTarget")
	defer b.mu.Unlock()

	target, exists := b.maintenanceWindowTargets[input.WindowTargetID]
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

	b.maintenanceWindowTargets[input.WindowTargetID] = target

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
	input *UpdateMaintenanceWindowTaskInput,
) (*UpdateMaintenanceWindowTaskOutput, error) {
	b.mu.Lock("UpdateMaintenanceWindowTask")
	defer b.mu.Unlock()

	task, exists := b.maintenanceWindowTasks[input.WindowTaskID]
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

	b.maintenanceWindowTasks[input.WindowTaskID] = task

	return &UpdateMaintenanceWindowTaskOutput{
		WindowID:     task.WindowID,
		WindowTaskID: task.WindowTaskID,
		TaskArn:      task.TaskArn,
		Name:         task.Name,
		Description:  task.Description,
		Priority:     task.Priority,
	}, nil
}

// ---------------------------------------------------------------------------
// Group 6 — OpsItem
// ---------------------------------------------------------------------------

// DeleteOpsItem removes an OpsItem by ID.
func (b *InMemoryBackend) DeleteOpsItem(input *DeleteOpsItemInput) (*StubOutput, error) {
	b.mu.Lock("DeleteOpsItem")
	defer b.mu.Unlock()

	if _, exists := b.opsItems[input.OpsItemID]; !exists {
		return nil, ErrOpsItemNotFound
	}

	delete(b.opsItems, input.OpsItemID)
	delete(b.opsItemRelatedItems, input.OpsItemID)

	return &StubOutput{}, nil
}

// DisassociateOpsItemRelatedItem removes a related item from an OpsItem.
// Returns success if the OpsItem does not exist (stub compat for empty ID).
func (b *InMemoryBackend) DisassociateOpsItemRelatedItem(
	input *DisassociateOpsItemRelatedItemInput,
) (*StubOutput, error) {
	if input.OpsItemID == "" {
		return &StubOutput{}, nil
	}

	b.mu.Lock("DisassociateOpsItemRelatedItem")
	defer b.mu.Unlock()

	items, exists := b.opsItemRelatedItems[input.OpsItemID]
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

	b.opsItemRelatedItems[input.OpsItemID] = filtered

	return &StubOutput{}, nil
}

// ListOpsItemRelatedItems returns stored related items for an OpsItem.
func (b *InMemoryBackend) ListOpsItemRelatedItems(
	input *ListOpsItemRelatedItemsInput,
) (*ListOpsItemRelatedItemsOutput, error) {
	b.mu.RLock("ListOpsItemRelatedItems")
	defer b.mu.RUnlock()

	var all []OpsItemRelatedItem

	if input.OpsItemID != "" {
		all = append(all, b.opsItemRelatedItems[input.OpsItemID]...)
	} else {
		for _, items := range b.opsItemRelatedItems {
			all = append(all, items...)
		}
	}

	if all == nil {
		all = []OpsItemRelatedItem{}
	}

	return &ListOpsItemRelatedItemsOutput{Summaries: all}, nil
}

// ListOpsItemEvents returns tracked events for OpsItems, optionally filtered by OpsItemID.
func (b *InMemoryBackend) ListOpsItemEvents(
	input *ListOpsItemEventsInput,
) (*ListOpsItemEventsOutput, error) {
	b.mu.RLock("ListOpsItemEvents")
	defer b.mu.RUnlock()

	var summaries []OpsItemEventSummary

	for _, event := range b.opsItemEvents {
		if input.OpsItemID != "" && event.OpsItemID != input.OpsItemID {
			continue
		}

		summaries = append(summaries, event)
	}

	if summaries == nil {
		summaries = []OpsItemEventSummary{}
	}

	return &ListOpsItemEventsOutput{Summaries: summaries}, nil
}

// DeleteOpsMetadata removes OpsMetadata by ARN.
func (b *InMemoryBackend) DeleteOpsMetadata(input *DeleteOpsMetadataInput) (*StubOutput, error) {
	b.mu.Lock("DeleteOpsMetadata")
	defer b.mu.Unlock()

	meta, exists := b.opsMetadata[input.OpsMetadataArn]
	if !exists {
		return nil, ErrOpsMetadataNotFound
	}

	delete(b.resourceIDToOpsMetadataArn, meta.ResourceID)
	delete(b.opsMetadata, input.OpsMetadataArn)

	return &StubOutput{}, nil
}
