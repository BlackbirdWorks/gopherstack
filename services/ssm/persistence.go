package ssm

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Parameters                 map[string]map[string]Parameter               `json:"parameters"`
	History                    map[string]map[string][]ParameterHistory      `json:"history"`
	Tags                       map[string]map[string]*tags.Tags              `json:"tags"`
	Documents                  map[string]map[string]Document                `json:"documents"`
	DocumentVersions           map[string]map[string][]DocumentVersion       `json:"document_versions"`
	DocumentPermissions        map[string]map[string][]string                `json:"document_permissions"`
	Commands                   map[string]map[string]Command                 `json:"commands"`
	CommandInvocations         map[string]map[string][]CommandInvocation     `json:"command_invocations"`
	Activations                map[string]map[string]Activation              `json:"activations"`
	Associations               map[string]map[string]Association             `json:"associations"`
	MaintenanceWindows         map[string]map[string]MaintenanceWindow       `json:"maintenance_windows"`
	MaintenanceWindowTargets   map[string]map[string]MaintenanceWindowTarget `json:"maintenance_window_targets"`
	MaintenanceWindowTasks     map[string]map[string]MaintenanceWindowTask   `json:"maintenance_window_tasks"`
	Sessions                   map[string]map[string]Session                 `json:"sessions"`
	PatchGroupToBaseline       map[string]map[string]string                  `json:"patch_group_to_baseline"`
	OpsItems                   map[string]map[string]OpsItem                 `json:"ops_items"`
	OpsItemRelatedItems        map[string]map[string][]OpsItemRelatedItem    `json:"ops_item_related_items"`
	OpsMetadata                map[string]map[string]OpsMetadata             `json:"ops_metadata"`
	PatchBaselines             map[string]map[string]PatchBaseline           `json:"patch_baselines"`
	Inventory                  map[string]map[string][]InventoryItem         `json:"inventory"`
	Compliance                 map[string]map[string][]ComplianceItem        `json:"compliance"`
	ResourceDataSyncs          map[string]map[string]*ResourceDataSync       `json:"resource_data_syncs"`
	ParameterLabels            map[string]map[string]map[int64][]string      `json:"parameter_labels"`
	AutomationExecutions       map[string]map[string]*AutomationExecution    `json:"automation_executions"`
	ServiceSettings            map[string]map[string]*ServiceSetting         `json:"service_settings"`
	ResourcePolicies           map[string]map[string][]*ResourcePolicy       `json:"resource_policies"`
	ExecutionPreviews          map[string]map[string]*ExecutionPreview       `json:"execution_previews"`
	MiscResourceTags           map[string]map[string]map[string]string       `json:"misc_resource_tags"`
	ResourceIDToOpsMetadataArn map[string]map[string]string                  `json:"resource_id_to_ops_metadata_arn"`
	OpsItemEvents              map[string][]OpsItemEventSummary              `json:"ops_item_events"`
}

// initSnapshotDefaults initializes nil maps in the snapshot for core fields.
func initSnapshotDefaults(snap *backendSnapshot) {
	if snap.Parameters == nil {
		snap.Parameters = make(map[string]map[string]Parameter)
	}

	if snap.History == nil {
		snap.History = make(map[string]map[string][]ParameterHistory)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]*tags.Tags)
	}

	if snap.Documents == nil {
		snap.Documents = make(map[string]map[string]Document)
	}

	if snap.DocumentVersions == nil {
		snap.DocumentVersions = make(map[string]map[string][]DocumentVersion)
	}

	if snap.DocumentPermissions == nil {
		snap.DocumentPermissions = make(map[string]map[string][]string)
	}

	if snap.Commands == nil {
		snap.Commands = make(map[string]map[string]Command)
	}

	if snap.CommandInvocations == nil {
		snap.CommandInvocations = make(map[string]map[string][]CommandInvocation)
	}
}

// initSnapshotNewFields initializes nil maps for newer resource types.
func initSnapshotNewFields(snap *backendSnapshot) { //nolint:gocognit,cyclop // existing issue.
	if snap.Activations == nil {
		snap.Activations = make(map[string]map[string]Activation)
	}

	if snap.Associations == nil {
		snap.Associations = make(map[string]map[string]Association)
	}

	if snap.MaintenanceWindows == nil {
		snap.MaintenanceWindows = make(map[string]map[string]MaintenanceWindow)
	}

	if snap.MaintenanceWindowTargets == nil {
		snap.MaintenanceWindowTargets = make(map[string]map[string]MaintenanceWindowTarget)
	}

	if snap.MaintenanceWindowTasks == nil {
		snap.MaintenanceWindowTasks = make(map[string]map[string]MaintenanceWindowTask)
	}

	if snap.Sessions == nil {
		snap.Sessions = make(map[string]map[string]Session)
	}

	if snap.PatchGroupToBaseline == nil {
		snap.PatchGroupToBaseline = make(map[string]map[string]string)
	}

	if snap.OpsItems == nil {
		snap.OpsItems = make(map[string]map[string]OpsItem)
	}

	if snap.OpsItemRelatedItems == nil {
		snap.OpsItemRelatedItems = make(map[string]map[string][]OpsItemRelatedItem)
	}

	if snap.OpsMetadata == nil {
		snap.OpsMetadata = make(map[string]map[string]OpsMetadata)
	}

	if snap.PatchBaselines == nil {
		snap.PatchBaselines = make(map[string]map[string]PatchBaseline)
	}

	if snap.Inventory == nil {
		snap.Inventory = make(map[string]map[string][]InventoryItem)
	}

	if snap.Compliance == nil {
		snap.Compliance = make(map[string]map[string][]ComplianceItem)
	}

	if snap.ResourceDataSyncs == nil {
		snap.ResourceDataSyncs = make(map[string]map[string]*ResourceDataSync)
	}

	if snap.ParameterLabels == nil {
		snap.ParameterLabels = make(map[string]map[string]map[int64][]string)
	}

	if snap.AutomationExecutions == nil {
		snap.AutomationExecutions = make(map[string]map[string]*AutomationExecution)
	}

	if snap.ServiceSettings == nil {
		snap.ServiceSettings = make(map[string]map[string]*ServiceSetting)
	}

	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]map[string][]*ResourcePolicy)
	}

	if snap.ExecutionPreviews == nil {
		snap.ExecutionPreviews = make(map[string]map[string]*ExecutionPreview)
	}

	if snap.MiscResourceTags == nil {
		snap.MiscResourceTags = make(map[string]map[string]map[string]string)
	}

	if snap.ResourceIDToOpsMetadataArn == nil {
		snap.ResourceIDToOpsMetadataArn = make(map[string]map[string]string)
	}

	if snap.OpsItemEvents == nil {
		snap.OpsItemEvents = make(map[string][]OpsItemEventSummary)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Parameters:                 b.parameters,
		History:                    b.history,
		Tags:                       b.tags,
		Documents:                  b.documents,
		DocumentVersions:           b.documentVersions,
		DocumentPermissions:        b.documentPermissions,
		Commands:                   b.commands,
		CommandInvocations:         b.commandInvocations,
		Activations:                b.activations,
		Associations:               b.associations,
		MaintenanceWindows:         b.maintenanceWindows,
		MaintenanceWindowTargets:   b.maintenanceWindowTargets,
		MaintenanceWindowTasks:     b.maintenanceWindowTasks,
		Sessions:                   b.sessions,
		PatchGroupToBaseline:       b.patchGroupToBaseline,
		OpsItems:                   b.opsItems,
		OpsItemRelatedItems:        b.opsItemRelatedItems,
		OpsMetadata:                b.opsMetadata,
		PatchBaselines:             b.patchBaselines,
		Inventory:                  b.inventory,
		Compliance:                 b.compliance,
		ResourceDataSyncs:          b.resourceDataSyncs,
		ParameterLabels:            b.parameterLabels,
		AutomationExecutions:       b.automationExecutions,
		ServiceSettings:            b.serviceSettings,
		ResourcePolicies:           b.resourcePolicies,
		ExecutionPreviews:          b.executionPreviews,
		MiscResourceTags:           b.miscResourceTags,
		ResourceIDToOpsMetadataArn: b.resourceIDToOpsMetadataArn,
		OpsItemEvents:              b.opsItemEvents,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("ssm: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	initSnapshotDefaults(&snap)
	initSnapshotNewFields(&snap)

	for _, regionTags := range b.tags {
		for _, t := range regionTags {
			t.Close()
		}
	}

	b.parameters = snap.Parameters
	b.history = snap.History
	b.tags = snap.Tags
	b.documents = snap.Documents
	b.documentVersions = snap.DocumentVersions
	b.documentPermissions = snap.DocumentPermissions
	b.commands = snap.Commands
	b.commandInvocations = snap.CommandInvocations
	b.activations = snap.Activations
	b.associations = snap.Associations
	b.maintenanceWindows = snap.MaintenanceWindows
	b.maintenanceWindowTargets = snap.MaintenanceWindowTargets
	b.maintenanceWindowTasks = snap.MaintenanceWindowTasks
	b.sessions = snap.Sessions
	b.patchGroupToBaseline = snap.PatchGroupToBaseline
	b.opsItems = snap.OpsItems
	b.opsItemRelatedItems = snap.OpsItemRelatedItems
	b.opsMetadata = snap.OpsMetadata
	b.patchBaselines = snap.PatchBaselines
	b.inventory = snap.Inventory
	b.compliance = snap.Compliance
	b.resourceDataSyncs = snap.ResourceDataSyncs
	b.parameterLabels = snap.ParameterLabels
	b.automationExecutions = snap.AutomationExecutions
	b.serviceSettings = snap.ServiceSettings
	b.resourcePolicies = snap.ResourcePolicies
	b.executionPreviews = snap.ExecutionPreviews
	b.miscResourceTags = snap.MiscResourceTags
	b.resourceIDToOpsMetadataArn = snap.ResourceIDToOpsMetadataArn
	b.opsItemEvents = snap.OpsItemEvents

	// Re-seed built-in documents if they are absent from the snapshot
	// (e.g. snapshots taken before document support was added).
	for _, region := range []string{defaultRegion} {
		for _, name := range []string{"AWS-RunShellScript", "AWS-RunPowerShellScript"} {
			if b.documents[region] == nil || b.documents[region][name].Name == "" {
				b.registerDefaultDocuments(region)

				break
			}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
