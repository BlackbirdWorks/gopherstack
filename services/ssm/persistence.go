package ssm

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Parameters                 map[string]Parameter            `json:"parameters"`
	History                    map[string][]ParameterHistory   `json:"history"`
	Tags                       map[string]*tags.Tags           `json:"tags"`
	Documents                  map[string]Document             `json:"documents"`
	DocumentVersions           map[string][]DocumentVersion    `json:"document_versions"`
	DocumentPermissions        map[string][]string             `json:"document_permissions"`
	Commands                   map[string]Command              `json:"commands"`
	CommandInvocations         map[string][]CommandInvocation  `json:"command_invocations"`
	Activations                map[string]Activation           `json:"activations"`
	Associations               map[string]Association          `json:"associations"`
	MaintenanceWindows         map[string]MaintenanceWindow    `json:"maintenance_windows"`
	OpsItems                   map[string]OpsItem              `json:"ops_items"`
	OpsItemRelatedItems        map[string][]OpsItemRelatedItem `json:"ops_item_related_items"`
	OpsMetadata                map[string]OpsMetadata          `json:"ops_metadata"`
	PatchBaselines             map[string]PatchBaseline        `json:"patch_baselines"`
	ResourceIDToOpsMetadataArn map[string]string               `json:"resource_id_to_ops_metadata_arn"`
	MiscResourceTags           map[string]map[string]string    `json:"misc_resource_tags"`
}

// initSnapshotDefaults initializes nil maps in the snapshot for core fields.
func initSnapshotDefaults(snap *backendSnapshot) {
	if snap.Parameters == nil {
		snap.Parameters = make(map[string]Parameter)
	}

	if snap.History == nil {
		snap.History = make(map[string][]ParameterHistory)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]*tags.Tags)
	}

	if snap.Documents == nil {
		snap.Documents = make(map[string]Document)
	}

	if snap.DocumentVersions == nil {
		snap.DocumentVersions = make(map[string][]DocumentVersion)
	}

	if snap.DocumentPermissions == nil {
		snap.DocumentPermissions = make(map[string][]string)
	}

	if snap.Commands == nil {
		snap.Commands = make(map[string]Command)
	}

	if snap.CommandInvocations == nil {
		snap.CommandInvocations = make(map[string][]CommandInvocation)
	}
}

// initSnapshotNewFields initializes nil maps for newer resource types.
func initSnapshotNewFields(snap *backendSnapshot) {
	if snap.Activations == nil {
		snap.Activations = make(map[string]Activation)
	}

	if snap.Associations == nil {
		snap.Associations = make(map[string]Association)
	}

	if snap.MaintenanceWindows == nil {
		snap.MaintenanceWindows = make(map[string]MaintenanceWindow)
	}

	if snap.OpsItems == nil {
		snap.OpsItems = make(map[string]OpsItem)
	}

	if snap.OpsItemRelatedItems == nil {
		snap.OpsItemRelatedItems = make(map[string][]OpsItemRelatedItem)
	}

	if snap.OpsMetadata == nil {
		snap.OpsMetadata = make(map[string]OpsMetadata)
	}

	if snap.PatchBaselines == nil {
		snap.PatchBaselines = make(map[string]PatchBaseline)
	}

	if snap.ResourceIDToOpsMetadataArn == nil {
		snap.ResourceIDToOpsMetadataArn = make(map[string]string)
	}

	if snap.MiscResourceTags == nil {
		snap.MiscResourceTags = make(map[string]map[string]string)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
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
		OpsItems:                   b.opsItems,
		OpsItemRelatedItems:        b.opsItemRelatedItems,
		OpsMetadata:                b.opsMetadata,
		PatchBaselines:             b.patchBaselines,
		ResourceIDToOpsMetadataArn: b.resourceIDToOpsMetadataArn,
		MiscResourceTags:           b.miscResourceTags,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("ssm: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	initSnapshotDefaults(&snap)
	initSnapshotNewFields(&snap)

	b.parameters = snap.Parameters
	b.history = snap.History

	for _, t := range b.tags {
		t.Close()
	}

	b.tags = snap.Tags
	b.documents = snap.Documents
	b.documentVersions = snap.DocumentVersions
	b.documentPermissions = snap.DocumentPermissions
	b.commands = snap.Commands
	b.commandInvocations = snap.CommandInvocations
	b.activations = snap.Activations
	b.associations = snap.Associations
	b.maintenanceWindows = snap.MaintenanceWindows
	b.opsItems = snap.OpsItems
	b.opsItemRelatedItems = snap.OpsItemRelatedItems
	b.opsMetadata = snap.OpsMetadata
	b.patchBaselines = snap.PatchBaselines
	b.resourceIDToOpsMetadataArn = snap.ResourceIDToOpsMetadataArn
	b.miscResourceTags = snap.MiscResourceTags

	// Re-seed built-in documents if they are absent from the snapshot
	// (e.g. snapshots taken before document support was added).
	for _, name := range []string{"AWS-RunShellScript", "AWS-RunPowerShellScript"} {
		if _, exists := b.documents[name]; !exists {
			b.registerDefaultDocuments()

			break
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
