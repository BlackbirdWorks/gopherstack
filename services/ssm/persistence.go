package ssm

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Parameters          map[string]Parameter           `json:"parameters"`
	History             map[string][]ParameterHistory  `json:"history"`
	Tags                map[string]*tags.Tags          `json:"tags"`
	Documents           map[string]Document            `json:"documents"`
	DocumentVersions    map[string][]DocumentVersion   `json:"document_versions"`
	DocumentPermissions map[string][]string            `json:"document_permissions"`
	Commands            map[string]Command             `json:"commands"`
	CommandInvocations  map[string][]CommandInvocation `json:"command_invocations"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Parameters:          b.parameters,
		History:             b.history,
		Tags:                b.tags,
		Documents:           b.documents,
		DocumentVersions:    b.documentVersions,
		DocumentPermissions: b.documentPermissions,
		Commands:            b.commands,
		CommandInvocations:  b.commandInvocations,
	}

	data, err := json.Marshal(snap)
	if err != nil {
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

	b.parameters = snap.Parameters
	b.history = snap.History
	b.tags = snap.Tags
	b.documents = snap.Documents
	b.documentVersions = snap.DocumentVersions
	b.documentPermissions = snap.DocumentPermissions
	b.commands = snap.Commands
	b.commandInvocations = snap.CommandInvocations

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
