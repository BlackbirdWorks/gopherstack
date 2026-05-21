package ssm

import "time"

// Exported wrappers for internal state used in tests.

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultSSMJanitorInterval

// DefaultCommandExpirySecs exposes the package default command expiry seconds for testing.
const DefaultCommandExpirySecs = defaultCommandExpirySecs

// MaxHistoryCap exposes the history cap constant for test assertions.
const MaxHistoryCap = maxHistoryCap

// MaxDocumentVersionCap exposes the document version cap constant for test assertions.
const MaxDocumentVersionCap = maxDocumentVersionCap

// HistoryLen returns the number of history entries stored for the given parameter name.
func (b *InMemoryBackend) HistoryLen(name string) int {
	b.mu.RLock("HistoryLen")
	defer b.mu.RUnlock()

	return len(b.history[name])
}

// CommandCount returns the number of commands currently stored.
func (b *InMemoryBackend) CommandCount() int {
	b.mu.RLock("CommandCount")
	defer b.mu.RUnlock()

	return len(b.commands)
}

// CommandInvocationCount returns the number of command invocation sets stored.
func (b *InMemoryBackend) CommandInvocationCount() int {
	b.mu.RLock("CommandInvocationCount")
	defer b.mu.RUnlock()

	return len(b.commandInvocations)
}

// SetCommandExpiresAfter overrides the ExpiresAfter timestamp of the given command.
// Used in tests to force a command into an expired state.
func (b *InMemoryBackend) SetCommandExpiresAfter(cmdID string, expiresAfter float64) {
	b.mu.Lock("SetCommandExpiresAfter")
	defer b.mu.Unlock()

	if cmd, ok := b.commands[cmdID]; ok {
		cmd.ExpiresAfter = expiresAfter
		b.commands[cmdID] = cmd
	}
}

// HasTagEntry reports whether the tags map contains an entry for the given parameter name.
// Returns false when the parameter's tag entry has been cleaned up (nil or absent).
func (b *InMemoryBackend) HasTagEntry(name string) bool {
	b.mu.RLock("HasTagEntry")
	defer b.mu.RUnlock()

	return b.tags[name] != nil
}

// DocumentVersionCount returns the number of versions stored for the given document.
func (b *InMemoryBackend) DocumentVersionCount(name string) int {
	b.mu.RLock("DocumentVersionCount")
	defer b.mu.RUnlock()

	return len(b.documentVersions[name])
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// GetCommandExpirySecs returns the commandExpirySecs configured on the backend.
// Used in tests to verify WithCommandTTL correctly propagates the TTL.
func (b *InMemoryBackend) GetCommandExpirySecs() float64 {
	b.mu.RLock("GetCommandExpirySecs")
	defer b.mu.RUnlock()

	return b.commandExpirySecs
}

// ActivationCount returns the number of activations stored.
func (b *InMemoryBackend) ActivationCount() int {
	b.mu.RLock("ActivationCount")
	defer b.mu.RUnlock()

	return len(b.activations)
}

// AssociationCount returns the number of associations stored.
func (b *InMemoryBackend) AssociationCount() int {
	b.mu.RLock("AssociationCount")
	defer b.mu.RUnlock()

	return len(b.associations)
}

// MaintenanceWindowCount returns the number of maintenance windows stored.
func (b *InMemoryBackend) MaintenanceWindowCount() int {
	b.mu.RLock("MaintenanceWindowCount")
	defer b.mu.RUnlock()

	return len(b.maintenanceWindows)
}

// OpsItemCount returns the number of OpsItems stored.
func (b *InMemoryBackend) OpsItemCount() int {
	b.mu.RLock("OpsItemCount")
	defer b.mu.RUnlock()

	return len(b.opsItems)
}

// OpsMetadataCount returns the number of OpsMetadata entries stored.
func (b *InMemoryBackend) OpsMetadataCount() int {
	b.mu.RLock("OpsMetadataCount")
	defer b.mu.RUnlock()

	return len(b.opsMetadata)
}

// PatchBaselineCount returns the number of patch baselines stored.
func (b *InMemoryBackend) PatchBaselineCount() int {
	b.mu.RLock("PatchBaselineCount")
	defer b.mu.RUnlock()

	return len(b.patchBaselines)
}

// HandlerOpsLen returns the number of supported operations.
func (h *Handler) HandlerOpsLen() int {
	return len(h.GetSupportedOperations())
}

// AddActivationInternal seeds an activation directly into the backend for testing.
func (b *InMemoryBackend) AddActivationInternal(act Activation) {
	b.mu.Lock("AddActivationInternal")
	defer b.mu.Unlock()
	b.activations[act.ActivationID] = act
}

// AddAssociationInternal seeds an association directly into the backend for testing.
func (b *InMemoryBackend) AddAssociationInternal(assoc Association) {
	b.mu.Lock("AddAssociationInternal")
	defer b.mu.Unlock()
	b.associations[assoc.AssociationID] = assoc
}

// AddMaintenanceWindowInternal seeds a maintenance window directly into the backend for testing.
func (b *InMemoryBackend) AddMaintenanceWindowInternal(mw MaintenanceWindow) {
	b.mu.Lock("AddMaintenanceWindowInternal")
	defer b.mu.Unlock()
	b.maintenanceWindows[mw.WindowID] = mw
}

// AddOpsItemInternal seeds an OpsItem directly into the backend for testing.
func (b *InMemoryBackend) AddOpsItemInternal(item OpsItem) {
	b.mu.Lock("AddOpsItemInternal")
	defer b.mu.Unlock()
	b.opsItems[item.OpsItemID] = item
}

// AddOpsMetadataInternal seeds OpsMetadata directly into the backend for testing.
func (b *InMemoryBackend) AddOpsMetadataInternal(meta OpsMetadata) {
	b.mu.Lock("AddOpsMetadataInternal")
	defer b.mu.Unlock()
	b.opsMetadata[meta.OpsMetadataArn] = meta
}

// AddPatchBaselineInternal seeds a patch baseline directly into the backend for testing.
func (b *InMemoryBackend) AddPatchBaselineInternal(bl PatchBaseline) {
	b.mu.Lock("AddPatchBaselineInternal")
	defer b.mu.Unlock()
	b.patchBaselines[bl.BaselineID] = bl
}

// OpsItemRelatedItemCount returns the total number of related items across all OpsItems.
func (b *InMemoryBackend) OpsItemRelatedItemCount(opsItemID string) int {
	b.mu.RLock("OpsItemRelatedItemCount")
	defer b.mu.RUnlock()

	return len(b.opsItemRelatedItems[opsItemID])
}

// GetPatchBaselineInternal retrieves a patch baseline directly from the backend for testing.
func (b *InMemoryBackend) GetPatchBaselineInternal(id string) PatchBaseline {
	b.mu.RLock("GetPatchBaselineInternal")
	defer b.mu.RUnlock()

	return b.patchBaselines[id]
}

// ForceInsertParameter injects a parameter directly into the backend, bypassing
// all validation. Used to test error-handling paths (e.g., corrupted ciphertext).
func (b *InMemoryBackend) ForceInsertParameter(p Parameter) {
	b.mu.Lock("ForceInsertParameter")
	defer b.mu.Unlock()
	b.parameters[p.Name] = p
}
