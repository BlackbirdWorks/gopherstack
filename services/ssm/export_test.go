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

	return len(b.historyStore(b.Region())[name])
}

// CommandCount returns the number of commands currently stored across all regions.
func (b *InMemoryBackend) CommandCount() int {
	b.mu.RLock("CommandCount")
	defer b.mu.RUnlock()

	total := 0
	for _, cmds := range b.commands {
		total += len(cmds)
	}

	return total
}

// CommandInvocationCount returns the number of command invocation sets stored across all regions.
func (b *InMemoryBackend) CommandInvocationCount() int {
	b.mu.RLock("CommandInvocationCount")
	defer b.mu.RUnlock()

	total := 0
	for _, invs := range b.commandInvocations {
		total += len(invs)
	}

	return total
}

// SetCommandExpiresAfter overrides the ExpiresAfter timestamp of the given command.
// Used in tests to force a command into an expired state.
func (b *InMemoryBackend) SetCommandExpiresAfter(cmdID string, expiresAfter float64) {
	b.mu.Lock("SetCommandExpiresAfter")
	defer b.mu.Unlock()

	// Try all regions.
	for _, cmds := range b.commands {
		if cmd, ok := cmds[cmdID]; ok {
			cmd.ExpiresAfter = expiresAfter
			cmds[cmdID] = cmd

			return
		}
	}
}

// HasTagEntry reports whether the tags map contains an entry for the given parameter name.
// Returns false when the parameter's tag entry has been cleaned up (nil or absent).
func (b *InMemoryBackend) HasTagEntry(name string) bool {
	b.mu.RLock("HasTagEntry")
	defer b.mu.RUnlock()

	return b.tagsStore(b.Region())[name] != nil
}

// DocumentVersionCount returns the number of versions stored for the given document.
func (b *InMemoryBackend) DocumentVersionCount(name string) int {
	b.mu.RLock("DocumentVersionCount")
	defer b.mu.RUnlock()

	return len(b.documentVersionsStore(b.Region())[name])
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

// ActivationCount returns the number of activations stored in the default region.
func (b *InMemoryBackend) ActivationCount() int {
	b.mu.RLock("ActivationCount")
	defer b.mu.RUnlock()

	return len(b.activationsStore(b.Region()))
}

// AssociationCount returns the number of associations stored in the default region.
func (b *InMemoryBackend) AssociationCount() int {
	b.mu.RLock("AssociationCount")
	defer b.mu.RUnlock()

	return len(b.associationsStore(b.Region()))
}

// MaintenanceWindowCount returns the number of maintenance windows stored in the default region.
func (b *InMemoryBackend) MaintenanceWindowCount() int {
	b.mu.RLock("MaintenanceWindowCount")
	defer b.mu.RUnlock()

	return len(b.maintenanceWindowsStore(b.Region()))
}

// OpsItemCount returns the number of OpsItems stored in the default region.
func (b *InMemoryBackend) OpsItemCount() int {
	b.mu.RLock("OpsItemCount")
	defer b.mu.RUnlock()

	return len(b.opsItemsStore(b.Region()))
}

// OpsMetadataCount returns the number of OpsMetadata entries stored in the default region.
func (b *InMemoryBackend) OpsMetadataCount() int {
	b.mu.RLock("OpsMetadataCount")
	defer b.mu.RUnlock()

	return len(b.opsMetadataStore(b.Region()))
}

// PatchBaselineCount returns the number of patch baselines stored in the default region.
func (b *InMemoryBackend) PatchBaselineCount() int {
	b.mu.RLock("PatchBaselineCount")
	defer b.mu.RUnlock()

	return len(b.patchBaselinesStore(b.Region()))
}

// HandlerOpsLen returns the number of supported operations.
func (h *Handler) HandlerOpsLen() int {
	return len(h.GetSupportedOperations())
}

// AddActivationInternal seeds an activation directly into the backend for testing.
func (b *InMemoryBackend) AddActivationInternal(act Activation) {
	b.mu.Lock("AddActivationInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.activations[r] == nil {
		b.activations[r] = make(map[string]Activation)
	}
	b.activationsStore(r)[act.ActivationID] = act
}

// AddAssociationInternal seeds an association directly into the backend for testing.
func (b *InMemoryBackend) AddAssociationInternal(assoc Association) {
	b.mu.Lock("AddAssociationInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.associations[r] == nil {
		b.associations[r] = make(map[string]Association)
	}
	b.associationsStore(r)[assoc.AssociationID] = assoc
}

// AddMaintenanceWindowInternal seeds a maintenance window directly into the backend for testing.
func (b *InMemoryBackend) AddMaintenanceWindowInternal(mw MaintenanceWindow) {
	b.mu.Lock("AddMaintenanceWindowInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.maintenanceWindows[r] == nil {
		b.maintenanceWindows[r] = make(map[string]MaintenanceWindow)
	}
	b.maintenanceWindowsStore(r)[mw.WindowID] = mw
}

// AddOpsItemInternal seeds an OpsItem directly into the backend for testing.
func (b *InMemoryBackend) AddOpsItemInternal(item OpsItem) {
	b.mu.Lock("AddOpsItemInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.opsItems[r] == nil {
		b.opsItems[r] = make(map[string]OpsItem)
	}
	b.opsItemsStore(r)[item.OpsItemID] = item
}

// AddOpsMetadataInternal seeds OpsMetadata directly into the backend for testing.
func (b *InMemoryBackend) AddOpsMetadataInternal(meta OpsMetadata) {
	b.mu.Lock("AddOpsMetadataInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.opsMetadata[r] == nil {
		b.opsMetadata[r] = make(map[string]OpsMetadata)
	}
	b.opsMetadataStore(r)[meta.OpsMetadataArn] = meta
}

// AddPatchBaselineInternal seeds a patch baseline directly into the backend for testing.
func (b *InMemoryBackend) AddPatchBaselineInternal(bl PatchBaseline) {
	b.mu.Lock("AddPatchBaselineInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.patchBaselines[r] == nil {
		b.patchBaselines[r] = make(map[string]PatchBaseline)
	}
	b.patchBaselinesStore(r)[bl.BaselineID] = bl
}

// OpsItemRelatedItemCount returns the total number of related items across all OpsItems.
func (b *InMemoryBackend) OpsItemRelatedItemCount(opsItemID string) int {
	b.mu.RLock("OpsItemRelatedItemCount")
	defer b.mu.RUnlock()

	return len(b.opsItemRelatedItemsStore(b.Region())[opsItemID])
}

// GetPatchBaselineInternal retrieves a patch baseline directly from the backend for testing.
func (b *InMemoryBackend) GetPatchBaselineInternal(id string) PatchBaseline {
	b.mu.RLock("GetPatchBaselineInternal")
	defer b.mu.RUnlock()

	return b.patchBaselinesStore(b.Region())[id]
}

// ForceInsertParameter injects a parameter directly into the backend, bypassing
// all validation. Used to test error-handling paths (e.g., corrupted ciphertext).
func (b *InMemoryBackend) ForceInsertParameter(p Parameter) {
	b.mu.Lock("ForceInsertParameter")
	defer b.mu.Unlock()
	r := b.Region()
	if b.parameters[r] == nil {
		b.parameters[r] = make(map[string]Parameter)
	}
	b.parametersStore(r)[p.Name] = p
}

// AddInstancePatchStateInternal seeds an InstancePatchState directly into the backend for testing.
func (b *InMemoryBackend) AddInstancePatchStateInternal(s InstancePatchState) {
	b.mu.Lock("AddInstancePatchStateInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.instancePatchStates[r] == nil {
		b.instancePatchStates[r] = make(map[string]*InstancePatchState)
	}
	b.instancePatchStatesStore(r)[s.InstanceID] = &s
}

// AddInstancePatchesInternal seeds PatchComplianceData for an instance directly into the backend for testing.
func (b *InMemoryBackend) AddInstancePatchesInternal(instanceID string, patches []PatchComplianceData) {
	b.mu.Lock("AddInstancePatchesInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.instancePatches[r] == nil {
		b.instancePatches[r] = make(map[string][]PatchComplianceData)
	}
	b.instancePatchesStore(r)[instanceID] = patches
}

// AddInstancePropertyInternal seeds an InstanceProperty directly into the backend for testing.
func (b *InMemoryBackend) AddInstancePropertyInternal(p InstanceProperty) {
	b.mu.Lock("AddInstancePropertyInternal")
	defer b.mu.Unlock()
	r := b.Region()
	if b.instanceProperties[r] == nil {
		b.instanceProperties[r] = make(map[string]*InstanceProperty)
	}
	b.instancePropertiesStore(r)[p.InstanceID] = &p
}

// AddAvailablePatchInternal seeds a Patch into the available patches catalog for testing.
func (b *InMemoryBackend) AddAvailablePatchInternal(p Patch) {
	b.mu.Lock("AddAvailablePatchInternal")
	defer b.mu.Unlock()
	b.availablePatches[b.Region()] = append(b.availablePatches[b.Region()], p)
}
