package cloudformation

import "context"

// RollbackUpdateResourcesForTest exposes rollbackUpdateResources for
// white-box testing: updateResources creates newly-added resources by
// iterating a Go map, so which of two new resources is created first (and
// thus whether it's in `created` when a sibling fails) isn't deterministic
// through UpdateStack. This drives the rollback directly against
// already-registered resources instead.
func (b *InMemoryBackend) RollbackUpdateResourcesForTest(
	ctx context.Context, stackName string, created []string,
) {
	b.mu.Lock("RollbackUpdateResourcesForTest")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(stackName)
	if !ok {
		return
	}

	prevResources := make(map[string]*StackResource, len(b.resources[stack.StackID]))
	for k, v := range b.resources[stack.StackID] {
		cp := *v
		prevResources[k] = &cp
	}

	b.rollbackUpdateResources(ctx, stack, prevResources, created)
}

// RegisterForTest exposes MacroRegistry.register for test-only use.
func (r *MacroRegistry) RegisterForTest(name, functionARN, description string) {
	r.register(name, functionARN, description)
}

// TopoSortResources exposes topoSortResources for white-box testing.
func TopoSortResources(resources map[string]TemplateResource) []string {
	return topoSortResources(resources)
}

// AddStackEventInternal appends a fully-formed StackEvent directly into
// b.events[stackID], bypassing addEvent's time.Now() Timestamp assignment so
// callers can construct Timestamp ties across different stacks.
func (b *InMemoryBackend) AddStackEventInternal(stackID string, evt StackEvent) {
	b.mu.Lock("AddStackEventInternal")
	defer b.mu.Unlock()

	b.events[stackID] = append(b.events[stackID], evt)
}

// AddStackSetOperationInternal inserts a fully-formed StackSetOperation
// directly into b.stackSetOperations[stackSetName], bypassing
// recordStackSetOperation's time.Now() CreatedAt assignment so callers can
// construct CreatedAt ties.
func (b *InMemoryBackend) AddStackSetOperationInternal(stackSetName string, op *StackSetOperation) {
	b.mu.Lock("AddStackSetOperationInternal")
	defer b.mu.Unlock()

	if b.stackSetOperations[stackSetName] == nil {
		b.stackSetOperations[stackSetName] = make(map[string]*StackSetOperation)
	}

	b.stackSetOperations[stackSetName][op.OperationID] = op
}

// ParseDependsOn exposes parseDependsOn for white-box testing.
func ParseDependsOn(v any) []string {
	return parseDependsOn(v)
}

// GetResourceAttribute exposes getResourceAttribute for white-box GetAtt testing.
func GetResourceAttribute(resType, physID, attrName, accountID, region string) string {
	return getResourceAttribute(resType, physID, attrName, accountID, region)
}

// ForceStackStatus sets the status of a stack by name for test purposes.
func (b *InMemoryBackend) ForceStackStatus(stackName, status string) {
	b.mu.Lock("ForceStackStatus")
	defer b.mu.Unlock()

	if s, ok := b.stacks.Get(stackName); ok {
		s.StackStatus = status
	}
}

// InjectCreateHook installs a persistent hook on the ResourceCreator that is
// called before any real creation logic. The hook remains active for all
// subsequent Create calls until replaced or cleared (set to nil). If the hook
// returns a non-nil error the Create call fails with that error. Used only for
// testing error and rollback paths.
func (rc *ResourceCreator) InjectCreateHook(fn func(resourceType string) error) {
	rc.createHook = fn
}

// InjectDeleteHook installs a hook that is called when Delete is invoked, before
// any actual deletion logic. Used to observe which resource types are deleted.
func (rc *ResourceCreator) InjectDeleteHook(fn func(resourceType string)) {
	rc.deleteHook = fn
}

// GetCreator returns the backend's ResourceCreator for test-only hook injection.
func (b *InMemoryBackend) GetCreator() *ResourceCreator {
	return b.creator
}

// ResourcesEntryExists reports whether b.resources has an entry for stackID.
func (b *InMemoryBackend) ResourcesEntryExists(stackID string) bool {
	b.mu.RLock("ResourcesEntryExists")
	defer b.mu.RUnlock()

	_, ok := b.resources[stackID]

	return ok
}

// ChangeSetsEntryExists reports whether b.changeSets has an entry for stackName.
func (b *InMemoryBackend) ChangeSetsEntryExists(stackName string) bool {
	b.mu.RLock("ChangeSetsEntryExists")
	defer b.mu.RUnlock()

	_, ok := b.changeSets[stackName]

	return ok
}

// DriftDetectionCount returns the number of drift detection entries for stackID.
func (b *InMemoryBackend) DriftDetectionCount(stackID string) int {
	b.mu.RLock("DriftDetectionCount")
	defer b.mu.RUnlock()

	count := 0

	for _, status := range b.driftDetections.All() {
		if status.StackID == stackID {
			count++
		}
	}

	return count
}

// ResourceCountForStack returns the number of resources tracked for stackID.
func (b *InMemoryBackend) ResourceCountForStack(stackID string) int {
	b.mu.RLock("ResourceCountForStack")
	defer b.mu.RUnlock()

	return len(b.resources[stackID])
}

// ForceRemoveResource removes a resource from the stack's resource map, simulating
// an out-of-band deletion for drift detection tests.
func (b *InMemoryBackend) ForceRemoveResource(stackName, logicalID string) {
	b.mu.Lock("ForceRemoveResource")
	defer b.mu.Unlock()

	stack, ok := b.stacks.Get(stackName)
	if !ok {
		return
	}

	delete(b.resources[stack.StackID], logicalID)
}

// ForceModifyResourceProperties overwrites a resource's stored properties, simulating
// an out-of-band modification for drift detection tests.
func (b *InMemoryBackend) ForceModifyResourceProperties(stackName, logicalID string, props map[string]any) {
	b.mu.Lock("ForceModifyResourceProperties")
	defer b.mu.Unlock()

	stack, ok := b.stacks.Get(stackName)
	if !ok {
		return
	}

	if res, ok2 := b.resources[stack.StackID][logicalID]; ok2 {
		res.Properties = props
	}
}
