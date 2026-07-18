package cloudformation

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/google/uuid"
)

type StackOptions struct {
	RollbackConfiguration *RollbackConfiguration
	RoleARN               string
	OnFailure             string // DELETE | ROLLBACK | DO_NOTHING
	Capabilities          []string
	NotificationARNs      []string
	Tags                  []Tag
	TimeoutInMinutes      int
	DisableRollback       bool
}

// CreateNestedStack implements NestedStackCreator. Must be called while b.mu is held by caller.
func (b *InMemoryBackend) CreateNestedStack(
	ctx context.Context,
	name, _ /* templateURL */, templateBody string,
	params []Parameter,
) (string, error) {
	// Lock already held by parent CreateStack — use the no-lock variant.
	stack, err := b.createStackLocked(ctx, name, templateBody, params, StackOptions{}, "")
	if err != nil {
		return "", err
	}

	return stack.StackID, nil
}

// DeleteNestedStack implements NestedStackCreator. Must be called while b.mu is held by caller.
func (b *InMemoryBackend) DeleteNestedStack(ctx context.Context, stackID string) error {
	return b.deleteStackLocked(ctx, stackID)
}

// deleteStackLocked deletes a stack without acquiring the mutex — callers must hold b.mu.
func (b *InMemoryBackend) deleteStackLocked(ctx context.Context, nameOrID string) error {
	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		// DeleteStack is idempotent in real AWS: deleting a stack that never
		// existed (or was already deleted) is a silent no-op, not an error —
		// the DeleteStack operation has no modeled "stack not found" error.
		return nil
	}

	if stack.EnableTerminationProtection {
		return fmt.Errorf("%w: %s", ErrTerminationProtectionEnabled, stack.StackName)
	}

	if name, importer, inUse := b.stackExportsInUse(stack.StackID, nil); inUse {
		return fmt.Errorf(
			"%w: Export %s cannot be deleted as it is in use by %s",
			ErrExportInUse, name, importer,
		)
	}

	stack.StackStatus = statusDeleteInProgress
	b.addEvent(
		stack.StackID,
		stack.StackName,
		stack.StackName,
		stack.StackID,
		cfnStackType,
		statusDeleteInProgress,
		reasonUserInitiated,
	)

	for logicalID, res := range b.resources[stack.StackID] {
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteInProgress,
			"",
		)
		if res.DeletionPolicy != "Retain" && res.DeletionPolicy != "Snapshot" {
			_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		}
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteComplete,
			"",
		)
	}

	now := time.Now()
	stack.DeletionTime = &now
	stack.StackStatus = statusDeleteComplete
	b.removeExports(stack.StackID)
	delete(b.stackPolicies, stack.StackID)
	delete(b.events, stack.StackID)
	delete(b.resources, stack.StackID)
	delete(b.changeSets, stack.StackName)
	b.pruneDriftDetections(stack.StackID)
	b.evictDeletedStacks()

	return nil
}

// evictDeletedStacks caps the number of DELETE_COMPLETE stacks at maxDeletedStacks.
// Caller must hold b.mu.Lock.
func (b *InMemoryBackend) evictDeletedStacks() {
	const maxDeletedStacks = 1000
	deleted := make([]*Stack, 0)
	for _, s := range b.stacks.All() {
		if s.StackStatus == statusDeleteComplete {
			deleted = append(deleted, s)
		}
	}
	if len(deleted) <= maxDeletedStacks {
		return
	}
	sort.Slice(deleted, func(i, j int) bool {
		if deleted[i].DeletionTime == nil {
			return true
		}

		if deleted[j].DeletionTime == nil {
			return false
		}

		return deleted[i].DeletionTime.Before(*deleted[j].DeletionTime)
	})
	for _, s := range deleted[:len(deleted)-maxDeletedStacks] {
		b.stacks.Delete(s.StackName)
		delete(b.stackIDIndex, s.StackID)
	}
}

func (b *InMemoryBackend) buildStackARN(stackName, stackID string) string {
	return arn.Build("cloudformation", b.region, b.accountID, "stack/"+stackName+"/"+stackID)
}

func (b *InMemoryBackend) resolveStack(nameOrID string) (*Stack, bool) {
	if s, ok := b.stacks.Get(nameOrID); ok {
		return s, true
	}

	if name, ok := b.stackIDIndex[nameOrID]; ok {
		if s, found := b.stacks.Get(name); found {
			return s, true
		}
	}

	return nil, false
}

// addEvent appends an event to the stack's event history.
func (b *InMemoryBackend) addEvent(
	stackID, stackName, logicalID, physicalID, resourceType, status, reason string,
) {
	evt := StackEvent{
		EventID:              uuid.New().String(),
		StackID:              stackID,
		StackName:            stackName,
		LogicalResourceID:    logicalID,
		PhysicalResourceID:   physicalID,
		ResourceType:         resourceType,
		ResourceStatus:       status,
		ResourceStatusReason: reason,
		Timestamp:            time.Now(),
	}
	b.events[stackID] = append(b.events[stackID], evt)

	const maxEvents = 1000
	if len(b.events[stackID]) > maxEvents {
		b.events[stackID] = b.events[stackID][len(b.events[stackID])-maxEvents:]
	}
}

// CreateStack creates a new stack from a template.
func (b *InMemoryBackend) CreateStack(
	ctx context.Context,
	name, templateBody string,
	params []Parameter,
	opts StackOptions,
) (*Stack, error) {
	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	return b.createStackLocked(ctx, name, templateBody, params, opts, "")
}

// createStackLocked is the lock-free body of CreateStack — callers must hold b.mu.
// parentID is set when provisioning a nested stack.
func (b *InMemoryBackend) createStackLocked(
	ctx context.Context,
	name, templateBody string,
	params []Parameter,
	opts StackOptions,
	parentID string,
) (*Stack, error) {
	// Validate RoleARN format and IAM capability requirements (top-level stacks only).
	if parentID == "" {
		if err := validateStackOptions(templateBody, opts); err != nil {
			return nil, err
		}
	}

	if existing, ok := b.stacks.Get(name); ok {
		if existing.StackStatus != statusDeleteComplete {
			return nil, ErrStackAlreadyExists
		}
		// Remove the old stack ID from the index before re-creating.
		delete(b.stackIDIndex, existing.StackID)
	}

	stackID := uuid.New().String()
	arn := b.buildStackARN(name, stackID)
	now := time.Now()

	stack := &Stack{
		StackID:               arn,
		StackName:             name,
		StackStatus:           statusCreateInProgress,
		CreationTime:          now,
		Parameters:            params,
		Tags:                  opts.Tags,
		TemplateBody:          templateBody,
		Capabilities:          opts.Capabilities,
		NotificationARNs:      opts.NotificationARNs,
		RoleARN:               opts.RoleARN,
		TimeoutInMinutes:      opts.TimeoutInMinutes,
		DisableRollback:       opts.DisableRollback,
		RollbackConfiguration: opts.RollbackConfiguration,
		ParentID:              parentID,
	}

	b.stacks.Put(stack)
	b.stackIDIndex[arn] = name
	b.events[arn] = nil
	b.resources[arn] = make(map[string]*StackResource)

	b.addEvent(arn, name, name, arn, cfnStackType, statusCreateInProgress, reasonUserInitiated)

	// Parse and provision resources.
	if templateBody != "" {
		b.createStackFromTemplate(ctx, stack, params)
	}

	if stack.StackStatus != statusCreateFailed && stack.StackStatus != statusRollbackComplete {
		stack.StackStatus = statusCreateComplete
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateComplete, "")
	}

	// OnFailure=DELETE: remove the stack entirely when creation fails.
	if opts.OnFailure == "DELETE" &&
		(stack.StackStatus == statusCreateFailed || stack.StackStatus == statusRollbackComplete) {
		stack.StackStatus = statusDeleteInProgress
		b.addEvent(arn, name, name, arn, cfnStackType, statusDeleteInProgress, "")
		now2 := time.Now()
		stack.DeletionTime = &now2
		stack.StackStatus = statusDeleteComplete
		b.removeExports(arn)
		delete(b.events, arn)
		delete(b.resources, arn)
		delete(b.changeSets, name)
		b.pruneDriftDetections(arn)
	}

	return stack, nil
}

// createStackFromTemplate parses and applies a template during CreateStack.
// It updates stack.StackStatus on failure.
func (b *InMemoryBackend) createStackFromTemplate(
	ctx context.Context,
	stack *Stack,
	params []Parameter,
) {
	arn := stack.StackID
	name := stack.StackName

	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		stack.StackStatus = statusCreateFailed
		stack.StackStatusReason = err.Error()
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, err.Error())

		return
	}
	stack.Description = tmpl.Description

	if dynErr := ResolveDynamicRefsInTemplate(ctx, tmpl, b.resolver); dynErr != nil {
		stack.StackStatus = statusCreateFailed
		stack.StackStatusReason = dynErr.Error()
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, dynErr.Error())

		return
	}

	resolvedParams := ResolveParameters(tmpl, params)

	if valErr := ValidateParameters(tmpl, resolvedParams); valErr != nil {
		b.failAndRollback(stack, valErr.Error())

		return
	}

	// Validate intrinsic references (Fn::GetAtt / Fn::Sub to undefined
	// resources, unsupported resource types) before provisioning anything.
	if intErr := validateIntrinsics(tmpl); intErr != nil {
		b.failAndRollback(stack, intErr.Error())

		return
	}

	// Validate that all Fn::ImportValue references can be satisfied before
	// creating any resources.
	if impErr := validateImportValues(tmpl, resolvedParams, b.buildExportsMap()); impErr != nil {
		b.failAndRollback(stack, impErr.Error())

		return
	}

	physicalIDs := b.provisionResources(ctx, stack, tmpl, resolvedParams)
	if stack.StackStatus == statusCreateFailed || stack.StackStatus == statusRollbackComplete {
		return
	}

	resourceTypes := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		resourceTypes[logicalID] = res.Type
	}
	rctx := resolveCtx{
		params:        resolvedParams,
		physicalIDs:   physicalIDs,
		resourceTypes: resourceTypes,
		exports:       b.buildExportsMap(),
		conditions:    evaluateConditions(tmpl.Conditions, resolvedParams, physicalIDs),
		mappings:      tmpl.Mappings,
		accountID:     b.accountID,
		region:        b.region,
		stackName:     name,
	}
	var exportMap map[string]string
	stack.Outputs, exportMap = resolveOutputsWithContext(tmpl, rctx)

	if regErr := b.registerExports(stack.StackID, exportMap); regErr != nil {
		stack.StackStatus = statusCreateFailed
		stack.StackStatusReason = regErr.Error()
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, regErr.Error())
	}
}

// updateFailAndRollback records UPDATE_FAILED then emits UPDATE_ROLLBACK_IN_PROGRESS /
// UPDATE_ROLLBACK_COMPLETE for pre-flight update failures (no resources changed).
func (b *InMemoryBackend) updateFailAndRollback(stack *Stack, reason string) {
	arn := stack.StackID
	name := stack.StackName
	stack.StackStatusReason = reason
	b.addEvent(arn, name, name, arn, cfnStackType, statusUpdateFailed, reason)
	b.addEvent(arn, name, name, arn, cfnStackType, statusUpdateRollbackInProgress, reason)
	b.addEvent(arn, name, name, arn, cfnStackType, statusUpdateRollbackComplete, "")
	stack.StackStatus = statusUpdateRollbackComplete
}

// failAndRollback records a pre-flight CREATE_FAILED then immediately emits
// ROLLBACK_IN_PROGRESS / ROLLBACK_COMPLETE (no resources to undo).
func (b *InMemoryBackend) failAndRollback(stack *Stack, reason string) {
	arn := stack.StackID
	name := stack.StackName
	stack.StackStatusReason = reason
	b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, reason)
	b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackInProgress, reason)
	b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackComplete, "")
	stack.StackStatus = statusRollbackComplete
}

// provisionResources creates all resources defined in the template.
// Returns the physicalIDs map. On resource creation failure, rollback is
// performed in reverse order; stack.StackStatus is then set to
// statusRollbackComplete (matching real AWS behaviour). If the creation failure
// itself needs to be recorded separately, it is preserved in StackStatusReason.
func (b *InMemoryBackend) provisionResources(
	ctx context.Context,
	stack *Stack,
	tmpl *Template,
	resolvedParams map[string]string,
) map[string]string {
	arn := stack.StackID
	name := stack.StackName
	physicalIDs := make(map[string]string)

	// Inject stack metadata for custom resource event payloads.
	physicalIDs["_StackId"] = arn
	physicalIDs["_StackName"] = name

	ordered := topoSortResources(tmpl.Resources)

	created := make([]string, 0, len(ordered))

	for _, logicalID := range ordered {
		res := tmpl.Resources[logicalID]
		b.addEvent(arn, name, logicalID, "", res.Type, statusCreateInProgress, "")
		physicalID, cerr := b.creator.Create(
			ctx,
			logicalID,
			res.Type,
			res.Properties,
			resolvedParams,
			physicalIDs,
		)
		if cerr != nil {
			stack.StackStatusReason = fmt.Sprintf("resource %s: %v", logicalID, cerr)
			b.addEvent(arn, name, logicalID, "", res.Type, statusCreateFailed, cerr.Error())
			b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackInProgress, cerr.Error())
			b.rollbackCreateResources(ctx, stack, created)
			b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackComplete, "")
			stack.StackStatus = statusRollbackComplete

			return physicalIDs
		}
		physicalIDs[logicalID] = physicalID
		b.resources[arn][logicalID] = &StackResource{
			Timestamp:      time.Now(),
			LogicalID:      logicalID,
			PhysicalID:     physicalID,
			Type:           res.Type,
			Status:         statusCreateComplete,
			Properties:     res.Properties,
			StackID:        arn,
			StackName:      name,
			DeletionPolicy: res.DeletionPolicy,
		}
		b.addEvent(arn, name, logicalID, physicalID, res.Type, statusCreateComplete, "")
		created = append(created, logicalID)
	}

	return physicalIDs
}

// rollbackCreateResources deletes all resources that were created during a
// failed CreateStack provisioning pass, in reverse order.
func (b *InMemoryBackend) rollbackCreateResources(
	ctx context.Context,
	stack *Stack,
	created []string,
) {
	for _, v := range slices.Backward(created) {
		logicalID := v
		res, ok := b.resources[stack.StackID][logicalID]
		if !ok {
			continue
		}

		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteInProgress,
			"",
		)
		_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteComplete,
			"",
		)
		delete(b.resources[stack.StackID], logicalID)
	}
}

// topoSortResources returns the logical resource IDs in an order that respects
// DependsOn declarations. Resources with no dependencies come first; within the
// same dependency level they are ordered alphabetically for determinism.
// If a cycle is detected the function falls back to plain alphabetical order.
func topoSortResources(resources map[string]TemplateResource) []string {
	// Collect all known IDs in alphabetical order for determinism.
	all := collections.SortedKeys(resources)

	// Build forward-dependency map (id → ids it depends on) and
	// reverse-dependency map (id → ids that depend on it) simultaneously.
	deps := make(map[string][]string, len(resources))
	revDeps := make(map[string][]string, len(resources))

	for _, id := range all {
		res := resources[id]
		if len(res.DependsOn) > 0 {
			deps[id] = res.DependsOn
			for _, dep := range res.DependsOn {
				revDeps[dep] = append(revDeps[dep], id)
			}
		}
	}

	// Kahn's algorithm for topological sort.
	// inDegree counts how many declared dependencies are still unprocessed.
	inDegree := make(map[string]int, len(all))
	for _, id := range all {
		inDegree[id] = len(deps[id])
	}

	// Process nodes with zero in-degree first (alphabetical order for determinism).
	queue := make([]string, 0, len(all))
	for _, id := range all {
		if inDegree[id] == 0 {
			queue = append(queue, id)
		}
	}

	result := make([]string, 0, len(all))

	for len(queue) > 0 {
		// Pop first element (queue is kept sorted).
		cur := queue[0]
		queue = queue[1:]
		result = append(result, cur)

		// Use the reverse-dependency map for O(1) lookup of dependents.
		for _, dependent := range revDeps[cur] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				insertSorted(&queue, dependent)
			}
		}
	}

	// Cycle detected: fall back to alphabetical order (best-effort).
	if len(result) < len(all) {
		return all
	}

	return result
}

// insertSorted inserts s into the sorted slice ss, maintaining ascending order.
func insertSorted(ss *[]string, s string) {
	i := sort.SearchStrings(*ss, s)
	*ss = append(*ss, "")
	copy((*ss)[i+1:], (*ss)[i:])
	(*ss)[i] = s
}

// UpdateStack updates an existing stack.
func (b *InMemoryBackend) UpdateStack(
	ctx context.Context,
	nameOrID, templateBody string,
	params []Parameter,
	opts StackOptions,
) (*Stack, error) {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	// Validate RoleARN format and IAM capability requirements.
	if err := validateStackOptions(templateBody, opts); err != nil {
		return nil, err
	}

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	now := time.Now()
	stack.LastUpdatedTime = &now
	stack.StackStatus = statusUpdateInProgress

	if templateBody != "" {
		stack.TemplateBody = templateBody
	}
	if params != nil {
		stack.Parameters = params
	}
	if opts.RoleARN != "" {
		stack.RoleARN = opts.RoleARN
	}
	if len(opts.Capabilities) > 0 {
		stack.Capabilities = opts.Capabilities
	}
	if len(opts.Tags) > 0 {
		stack.Tags = opts.Tags
	}
	if opts.RollbackConfiguration != nil {
		stack.RollbackConfiguration = opts.RollbackConfiguration
	}

	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateInProgress, reasonUserInitiated,
	)

	if !b.applyTemplateToStack(ctx, stack) {
		return stack, nil
	}

	stack.StackStatus = statusUpdateComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateComplete, "",
	)

	return stack, nil
}

// applyTemplateToStack parses the stack's template and creates or updates resources.
// Returns true on success; on failure it sets the stack status and returns false.
func (b *InMemoryBackend) applyTemplateToStack(ctx context.Context, stack *Stack) bool {
	if stack.TemplateBody == "" {
		return true
	}

	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = err.Error()
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateFailed, err.Error(),
		)

		return false
	}

	stack.Description = tmpl.Description

	if dynErr := ResolveDynamicRefsInTemplate(ctx, tmpl, b.resolver); dynErr != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = dynErr.Error()
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateFailed, dynErr.Error(),
		)

		return false
	}

	resolvedParams := ResolveParameters(tmpl, stack.Parameters)

	if valErr := ValidateParameters(tmpl, resolvedParams); valErr != nil {
		b.updateFailAndRollback(stack, valErr.Error())

		return false
	}

	// Validate intrinsic references before mutating any resource.
	if intErr := validateIntrinsics(tmpl); intErr != nil {
		b.updateFailAndRollback(stack, intErr.Error())

		return false
	}

	// Pre-populate physicalIDs from existing resources.
	physicalIDs := make(map[string]string, len(b.resources[stack.StackID]))
	for logicalID, res := range b.resources[stack.StackID] {
		physicalIDs[logicalID] = res.PhysicalID
	}

	// Validate that all Fn::ImportValue references can be satisfied before
	// updating any resources.
	if impErr := validateImportValues(tmpl, resolvedParams, b.buildExportsMap()); impErr != nil {
		b.updateFailAndRollback(stack, impErr.Error())

		return false
	}

	// Validate that the update does not drop an export that another active
	// stack still imports via Fn::ImportValue (computed against pre-update
	// resource state, mirroring AWS's validate-before-apply semantics).
	if expErr := b.validateExportsStillInUse(stack, tmpl, resolvedParams, physicalIDs); expErr != nil {
		b.updateFailAndRollback(stack, expErr.Error())

		return false
	}

	if !b.updateResources(ctx, stack, tmpl, resolvedParams, physicalIDs) {
		return false
	}

	b.removeExports(stack.StackID)

	updateResourceTypes := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		updateResourceTypes[logicalID] = res.Type
	}
	rctx := resolveCtx{
		params:        resolvedParams,
		physicalIDs:   physicalIDs,
		resourceTypes: updateResourceTypes,
		exports:       b.buildExportsMap(),
		conditions:    evaluateConditions(tmpl.Conditions, resolvedParams, physicalIDs),
		mappings:      tmpl.Mappings,
		accountID:     b.accountID,
		region:        b.region,
		stackName:     stack.StackName,
	}

	var exportMap map[string]string
	stack.Outputs, exportMap = resolveOutputsWithContext(tmpl, rctx)

	if regErr := b.registerExports(stack.StackID, exportMap); regErr != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = regErr.Error()
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateFailed, regErr.Error(),
		)

		return false
	}

	return true
}

// updateResources reconciles existing resources and creates newly declared ones.
// On creation failure it rolls back: newly-created resources are deleted and
// previously-existing resources are restored to their pre-update state.
// Stale resources (present in the stack but absent from the new template) are
// deleted after all new resources are created successfully.
// Returns true on success; on failure it sets stack.StackStatus to
// UPDATE_ROLLBACK_COMPLETE and returns false.
func (b *InMemoryBackend) updateResources(
	ctx context.Context,
	stack *Stack,
	tmpl *Template,
	resolvedParams map[string]string,
	physicalIDs map[string]string,
) bool {
	// Snapshot pre-update state for rollback.
	prevResources := make(map[string]*StackResource, len(b.resources[stack.StackID]))
	for k, v := range b.resources[stack.StackID] {
		cp := *v
		prevResources[k] = &cp
	}

	var created []string

	for logicalID, res := range tmpl.Resources {
		existing, exists := b.resources[stack.StackID][logicalID]
		if !exists {
			physicalID, cerr := b.createUpdateResource(
				ctx,
				stack,
				logicalID,
				res,
				resolvedParams,
				physicalIDs,
			)
			if cerr != nil {
				b.rollbackUpdateResources(ctx, stack, prevResources, created)
				stack.StackStatusReason = fmt.Sprintf("resource %s: %v", logicalID, cerr)

				return false
			}

			physicalIDs[logicalID] = physicalID
			created = append(created, logicalID)

			continue
		}

		if uerr := b.updateExistingResource(ctx, stack, logicalID, res, existing); uerr != nil {
			b.rollbackUpdateResources(ctx, stack, prevResources, created)
			stack.StackStatusReason = fmt.Sprintf("resource %s update: %v", logicalID, uerr)

			return false
		}
	}

	b.deleteStaleResources(ctx, stack, tmpl)

	return true
}

// createUpdateResource creates a new resource during a stack update and registers it.
func (b *InMemoryBackend) createUpdateResource(
	ctx context.Context,
	stack *Stack,
	logicalID string,
	res TemplateResource,
	resolvedParams, physicalIDs map[string]string,
) (string, error) {
	b.addEvent(stack.StackID, stack.StackName, logicalID, "", res.Type, statusCreateInProgress, "")
	physicalID, cerr := b.creator.Create(
		ctx,
		logicalID,
		res.Type,
		res.Properties,
		resolvedParams,
		physicalIDs,
	)
	if cerr != nil {
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			"",
			res.Type,
			statusCreateFailed,
			cerr.Error(),
		)

		return "", cerr
	}

	b.resources[stack.StackID][logicalID] = &StackResource{
		Timestamp:      time.Now(),
		LogicalID:      logicalID,
		PhysicalID:     physicalID,
		Type:           res.Type,
		Status:         statusCreateComplete,
		Properties:     res.Properties,
		StackID:        stack.StackID,
		StackName:      stack.StackName,
		DeletionPolicy: res.DeletionPolicy,
	}
	b.addEvent(
		stack.StackID,
		stack.StackName,
		logicalID,
		physicalID,
		res.Type,
		statusCreateComplete,
		"",
	)

	return physicalID, nil
}

// updateExistingResource processes an existing resource during a stack update.
// For CFN extensibility types it sends an Update event to the backing Lambda/SNS.
func (b *InMemoryBackend) updateExistingResource(
	ctx context.Context,
	stack *Stack,
	logicalID string,
	res TemplateResource,
	existing *StackResource,
) error {
	if isCFNExtensibilityType(res.Type) {
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			existing.PhysicalID,
			res.Type,
			statusUpdateInProgress,
			"",
		)
		uerr := b.creator.Update(
			ctx,
			logicalID,
			res.Type,
			existing.PhysicalID,
			res.Properties,
			existing.Properties,
		)
		if uerr != nil {
			b.addEvent(
				stack.StackID, stack.StackName, logicalID,
				existing.PhysicalID, res.Type, statusUpdateFailed, uerr.Error(),
			)

			return uerr
		}

		existing.Properties = res.Properties
	}

	existing.Status = statusUpdateComplete
	existing.Timestamp = time.Now()
	b.addEvent(
		stack.StackID,
		stack.StackName,
		logicalID,
		existing.PhysicalID,
		res.Type,
		statusUpdateComplete,
		"",
	)

	return nil
}

// deleteStaleResources removes logical IDs present in the stack but absent from the new template.
func (b *InMemoryBackend) deleteStaleResources(ctx context.Context, stack *Stack, tmpl *Template) {
	var stale []string
	for logicalID := range b.resources[stack.StackID] {
		if _, inTemplate := tmpl.Resources[logicalID]; !inTemplate {
			stale = append(stale, logicalID)
		}
	}

	sort.Strings(stale)

	for _, logicalID := range stale {
		res := b.resources[stack.StackID][logicalID]
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteInProgress,
			"",
		)
		if res.DeletionPolicy != "Retain" && res.DeletionPolicy != "Snapshot" {
			_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		}
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteComplete,
			"",
		)
		delete(b.resources[stack.StackID], logicalID)
	}
}

// rollbackUpdateResources undoes a partially-applied update: it deletes every
// resource that was newly created in this update pass and restores resources that
// were modified to their pre-update snapshots, then sets the stack status to
// UPDATE_ROLLBACK_COMPLETE.
func (b *InMemoryBackend) rollbackUpdateResources(
	ctx context.Context,
	stack *Stack,
	prevResources map[string]*StackResource,
	created []string,
) {
	stack.StackStatus = statusUpdateRollbackInProgress
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateRollbackInProgress, "",
	)

	for _, logicalID := range created {
		res, ok := b.resources[stack.StackID][logicalID]
		if !ok {
			continue
		}

		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteInProgress,
			"",
		)
		_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteComplete,
			"",
		)
		delete(b.resources[stack.StackID], logicalID)
	}

	// Restore resources that existed before the update.
	maps.Copy(b.resources[stack.StackID], prevResources)

	stack.StackStatus = statusUpdateRollbackComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateRollbackComplete, "",
	)
}
