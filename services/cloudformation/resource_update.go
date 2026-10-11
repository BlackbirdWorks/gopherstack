package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"time"
)

const (
	reasonReplacementRequired   = "Requested update requires the creation of a new physical resource; hence creating one."
	reasonCreationInitiated     = "Resource creation Initiated"
	statusUpdateCleanupProgress = "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS"
)

// updateContext carries the pre-update view needed to tell which resources the
// new template actually changes.
type updateContext struct {
	oldTmpl   *Template
	oldParams map[string]string
	oldPhys   map[string]string
}

// replacedResource is an old physical resource awaiting cleanup after a replacement.
type replacedResource struct {
	old    StackResource
	policy string
}

type resourceChange struct {
	changed []string
	replace bool
}

// ErrCustomNamedReplacement is the failure for a replacement that would collide on a fixed physical name.
var ErrCustomNamedReplacement = errors.New(
	"CloudFormation cannot update a stack when a custom-named resource requires replacing",
)

func customNameProperty(resourceType string) string {
	switch resourceType {
	case resTypeS3Bucket:
		return "BucketName"
	case resTypeSQSQueue:
		return "QueueName"
	case resTypeSNSTopic:
		return "TopicName"
	case resTypeDynamoDBTable:
		return samKeyTableName
	case resTypeLambdaFunction:
		return "FunctionName"
	case resTypeIAMRole:
		return "RoleName"
	case resTypeLogGroup:
		return "LogGroupName"
	}

	return ""
}

// updatePass accumulates what a stack update has done so far, for rollback and cleanup.
type updatePass struct {
	uc       *updateContext
	params   map[string]string
	phys     map[string]string
	created  []string
	replaced []*replacedResource
}

// applyResourceUpdate creates, replaces or updates one resource; the error carries the stack status reason.
func (b *InMemoryBackend) applyResourceUpdate(
	ctx context.Context, stack *Stack, res TemplateResource, logicalID string, pass *updatePass,
) error {
	existing, exists := b.resources[stack.StackID][logicalID]
	if !exists {
		physicalID, err := b.createUpdateResource(ctx, stack, logicalID, res, pass.params, pass.phys)
		if err != nil {
			return fmt.Errorf("resource %s: %w", logicalID, err)
		}

		pass.phys[logicalID] = physicalID
		pass.created = append(pass.created, logicalID)

		return nil
	}

	out, err := b.updateExistingResource(ctx, stack, logicalID, res, existing, pass.uc, pass.params, pass.phys)
	if err != nil {
		return fmt.Errorf("resource %s update: %w", logicalID, err)
	}

	if out.replaced != nil {
		pass.replaced = append(pass.replaced, out.replaced)
		pass.created = append(pass.created, logicalID)
	}

	return nil
}

type updateOutcome struct {
	replaced *replacedResource
}

func resolvedProps(props map[string]any, params, phys map[string]string) map[string]any {
	out, _ := resolveDeep(props, params, phys).(map[string]any)

	return out
}

// planResourceUpdate compares the resource as last deployed with the new
// template (both fully resolved) and classifies the change.
func (b *InMemoryBackend) planResourceUpdate(
	logicalID string,
	res TemplateResource,
	existing *StackResource,
	uc *updateContext,
	params, phys map[string]string,
) resourceChange {
	oldType, oldProps := existing.Type, existing.Properties

	if uc != nil && uc.oldTmpl != nil {
		if old, ok := uc.oldTmpl.Resources[logicalID]; ok {
			oldType, oldProps = old.Type, old.Properties
		}
	}

	if oldType != res.Type {
		return resourceChange{changed: []string{"Type"}, replace: true}
	}

	oldParams, oldPhys := params, phys
	if uc != nil && uc.oldTmpl != nil {
		oldParams, oldPhys = uc.oldParams, uc.oldPhys
	}

	oldResolved := resolvedProps(oldProps, oldParams, oldPhys)
	newResolved := resolvedProps(res.Properties, params, phys)

	keys := make(map[string]struct{}, len(oldResolved)+len(newResolved))
	for k := range oldResolved {
		keys[k] = struct{}{}
	}

	for k := range newResolved {
		keys[k] = struct{}{}
	}

	var change resourceChange

	for k := range keys {
		if reflect.DeepEqual(oldResolved[k], newResolved[k]) {
			continue
		}

		change.changed = append(change.changed, k)

		if requiresRecreation(res.Type, k) == recreateAlways {
			change.replace = true
		}
	}

	sort.Strings(change.changed)

	return change
}

func customNameUnchanged(res TemplateResource, existing *StackResource, uc *updateContext,
	params, phys map[string]string,
) (string, bool) {
	key := customNameProperty(res.Type)
	if key == "" || res.Properties[key] == nil {
		return "", false
	}

	name := resolve(res.Properties[key], params, phys)
	if name == "" {
		return "", false
	}

	oldProps := existing.Properties
	oldParams, oldPhys := params, phys

	if uc != nil && uc.oldTmpl != nil {
		oldParams, oldPhys = uc.oldParams, uc.oldPhys

		if old, found := uc.oldTmpl.Resources[existing.LogicalID]; found {
			oldProps = old.Properties
		}
	}

	return name, resolve(oldProps[key], oldParams, oldPhys) == name
}

// updateExistingResource applies a template change to a resource that already
// exists: nothing for an unchanged resource, create-new for a replacement, an
// in-place update otherwise. It reports whether a replacement was started.
func (b *InMemoryBackend) updateExistingResource(
	ctx context.Context,
	stack *Stack,
	logicalID string,
	res TemplateResource,
	existing *StackResource,
	uc *updateContext,
	params, phys map[string]string,
) (updateOutcome, error) {
	if isCFNExtensibilityType(res.Type) {
		return updateOutcome{}, b.updateExtensibilityResource(ctx, stack, logicalID, res, existing)
	}

	change := b.planResourceUpdate(logicalID, res, existing, uc, params, phys)
	if len(change.changed) == 0 {
		existing.DeletionPolicy = res.DeletionPolicy

		return updateOutcome{}, nil
	}

	if change.replace {
		repl, err := b.replaceResource(ctx, stack, logicalID, res, existing, uc, params, phys)

		return updateOutcome{replaced: repl}, err
	}

	b.addEvent(stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateInProgress, "")

	_, err := b.creator.UpdateInPlace(res.Type, existing.PhysicalID, res.Properties, params, phys)
	if err != nil {
		b.addEvent(
			stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateFailed, err.Error(),
		)

		return updateOutcome{}, err
	}

	existing.Properties = res.Properties
	existing.DeletionPolicy = res.DeletionPolicy
	existing.Status = statusUpdateComplete
	existing.Timestamp = time.Now()
	b.addEvent(stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateComplete, "")

	return updateOutcome{}, nil
}

func (b *InMemoryBackend) replaceResource(
	ctx context.Context,
	stack *Stack,
	logicalID string,
	res TemplateResource,
	existing *StackResource,
	uc *updateContext,
	params, phys map[string]string,
) (*replacedResource, error) {
	if name, same := customNameUnchanged(res, existing, uc, params, phys); same {
		err := fmt.Errorf("%w. Rename %s and update the stack again", ErrCustomNamedReplacement, name)
		b.addEvent(
			stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateFailed, err.Error(),
		)

		return nil, err
	}

	b.addEvent(
		stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type,
		statusUpdateInProgress, reasonReplacementRequired,
	)

	old := *existing

	newPhysical, err := b.creator.Create(
		parentStackIDKey.Set(ctx, stack.StackID),
		logicalID,
		res.Type,
		res.Properties,
		params,
		phys,
	)
	if err != nil {
		b.addEvent(
			stack.StackID, stack.StackName, logicalID, "", res.Type, statusUpdateFailed, err.Error(),
		)

		return nil, err
	}

	b.addEvent(
		stack.StackID,
		stack.StackName,
		logicalID,
		newPhysical,
		res.Type,
		statusUpdateInProgress,
		reasonCreationInitiated,
	)

	b.resources[stack.StackID][logicalID] = &StackResource{
		Timestamp:      time.Now(),
		LogicalID:      logicalID,
		PhysicalID:     newPhysical,
		Type:           res.Type,
		Status:         statusUpdateComplete,
		Properties:     res.Properties,
		StackID:        stack.StackID,
		StackName:      stack.StackName,
		DeletionPolicy: res.DeletionPolicy,
	}
	phys[logicalID] = newPhysical

	b.addEvent(stack.StackID, stack.StackName, logicalID, newPhysical, res.Type, statusUpdateComplete, "")

	return &replacedResource{old: old, policy: res.UpdateReplacePolicy}, nil
}

// cleanupReplacedResources deletes (or, per UpdateReplacePolicy, retains) the
// old physical resources once the whole update has succeeded.
func (b *InMemoryBackend) cleanupReplacedResources(ctx context.Context, stack *Stack, replaced []*replacedResource) {
	if len(replaced) == 0 {
		return
	}

	physIDs := b.deleteResolveContext(stack)

	for _, r := range slices.Backward(replaced) {
		b.addEvent(
			stack.StackID, stack.StackName, r.old.LogicalID, r.old.PhysicalID, r.old.Type, statusDeleteInProgress, "",
		)

		if r.policy != deletionPolicyRetain && r.policy != deletionPolicySnapshot {
			if err := b.creator.Delete(ctx, r.old.Type, r.old.PhysicalID, r.old.Properties, physIDs); err != nil {
				b.addEvent(
					stack.StackID, stack.StackName, r.old.LogicalID, r.old.PhysicalID, r.old.Type,
					statusDeleteFailed, err.Error(),
				)

				continue
			}
		}

		b.addEvent(
			stack.StackID, stack.StackName, r.old.LogicalID, r.old.PhysicalID, r.old.Type, statusDeleteComplete, "",
		)
	}
}

func (b *InMemoryBackend) updateExtensibilityResource(
	ctx context.Context, stack *Stack, logicalID string, res TemplateResource, existing *StackResource,
) error {
	b.addEvent(stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateInProgress, "")

	err := b.creator.Update(ctx, logicalID, res.Type, existing.PhysicalID, res.Properties, existing.Properties)
	if err != nil {
		b.addEvent(
			stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateFailed, err.Error(),
		)

		return err
	}

	existing.Properties = res.Properties
	existing.Status = statusUpdateComplete
	existing.Timestamp = time.Now()
	b.addEvent(stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateComplete, "")

	return nil
}

func newUpdateContext(oldTemplateBody string, oldParams []Parameter, oldPhys map[string]string) *updateContext {
	uc := &updateContext{oldPhys: maps.Clone(oldPhys)}

	if oldTemplateBody == "" {
		return uc
	}

	tmpl, err := ParseTemplate(oldTemplateBody)
	if err != nil {
		return uc
	}

	uc.oldTmpl = tmpl
	uc.oldParams = ResolveParameters(tmpl, oldParams)

	return uc
}
