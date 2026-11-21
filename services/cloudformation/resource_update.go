package cloudformation

import (
	"context"
	"errors"
	"maps"
	"reflect"
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

	retainExceptOnCreate bool
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
	case resTypeELBv2LB:
		return "Name"
	}

	return ""
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

	keys := make(map[string]struct{})
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

func (b *InMemoryBackend) replaceResource(
	ctx context.Context,
	stack *Stack,
	logicalID string,
	res TemplateResource,
	existing *StackResource,
	pass *updatePass,
) (*replacedRecord, error) {
	old := *existing

	newPhysical, err := b.creator.Create(ctx, logicalID, res.Type, res.Properties, pass.params, pass.phys)
	if err != nil {
		b.addResourceEvent(stack, logicalID, "", res.Type, statusUpdateFailed, err.Error())

		return nil, err
	}

	b.addResourceEvent(stack, logicalID, newPhysical, res.Type, statusUpdateInProgress, reasonCreationInitiated)

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
	pass.phys[logicalID] = newPhysical

	b.addResourceEvent(stack, logicalID, newPhysical, res.Type, statusUpdateComplete, "")

	return &replacedRecord{Old: old, Policy: res.UpdateReplacePolicy}, nil
}

func (b *InMemoryBackend) updateExtensibilityResource(
	ctx context.Context, stack *Stack, logicalID string, res TemplateResource, existing *StackResource,
) error {
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
