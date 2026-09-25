package cloudformation

import (
	"context"
	"fmt"

	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
)

const (
	resTypeBatchSchedulingPolicy   = "AWS::Batch::SchedulingPolicy"
	resTypeBatchServiceEnvironment = "AWS::Batch::ServiceEnvironment"
)

// createBatchMoreResource handles the Batch resource types listed above.
// Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createBatchMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeBatchSchedulingPolicy:
		id, err := rc.createBatchSchedulingPolicy(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeBatchServiceEnvironment:
		id, err := rc.createBatchServiceEnvironment(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteBatchMoreResource handles deletion for the types created above; both
// delete by the ARN-shaped physicalID directly.
func (rc *ResourceCreator) deleteBatchMoreResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if rc.backends.Batch == nil {
		switch resourceType {
		case resTypeBatchSchedulingPolicy, resTypeBatchServiceEnvironment:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeBatchSchedulingPolicy:
		return true, rc.backends.Batch.Backend.DeleteSchedulingPolicy(ctx, physicalID)
	case resTypeBatchServiceEnvironment:
		return true, rc.backends.Batch.Backend.DeleteServiceEnvironment(ctx, physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Batch::SchedulingPolicy ----
// Ref returns the scheduling policy ARN (documented).

func (rc *ResourceCreator) createBatchSchedulingPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Batch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	sp, err := rc.backends.Batch.Backend.CreateSchedulingPolicy(
		ctx, name, tagListProp(props, params, physicalIDs), nil, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create Batch scheduling policy %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = sp.Arn

	return sp.Arn, nil
}

// ---- AWS::Batch::ServiceEnvironment ----
// Ref returns the service environment ARN (documented).

func (rc *ResourceCreator) createBatchServiceEnvironment(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Batch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ServiceEnvironmentName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	se, err := rc.backends.Batch.Backend.CreateServiceEnvironment(
		ctx,
		name,
		strProp(props, "ServiceEnvironmentType", params, physicalIDs),
		strProp(props, "State", params, physicalIDs),
		batchCapacityLimits(props["CapacityLimits"], params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Batch service environment %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ServiceEnvironmentArn"] = se.ServiceEnvironmentArn

	return se.ServiceEnvironmentArn, nil
}

// batchCapacityLimits converts a CFN CapacityLimits property (a list of
// {MaxCapacity, CapacityUnit} maps) into []batchbackend.CapacityLimit.
func batchCapacityLimits(v any, params, physicalIDs map[string]string) []batchbackend.CapacityLimit {
	list, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make([]batchbackend.CapacityLimit, 0, len(list))

	for _, item := range list {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		out = append(out, batchbackend.CapacityLimit{
			CapacityUnit: strProp(m, "CapacityUnit", params, physicalIDs),
			MaxCapacity:  int32Prop(m, "MaxCapacity", params, physicalIDs),
		})
	}

	return out
}
