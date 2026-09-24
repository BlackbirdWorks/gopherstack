package cloudformation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

const (
	resTypeSSMMaintenanceWindowTarget = "AWS::SSM::MaintenanceWindowTarget"
	resTypeSSMMaintenanceWindowTask   = "AWS::SSM::MaintenanceWindowTask"
	resTypeSSMResourcePolicy          = "AWS::SSM::ResourcePolicy"
)

func (rc *ResourceCreator) createSSMMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeSSMMaintenanceWindowTarget:
		id, err := rc.createSSMMaintenanceWindowTarget(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSSMMaintenanceWindowTask:
		id, err := rc.createSSMMaintenanceWindowTask(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SSM::PatchBaseline":
		id, err := rc.createSSMPatchBaseline(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SSM::ResourceDataSync":
		id, err := rc.createSSMResourceDataSync(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSSMResourcePolicy:
		id, err := rc.createSSMResourcePolicy(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteSSMMoreResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::SSM::PatchBaseline":
		return true, rc.deleteSSMPatchBaseline(ctx, physicalID)
	case "AWS::SSM::ResourceDataSync":
		return true, rc.deleteSSMResourceDataSync(ctx, physicalID)
	default:
		return false, nil
	}
}

// windowTargetsProp decodes a Targets-shaped array property ([{Key, Values}, ...]),
// shared by MaintenanceWindowTarget and MaintenanceWindowTask.
func windowTargetsProp(props map[string]any, params, physicalIDs map[string]string) []ssmbackend.WindowTarget {
	raw, ok := props["Targets"].([]any)
	if !ok {
		return nil
	}

	out := make([]ssmbackend.WindowTarget, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		out = append(out, ssmbackend.WindowTarget{
			Key:    strProp(m, "Key", params, physicalIDs),
			Values: strSliceProp(m["Values"], params, physicalIDs),
		})
	}

	return out
}

// ---- SSM MaintenanceWindowTarget ----

func (rc *ResourceCreator) createSSMMaintenanceWindowTarget(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	out, err := rc.backends.SSM.Backend.RegisterTargetWithMaintenanceWindow(
		ctx,
		&ssmbackend.RegisterTargetWithMaintenanceWindowInput{
			WindowID:     strProp(props, "WindowId", params, physicalIDs),
			ResourceType: strProp(props, "ResourceType", params, physicalIDs),
			OwnerInfo:    strProp(props, "OwnerInformation", params, physicalIDs),
			Name:         strProp(props, "Name", params, physicalIDs),
			Description:  strProp(props, "Description", params, physicalIDs),
			Targets:      windowTargetsProp(props, params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf("register SSM maintenance window target: %w", err)
	}

	return out.WindowTargetID, nil
}

// deleteSSMMaintenanceWindowTarget deregisters a target. WindowId is a real CFN
// property of this resource type but isn't embedded in the target's physical ID
// (WindowTargetId alone), so it comes from props, same as deleteEKSAccessEntry.
func (rc *ResourceCreator) deleteSSMMaintenanceWindowTarget(
	ctx context.Context,
	props map[string]any, stackPhysicalIDs map[string]string, physicalID string,
) error {
	if rc.backends.SSM == nil {
		return nil
	}

	_, err := rc.backends.SSM.Backend.DeregisterTargetFromMaintenanceWindow(
		ctx,
		&ssmbackend.DeregisterTargetFromMaintenanceWindowInput{
			WindowID:       strProp(props, "WindowId", nil, stackPhysicalIDs),
			WindowTargetID: physicalID,
		},
	)
	if errors.Is(err, ssmbackend.ErrMaintenanceWindowNotFound) {
		return nil
	}

	return err
}

// ---- SSM MaintenanceWindowTask ----

func (rc *ResourceCreator) createSSMMaintenanceWindowTask(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	out, err := rc.backends.SSM.Backend.RegisterTaskWithMaintenanceWindow(
		ctx,
		&ssmbackend.RegisterTaskWithMaintenanceWindowInput{
			WindowID:       strProp(props, "WindowId", params, physicalIDs),
			TaskArn:        strProp(props, "TaskArn", params, physicalIDs),
			TaskType:       strProp(props, "TaskType", params, physicalIDs),
			Name:           strProp(props, "Name", params, physicalIDs),
			Description:    strProp(props, "Description", params, physicalIDs),
			ServiceRoleArn: strProp(props, "ServiceRoleArn", params, physicalIDs),
			MaxConcurrency: strProp(props, "MaxConcurrency", params, physicalIDs),
			MaxErrors:      strProp(props, "MaxErrors", params, physicalIDs),
			CutoffBehavior: strProp(props, "CutoffBehavior", params, physicalIDs),
			Targets:        windowTargetsProp(props, params, physicalIDs),
			Priority:       int32Prop(props, "Priority", params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf("register SSM maintenance window task: %w", err)
	}

	return out.WindowTaskID, nil
}

// deleteSSMMaintenanceWindowTask deregisters a task; see
// deleteSSMMaintenanceWindowTarget for why WindowId comes from props.
func (rc *ResourceCreator) deleteSSMMaintenanceWindowTask(
	ctx context.Context,
	props map[string]any, stackPhysicalIDs map[string]string, physicalID string,
) error {
	if rc.backends.SSM == nil {
		return nil
	}

	_, err := rc.backends.SSM.Backend.DeregisterTaskFromMaintenanceWindow(
		ctx,
		&ssmbackend.DeregisterTaskFromMaintenanceWindowInput{
			WindowID:     strProp(props, "WindowId", nil, stackPhysicalIDs),
			WindowTaskID: physicalID,
		},
	)
	if errors.Is(err, ssmbackend.ErrMaintenanceWindowNotFound) {
		return nil
	}

	return err
}

// ---- SSM PatchBaseline ----

func (rc *ResourceCreator) createSSMPatchBaseline(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	out, err := rc.backends.SSM.Backend.CreatePatchBaseline(ctx, &ssmbackend.CreatePatchBaselineInput{
		Name:                  strProp(props, "Name", params, physicalIDs),
		Description:           strProp(props, "Description", params, physicalIDs),
		OperatingSystem:       strProp(props, "OperatingSystem", params, physicalIDs),
		ApprovedPatches:       strSliceProp(props["ApprovedPatches"], params, physicalIDs),
		RejectedPatches:       strSliceProp(props["RejectedPatches"], params, physicalIDs),
		RejectedPatchesAction: strProp(props, "RejectedPatchesAction", params, physicalIDs),
		ApprovedPatchesComplianceLevel: strProp(
			props, "ApprovedPatchesComplianceLevel", params, physicalIDs,
		),
	})
	if err != nil {
		return "", fmt.Errorf("create SSM patch baseline: %w", err)
	}

	return out.BaselineID, nil
}

func (rc *ResourceCreator) deleteSSMPatchBaseline(ctx context.Context, baselineID string) error {
	if rc.backends.SSM == nil {
		return nil
	}

	_, err := rc.backends.SSM.Backend.DeletePatchBaseline(ctx, &ssmbackend.DeletePatchBaselineInput{
		BaselineID: baselineID,
	})

	return err
}

// ---- SSM ResourceDataSync ----

func (rc *ResourceCreator) createSSMResourceDataSync(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	syncName := strProp(props, "SyncName", params, physicalIDs)
	if syncName == "" {
		syncName = logicalID
	}

	input := &ssmbackend.CreateResourceDataSyncInput{
		SyncName: syncName,
		SyncType: strProp(props, "SyncType", params, physicalIDs),
	}

	if bucket := strProp(props, "BucketName", params, physicalIDs); bucket != "" {
		input.S3Destination = &ssmbackend.ResourceDataSyncS3Destination{
			BucketName:   bucket,
			Prefix:       strProp(props, "BucketPrefix", params, physicalIDs),
			Region:       strProp(props, "BucketRegion", params, physicalIDs),
			SyncFormat:   strProp(props, "SyncFormat", params, physicalIDs),
			AWSKMSKeyARN: strProp(props, "KMSKeyArn", params, physicalIDs),
		}
	}

	_, err := rc.backends.SSM.Backend.CreateResourceDataSync(ctx, input)
	if err != nil {
		return "", fmt.Errorf("create SSM resource data sync %s: %w", syncName, err)
	}

	return syncName, nil
}

func (rc *ResourceCreator) deleteSSMResourceDataSync(ctx context.Context, syncName string) error {
	if rc.backends.SSM == nil {
		return nil
	}

	_, err := rc.backends.SSM.Backend.DeleteResourceDataSync(ctx, &ssmbackend.DeleteResourceDataSyncInput{
		SyncName: syncName,
	})
	if errors.Is(err, ssmbackend.ErrResourceDataSyncNotFound) {
		return nil
	}

	return err
}

// ---- SSM ResourcePolicy ----

func (rc *ResourceCreator) createSSMResourcePolicy(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	out, err := rc.backends.SSM.Backend.PutResourcePolicy(ctx, &ssmbackend.PutResourcePolicyInput{
		ResourceARN: strProp(props, "ResourceArn", params, physicalIDs),
		Policy:      jsonProp(props, "Policy"),
	})
	if err != nil {
		return "", fmt.Errorf("put SSM resource policy: %w", err)
	}

	physicalIDs[logicalID+"/PolicyHash"] = out.PolicyHash

	return out.PolicyID, nil
}

// deleteSSMResourcePolicy looks up the policy's current PolicyHash via
// GetResourcePolicies before deleting -- DeleteResourcePolicyInput requires it
// for optimistic concurrency (api_op_DeleteResourcePolicy.go) and it isn't a
// CFN property of this resource, so it can't come from props like ResourceArn.
func (rc *ResourceCreator) deleteSSMResourcePolicy(
	ctx context.Context,
	props map[string]any, stackPhysicalIDs map[string]string, physicalID string,
) error {
	if rc.backends.SSM == nil {
		return nil
	}

	resourceARN := strProp(props, "ResourceArn", nil, stackPhysicalIDs)

	policies, err := rc.backends.SSM.Backend.GetResourcePolicies(ctx, &ssmbackend.GetResourcePoliciesInput{
		ResourceARN: resourceARN,
	})
	if err != nil {
		return err
	}

	var hash string

	for _, p := range policies.Policies {
		if p.PolicyID == physicalID {
			hash = p.PolicyHash

			break
		}
	}

	if hash == "" {
		return nil
	}

	_, err = rc.backends.SSM.Backend.DeleteResourcePolicy(ctx, &ssmbackend.DeleteResourcePolicyInput{
		ResourceARN: resourceARN,
		PolicyID:    physicalID,
		PolicyHash:  hash,
	})
	if errors.Is(err, ssmbackend.ErrResourcePolicyNotFound) {
		return nil
	}

	return err
}

// jsonProp reads a property that may be a JSON-typed value (map/array/string)
// and returns it as a JSON string, matching AWS::SSM::ResourcePolicy's Policy
// property (CFN type "Json").
func jsonProp(props map[string]any, key string) string {
	v, ok := props[key]
	if !ok {
		return ""
	}

	if s, isStr := v.(string); isStr {
		return s
	}

	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}

	return string(b)
}
