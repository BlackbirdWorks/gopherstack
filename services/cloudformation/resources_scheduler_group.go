package cloudformation

import (
	"context"
	"fmt"
	"time"

	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
)

const resTypeSchedulerScheduleGroup = "AWS::Scheduler::ScheduleGroup"

// createSchedulerGroupResource handles AWS::Scheduler::ScheduleGroup creation.
func (rc *ResourceCreator) createSchedulerGroupResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeSchedulerScheduleGroup {
		return "", false, nil
	}

	id, err := rc.createSchedulerScheduleGroup(ctx, logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteSchedulerGroupResource handles AWS::Scheduler::ScheduleGroup deletion.
func (rc *ResourceCreator) deleteSchedulerGroupResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if resourceType != resTypeSchedulerScheduleGroup {
		return false, nil
	}

	return true, rc.deleteSchedulerScheduleGroup(ctx, physicalID)
}

// ---- AWS::Scheduler::ScheduleGroup ----
// Ref returns the Name attribute (docs); Arn/CreationDate/LastModificationDate/
// State are backend-computed and stashed for Fn::GetAtt.

func (rc *ResourceCreator) createSchedulerScheduleGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Scheduler == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	g, err := rc.backends.Scheduler.Backend.CreateScheduleGroup(ctx, name, tagListProp(props, params, physicalIDs))
	if err != nil {
		return "", fmt.Errorf("create Scheduler schedule group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = g.ARN
	physicalIDs[logicalID+"/CreationDate"] = g.CreationDate.UTC().Format(time.RFC3339)
	physicalIDs[logicalID+"/LastModificationDate"] = g.LastModificationDate.UTC().Format(time.RFC3339)
	physicalIDs[logicalID+"/State"] = g.State

	return g.Name, nil
}

func (rc *ResourceCreator) deleteSchedulerScheduleGroup(ctx context.Context, physicalID string) error {
	if rc.backends.Scheduler == nil {
		return nil
	}

	err := rc.backends.Scheduler.Backend.DeleteScheduleGroup(ctx, physicalID)

	return ignoreNotFound(err, schedulerbackend.ErrNotFound)
}
