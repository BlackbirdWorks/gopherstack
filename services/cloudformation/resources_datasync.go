package cloudformation

import (
	"context"
	"fmt"

	datasyncbackend "github.com/blackbirdworks/gopherstack/services/datasync"
)

const (
	resTypeDataSyncAgent      = "AWS::DataSync::Agent"
	resTypeDataSyncLocationS3 = "AWS::DataSync::LocationS3"
	resTypeDataSyncTask       = "AWS::DataSync::Task"
)

// createDataSyncResource handles the DataSync resource types listed above.
// Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createDataSyncResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeDataSyncAgent:
		id, err := rc.createDataSyncAgent(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeDataSyncLocationS3:
		id, err := rc.createDataSyncLocationS3(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeDataSyncTask:
		id, err := rc.createDataSyncTask(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteDataSyncResource handles deletion for the types created above. Agent
// and LocationS3 both delete by their ARN-shaped physicalID directly;
// LocationS3 shares DataSync's generic DeleteLocation.
func (rc *ResourceCreator) deleteDataSyncResource(
	_ context.Context, resourceType, physicalID string,
) (bool, error) {
	if rc.backends.DataSync == nil {
		switch resourceType {
		case resTypeDataSyncAgent, resTypeDataSyncLocationS3, resTypeDataSyncTask:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeDataSyncAgent:
		return true, ignoreNotFound(rc.backends.DataSync.Backend.DeleteAgent(physicalID), datasyncbackend.ErrNotFound)
	case resTypeDataSyncLocationS3:
		return true, ignoreNotFound(
			rc.backends.DataSync.Backend.DeleteLocation(physicalID), datasyncbackend.ErrNotFound,
		)
	case resTypeDataSyncTask:
		return true, ignoreNotFound(rc.backends.DataSync.Backend.DeleteTask(physicalID), datasyncbackend.ErrNotFound)
	default:
		return false, nil
	}
}

// ---- AWS::DataSync::Agent ----
// Ref returns the agent ARN (documented).

func (rc *ResourceCreator) createDataSyncAgent(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DataSync == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "AgentName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	a, err := rc.backends.DataSync.Backend.CreateAgent(
		name, strProp(props, "ActivationKey", params, physicalIDs), tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create DataSync agent %s: %w", name, err)
	}

	physicalIDs[logicalID+"/AgentArn"] = a.AgentArn
	physicalIDs[logicalID+"/EndpointType"] = a.EndpointType

	return a.AgentArn, nil
}

// ---- AWS::DataSync::LocationS3 ----
// Ref returns the location ARN (documented).

func (rc *ResourceCreator) createDataSyncLocationS3(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DataSync == nil {
		return logicalID + "-stub", nil
	}

	s3Config, _ := props["S3Config"].(map[string]any)

	l, err := rc.backends.DataSync.Backend.CreateLocationS3(
		strProp(props, "Subdirectory", params, physicalIDs),
		strProp(props, "S3BucketArn", params, physicalIDs),
		strProp(props, "S3StorageClass", params, physicalIDs),
		datasyncbackend.S3Config{
			BucketAccessRoleArn: strProp(s3Config, "BucketAccessRoleArn", params, physicalIDs),
		},
		strSliceProp(props["AgentArns"], params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create DataSync S3 location: %w", err)
	}

	physicalIDs[logicalID+"/LocationArn"] = l.LocationArn
	physicalIDs[logicalID+"/LocationUri"] = l.LocationURI

	return l.LocationArn, nil
}

// ---- AWS::DataSync::Task ----
// Ref returns the task ARN. DestinationNetworkInterfaceArns and
// SourceNetworkInterfaceArns are documented Fn::GetAtt attributes this
// backend has no honest value for (it never provisions ENIs), so they are
// left unimplemented rather than fabricated.

func (rc *ResourceCreator) createDataSyncTask(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DataSync == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	options, _ := props["Options"].(map[string]any)

	t, err := rc.backends.DataSync.Backend.CreateTask(
		strProp(props, "SourceLocationArn", params, physicalIDs),
		strProp(props, "DestinationLocationArn", params, physicalIDs),
		name,
		strProp(props, "CloudWatchLogGroupArn", params, physicalIDs),
		datasyncbackend.TaskSettings{
			TaskMode: strProp(props, "TaskMode", params, physicalIDs),
			Options:  options,
		},
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create DataSync task %s: %w", name, err)
	}

	physicalIDs[logicalID+"/TaskArn"] = t.TaskArn
	physicalIDs[logicalID+"/Status"] = t.Status

	return t.TaskArn, nil
}
