package cloudformation

import (
	"context"
	"fmt"

	cloudhsmv2backend "github.com/blackbirdworks/gopherstack/services/cloudhsmv2"
)

const (
	resTypeCloudHSMCluster = "AWS::CloudHSM::Cluster"

	hsmClusterCreating     = "CREATE_IN_PROGRESS"
	hsmClusterModifying    = "MODIFY_IN_PROGRESS"
	hsmClusterInitializing = "INITIALIZE_IN_PROGRESS"
	hsmStateActive         = "ACTIVE"
)

func (rc *ResourceCreator) createCloudHSMResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeCloudHSMCluster:
		id, err := rc.createCloudHSMCluster(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteCloudHSMResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeCloudHSMCluster {
		return false, nil
	}

	if rc.backends.CloudHSMV2 == nil {
		return true, nil
	}

	be := rc.backends.CloudHSMV2.Backend

	if err := be.CFNDeleteCluster(ctx, physicalID); err != nil {
		return true, ignoreNotFound(err, cloudhsmv2backend.ErrResourceNotFound)
	}

	return true, awaitGone("CloudHSM cluster "+physicalID, func() error {
		_, err := be.CFNDescribeCluster(ctx, physicalID)

		return err
	}, cloudhsmv2backend.ErrResourceNotFound)
}

func cloudHSMClusterSpec(props map[string]any, params, physicalIDs map[string]string) cloudhsmv2backend.CFNClusterSpec {
	policy := asMap(props["BackupRetentionPolicy"])

	return cloudhsmv2backend.CFNClusterSpec{
		HsmType:        strProp(props, "HsmType", params, physicalIDs),
		Mode:           strProp(props, "Mode", params, physicalIDs),
		NetworkType:    strProp(props, "NetworkType", params, physicalIDs),
		SubnetIDs:      strSliceProp(props["SubnetIds"], params, physicalIDs),
		RetentionType:  strProp(policy, "Type", params, physicalIDs),
		RetentionValue: strProp(policy, "Value", params, physicalIDs),
		Tags:           tagListProp(props, params, physicalIDs),
	}
}

func (rc *ResourceCreator) awaitCloudHSMClusterSettled(
	ctx context.Context, id string,
) (cloudhsmv2backend.CFNCluster, error) {
	var out cloudhsmv2backend.CFNCluster

	err := awaitResource("CloudHSM cluster "+id, func() (bool, error) {
		c, err := rc.backends.CloudHSMV2.Backend.CFNDescribeCluster(ctx, id)
		if err != nil {
			return false, err
		}

		out = c

		return c.State != hsmClusterCreating && c.State != hsmClusterModifying && c.State != hsmClusterInitializing, nil
	})

	return out, err
}

// createCloudHSMCluster provisions an AWS::CloudHSM::Cluster. Ref returns the ClusterId; the other attributes
// named by the resource specification are stashed for Fn::GetAtt.
func (rc *ResourceCreator) createCloudHSMCluster(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudHSMV2 == nil {
		return logicalID + "-stub", nil
	}

	spec := cloudHSMClusterSpec(props, params, physicalIDs)

	created, err := rc.backends.CloudHSMV2.Backend.CFNCreateCluster(ctx, spec)
	if err != nil {
		return "", fmt.Errorf("create CloudHSM cluster %s: %w", logicalID, err)
	}

	settled, err := rc.awaitCloudHSMClusterSettled(ctx, created.ID)
	if err != nil {
		return "", fmt.Errorf("create CloudHSM cluster %s: %w", logicalID, err)
	}

	physicalIDs[logicalID+"/Arn"] = settled.ARN
	physicalIDs[logicalID+"/VpcId"] = settled.VpcID
	physicalIDs[logicalID+"/SecurityGroup"] = settled.SecurityGroup
	physicalIDs[logicalID+"/BackupPolicy"] = settled.BackupPolicy
	physicalIDs[logicalID+"/State"] = settled.State

	return created.ID, nil
}

func (rc *ResourceCreator) updateCloudHSMCluster(
	ctx context.Context, physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	if rc.backends.CloudHSMV2 == nil {
		return nil
	}

	if _, err := rc.awaitCloudHSMClusterSettled(ctx, physicalID); err != nil {
		return fmt.Errorf("update CloudHSM cluster %s: %w", physicalID, err)
	}

	spec := cloudHSMClusterSpec(props, params, physicalIDs)
	if err := rc.backends.CloudHSMV2.Backend.CFNUpdateCluster(ctx, physicalID, spec); err != nil {
		return fmt.Errorf("update CloudHSM cluster %s: %w", physicalID, err)
	}

	return nil
}
