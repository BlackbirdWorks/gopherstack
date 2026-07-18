package cloudformation

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

// ---- EMR ----

func (rc *ResourceCreator) createEMRCluster(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EMR == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	releaseLabel := strProp(props, "ReleaseLabel", params, physicalIDs)
	if releaseLabel == "" {
		releaseLabel = "emr-6.0.0"
	}

	cluster, err := rc.backends.EMR.Backend.RunJobFlow(ctx, emr.RunJobFlowParams{
		Name:         name,
		ReleaseLabel: releaseLabel,
	})
	if err != nil {
		return "", fmt.Errorf("create EMR cluster %s: %w", name, err)
	}

	return cluster.ARN, nil
}

func (rc *ResourceCreator) deleteEMRCluster(ctx context.Context, arn string) error {
	if rc.backends.EMR == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	return rc.backends.EMR.Backend.TerminateJobFlows(ctx, []string{id})
}
