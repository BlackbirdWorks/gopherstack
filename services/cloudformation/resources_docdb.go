package cloudformation

import (
	"context"
	"fmt"
	"strings"

	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
)

// ---- DocDB ----

func (rc *ResourceCreator) createDocDBCluster(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DocDB == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "DBClusterIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	engine := strProp(props, "Engine", params, physicalIDs)
	masterUser := strProp(props, "MasterUsername", params, physicalIDs)
	dbName := strProp(props, "DatabaseName", params, physicalIDs)
	paramGroupName := strProp(props, "DBClusterParameterGroupName", params, physicalIDs)

	cluster, err := rc.backends.DocDB.Backend.CreateDBCluster(
		ctx,
		id,
		engine,
		"",
		masterUser,
		"",
		dbName,
		paramGroupName,
		"",
		0,
		false,
		false,
		0,
		"",
		"",
		nil,
		nil,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create DocDB cluster %s: %w", id, err)
	}

	return cluster.DBClusterIdentifier, nil
}

func (rc *ResourceCreator) deleteDocDBCluster(ctx context.Context, arn string) error {
	if rc.backends.DocDB == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	_, err := rc.backends.DocDB.Backend.DeleteDBCluster(ctx, id,
		&docdbbackend.DeleteDBClusterOptions{SkipFinalSnapshot: true})

	return err
}

func (rc *ResourceCreator) createDocDBInstance(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DocDB == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "DBInstanceIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	clusterID := strProp(props, "DBClusterIdentifier", params, physicalIDs)
	instanceClass := strProp(props, "DBInstanceClass", params, physicalIDs)
	engine := strProp(props, "Engine", params, physicalIDs)

	instance, err := rc.backends.DocDB.Backend.CreateDBInstance(
		ctx,
		id,
		clusterID,
		instanceClass,
		engine,
		0,
		nil,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create DocDB instance %s: %w", id, err)
	}

	return instance.DBInstanceIdentifier, nil
}

func (rc *ResourceCreator) deleteDocDBInstance(ctx context.Context, arn string) error {
	if rc.backends.DocDB == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	_, err := rc.backends.DocDB.Backend.DeleteDBInstance(ctx, id)

	return err
}
