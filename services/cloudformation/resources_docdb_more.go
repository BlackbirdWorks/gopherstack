package cloudformation

import (
	"context"
	"errors"
	"fmt"

	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
)

const (
	resTypeDocDBDBSubnetGroup           = "AWS::DocDB::DBSubnetGroup"
	resTypeDocDBDBClusterParameterGroup = "AWS::DocDB::DBClusterParameterGroup"
	resTypeDocDBGlobalCluster           = "AWS::DocDB::GlobalCluster"
)

func (rc *ResourceCreator) createDocDBMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeDocDBDBSubnetGroup:
		id, err := rc.createDocDBDBSubnetGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeDocDBDBClusterParameterGroup:
		id, err := rc.createDocDBDBClusterParameterGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeDocDBGlobalCluster:
		id, err := rc.createDocDBGlobalCluster(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteDocDBMoreResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case resTypeDocDBDBSubnetGroup:
		return true, rc.deleteDocDBDBSubnetGroup(ctx, physicalID)
	case resTypeDocDBDBClusterParameterGroup:
		return true, rc.deleteDocDBDBClusterParameterGroup(ctx, physicalID)
	case resTypeDocDBGlobalCluster:
		return true, rc.deleteDocDBGlobalCluster(ctx, physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::DocDB::DBSubnetGroup ----
// Ref returns the DBSubnetGroup's name.

func (rc *ResourceCreator) createDocDBDBSubnetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DocDB == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DBSubnetGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	sg, err := rc.backends.DocDB.Backend.CreateDBSubnetGroup(
		ctx, name,
		strProp(props, "DBSubnetGroupDescription", params, physicalIDs),
		"",
		strSliceProp(props["SubnetIds"], params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create DocDB DB subnet group %s: %w", name, err)
	}

	return sg.DBSubnetGroupName, nil
}

func (rc *ResourceCreator) deleteDocDBDBSubnetGroup(ctx context.Context, name string) error {
	if rc.backends.DocDB == nil {
		return nil
	}

	err := rc.backends.DocDB.Backend.DeleteDBSubnetGroup(ctx, name)
	if errors.Is(err, docdbbackend.ErrSubnetGroupNotFound) {
		return nil
	}

	return err
}

// ---- AWS::DocDB::DBClusterParameterGroup ----
// Ref returns the DBClusterParameterGroup's name.

func (rc *ResourceCreator) createDocDBDBClusterParameterGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DocDB == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	pg, err := rc.backends.DocDB.Backend.CreateDBClusterParameterGroup(
		ctx, name,
		strProp(props, "Family", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create DocDB DB cluster parameter group %s: %w", name, err)
	}

	return pg.DBClusterParameterGroupName, nil
}

func (rc *ResourceCreator) deleteDocDBDBClusterParameterGroup(ctx context.Context, name string) error {
	if rc.backends.DocDB == nil {
		return nil
	}

	err := rc.backends.DocDB.Backend.DeleteDBClusterParameterGroup(ctx, name)
	if errors.Is(err, docdbbackend.ErrClusterParameterGroupNotFound) {
		return nil
	}

	return err
}

// ---- AWS::DocDB::GlobalCluster ----
// Ref returns the resource name; Fn::GetAtt GlobalClusterArn is stashed.
// GlobalClusterResourceId is documented but this backend's GlobalCluster has
// no such field to report, so it is left unresolved rather than fabricated.

func (rc *ResourceCreator) createDocDBGlobalCluster(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DocDB == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "GlobalClusterIdentifier", params, physicalIDs)
	if id == "" {
		id = logicalID
	}

	gc, err := rc.backends.DocDB.Backend.CreateGlobalCluster(
		ctx, id,
		strProp(props, "SourceDBClusterIdentifier", params, physicalIDs),
		strProp(props, "Engine", params, physicalIDs),
		strProp(props, "EngineVersion", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create DocDB global cluster %s: %w", id, err)
	}

	physicalIDs[logicalID+"/GlobalClusterArn"] = gc.GlobalClusterArn

	return gc.GlobalClusterIdentifier, nil
}

func (rc *ResourceCreator) deleteDocDBGlobalCluster(ctx context.Context, id string) error {
	if rc.backends.DocDB == nil {
		return nil
	}

	_, err := rc.backends.DocDB.Backend.DeleteGlobalCluster(ctx, id)
	if errors.Is(err, docdbbackend.ErrGlobalClusterNotFound) {
		return nil
	}

	return err
}
