package cloudformation

import (
	"context"
	"errors"
	"fmt"

	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
)

const (
	resTypeNeptuneDBSubnetGroup           = "AWS::Neptune::DBSubnetGroup"
	resTypeNeptuneDBClusterParameterGroup = "AWS::Neptune::DBClusterParameterGroup"
	resTypeNeptuneDBParameterGroup        = "AWS::Neptune::DBParameterGroup"
	resTypeNeptuneGlobalCluster           = "AWS::Neptune::GlobalCluster"
)

func (rc *ResourceCreator) createNeptuneMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeNeptuneDBSubnetGroup:
		id, err := rc.createNeptuneDBSubnetGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeNeptuneDBClusterParameterGroup:
		id, err := rc.createNeptuneDBClusterParameterGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeNeptuneDBParameterGroup:
		id, err := rc.createNeptuneDBParameterGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeNeptuneGlobalCluster:
		id, err := rc.createNeptuneGlobalCluster(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteNeptuneMoreResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case resTypeNeptuneDBSubnetGroup:
		return true, rc.deleteNeptuneDBSubnetGroup(ctx, physicalID)
	case resTypeNeptuneDBClusterParameterGroup:
		return true, rc.deleteNeptuneDBClusterParameterGroup(ctx, physicalID)
	case resTypeNeptuneDBParameterGroup:
		return true, rc.deleteNeptuneDBParameterGroup(ctx, physicalID)
	case resTypeNeptuneGlobalCluster:
		return true, rc.deleteNeptuneGlobalCluster(ctx, physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Neptune::DBSubnetGroup ----
// Ref returns the resource name.

func (rc *ResourceCreator) createNeptuneDBSubnetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Neptune == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DBSubnetGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	sg, err := rc.backends.Neptune.Backend.CreateDBSubnetGroup(
		ctx, name,
		strProp(props, "DBSubnetGroupDescription", params, physicalIDs),
		"",
		strSliceProp(props["SubnetIds"], params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Neptune DB subnet group %s: %w", name, err)
	}

	return sg.DBSubnetGroupName, nil
}

func (rc *ResourceCreator) deleteNeptuneDBSubnetGroup(ctx context.Context, name string) error {
	if rc.backends.Neptune == nil {
		return nil
	}

	err := rc.backends.Neptune.Backend.DeleteDBSubnetGroup(ctx, name)
	if errors.Is(err, neptunebackend.ErrSubnetGroupNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Neptune::DBClusterParameterGroup ----
// Ref returns the resource name.

func (rc *ResourceCreator) createNeptuneDBClusterParameterGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Neptune == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	pg, err := rc.backends.Neptune.Backend.CreateDBClusterParameterGroup(
		ctx, name,
		strProp(props, "Family", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Neptune DB cluster parameter group %s: %w", name, err)
	}

	return pg.DBClusterParameterGroupName, nil
}

func (rc *ResourceCreator) deleteNeptuneDBClusterParameterGroup(ctx context.Context, name string) error {
	if rc.backends.Neptune == nil {
		return nil
	}

	err := rc.backends.Neptune.Backend.DeleteDBClusterParameterGroup(ctx, name)
	if errors.Is(err, neptunebackend.ErrClusterParameterGroupNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Neptune::DBParameterGroup ----
// Ref returns the resource name.

func (rc *ResourceCreator) createNeptuneDBParameterGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Neptune == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	pg, err := rc.backends.Neptune.Backend.CreateDBParameterGroup(
		ctx, name,
		strProp(props, "Family", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Neptune DB parameter group %s: %w", name, err)
	}

	return pg.DBParameterGroupName, nil
}

func (rc *ResourceCreator) deleteNeptuneDBParameterGroup(ctx context.Context, name string) error {
	if rc.backends.Neptune == nil {
		return nil
	}

	err := rc.backends.Neptune.Backend.DeleteDBParameterGroup(ctx, name)
	if errors.Is(err, neptunebackend.ErrParameterGroupNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Neptune::GlobalCluster ----
// Ref returns the resource name; no Fn::GetAtt attributes are documented.

func (rc *ResourceCreator) createNeptuneGlobalCluster(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Neptune == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "GlobalClusterIdentifier", params, physicalIDs)
	if id == "" {
		id = logicalID
	}

	gc, err := rc.backends.Neptune.Backend.CreateGlobalCluster(
		ctx, id,
		strProp(props, "SourceDBClusterIdentifier", params, physicalIDs),
		"",
	)
	if err != nil {
		return "", fmt.Errorf("create Neptune global cluster %s: %w", id, err)
	}

	return gc.GlobalClusterIdentifier, nil
}

func (rc *ResourceCreator) deleteNeptuneGlobalCluster(ctx context.Context, id string) error {
	if rc.backends.Neptune == nil {
		return nil
	}

	_, err := rc.backends.Neptune.Backend.DeleteGlobalCluster(ctx, id)
	if errors.Is(err, neptunebackend.ErrGlobalClusterNotFound) {
		return nil
	}

	return err
}
