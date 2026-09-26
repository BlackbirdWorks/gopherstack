package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
)

const (
	resTypeElastiCacheParameterGroup       = "AWS::ElastiCache::ParameterGroup"
	resTypeElastiCacheSecurityGroup        = "AWS::ElastiCache::SecurityGroup"
	resTypeElastiCacheGlobalReplicationGrp = "AWS::ElastiCache::GlobalReplicationGroup"
	resTypeElastiCacheUserGroup            = "AWS::ElastiCache::UserGroup"
)

func (rc *ResourceCreator) createElastiCacheMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeElastiCacheParameterGroup:
		id, err := rc.createElastiCacheParameterGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeElastiCacheSecurityGroup:
		id, err := rc.createElastiCacheSecurityGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeElastiCacheGlobalReplicationGrp:
		id, err := rc.createElastiCacheGlobalReplicationGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeElastiCacheUserGroup:
		id, err := rc.createElastiCacheUserGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteElastiCacheMoreResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case resTypeElastiCacheParameterGroup:
		return true, rc.deleteElastiCacheParameterGroup(ctx, physicalID)
	case resTypeElastiCacheSecurityGroup:
		return true, rc.deleteElastiCacheSecurityGroup(ctx, physicalID)
	case resTypeElastiCacheGlobalReplicationGrp:
		return true, rc.deleteElastiCacheGlobalReplicationGroup(ctx, physicalID)
	case resTypeElastiCacheUserGroup:
		return true, rc.deleteElastiCacheUserGroup(ctx, physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::ElastiCache::ParameterGroup ----
// Ref returns the resource name; there is no name property, so (matching
// AWS::ElastiCache::SecurityGroup's own precedent) one is generated from the
// logical ID. Fn::GetAtt CacheParameterGroupName is stashed.

func (rc *ResourceCreator) createElastiCacheParameterGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	name := strings.ToLower(logicalID)

	pg, err := rc.backends.ElastiCache.Backend.CreateParameterGroup(
		ctx, name,
		strProp(props, "CacheParameterGroupFamily", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache parameter group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/CacheParameterGroupName"] = pg.Name

	return pg.Name, nil
}

func (rc *ResourceCreator) deleteElastiCacheParameterGroup(ctx context.Context, name string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	err := rc.backends.ElastiCache.Backend.DeleteParameterGroup(ctx, name)
	if errors.Is(err, elasticachebackend.ErrParameterGroupNotFound) {
		return nil
	}

	return err
}

// ---- AWS::ElastiCache::SecurityGroup ----
// Ref returns the resource name, generated from the logical ID (there is no
// name property).

func (rc *ResourceCreator) createElastiCacheSecurityGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	name := strings.ToLower(logicalID)

	sg, err := rc.backends.ElastiCache.Backend.CreateCacheSecurityGroup(
		ctx, name, strProp(props, "Description", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache security group %s: %w", name, err)
	}

	return sg.Name, nil
}

func (rc *ResourceCreator) deleteElastiCacheSecurityGroup(ctx context.Context, name string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	err := rc.backends.ElastiCache.Backend.DeleteCacheSecurityGroup(ctx, name)
	if errors.Is(err, elasticachebackend.ErrCacheSecurityGroupNotFound) {
		return nil
	}

	return err
}

// ---- AWS::ElastiCache::GlobalReplicationGroup ----
// Ref returns the resource name (GlobalReplicationGroupId); Fn::GetAtt
// Status is stashed.

func (rc *ResourceCreator) createElastiCacheGlobalReplicationGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	var primary string
	if members, ok := props["Members"].([]any); ok && len(members) > 0 {
		if m, isMap := members[0].(map[string]any); isMap {
			primary = strProp(m, "ReplicationGroupId", params, physicalIDs)
		}
	}

	grg, err := rc.backends.ElastiCache.Backend.CreateGlobalReplicationGroup(
		ctx,
		strProp(props, "GlobalReplicationGroupIdSuffix", params, physicalIDs),
		strProp(props, "GlobalReplicationGroupDescription", params, physicalIDs),
		primary,
	)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache global replication group: %w", err)
	}

	physicalIDs[logicalID+"/Status"] = grg.Status

	return grg.GlobalReplicationGroupID, nil
}

func (rc *ResourceCreator) deleteElastiCacheGlobalReplicationGroup(ctx context.Context, id string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	_, err := rc.backends.ElastiCache.Backend.DeleteGlobalReplicationGroup(ctx, id, true)
	if errors.Is(err, elasticachebackend.ErrGlobalReplicationGroupNotFound) {
		return nil
	}

	return err
}

// ---- AWS::ElastiCache::UserGroup ----
// Ref returns the resource name (UserGroupId); Fn::GetAtt Arn/Status are
// stashed.

func (rc *ResourceCreator) createElastiCacheUserGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	groupID := strProp(props, "UserGroupId", params, physicalIDs)
	if groupID == "" {
		groupID = strings.ToLower(logicalID)
	}

	ug, err := rc.backends.ElastiCache.Backend.CreateUserGroup(
		ctx, groupID,
		strProp(props, "Engine", params, physicalIDs),
		strSliceProp(props["UserIds"], params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache user group %s: %w", groupID, err)
	}

	physicalIDs[logicalID+"/Arn"] = ug.ARN
	physicalIDs[logicalID+"/Status"] = ug.Status

	return ug.UserGroupID, nil
}

func (rc *ResourceCreator) deleteElastiCacheUserGroup(ctx context.Context, groupID string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	_, err := rc.backends.ElastiCache.Backend.DeleteUserGroup(ctx, groupID)
	if errors.Is(err, elasticachebackend.ErrUserGroupNotFound) {
		return nil
	}

	return err
}
