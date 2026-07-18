package cloudformation

import (
	"context"
	"fmt"
	"strings"
)

// ---- ElastiCache extensions ----

func (rc *ResourceCreator) createElastiCacheReplicationGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "ReplicationGroupId", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	description := strProp(props, "ReplicationGroupDescription", params, physicalIDs)

	rg, err := rc.backends.ElastiCache.Backend.CreateReplicationGroup(ctx, id, description)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache replication group %s: %w", id, err)
	}

	return rg.ReplicationGroupID, nil
}

func (rc *ResourceCreator) deleteElastiCacheReplicationGroup(ctx context.Context, id string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	return rc.backends.ElastiCache.Backend.DeleteReplicationGroup(ctx, id)
}

func (rc *ResourceCreator) createElastiCacheSubnetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "CacheSubnetGroupName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	description := strProp(props, "CacheSubnetGroupDescription", params, physicalIDs)

	var subnetIDs []string
	if list, ok := props["SubnetIds"].([]any); ok {
		for _, v := range list {
			if s := resolve(v, params, physicalIDs); s != "" {
				subnetIDs = append(subnetIDs, s)
			}
		}
	}

	grp, err := rc.backends.ElastiCache.Backend.CreateSubnetGroup(ctx, name, description, subnetIDs)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache subnet group %s: %w", name, err)
	}

	return grp.Name, nil
}

func (rc *ResourceCreator) deleteElastiCacheSubnetGroup(ctx context.Context, name string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	return rc.backends.ElastiCache.Backend.DeleteSubnetGroup(ctx, name)
}
