package cloudformation

import (
	"context"
	"fmt"

	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
)

const resTypeElastiCacheUser = "AWS::ElastiCache::User"

// createElastiCacheUserResource handles AWS::ElastiCache::User creation.
// Returns handled=false otherwise.
func (rc *ResourceCreator) createElastiCacheUserResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeElastiCacheUser {
		return "", false, nil
	}

	id, err := rc.createElastiCacheUser(ctx, logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteElastiCacheUserResource handles deletion for the type created above.
func (rc *ResourceCreator) deleteElastiCacheUserResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if resourceType != resTypeElastiCacheUser {
		return false, nil
	}

	if rc.backends.ElastiCache == nil {
		return true, nil
	}

	_, err := rc.backends.ElastiCache.Backend.DeleteUser(ctx, physicalID)

	return true, ignoreNotFound(err, elasticachebackend.ErrUserNotFound)
}

// ---- AWS::ElastiCache::User ----
// Ref returns the resource name (the UserId, documented). Arn and Status
// are stashed from the backend's real values.

func (rc *ResourceCreator) createElastiCacheUser(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	userID := strProp(props, "UserId", params, physicalIDs)
	if userID == "" {
		userID = logicalID
	}

	u, err := rc.backends.ElastiCache.Backend.CreateUser(
		ctx,
		userID,
		strProp(props, "UserName", params, physicalIDs),
		strProp(props, "AccessString", params, physicalIDs),
		strProp(props, "Engine", params, physicalIDs),
		boolProp(props, "NoPasswordRequired"),
	)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache user %s: %w", userID, err)
	}

	physicalIDs[logicalID+"/Arn"] = u.ARN
	physicalIDs[logicalID+"/Status"] = u.Status

	return u.UserID, nil
}
