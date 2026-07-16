package elasticache

import "errors"

var (
	ErrClusterNotFound                    = errors.New("CacheClusterNotFound")
	ErrClusterAlreadyExists               = errors.New("CacheClusterAlreadyExists")
	ErrReplicationGroupNotFound           = errors.New("ReplicationGroupNotFound")
	ErrReplicationGroupAlreadyExists      = errors.New("ReplicationGroupAlreadyExists")
	ErrResourceNotFound                   = errors.New("resource not found")
	ErrParameterGroupNotFound             = errors.New("CacheParameterGroupNotFound")
	ErrParameterGroupAlreadyExists        = errors.New("CacheParameterGroupAlreadyExists")
	ErrParameterGroupDefaultNotModifiable = errors.New("default parameter group cannot be deleted or modified")
	ErrInvalidParameterGroupFamily        = errors.New("InvalidParameterGroupFamily")
	ErrSubnetGroupNotFound                = errors.New("CacheSubnetGroupNotFound")
	ErrSubnetGroupAlreadyExists           = errors.New("CacheSubnetGroupAlreadyExists")
	ErrSnapshotNotFound                   = errors.New("SnapshotNotFound")
	ErrSnapshotAlreadyExists              = errors.New("SnapshotAlreadyExistsFault")
	ErrInvalidSnapshotSource              = errors.New(
		"exactly one of CacheClusterId or ReplicationGroupId must be specified",
	)
)

var (
	ErrClusterModeRequired          = errors.New("cluster mode must be enabled for shard configuration changes")
	ErrDataTieringInvalid           = errors.New("data tiering requires r7g node type and Redis/Valkey 7.0+")
	ErrTransitEncryptionModeInvalid = errors.New("transit encryption mode 'required' requires an auth token")
	ErrAuthTokenRequiredForMode     = errors.New(
		"auth token must be provided when transit encryption mode is 'required'",
	)
)

// ----------------------------------------
// New model types (gaps #1–#15)
// ----------------------------------------

var (
	ErrUserGroupNotFound                  = errors.New("UserGroupNotFound")
	ErrUserGroupAlreadyExists             = errors.New("UserGroupAlreadyExistsFault")
	ErrReservedCacheNodeNotFound          = errors.New("ReservedCacheNodeNotFound")
	ErrReservedCacheNodeAlreadyExists     = errors.New("ReservedCacheNodeAlreadyExists")
	ErrReservedCacheNodesOfferingNotFound = errors.New("ReservedCacheNodesOfferingNotFound")
)

var (
	ErrUserNotInGroup    = errors.New("user is a member of a user group and cannot be deleted")
	ErrUserNotFound2     = ErrUserNotFound
	ErrGroupUserNotFound = errors.New("one or more specified user IDs do not exist")
)

// ----------------------------------------
// ServerlessCreateOpts holds all fields for full serverless cache creation.
// ----------------------------------------

var (
	ErrCacheSecurityGroupNotFound      = errors.New("CacheSecurityGroupNotFound")
	ErrCacheSecurityGroupAlreadyExists = errors.New("CacheSecurityGroupAlreadyExists")
	ErrGlobalReplicationGroupNotFound  = errors.New("GlobalReplicationGroupNotFound")
	ErrGlobalReplicationGroupExists    = errors.New("GlobalReplicationGroupAlreadyExistsFault")
	ErrServerlessCacheNotFound         = errors.New("ServerlessCacheNotFound")
	ErrServerlessCacheAlreadyExists    = errors.New("ServerlessCacheAlreadyExistsFault")
	ErrServerlessCacheSnapshotNotFound = errors.New("ServerlessCacheSnapshotNotFoundFault")
	ErrServerlessCacheSnapshotExists   = errors.New("ServerlessCacheSnapshotAlreadyExistsFault")
	ErrUserNotFound                    = errors.New("UserNotFound")
	ErrUserAlreadyExists               = errors.New("UserAlreadyExists")
)

// ----------------------------------------
// New model types
// ----------------------------------------
