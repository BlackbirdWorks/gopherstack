package dax

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// Sentinel errors for the DAX backend.
var (
	// ErrClusterNotFound is returned when a cluster does not exist.
	ErrClusterNotFound = awserr.New("ClusterNotFoundFault", awserr.ErrNotFound)
	// ErrClusterAlreadyExists is returned when a cluster with the same name already exists.
	ErrClusterAlreadyExists = awserr.New("ClusterAlreadyExistsFault", awserr.ErrConflict)
	// ErrParameterGroupNotFound is returned when a parameter group does not exist.
	ErrParameterGroupNotFound = awserr.New("ParameterGroupNotFoundFault", awserr.ErrNotFound)
	// ErrParameterGroupAlreadyExists is returned when a parameter group already exists.
	ErrParameterGroupAlreadyExists = awserr.New(
		"ParameterGroupAlreadyExistsFault",
		awserr.ErrConflict,
	)
	// ErrSubnetGroupNotFound is returned when a subnet group does not exist.
	ErrSubnetGroupNotFound = awserr.New("SubnetGroupNotFoundFault", awserr.ErrNotFound)
	// ErrSubnetGroupAlreadyExists is returned when a subnet group already exists.
	ErrSubnetGroupAlreadyExists = awserr.New("SubnetGroupAlreadyExistsFault", awserr.ErrConflict)
	// ErrInvalidClusterState is returned when an operation is not valid for the cluster state.
	ErrInvalidClusterState = awserr.New("InvalidClusterStateFault", awserr.ErrConflict)
	// ErrTagNotFound is returned when a tag or resource is not found.
	ErrTagNotFound = awserr.New("TagNotFoundFault", awserr.ErrNotFound)
	// ErrInvalidARN is returned for invalid ARNs.
	ErrInvalidARN = awserr.New("InvalidARNFault", awserr.ErrInvalidParameter)
	// ErrInvalidParameterValue is returned when a parameter value is invalid.
	ErrInvalidParameterValue = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)
	// ErrInvalidParameterCombination is returned for invalid parameter combinations.
	ErrInvalidParameterCombination = awserr.New("InvalidParameterCombinationException", awserr.ErrInvalidParameter)
	// ErrNodeNotFound is returned when a node does not exist.
	ErrNodeNotFound = awserr.New("NodeNotFoundFault", awserr.ErrNotFound)
	// ErrTagQuotaExceeded is returned when adding tags would exceed the per-resource limit.
	ErrTagQuotaExceeded = awserr.New("TagQuotaPerResourceExceeded", awserr.ErrInvalidParameter)
	// ErrSubnetGroupInUse is returned when attempting to delete a subnet group used by a cluster.
	ErrSubnetGroupInUse = awserr.New("SubnetGroupInUseFault", awserr.ErrConflict)
	// ErrParameterGroupInUse is returned when attempting to delete a parameter group used by a cluster.
	ErrParameterGroupInUse = awserr.New("ParameterGroupInUseFault", awserr.ErrConflict)
	// ErrInvalidSubnet is returned when a supplied subnet ID is malformed or missing.
	ErrInvalidSubnet = awserr.New("InvalidSubnet", awserr.ErrInvalidParameter)
)
