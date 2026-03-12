package memorydb

// ExportedCluster is a compatibility alias used by the dashboard package.
type ExportedCluster = Cluster

// The following exported aliases are used by external tests.

// ExportedCreateClusterRequest aliases createClusterRequest for testing.
type ExportedCreateClusterRequest = createClusterRequest

// ExportedCreateACLRequest aliases createACLRequest for testing.
type ExportedCreateACLRequest = createACLRequest

// ExportedCreateSubnetGroupRequest aliases createSubnetGroupRequest for testing.
type ExportedCreateSubnetGroupRequest = createSubnetGroupRequest

// ExportedCreateUserRequest aliases createUserRequest for testing.
type ExportedCreateUserRequest = createUserRequest

// ExportedAuthModeReq aliases authenticationModeReq for testing.
type ExportedAuthModeReq = authenticationModeReq

// ExportedCreateParameterGroupRequest aliases createParameterGroupRequest for testing.
type ExportedCreateParameterGroupRequest = createParameterGroupRequest
