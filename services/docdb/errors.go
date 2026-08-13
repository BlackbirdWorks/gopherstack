package docdb

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// Error code strings below are the literal wire values the real
// aws-sdk-go-v2/service/docdb query-protocol error deserializers switch on
// (see deserializers.go's awsAwsquery_deserializeOpError* functions for each
// operation), NOT the Go SDK type names (which all end in "...Fault"). AWS's
// DocDB API is inconsistent here: some resources put "Fault" on the wire code
// (DBCluster, DBClusterSnapshot, GlobalCluster, InvalidDBClusterStateFault,
// InvalidDBSubnetGroupStateFault), while others strip it (DBInstance,
// DBSubnetGroup-already-exists) or additionally drop "Cluster" and reuse the
// plain RDS DBParameterGroup codes for DBClusterParameterGroup operations
// (CreateDBClusterParameterGroup, ModifyDBClusterParameterGroup, etc. all
// switch on "DBParameterGroupNotFound"/"DBParameterGroupAlreadyExists"/
// "InvalidDBParameterGroupState", never "DBClusterParameterGroup...Fault").
// A wire code that doesn't match the SDK's switch falls through to
// smithy.GenericAPIError, so real callers doing errors.As against the typed
// fault (e.g. *types.DBInstanceNotFoundFault) silently stop matching.
var (
	ErrClusterNotFound                    = awserr.New("DBClusterNotFoundFault", awserr.ErrNotFound)
	ErrClusterAlreadyExists               = awserr.New("DBClusterAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrInstanceNotFound                   = awserr.New("DBInstanceNotFound", awserr.ErrNotFound)
	ErrInstanceAlreadyExists              = awserr.New("DBInstanceAlreadyExists", awserr.ErrAlreadyExists)
	ErrSubnetGroupNotFound                = awserr.New("DBSubnetGroupNotFoundFault", awserr.ErrNotFound)
	ErrSubnetGroupAlreadyExists           = awserr.New("DBSubnetGroupAlreadyExists", awserr.ErrAlreadyExists)
	ErrSubnetGroupInUse                   = awserr.New("InvalidDBSubnetGroupStateFault", awserr.ErrInvalidParameter)
	ErrClusterParameterGroupNotFound      = awserr.New("DBParameterGroupNotFound", awserr.ErrNotFound)
	ErrClusterParameterGroupAlreadyExists = awserr.New(
		"DBParameterGroupAlreadyExists",
		awserr.ErrAlreadyExists,
	)
	ErrParameterGroupInUse            = awserr.New("InvalidDBParameterGroupState", awserr.ErrInvalidParameter)
	ErrClusterSnapshotNotFound        = awserr.New("DBClusterSnapshotNotFoundFault", awserr.ErrNotFound)
	ErrClusterSnapshotAlreadyExists   = awserr.New("DBClusterSnapshotAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrEventSubscriptionNotFound      = awserr.New("SubscriptionNotFound", awserr.ErrNotFound)
	ErrEventSubscriptionAlreadyExists = awserr.New("SubscriptionAlreadyExist", awserr.ErrAlreadyExists)
	ErrGlobalClusterNotFound          = awserr.New("GlobalClusterNotFoundFault", awserr.ErrNotFound)
	ErrGlobalClusterAlreadyExists     = awserr.New("GlobalClusterAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrInvalidParameter               = awserr.New("InvalidParameterValue", awserr.ErrInvalidParameter)
	ErrUnknownAction                  = awserr.New("InvalidAction", awserr.ErrInvalidParameter)
	ErrInvalidClusterState            = awserr.New("InvalidDBClusterStateFault", awserr.ErrInvalidParameter)
	ErrInvalidInstanceState           = awserr.New("InvalidDBInstanceState", awserr.ErrInvalidParameter)
)
