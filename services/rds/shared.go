package rds

import (
	"maps"
	"slices"
)

const (
	// filterNameDBClusterID is the AWS Filters.Filter.N.Name value for
	// narrowing a Describe* result set by DB cluster identifier. Shared by
	// DescribeDBInstances, DescribeDBClusters, and DescribeDBClusterSnapshots.
	filterNameDBClusterID = "db-cluster-id"
	// filterNameDBInstanceID is the AWS Filters.Filter.N.Name value for
	// narrowing a Describe* result set by DB instance identifier. Shared by
	// DescribeDBInstances and DescribeDBSnapshots.
	filterNameDBInstanceID = "db-instance-id"
	// filterNameEngine is the AWS Filters.Filter.N.Name value for narrowing
	// a Describe* result set by database engine name. Shared by
	// DescribeDBInstances, DescribeDBClusters, DescribeDBSnapshots, and
	// DescribeDBClusterSnapshots.
	filterNameEngine = "engine"
	// filterNameDomain is the AWS Filters.Filter.N.Name value for narrowing
	// a Describe* result set by Active Directory domain. Accepted but not
	// modeled as a real predicate — this emulator has no Directory Service
	// domain-membership state. Shared by DescribeDBInstances and
	// DescribeDBClusters.
	filterNameDomain = "domain"
	// filterNameDbiResourceID is the AWS Filters.Filter.N.Name value for
	// narrowing a Describe* result set by DB instance resource ID. Shared by
	// DescribeDBInstances and DescribeDBSnapshots.
	filterNameDbiResourceID = "dbi-resource-id"
	// filterNameSnapshotType is the AWS Filters.Filter.N.Name value for
	// narrowing a Describe* result set by snapshot type (manual/automated).
	// Shared by DescribeDBSnapshots and DescribeDBClusterSnapshots.
	filterNameSnapshotType = "snapshot-type"
	// snapshotTypeManual is the SnapshotType value AWS assigns to
	// user-initiated (as opposed to automated) DB and DB cluster snapshots.
	snapshotTypeManual = "manual"
	// errCodeInvalidDBClusterStateFault is the wire error code AWS returns
	// when a DB cluster operation (including activity-stream operations,
	// which key off DB cluster state) is invalid given the cluster's
	// current state.
	errCodeInvalidDBClusterStateFault = "InvalidDBClusterStateFault"
)

// copyParameterGroupTo returns a new DBParameterGroup that is a copy of src with the given
// target name and description. The caller is responsible for storing it in the appropriate map.
func copyParameterGroupTo(src *DBParameterGroup, targetName, targetDescription string) *DBParameterGroup {
	if targetDescription == "" {
		targetDescription = src.Description
	}

	pg := &DBParameterGroup{
		DBParameterGroupName:   targetName,
		DBParameterGroupFamily: src.DBParameterGroupFamily,
		Description:            targetDescription,
		Parameters:             make(map[string]DBParameter, len(src.Parameters)),
	}
	maps.Copy(pg.Parameters, src.Parameters)

	return pg
}

// applySnapshotAttributeChange modifies a list of snapshot attributes by adding and removing values.
func applySnapshotAttributeChange(
	attrs *[]DBSnapshotAttribute,
	attributeName string,
	valuesToAdd, valuesToRemove []string,
) {
	var attr *DBSnapshotAttribute
	for i := range *attrs {
		if (*attrs)[i].AttributeName == attributeName {
			attr = &(*attrs)[i]

			break
		}
	}
	if attr == nil {
		*attrs = append(*attrs, DBSnapshotAttribute{AttributeName: attributeName})
		attr = &(*attrs)[len(*attrs)-1]
	}
	for _, v := range valuesToAdd {
		if !slices.Contains(attr.AttributeValues, v) {
			attr.AttributeValues = append(attr.AttributeValues, v)
		}
	}
	attr.AttributeValues = slices.DeleteFunc(attr.AttributeValues, func(v string) bool {
		return slices.Contains(valuesToRemove, v)
	})
}
