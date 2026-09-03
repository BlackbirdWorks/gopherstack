package rds

import (
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
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

// normalizeID folds an AWS resource identifier to its canonical store-lookup
// key, via the shared github.com/blackbirdworks/gopherstack/pkgs/strs
// helper (kept as a local wrapper so every one of this file's many call
// sites doesn't need an strs-qualified name). Real RDS treats
// DBInstanceIdentifier, DBClusterIdentifier, DBSnapshotIdentifier,
// DBClusterSnapshotIdentifier, DBParameterGroupName, and
// DBClusterParameterGroupName as case-INsensitive persistent handles —
// creating "MyDB" then "mydb" collides with DBInstanceAlreadyExistsFault on
// real AWS. gopherstack's store.Table[V] keys are plain Go map keys (case
// sensitive) and store.Table performs no normalization of its own (by
// design — see pkgs/store's package doc), so every store boundary for these
// six identifier families normalizes through this helper: each table's keyFn
// (used by Put/Restore) and every raw string passed to Get/Has/Delete (which,
// unlike Put, do NOT invoke keyFn — they index the map directly). The
// ORIGINAL caller-supplied casing is preserved in the stored struct's
// identifier field (and in the transient instanceReadyAt/clusterReadyAt
// scheduling maps' values are unaffected either way), so wire responses
// continue to echo back exactly what the caller sent — only the lookup key
// folds case, never the data.
func normalizeID(id string) string {
	return strs.Fold(id)
}

// containsFold reports whether values contains target under a
// case-insensitive comparison (via pkgs/strs). Used for Describe* Filters
// matching on the case-insensitive identifier families (db-instance-id,
// db-cluster-id, db-snapshot-id, db-cluster-snapshot-id, ...) so that a
// Filters value with different casing than the stored identifier still
// matches, consistent with normalizeID's store-lookup behavior.
func containsFold(values []string, target string) bool {
	return strs.ContainsFold(values, target)
}

// containsFoldIDOrARN reports whether values contains target (a bare
// identifier) under a case-insensitive comparison, accepting each candidate
// value in either bare-identifier or ARN form. Used for the db-cluster-id
// and db-instance-id Describe* Filters, whose own doc comments say "Accepts
// ... identifiers and ... Amazon Resource Names (ARNs)" — unlike
// containsFold's other callers (db-snapshot-id, db-cluster-snapshot-id,
// dbi-resource-id, ...), whose doc comments accept identifiers only.
func containsFoldIDOrARN(values []string, target string) bool {
	for _, v := range values {
		if strs.Equal(rdsIDFromARN(v), target) {
			return true
		}
	}

	return false
}

// idEqual reports whether a and b are the same case-insensitive AWS
// identifier (via pkgs/strs). Used wherever a plain map (rather than a
// store.Table[V] with a normalizeID-folded keyFn) holds identifier-shaped
// data and needs an equality check instead of a map lookup — e.g. scanning
// b.clusterEndpoints for the endpoints belonging to a given DBClusterIdentifier.
func idEqual(a, b string) bool {
	return strs.Equal(a, b)
}

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
