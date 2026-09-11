package rds

import (
	"fmt"
	"maps"
	"net/url"
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
	// filterNameParameterName is the AWS Filters.Filter.N.Name value for
	// narrowing a Describe* result set by parameter name. Shared by
	// DescribeDBParameters and DescribeDBClusterParameters, whose own doc
	// comments each say "The only supported filter is parameter-name".
	filterNameParameterName = "parameter-name"
	// filterNameDBClusterEndpointType is the Filters.Filter.N.Name value for
	// narrowing DescribeDBClusterEndpoints by endpoint type (reader/writer/custom).
	filterNameDBClusterEndpointType = "db-cluster-endpoint-type"
	// filterNameDBClusterEndpointCustomType is the Filters.Filter.N.Name
	// value for narrowing DescribeDBClusterEndpoints by a custom endpoint's
	// sub-type (reader/any). Accepted but not modeled — see
	// isKnownDBClusterEndpointFilterName.
	filterNameDBClusterEndpointCustomType = "db-cluster-endpoint-custom-type"
	// filterNameDBClusterEndpointID is the Filters.Filter.N.Name value for
	// narrowing DescribeDBClusterEndpoints by endpoint identifier.
	filterNameDBClusterEndpointID = "db-cluster-endpoint-id"
	// filterNameDBClusterEndpointStatus is the Filters.Filter.N.Name value
	// for narrowing DescribeDBClusterEndpoints by endpoint status.
	filterNameDBClusterEndpointStatus = "db-cluster-endpoint-status"
	// filterNameDBParameterGroupFamily is the Filters.Filter.N.Name value for
	// narrowing DescribeDBEngineVersions by parameter group family. Accepted
	// but not modeled — DBEngineVersion carries no family attribute.
	filterNameDBParameterGroupFamily = "db-parameter-group-family"
	// filterNameEngineMode is the Filters.Filter.N.Name value for narrowing
	// DescribeDBEngineVersions by engine mode. Accepted but not modeled —
	// DBEngineVersion carries no engine-mode attribute.
	filterNameEngineMode = "engine-mode"
	// filterNameEngineVersion is the Filters.Filter.N.Name value for
	// narrowing DescribeDBEngineVersions by engine version.
	filterNameEngineVersion = "engine-version"
	// filterNameStatus is the Filters.Filter.N.Name value for narrowing
	// DescribeDBEngineVersions and DescribeExportTasks by status. Accepted
	// but not modeled for DescribeDBEngineVersions — DBEngineVersion carries
	// no status attribute.
	filterNameStatus = "status"
	// filterNameRegion is the Filters.Filter.N.Name value for narrowing
	// DescribeGlobalClusters by member region. Accepted but not modeled — no
	// gopherstack API path ever populates GlobalCluster.PrimaryRegion or
	// GlobalClusterMembers (handler_global_clusters.go's own comment on
	// AddGlobalClusterMemberInternal), so there is no real region data to
	// match against.
	filterNameRegion = "region"
	// filterNameExportTaskIdentifier is the Filters.Filter.N.Name value for
	// narrowing DescribeExportTasks by export task identifier.
	filterNameExportTaskIdentifier = "export-task-identifier"
	// filterNameS3Bucket is the Filters.Filter.N.Name value for narrowing
	// DescribeExportTasks by destination S3 bucket.
	filterNameS3Bucket = "s3-bucket"
	// filterNameSourceArn is the Filters.Filter.N.Name value for narrowing
	// DescribeExportTasks by the exported resource's ARN.
	filterNameSourceArn = "source-arn"
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

// applyDBParameterFilters narrows params per the Filters contract shared by
// DescribeDBParameters (api_op_DescribeDBParameters.go:40-44) and
// DescribeDBClusterParameters (api_op_DescribeDBClusterParameters.go:48-52):
// each op's own doc comment says, verbatim, "The only supported filter is
// parameter-name." An unrecognized filter name returns InvalidParameterValue,
// matching real AWS.
func applyDBParameterFilters(vals url.Values, params []DBParameter) ([]DBParameter, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return params, nil
	}

	for name := range filters {
		if name != filterNameParameterName {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	values := filters[filterNameParameterName]
	filtered := make([]DBParameter, 0, len(params))
	for _, p := range params {
		if slices.Contains(values, p.ParameterName) {
			filtered = append(filtered, p)
		}
	}

	return filtered, nil
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
