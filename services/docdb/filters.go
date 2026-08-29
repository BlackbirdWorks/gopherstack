package docdb

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	filterNameDBClusterID  = "db-cluster-id"
	filterNameDBInstanceID = "db-instance-id"
)

// parseDescribeFilters parses the AWS query-protocol "Filters.Filter.N.Name" /
// "Filters.Filter.N.Values.Value.M" parameters into a filter-name -> values
// map. Confirmed against docdb@v1.51.4 serializers.go:
// awsAwsquery_serializeDocumentFilterList (array element name "Filter", not
// the generic "member") and awsAwsquery_serializeDocumentFilterValueList
// (array element name "Value", also not "member") -- a real client's Filters
// never appear on the wire as "Filters.Filter.N.Values.member.M".
func parseDescribeFilters(vals url.Values) map[string][]string {
	filters := make(map[string][]string)
	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("Filters.Filter.%d.Name", i))
		if name == "" {
			return filters
		}
		for j := 1; ; j++ {
			v := vals.Get(fmt.Sprintf("Filters.Filter.%d.Values.Value.%d", i, j))
			if v == "" {
				break
			}
			filters[name] = append(filters[name], v)
		}
	}
}

// identifierFromARN extracts the trailing identifier from an ARN built by
// this backend's own clusterARN/instanceARN/globalClusterARN helpers
// (store.go: arn.Build("rds", region, account, "cluster:"+id) and siblings,
// each of the form "arn:aws:rds:<region>:<account>:<type>:<id>"). DocDB
// identifiers never contain a colon, so the segment after the final colon is
// always the id regardless of resource type. A value that isn't ARN-shaped
// is returned unchanged, so a plain identifier filter value passes through.
func identifierFromARN(value string) string {
	if !strings.HasPrefix(value, "arn:") {
		return value
	}
	if idx := strings.LastIndex(value, ":"); idx >= 0 {
		return value[idx+1:]
	}

	return value
}

// matchesIdentifierOrARN reports whether values (a Filter's OR-matched value
// list, which per AWS's own doc comments "accepts identifiers and ARNs")
// names ident, case-insensitively -- DocDB identifiers aren't case sensitive.
func matchesIdentifierOrARN(values []string, ident string) bool {
	for _, v := range values {
		if strs.Equal(identifierFromARN(v), ident) {
			return true
		}
	}

	return false
}

// rejectUnknownFilterNames returns ErrInvalidParameter if filters contains a
// name outside known, matching real AWS's behavior for an unrecognized
// Filters.Filter.N.Name.
func rejectUnknownFilterNames(filters map[string][]string, known ...string) error {
	for name := range filters {
		if !slices.Contains(known, name) {
			return fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	return nil
}

// filterDBClusters applies DescribeDBClustersInput's Filters contract: the
// only documented supported filter is db-cluster-id (cluster identifiers or
// ARNs). See DescribeDBClustersInput's own doc comment in docdb@v1.51.4
// api_op_DescribeDBClusters.go.
func filterDBClusters(vals url.Values, clusters []DBCluster) ([]DBCluster, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return clusters, nil
	}
	if err := rejectUnknownFilterNames(filters, filterNameDBClusterID); err != nil {
		return nil, err
	}
	values := filters[filterNameDBClusterID]
	filtered := make([]DBCluster, 0, len(clusters))
	for _, c := range clusters {
		if matchesIdentifierOrARN(values, c.DBClusterIdentifier) {
			filtered = append(filtered, c)
		}
	}

	return filtered, nil
}

// filterDBInstances applies DescribeDBInstancesInput's Filters contract:
// db-cluster-id and db-instance-id (each identifiers or ARNs), per
// api_op_DescribeDBInstances.go's doc comment. Multiple filter names AND
// together; a single filter's Values list OR-matches.
func filterDBInstances(vals url.Values, instances []DBInstance) ([]DBInstance, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return instances, nil
	}
	if err := rejectUnknownFilterNames(filters, filterNameDBClusterID, filterNameDBInstanceID); err != nil {
		return nil, err
	}
	filtered := make([]DBInstance, 0, len(instances))
	for _, inst := range instances {
		if v, ok := filters[filterNameDBClusterID]; ok && !matchesIdentifierOrARN(v, inst.DBClusterIdentifier) {
			continue
		}
		if v, ok := filters[filterNameDBInstanceID]; ok && !matchesIdentifierOrARN(v, inst.DBInstanceIdentifier) {
			continue
		}
		filtered = append(filtered, inst)
	}

	return filtered, nil
}

// filterGlobalClusters applies DescribeGlobalClustersInput's Filters
// contract: the only documented supported filter is db-cluster-id, matched
// against the global cluster's own identifier/ARN (api_op_DescribeGlobalClusters.go's
// doc comment names the filter "db-cluster-id" even though it targets the
// global cluster itself, not a member DBCluster).
func filterGlobalClusters(vals url.Values, gcs []GlobalCluster) ([]GlobalCluster, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return gcs, nil
	}
	if err := rejectUnknownFilterNames(filters, filterNameDBClusterID); err != nil {
		return nil, err
	}
	values := filters[filterNameDBClusterID]
	filtered := make([]GlobalCluster, 0, len(gcs))
	for _, gc := range gcs {
		if matchesIdentifierOrARN(values, gc.GlobalClusterIdentifier) {
			filtered = append(filtered, gc)
		}
	}

	return filtered, nil
}

// filterPendingMaintenanceActions applies
// DescribePendingMaintenanceActionsInput's Filters contract: db-cluster-id
// and db-instance-id (each identifiers or ARNs), per
// api_op_DescribePendingMaintenanceActions.go's doc comment. Each entry's
// ResourceIdentifier is always stored as a full ARN (pending_maintenance.go),
// so matching extracts its trailing identifier for comparison.
func filterPendingMaintenanceActions(
	vals url.Values, actions []ResourcePendingMaintenanceActions,
) ([]ResourcePendingMaintenanceActions, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return actions, nil
	}
	if err := rejectUnknownFilterNames(filters, filterNameDBClusterID, filterNameDBInstanceID); err != nil {
		return nil, err
	}
	filtered := make([]ResourcePendingMaintenanceActions, 0, len(actions))
	for _, a := range actions {
		ident := identifierFromARN(a.ResourceIdentifier)
		if v, ok := filters[filterNameDBClusterID]; ok && !matchesIdentifierOrARN(v, ident) {
			continue
		}
		if v, ok := filters[filterNameDBInstanceID]; ok && !matchesIdentifierOrARN(v, ident) {
			continue
		}
		filtered = append(filtered, a)
	}

	return filtered, nil
}
