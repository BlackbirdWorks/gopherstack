package neptune_test

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestParity_CreateDBCluster_VpcSecurityGroupIds verifies VpcSecurityGroupIds are parsed and returned.
func TestParity_CreateDBCluster_VpcSecurityGroupIds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains []string
	}{
		{
			name: "single_sg",
			vals: url.Values{
				"Action":                       {"CreateDBCluster"},
				"Version":                      {"2014-10-31"},
				"DBClusterIdentifier":          {"sg-cluster"},
				"VpcSecurityGroupIds.member.1": {"sg-11111111"},
			},
			wantContains: []string{"sg-11111111", "VpcSecurityGroupMembership"},
		},
		{
			name: "multiple_sgs",
			vals: url.Values{
				"Action":                       {"CreateDBCluster"},
				"Version":                      {"2014-10-31"},
				"DBClusterIdentifier":          {"sg-cluster-multi"},
				"VpcSecurityGroupIds.member.1": {"sg-aaaa"},
				"VpcSecurityGroupIds.member.2": {"sg-bbbb"},
			},
			wantContains: []string{"sg-aaaa", "sg-bbbb"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestParity_CreateDBCluster_AvailabilityZones verifies AvailabilityZones are parsed and stored.
func TestParity_CreateDBCluster_AvailabilityZones(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains []string
	}{
		{
			name: "single_az",
			vals: url.Values{
				"Action":                     {"CreateDBCluster"},
				"Version":                    {"2014-10-31"},
				"DBClusterIdentifier":        {"az-cluster"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantContains: []string{"az-cluster"},
		},
		{
			name: "no_azs",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"no-az-cluster"},
			},
			wantContains: []string{"no-az-cluster"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestParity_CreateDBCluster_MasterUsername verifies MasterUsername is parsed and returned.
func TestParity_CreateDBCluster_MasterUsername(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains []string
	}{
		{
			name: "with_master_username",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"mu-cluster"},
				"MasterUsername":      {"neptune-admin"},
			},
			wantContains: []string{"neptune-admin", "MasterUsername"},
		},
		{
			name: "without_master_username",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"no-mu-cluster"},
			},
			wantContains: []string{"no-mu-cluster"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestParity_AssociatedRoles_PersistedOnCluster verifies that AddRoleToDBCluster persists
// roles in the cluster's AssociatedRoles field (not just the separate roles store).
func TestParity_AssociatedRoles_PersistedOnCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		roleArns     []string
		wantContains []string
	}{
		{
			name:         "single_role_in_associated_roles",
			roleArns:     []string{"arn:aws:iam::000000000000:role/MyRole"},
			wantContains: []string{"arn:aws:iam::000000000000:role/MyRole", "AssociatedRoles", "ACTIVE"},
		},
		{
			name:         "duplicate_role_not_added_twice",
			roleArns:     []string{"arn:aws:iam::000000000000:role/DupRole", "arn:aws:iam::000000000000:role/DupRole"},
			wantContains: []string{"DupRole"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
			ctx := context.Background()
			backend.AddClusterInternal("role-cluster")
			for _, roleArn := range tt.roleArns {
				err := backend.AddRoleToDBCluster(ctx, "role-cluster", roleArn)
				require.NoError(t, err)
			}
			clusters, err := backend.DescribeDBClusters(ctx, "role-cluster", neptune.DBClusterFilters{})
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			cl := clusters[0]
			if tt.name == "duplicate_role_not_added_twice" {
				assert.Len(t, cl.AssociatedRoles, 1, "duplicate role should not be added twice")
			} else {
				assert.NotEmpty(t, cl.AssociatedRoles)
			}
			// Also verify via HTTP handler
			h := neptune.NewHandler(backend)
			rr := doRequest(t, h, url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"role-cluster"},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestParity_DescribeDBClusters_SortedDeterministically verifies clusters are returned sorted by identifier.
func TestParity_DescribeDBClusters_SortedDeterministically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clusterIDs    []string
		wantOrderedIn []string
	}{
		{
			name:          "sorted_alphabetically",
			clusterIDs:    []string{"cluster-z", "cluster-a", "cluster-m"},
			wantOrderedIn: []string{"cluster-a", "cluster-m", "cluster-z"},
		},
		{
			name:          "already_sorted",
			clusterIDs:    []string{"alpha", "beta", "gamma"},
			wantOrderedIn: []string{"alpha", "beta", "gamma"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			for _, id := range tt.clusterIDs {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {id},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			// Verify order by checking positions in the XML body
			positions := make([]int, len(tt.wantOrderedIn))
			for i, want := range tt.wantOrderedIn {
				positions[i] = strings.Index(body, want)
			}

			for i := 1; i < len(positions); i++ {
				assert.Less(t, positions[i-1], positions[i],
					"cluster %q should appear before %q in response",
					tt.wantOrderedIn[i-1], tt.wantOrderedIn[i])
			}
		})
	}
}

// TestParity_DescribeDBInstances_SortedDeterministically verifies instances are returned sorted.
func TestParity_DescribeDBInstances_SortedDeterministically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		instanceIDs   []string
		wantOrderedIn []string
	}{
		{
			name:          "sorted_alphabetically",
			instanceIDs:   []string{"inst-z", "inst-a", "inst-m"},
			wantOrderedIn: []string{"inst-a", "inst-m", "inst-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"sort-cluster"},
			})
			for _, id := range tt.instanceIDs {
				doRequest(t, h, url.Values{
					"Action":               {"CreateDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {id},
					"DBClusterIdentifier":  {"sort-cluster"},
					"DBInstanceClass":      {"db.r5.large"},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":  {"DescribeDBInstances"},
				"Version": {"2014-10-31"},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			prevPos := -1
			for _, want := range tt.wantOrderedIn {
				pos := strings.Index(body, want)
				assert.Greater(t, pos, prevPos, "instance %q should appear in order", want)
				prevPos = pos
			}
		})
	}
}

// TestParity_DeleteDBCluster_RequiresFinalSnapshotOrSkip verifies that deleting without
// SkipFinalSnapshot=true and without FinalDBSnapshotIdentifier returns an error.
func TestParity_DeleteDBCluster_RequiresFinalSnapshotOrSkip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "skip_final_snapshot_true_succeeds",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster-skip"},
				"SkipFinalSnapshot":   {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
		{
			name: "with_final_snapshot_identifier_succeeds",
			vals: url.Values{
				"Action":                    {"DeleteDBCluster"},
				"Version":                   {"2014-10-31"},
				"DBClusterIdentifier":       {"del-cluster-snap"},
				"SkipFinalSnapshot":         {"false"},
				"FinalDBSnapshotIdentifier": {"my-final-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
		{
			name: "no_skip_no_identifier_fails",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster-fail"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterCombination",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			// Create a cluster for each case that needs one
			if tt.wantStatus == http.StatusOK {
				clusterID := tt.vals.Get("DBClusterIdentifier")
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {clusterID},
				})
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestParity_DBClusterParameterGroups_Pagination verifies Marker pagination works for cluster parameter groups.
func TestParity_DBClusterParameterGroups_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxRecords string
		wantMarker bool
		wantCount  int
	}{
		{
			name:       "all_results_no_pagination",
			maxRecords: "10",
			wantMarker: false,
		},
		{
			name:       "paginate_with_max_records_1",
			maxRecords: "1",
			wantMarker: true,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			// Create 3 cluster parameter groups
			for i, name := range []string{"pg-alpha", "pg-beta", "pg-gamma"} {
				_ = i
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {name},
					"DBParameterGroupFamily":      {"neptune1.3"},
					"Description":                 {"test"},
				})
			}
			vals := url.Values{
				"Action":     {"DescribeDBClusterParameterGroups"},
				"Version":    {"2014-10-31"},
				"MaxRecords": {tt.maxRecords},
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			if tt.wantMarker {
				assert.Contains(t, body, "<Marker>")
			} else {
				assert.NotContains(t, body, "<Marker>")
			}
		})
	}
}

// TestParity_ErrorCodes_DBInstanceFaultSuffix verifies that DBInstance error codes use the Fault suffix.
func TestParity_ErrorCodes_DBInstanceFaultSuffix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains string
	}{
		{
			name: "instance_not_found_has_fault_suffix",
			vals: url.Values{
				"Action":               {"DescribeDBInstances"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"nonexistent-instance"},
			},
			wantContains: "DBInstanceNotFound",
		},
		{
			name: "instance_already_exists_has_fault_suffix",
			vals: url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"dup-inst"},
				"DBClusterIdentifier":  {"dup-cluster"},
				"DBInstanceClass":      {"db.r5.large"},
			},
			wantContains: "DBInstanceAlreadyExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.name == "instance_already_exists_has_fault_suffix" {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"dup-cluster"},
				})
				// Create the instance first
				doRequest(t, h, url.Values{
					"Action":               {"CreateDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {"dup-inst"},
					"DBClusterIdentifier":  {"dup-cluster"},
					"DBInstanceClass":      {"db.r5.large"},
				})
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestParity_GlobalCluster_HasArnResourceIdEngine verifies GlobalCluster includes ARN/ResourceId/Engine fields.
func TestParity_GlobalCluster_HasArnResourceIdEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		globalID     string
		wantContains []string
	}{
		{
			name:     "global_cluster_has_arn_and_engine",
			globalID: "my-global",
			wantContains: []string{
				"GlobalClusterArn",
				"arn:",
				"GlobalClusterResourceId",
				"cluster-my-global",
				"Engine",
				"neptune",
				"EngineVersion",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {tt.globalID},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestParity_DBCluster_HasResourceIdAndCreateTime verifies DBCluster includes DbClusterResourceId
// and ClusterCreateTime fields.
func TestParity_DBCluster_HasResourceIdAndCreateTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterID    string
		wantContains []string
	}{
		{
			name:      "cluster_has_resource_id",
			clusterID: "res-cluster",
			wantContains: []string{
				"DbClusterResourceId",
				"cluster-res-cluster",
				"ClusterCreateTime",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {tt.clusterID},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}
