package docdb_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func newTestHandler(t *testing.T) *docdb.Handler {
	t.Helper()
	backend := docdb.NewInMemoryBackend("000000000000", "us-east-1")

	return docdb.NewHandler(backend)
}

func doRequest(t *testing.T, h *docdb.Handler, vals url.Values) *httptest.ResponseRecorder {
	t.Helper()
	body := vals.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "aws-sdk-go-v2/1.0 api/docdb#1.0")
	rr := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rr)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rr
}

func TestHandler_CreateDescribeDeleteDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		action       string
		wantContains string
		wantStatus   int
	}{
		{
			name:   "create_cluster",
			action: "CreateDBCluster",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
				"Engine":              {"docdb"},
				"MasterUsername":      {"admin"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-cluster",
		},
		{
			name:   "describe_clusters_all",
			action: "DescribeDBClusters",
			vals: url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClustersResponse",
		},
		{
			name:   "describe_cluster_by_id",
			action: "DescribeDBClusters",
			vals: url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-cluster",
		},
		{
			name:   "delete_cluster",
			action: "DeleteDBCluster",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			localH := newTestHandler(t)
			if tt.action != "CreateDBCluster" {
				createVals := url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"test-cluster"},
					"Engine":              {"docdb"},
				}
				doRequest(t, localH, createVals)
			}
			rr := doRequest(t, localH, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ClusterOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_cluster",
			vals: url.Values{
				"Action":                      {"ModifyDBCluster"},
				"Version":                     {"2014-10-31"},
				"DBClusterIdentifier":         {"my-cluster"},
				"DBClusterParameterGroupName": {"new-param-group"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "ModifyDBClusterResponse",
		},
		{
			name: "stop_cluster",
			vals: url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "stopped",
		},
		{
			name: "start_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"StopDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "available",
		},
		{
			name: "failover_cluster",
			vals: url.Values{
				"Action":              {"FailoverDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "FailoverDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			})

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DBInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_instance",
			vals: url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
				"DBClusterIdentifier":  {"my-cluster"},
				"DBInstanceClass":      {"db.t3.medium"},
				"Engine":               {"docdb"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-instance",
		},
		{
			name: "describe_instances_all",
			vals: url.Values{
				"Action":  {"DescribeDBInstances"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBInstancesResponse",
		},
		{
			name: "describe_instances_by_id",
			vals: url.Values{
				"Action":               {"DescribeDBInstances"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-instance",
		},
		{
			name: "modify_instance",
			vals: url.Values{
				"Action":               {"ModifyDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
				"DBInstanceClass":      {"db.r5.large"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db.r5.large",
		},
		{
			name: "reboot_instance",
			vals: url.Values{
				"Action":               {"RebootDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RebootDBInstanceResponse",
		},
		{
			name: "delete_instance",
			vals: url.Values{
				"Action":               {"DeleteDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBInstanceResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
				"Engine":              {"docdb"},
			})
			if tt.name != "create_instance" {
				doRequest(t, h, url.Values{
					"Action":               {"CreateDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {"my-instance"},
					"DBClusterIdentifier":  {"my-cluster"},
					"Engine":               {"docdb"},
				})
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_SubnetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_subnet_group",
			vals: url.Values{
				"Action":                   {"CreateDBSubnetGroup"},
				"Version":                  {"2014-10-31"},
				"DBSubnetGroupName":        {"my-sg"},
				"DBSubnetGroupDescription": {"test sg"},
				"VpcId":                    {"vpc-12345"},
				"SubnetIds.member.1":       {"subnet-aaa"},
				"SubnetIds.member.2":       {"subnet-bbb"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sg",
		},
		{
			name: "describe_subnet_groups_all",
			vals: url.Values{
				"Action":  {"DescribeDBSubnetGroups"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBSubnetGroupsResponse",
		},
		{
			name: "describe_subnet_group_by_name",
			vals: url.Values{
				"Action":            {"DescribeDBSubnetGroups"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"my-sg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sg",
		},
		{
			name: "delete_subnet_group",
			vals: url.Values{
				"Action":            {"DeleteDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"my-sg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBSubnetGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.name != "create_subnet_group" {
				doRequest(t, h, url.Values{
					"Action":                   {"CreateDBSubnetGroup"},
					"Version":                  {"2014-10-31"},
					"DBSubnetGroupName":        {"my-sg"},
					"DBSubnetGroupDescription": {"test sg"},
				})
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ClusterParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_param_group",
			vals: url.Values{
				"Action":                      {"CreateDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
				"DBParameterGroupFamily":      {"docdb4.0"},
				"Description":                 {"test param group"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-pg",
		},
		{
			name: "describe_param_groups_all",
			vals: url.Values{
				"Action":  {"DescribeDBClusterParameterGroups"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClusterParameterGroupsResponse",
		},
		{
			name: "describe_param_group_by_name",
			vals: url.Values{
				"Action":                      {"DescribeDBClusterParameterGroups"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-pg",
		},
		{
			name: "modify_param_group",
			vals: url.Values{
				"Action":                      {"ModifyDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-pg",
		},
		{
			name: "delete_param_group",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterParameterGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.name != "create_param_group" {
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"my-pg"},
					"DBParameterGroupFamily":      {"docdb4.0"},
					"Description":                 {"test"},
				})
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ClusterSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_snapshot",
			vals: url.Values{
				"Action":                      {"CreateDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
				"DBClusterIdentifier":         {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-snap",
		},
		{
			name: "describe_snapshots_all",
			vals: url.Values{
				"Action":  {"DescribeDBClusterSnapshots"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClusterSnapshotsResponse",
		},
		{
			name: "describe_snapshot_by_id",
			vals: url.Values{
				"Action":                      {"DescribeDBClusterSnapshots"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-snap",
		},
		{
			name: "delete_snapshot",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterSnapshotResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
				"Engine":              {"docdb"},
			})
			if tt.name != "create_snapshot" {
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"my-snap"},
					"DBClusterIdentifier":         {"my-cluster"},
				})
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "add_tags",
			vals: url.Values{
				"Action":           {"AddTagsToResource"},
				"Version":          {"2014-10-31"},
				"ResourceName":     {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
				"Tags.Tag.1.Key":   {"env"},
				"Tags.Tag.1.Value": {"prod"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "AddTagsToResourceResponse",
		},
		{
			name: "list_tags",
			vals: url.Values{
				"Action":       {"ListTagsForResource"},
				"Version":      {"2014-10-31"},
				"ResourceName": {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "ListTagsForResourceResponse",
		},
		{
			name: "remove_tags",
			vals: url.Values{
				"Action":           {"RemoveTagsFromResource"},
				"Version":          {"2014-10-31"},
				"ResourceName":     {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
				"TagKeys.member.1": {"env"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RemoveTagsFromResourceResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":           {"AddTagsToResource"},
				"Version":          {"2014-10-31"},
				"ResourceName":     {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
				"Tags.Tag.1.Key":   {"env"},
				"Tags.Tag.1.Value": {"prod"},
			})

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_MiscOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_engine_versions",
			vals: url.Values{
				"Action":  {"DescribeDBEngineVersions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "docdb",
		},
		{
			name: "describe_orderable_options",
			vals: url.Values{
				"Action":  {"DescribeOrderableDBInstanceOptions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db.t3.medium",
		},
		{
			name: "describe_global_clusters",
			vals: url.Values{
				"Action":  {"DescribeGlobalClusters"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeGlobalClustersResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		seedName     string
		wantStatus   int
	}{
		{
			name: "missing_action",
			vals: url.Values{
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "MissingAction",
		},
		{
			name: "unknown_action",
			vals: url.Values{
				"Action":  {"UnknownAction"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidAction",
		},
		{
			name: "cluster_not_found",
			vals: url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name:     "cluster_already_exists",
			seedName: "existing-cluster",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"existing-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterAlreadyExistsFault",
		},
		{
			name: "instance_not_found",
			vals: url.Values{
				"Action":               {"DescribeDBInstances"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBInstanceNotFound",
		},
		{
			name: "subnet_group_not_found",
			vals: url.Values{
				"Action":            {"DescribeDBSubnetGroups"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBSubnetGroupNotFoundFault",
		},
		{
			name: "cluster_snapshot_not_found",
			vals: url.Values{
				"Action":                      {"DescribeDBClusterSnapshots"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
		{
			name: "cluster_param_group_not_found",
			vals: url.Values{
				"Action":                      {"DescribeDBClusterParameterGroups"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterParameterGroupNotFoundFault",
		},
		{
			name: "missing_cluster_id",
			vals: url.Values{
				"Action":  {"CreateDBCluster"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seedName != "" {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {tt.seedName},
				})
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		ct     string
		ua     string
		body   string
		want   bool
	}{
		{
			name:   "valid_docdb_request",
			method: http.MethodPost,
			path:   "/",
			ct:     "application/x-www-form-urlencoded",
			ua:     "aws-sdk-go api/docdb#1.0",
			body:   "Action=DescribeDBClusters&Version=2014-10-31",
			want:   true,
		},
		{
			name:   "wrong_method",
			method: http.MethodGet,
			path:   "/",
			ct:     "application/x-www-form-urlencoded",
			ua:     "aws-sdk-go api/docdb#1.0",
			body:   "Action=DescribeDBClusters&Version=2014-10-31",
			want:   false,
		},
		{
			name:   "dashboard_path",
			method: http.MethodPost,
			path:   "/dashboard/docdb",
			ct:     "application/x-www-form-urlencoded",
			ua:     "aws-sdk-go api/docdb#1.0",
			body:   "Action=DescribeDBClusters&Version=2014-10-31",
			want:   false,
		},
		{
			name:   "wrong_user_agent",
			method: http.MethodPost,
			path:   "/",
			ct:     "application/x-www-form-urlencoded",
			ua:     "aws-sdk-go api/rds#1.0",
			body:   "Action=DescribeDBClusters&Version=2014-10-31",
			want:   false,
		},
		{
			name:   "wrong_content_type",
			method: http.MethodPost,
			path:   "/",
			ct:     "application/json",
			ua:     "aws-sdk-go api/docdb#1.0",
			body:   "Action=DescribeDBClusters&Version=2014-10-31",
			want:   false,
		},
		{
			name:   "wrong_version",
			method: http.MethodPost,
			path:   "/",
			ct:     "application/x-www-form-urlencoded",
			ua:     "aws-sdk-go api/docdb#1.0",
			body:   "Action=DescribeDBClusters&Version=2012-01-01",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.ct)
			req.Header.Set("User-Agent", tt.ua)
			rr := httptest.NewRecorder()
			c := e.NewContext(req, rr)
			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "DocDB", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateDBCluster")
	assert.Contains(t, ops, "CreateDBInstance")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 85, h.MatchPriority())
}

func TestHandler_XMLResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"xml-test"},
		"Engine":              {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	body, err := io.ReadAll(rr.Body)
	require.NoError(t, err)

	var resp struct {
		XMLName xml.Name `xml:"CreateDBClusterResponse"`
	}
	err = xml.Unmarshal(body[len("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"):], &resp)
	require.NoError(t, err)
	assert.Equal(t, "CreateDBClusterResponse", resp.XMLName.Local)
}

func TestHandler_EventSubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_event_subscription",
			vals: url.Values{
				"Action":           {"CreateEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
				"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:my-topic"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sub",
		},
		{
			name: "add_source_identifier",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:my-topic"},
				})
			},
			vals: url.Values{
				"Action":           {"AddSourceIdentifierToSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-cluster",
		},
		{
			name: "delete_event_subscription",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:my-topic"},
				})
			},
			vals: url.Values{
				"Action":           {"DeleteEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteEventSubscriptionResponse",
		},
		{
			name: "create_duplicate_subscription",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"dup-sub"},
				})
			},
			vals: url.Values{
				"Action":           {"CreateEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"dup-sub"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionAlreadyExistFault",
		},
		{
			name: "delete_nonexistent_subscription",
			vals: url.Values{
				"Action":           {"DeleteEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFoundFault",
		},
		{
			name: "add_source_id_nonexistent_subscription",
			vals: url.Values{
				"Action":           {"AddSourceIdentifierToSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"nonexistent"},
				"SourceIdentifier": {"some-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_GlobalClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_global_cluster",
			vals: url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"my-global"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-global",
		},
		{
			name: "delete_global_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"my-global"},
				})
			},
			vals: url.Values{
				"Action":                  {"DeleteGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"my-global"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteGlobalClusterResponse",
		},
		{
			name: "create_duplicate_global_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"dup-global"},
				})
			},
			vals: url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"dup-global"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterAlreadyExistsFault",
		},
		{
			name: "delete_nonexistent_global_cluster",
			vals: url.Values{
				"Action":                  {"DeleteGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_CopyOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "copy_parameter_group",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"source-pg"},
					"DBParameterGroupFamily":      {"docdb4.0"},
					"Description":                 {"source"},
				})
			},
			vals: url.Values{
				"Action":  {"CopyDBClusterParameterGroup"},
				"Version": {"2014-10-31"},
				"SourceDBClusterParameterGroupIdentifier":  {"source-pg"},
				"TargetDBClusterParameterGroupIdentifier":  {"target-pg"},
				"TargetDBClusterParameterGroupDescription": {"target"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "target-pg",
		},
		{
			name: "copy_parameter_group_source_not_found",
			vals: url.Values{
				"Action":  {"CopyDBClusterParameterGroup"},
				"Version": {"2014-10-31"},
				"SourceDBClusterParameterGroupIdentifier": {"nonexistent"},
				"TargetDBClusterParameterGroupIdentifier": {"target-pg"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterParameterGroupNotFoundFault",
		},
		{
			name: "copy_snapshot",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
					"Engine":              {"docdb"},
				})
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"source-snap"},
					"DBClusterIdentifier":         {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":                            {"CopyDBClusterSnapshot"},
				"Version":                           {"2014-10-31"},
				"SourceDBClusterSnapshotIdentifier": {"source-snap"},
				"TargetDBClusterSnapshotIdentifier": {"target-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "target-snap",
		},
		{
			name: "copy_snapshot_source_not_found",
			vals: url.Values{
				"Action":                            {"CopyDBClusterSnapshot"},
				"Version":                           {"2014-10-31"},
				"SourceDBClusterSnapshotIdentifier": {"nonexistent"},
				"TargetDBClusterSnapshotIdentifier": {"target-snap"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ApplyPendingMaintenanceAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "apply_action_success",
			vals: url.Values{
				"Action":             {"ApplyPendingMaintenanceAction"},
				"Version":            {"2014-10-31"},
				"ResourceIdentifier": {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
				"ApplyAction":        {"system-update"},
				"OptInType":          {"immediate"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "ApplyPendingMaintenanceActionResponse",
		},
		{
			name: "apply_action_missing_resource",
			vals: url.Values{
				"Action":      {"ApplyPendingMaintenanceAction"},
				"Version":     {"2014-10-31"},
				"ApplyAction": {"system-update"},
				"OptInType":   {"immediate"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_all_certificates",
			vals: url.Values{
				"Action":  {"DescribeCertificates"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "rds-ca-2019",
		},
		{
			name: "describe_certificate_by_id",
			vals: url.Values{
				"Action":                {"DescribeCertificates"},
				"Version":               {"2014-10-31"},
				"CertificateIdentifier": {"rds-ca-rsa2048-g1"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "rds-ca-rsa2048-g1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeDBClusterParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_parameters",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"my-pg"},
					"DBParameterGroupFamily":      {"docdb4.0"},
					"Description":                 {"test"},
				})
			},
			vals: url.Values{
				"Action":                      {"DescribeDBClusterParameters"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "tls",
		},
		{
			name: "describe_parameters_group_not_found",
			vals: url.Values{
				"Action":                      {"DescribeDBClusterParameters"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterParameterGroupNotFoundFault",
		},
		{
			name: "describe_parameters_missing_group_name",
			vals: url.Values{
				"Action":  {"DescribeDBClusterParameters"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- Refinement check 1 tests ----

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *docdb.InMemoryBackend)
		name  string
		want  int
	}{
		{
			name:  "empty_backend_reset",
			setup: nil,
			want:  0,
		},
		{
			name: "reset_with_data",
			setup: func(b *docdb.InMemoryBackend) {
				b.AddDBClusterInternal(&docdb.DBCluster{DBClusterIdentifier: "c1"})
				b.AddDBInstanceInternal(&docdb.DBInstance{DBInstanceIdentifier: "i1"})
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			b.Reset()

			assert.Equal(t, tt.want, b.ClusterCount())
			assert.Equal(t, tt.want, b.InstanceCount())
		})
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_reset_clears_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.Backend.AddDBClusterInternal(&docdb.DBCluster{DBClusterIdentifier: "c1"})
			require.Equal(t, 1, h.Backend.ClusterCount())

			h.Reset()

			assert.Equal(t, 0, h.Backend.ClusterCount())
		})
	}
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{
			name:    "nil_context_returns_error",
			wantErr: docdb.ErrNilAppContext,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &docdb.Provider{}
			_, err := p.Init(nil)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestRefinement1_SortedDescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{
			name: "sorted_order",
			ids:  []string{"c-beta", "c-alpha", "c-gamma"},
			want: []string{"c-alpha", "c-beta", "c-gamma"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, id := range tt.ids {
				b.AddDBClusterInternal(&docdb.DBCluster{DBClusterIdentifier: id})
			}

			got, err := b.DescribeDBClusters(context.Background(), "")
			require.NoError(t, err)

			gotIDs := make([]string, len(got))
			for i, c := range got {
				gotIDs[i] = c.DBClusterIdentifier
			}

			assert.Equal(t, tt.want, gotIDs)
		})
	}
}

func TestRefinement1_SortedDescribeInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{
			name: "sorted_order",
			ids:  []string{"i-z", "i-a", "i-m"},
			want: []string{"i-a", "i-m", "i-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, id := range tt.ids {
				b.AddDBInstanceInternal(&docdb.DBInstance{DBInstanceIdentifier: id})
			}

			got, err := b.DescribeDBInstances(context.Background(), "", "")
			require.NoError(t, err)

			gotIDs := make([]string, len(got))
			for i, inst := range got {
				gotIDs[i] = inst.DBInstanceIdentifier
			}

			assert.Equal(t, tt.want, gotIDs)
		})
	}
}

func TestRefinement1_SortedDescribeSubnetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
		want  []string
	}{
		{
			name:  "sorted_order",
			names: []string{"sg-z", "sg-a", "sg-m"},
			want:  []string{"sg-a", "sg-m", "sg-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, name := range tt.names {
				b.AddDBSubnetGroupInternal(&docdb.DBSubnetGroup{DBSubnetGroupName: name})
			}

			got, err := b.DescribeDBSubnetGroups(context.Background(), "")
			require.NoError(t, err)

			gotNames := make([]string, len(got))
			for i, sg := range got {
				gotNames[i] = sg.DBSubnetGroupName
			}

			assert.Equal(t, tt.want, gotNames)
		})
	}
}

func TestRefinement1_SortedDescribeParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
		want  []string
	}{
		{
			name:  "sorted_order",
			names: []string{"pg-z", "pg-a", "pg-m"},
			want:  []string{"pg-a", "pg-m", "pg-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, name := range tt.names {
				b.AddDBClusterParameterGroupInternal(&docdb.DBClusterParameterGroup{DBClusterParameterGroupName: name})
			}

			got, err := b.DescribeDBClusterParameterGroups(context.Background(), "")
			require.NoError(t, err)

			gotNames := make([]string, len(got))
			for i, pg := range got {
				gotNames[i] = pg.DBClusterParameterGroupName
			}

			assert.Equal(t, tt.want, gotNames)
		})
	}
}

func TestRefinement1_SortedDescribeSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{
			name: "sorted_order",
			ids:  []string{"snap-z", "snap-a", "snap-m"},
			want: []string{"snap-a", "snap-m", "snap-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, id := range tt.ids {
				b.AddDBClusterSnapshotInternal(&docdb.DBClusterSnapshot{DBClusterSnapshotIdentifier: id})
			}

			got, err := b.DescribeDBClusterSnapshots(context.Background(), "", "", "")
			require.NoError(t, err)

			gotIDs := make([]string, len(got))
			for i, s := range got {
				gotIDs[i] = s.DBClusterSnapshotIdentifier
			}

			assert.Equal(t, tt.want, gotIDs)
		})
	}
}

func TestRefinement1_SortedDescribeGlobalClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{
			name: "sorted_order",
			ids:  []string{"gc-z", "gc-a", "gc-m"},
			want: []string{"gc-a", "gc-m", "gc-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, id := range tt.ids {
				b.AddGlobalClusterInternal(&docdb.GlobalCluster{GlobalClusterIdentifier: id})
			}

			got := b.DescribeGlobalClusters(context.Background(), "")

			gotIDs := make([]string, len(got))
			for i, gc := range got {
				gotIDs[i] = gc.GlobalClusterIdentifier
			}

			assert.Equal(t, tt.want, gotIDs)
		})
	}
}

func TestRefinement1_SortedListTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []docdb.Tag
		wantKeys []string
	}{
		{
			name: "sorted_by_key",
			tags: []docdb.Tag{
				{Key: "z-key", Value: "v3"},
				{Key: "a-key", Value: "v1"},
				{Key: "m-key", Value: "v2"},
			},
			wantKeys: []string{"a-key", "m-key", "z-key"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(
				t,
				b.AddTagsToResource(context.Background(), "arn:aws:rds:us-east-1:000000000000:cluster:test", tt.tags),
			)

			got := b.ListTagsForResource(context.Background(), "arn:aws:rds:us-east-1:000000000000:cluster:test")

			gotKeys := make([]string, len(got))
			for i, t := range got {
				gotKeys[i] = t.Key
			}

			assert.Equal(t, tt.wantKeys, gotKeys)
		})
	}
}

func TestRefinement1_ClusterARNInResponse(t *testing.T) {
	t.Parallel()

	type wantFields struct {
		arnPrefix string
		engine    string
	}

	tests := []struct {
		name  string
		id    string
		wantF wantFields
	}{
		{
			name:  "arn_present",
			id:    "my-cluster",
			wantF: wantFields{arnPrefix: "arn:aws:rds:", engine: "docdb"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              []string{"CreateDBCluster"},
				"Version":             []string{"2014-10-31"},
				"DBClusterIdentifier": []string{tt.id},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			body := resp.Body.String()
			assert.Contains(t, body, "DBClusterArn")
			assert.Contains(t, body, tt.wantF.arnPrefix)
		})
	}
}

func TestRefinement1_InstanceARNInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "arn_present", id: "my-instance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":               []string{"CreateDBInstance"},
				"Version":              []string{"2014-10-31"},
				"DBInstanceIdentifier": []string{tt.id},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			body := resp.Body.String()
			assert.Contains(t, body, "DBInstanceArn")
			assert.Contains(t, body, "arn:aws:rds:")
		})
	}
}

func TestRefinement1_TagsOnCreate_Cluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		tagKey  string
		tagVal  string
		wantLen int
	}{
		{
			name:    "tags_stored",
			id:      "tagged-cluster",
			tagKey:  "env",
			tagVal:  "test",
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              []string{"CreateDBCluster"},
				"Version":             []string{"2014-10-31"},
				"DBClusterIdentifier": []string{tt.id},
				"Tags.Tag.1.Key":      []string{tt.tagKey},
				"Tags.Tag.1.Value":    []string{tt.tagVal},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			clusters, err := h.Backend.DescribeDBClusters(context.Background(), tt.id)
			require.NoError(t, err)
			require.Len(t, clusters, 1)

			assert.Len(t, clusters[0].Tags, tt.wantLen)
			assert.Equal(t, tt.tagVal, clusters[0].Tags[tt.tagKey])
		})
	}
}

func TestRefinement1_TagsOnCreate_Instance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		tagKey  string
		tagVal  string
		wantLen int
	}{
		{
			name:    "tags_stored",
			id:      "tagged-instance",
			tagKey:  "env",
			tagVal:  "prod",
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":               []string{"CreateDBInstance"},
				"Version":              []string{"2014-10-31"},
				"DBInstanceIdentifier": []string{tt.id},
				"Tags.Tag.1.Key":       []string{tt.tagKey},
				"Tags.Tag.1.Value":     []string{tt.tagVal},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			instances, err := h.Backend.DescribeDBInstances(context.Background(), tt.id, "")
			require.NoError(t, err)
			require.Len(t, instances, 1)

			assert.Len(t, instances[0].Tags, tt.wantLen)
			assert.Equal(t, tt.tagVal, instances[0].Tags[tt.tagKey])
		})
	}
}

func TestRefinement1_SnapshotClusterIdFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusterID string
		wantCount int
	}{
		{
			name:      "filter_by_cluster",
			clusterID: "cluster-a",
			wantCount: 2,
		},
		{
			name:      "no_filter",
			clusterID: "",
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			b.AddDBClusterSnapshotInternal(&docdb.DBClusterSnapshot{
				DBClusterSnapshotIdentifier: "snap-1",
				DBClusterIdentifier:         "cluster-a",
			})
			b.AddDBClusterSnapshotInternal(&docdb.DBClusterSnapshot{
				DBClusterSnapshotIdentifier: "snap-2",
				DBClusterIdentifier:         "cluster-a",
			})
			b.AddDBClusterSnapshotInternal(&docdb.DBClusterSnapshot{
				DBClusterSnapshotIdentifier: "snap-3",
				DBClusterIdentifier:         "cluster-b",
			})

			got, err := b.DescribeDBClusterSnapshots(context.Background(), "", tt.clusterID, "")
			require.NoError(t, err)

			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestRefinement1_DescribeGlobalClusters_RealData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filterID  string
		gcIDs     []string
		wantCount int
	}{
		{
			name:      "all_clusters",
			gcIDs:     []string{"gc-1", "gc-2"},
			filterID:  "",
			wantCount: 2,
		},
		{
			name:      "filtered_by_id",
			gcIDs:     []string{"gc-1", "gc-2"},
			filterID:  "gc-1",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, id := range tt.gcIDs {
				h.Backend.AddGlobalClusterInternal(&docdb.GlobalCluster{
					GlobalClusterIdentifier: id,
					Status:                  "available",
				})
			}

			vals := url.Values{
				"Action":  []string{"DescribeGlobalClusters"},
				"Version": []string{"2014-10-31"},
			}
			if tt.filterID != "" {
				vals.Set("GlobalClusterIdentifier", tt.filterID)
			}

			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			body := resp.Body.String()
			for _, id := range tt.gcIDs[:tt.wantCount] {
				assert.Contains(t, body, id)
			}
		})
	}
}

func TestRefinement1_OptInTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		optInType string
		wantErr   bool
	}{
		{name: "immediate_valid", optInType: "immediate", wantErr: false},
		{name: "next_maintenance_valid", optInType: "next-maintenance", wantErr: false},
		{name: "undo_opt_in_valid", optInType: "undo-opt-in", wantErr: false},
		{name: "invalid_opt_in_type", optInType: "bad-value", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.ApplyPendingMaintenanceAction(
				context.Background(),
				"arn:aws:rds:us-east-1:000000000000:cluster:c1",
				"system-update",
				tt.optInType,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seedFn  func(b *docdb.InMemoryBackend)
		checkFn func(t *testing.T, b *docdb.InMemoryBackend)
		name    string
	}{
		{
			name: "add_cluster",
			seedFn: func(b *docdb.InMemoryBackend) {
				b.AddDBClusterInternal(&docdb.DBCluster{DBClusterIdentifier: "seed-c"})
			},
			checkFn: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, b.ClusterCount())
			},
		},
		{
			name: "add_instance",
			seedFn: func(b *docdb.InMemoryBackend) {
				b.AddDBInstanceInternal(&docdb.DBInstance{DBInstanceIdentifier: "seed-i"})
			},
			checkFn: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, b.InstanceCount())
			},
		},
		{
			name: "add_subnet_group",
			seedFn: func(b *docdb.InMemoryBackend) {
				b.AddDBSubnetGroupInternal(&docdb.DBSubnetGroup{DBSubnetGroupName: "seed-sg"})
			},
			checkFn: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, b.SubnetGroupCount())
			},
		},
		{
			name: "add_parameter_group",
			seedFn: func(b *docdb.InMemoryBackend) {
				b.AddDBClusterParameterGroupInternal(
					&docdb.DBClusterParameterGroup{DBClusterParameterGroupName: "seed-pg"},
				)
			},
			checkFn: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, b.ParameterGroupCount())
			},
		},
		{
			name: "add_snapshot",
			seedFn: func(b *docdb.InMemoryBackend) {
				b.AddDBClusterSnapshotInternal(&docdb.DBClusterSnapshot{DBClusterSnapshotIdentifier: "seed-snap"})
			},
			checkFn: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, b.SnapshotCount())
			},
		},
		{
			name: "add_event_subscription",
			seedFn: func(b *docdb.InMemoryBackend) {
				b.AddEventSubscriptionInternal(&docdb.EventSubscription{SubscriptionName: "seed-sub"})
			},
			checkFn: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, b.EventSubscriptionCount())
			},
		},
		{
			name: "add_global_cluster",
			seedFn: func(b *docdb.InMemoryBackend) {
				b.AddGlobalClusterInternal(&docdb.GlobalCluster{GlobalClusterIdentifier: "seed-gc"})
			},
			checkFn: func(t *testing.T, b *docdb.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, b.GlobalClusterCount())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			tt.seedFn(b)
			tt.checkFn(t, b)
		})
	}
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
	}{
		{name: "empty_backend", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")

			assert.Equal(t, tt.wantCount, b.ClusterCount())
			assert.Equal(t, tt.wantCount, b.InstanceCount())
			assert.Equal(t, tt.wantCount, b.SubnetGroupCount())
			assert.Equal(t, tt.wantCount, b.ParameterGroupCount())
			assert.Equal(t, tt.wantCount, b.SnapshotCount())
			assert.Equal(t, tt.wantCount, b.EventSubscriptionCount())
			assert.Equal(t, tt.wantCount, b.GlobalClusterCount())
		})
	}
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupFn     func(b *docdb.InMemoryBackend)
		wantCluster string
	}{
		{
			name: "snapshot_and_restore",
			setupFn: func(b *docdb.InMemoryBackend) {
				b.AddDBClusterInternal(&docdb.DBCluster{
					DBClusterIdentifier: "restored-cluster",
					Engine:              "docdb",
					Status:              "available",
				})
			},
			wantCluster: "restored-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setupFn(b1)

			data := b1.Snapshot()
			require.NotEmpty(t, data)

			b2 := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			err := b2.Restore(data)
			require.NoError(t, err)

			clusters, err := b2.DescribeDBClusters(context.Background(), tt.wantCluster)
			require.NoError(t, err)
			require.Len(t, clusters, 1)

			assert.Equal(t, tt.wantCluster, clusters[0].DBClusterIdentifier)
		})
	}
}

func TestRefinement1_DeleteCluster_RequiresId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{name: "empty_id_returns_error", id: "", wantErr: true},
		{name: "missing_cluster_returns_error", id: "nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.DeleteDBCluster(context.Background(), tt.id, nil)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resetTimes int
	}{
		{name: "double_reset", resetTimes: 2},
		{name: "triple_reset", resetTimes: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			b.AddDBClusterInternal(&docdb.DBCluster{DBClusterIdentifier: "c1"})

			for range tt.resetTimes {
				b.Reset()
				b.AddDBClusterInternal(&docdb.DBCluster{DBClusterIdentifier: "c1"})
			}

			b.Reset()
			assert.Equal(t, 0, b.ClusterCount())
		})
	}
}

func TestRefinement1_EngineVersionInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		id            string
		wantEngineVer string
	}{
		{
			name:          "engine_version_present",
			id:            "engine-ver-cluster",
			wantEngineVer: "4.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              []string{"CreateDBCluster"},
				"Version":             []string{"2014-10-31"},
				"DBClusterIdentifier": []string{tt.id},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			body := resp.Body.String()
			assert.Contains(t, body, "EngineVersion")
			assert.Contains(t, body, tt.wantEngineVer)
		})
	}
}

func TestHandler_SnapshotAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_snapshot_attributes",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
					"Engine":              {"docdb"},
				})
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"my-snap"},
					"DBClusterIdentifier":         {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":                      {"DescribeDBClusterSnapshotAttributes"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClusterSnapshotAttributesResponse",
		},
		{
			name: "describe_snapshot_attributes_not_found",
			vals: url.Values{
				"Action":                      {"DescribeDBClusterSnapshotAttributes"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
		{
			name: "modify_snapshot_attribute",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
					"Engine":              {"docdb"},
				})
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"my-snap"},
					"DBClusterIdentifier":         {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":                      {"ModifyDBClusterSnapshotAttribute"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
				"AttributeName":               {"restore"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "ModifyDBClusterSnapshotAttributeResponse",
		},
		{
			name: "modify_snapshot_attribute_not_found",
			vals: url.Values{
				"Action":                      {"ModifyDBClusterSnapshotAttribute"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"nonexistent"},
				"AttributeName":               {"restore"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_EngineDefaultParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_engine_default_params",
			vals: url.Values{
				"Action":                 {"DescribeEngineDefaultClusterParameters"},
				"Version":                {"2014-10-31"},
				"DBParameterGroupFamily": {"docdb4.0"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeEngineDefaultClusterParametersResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ResetDBClusterParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "reset_parameter_group",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"my-pg"},
					"DBParameterGroupFamily":      {"docdb4.0"},
					"Description":                 {"test"},
				})
			},
			vals: url.Values{
				"Action":                      {"ResetDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-pg",
		},
		{
			name: "reset_parameter_group_not_found",
			vals: url.Values{
				"Action":                      {"ResetDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterParameterGroupNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeEventSubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_all_subscriptions",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
				})
			},
			vals: url.Values{
				"Action":  {"DescribeEventSubscriptions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sub",
		},
		{
			name: "describe_subscription_by_name",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
				})
			},
			vals: url.Values{
				"Action":           {"DescribeEventSubscriptions"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sub",
		},
		{
			name: "describe_subscriptions_empty",
			vals: url.Values{
				"Action":  {"DescribeEventSubscriptions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeEventSubscriptionsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ModifyEventSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_subscription",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:old-topic"},
				})
			},
			vals: url.Values{
				"Action":           {"ModifyEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
				"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:new-topic"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "new-topic",
		},
		{
			name: "modify_subscription_not_found",
			vals: url.Values{
				"Action":           {"ModifyEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_RemoveSourceIdentifierFromSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "remove_source_identifier",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":               {"CreateEventSubscription"},
					"Version":              {"2014-10-31"},
					"SubscriptionName":     {"my-sub"},
					"SourceIds.SourceId.1": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":           {"RemoveSourceIdentifierFromSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RemoveSourceIdentifierFromSubscriptionResponse",
		},
		{
			name: "remove_source_identifier_not_found",
			vals: url.Values{
				"Action":           {"RemoveSourceIdentifierFromSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"nonexistent"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_events",
			vals: url.Values{
				"Action":  {"DescribeEvents"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeEventsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeEventCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_event_categories",
			vals: url.Values{
				"Action":  {"DescribeEventCategories"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeEventCategoriesResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribePendingMaintenanceActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_pending_actions",
			vals: url.Values{
				"Action":  {"DescribePendingMaintenanceActions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribePendingMaintenanceActionsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ModifyDBSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_subnet_group",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                   {"CreateDBSubnetGroup"},
					"Version":                  {"2014-10-31"},
					"DBSubnetGroupName":        {"my-sg"},
					"DBSubnetGroupDescription": {"original"},
					"SubnetIds.member.1":       {"subnet-aaa"},
				})
			},
			vals: url.Values{
				"Action":                   {"ModifyDBSubnetGroup"},
				"Version":                  {"2014-10-31"},
				"DBSubnetGroupName":        {"my-sg"},
				"DBSubnetGroupDescription": {"updated"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "updated",
		},
		{
			name: "modify_subnet_group_not_found",
			vals: url.Values{
				"Action":            {"ModifyDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBSubnetGroupNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_GlobalClusterMutations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_global_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"my-global"},
				})
			},
			vals: url.Values{
				"Action":                  {"ModifyGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"my-global"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-global",
		},
		{
			name: "failover_global_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"my-global"},
				})
			},
			vals: url.Values{
				"Action":                    {"FailoverGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"my-global"},
				"TargetDbClusterIdentifier": {"secondary-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "failing-over",
		},
		{
			name: "switchover_global_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"my-global"},
				})
			},
			vals: url.Values{
				"Action":                    {"SwitchoverGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"my-global"},
				"TargetDbClusterIdentifier": {"secondary-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "switching-over",
		},
		{
			name: "remove_from_global_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"my-global"},
				})
			},
			vals: url.Values{
				"Action":                  {"RemoveFromGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"my-global"},
				"DbClusterIdentifier":     {"secondary-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RemoveFromGlobalClusterResponse",
		},
		{
			name: "modify_global_cluster_not_found",
			vals: url.Values{
				"Action":                  {"ModifyGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name: "failover_global_cluster_not_found",
			vals: url.Values{
				"Action":                    {"FailoverGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"nonexistent"},
				"TargetDbClusterIdentifier": {"secondary-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_RestoreClusterOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "restore_from_snapshot",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"source-cluster"},
					"Engine":              {"docdb"},
				})
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"my-snap"},
					"DBClusterIdentifier":         {"source-cluster"},
				})
			},
			vals: url.Values{
				"Action":                      {"RestoreDBClusterFromSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
				"DBClusterIdentifier":         {"restored-cluster"},
				"Engine":                      {"docdb"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "restored-cluster",
		},
		{
			name: "restore_from_snapshot_not_found",
			vals: url.Values{
				"Action":                      {"RestoreDBClusterFromSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"nonexistent"},
				"DBClusterIdentifier":         {"restored-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
		{
			name: "restore_to_point_in_time",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"source-cluster"},
					"Engine":              {"docdb"},
				})
			},
			vals: url.Values{
				"Action":                    {"RestoreDBClusterToPointInTime"},
				"Version":                   {"2014-10-31"},
				"SourceDBClusterIdentifier": {"source-cluster"},
				"DBClusterIdentifier":       {"restored-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "restored-cluster",
		},
		{
			name: "restore_to_point_in_time_not_found",
			vals: url.Values{
				"Action":                    {"RestoreDBClusterToPointInTime"},
				"Version":                   {"2014-10-31"},
				"SourceDBClusterIdentifier": {"nonexistent"},
				"DBClusterIdentifier":       {"restored-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement1_StopStartClusterStateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "stop_available_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "stopped",
		},
		{
			name: "stop_already_stopped_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
				doRequest(t, h, url.Values{
					"Action":              {"StopDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name: "start_stopped_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
				doRequest(t, h, url.Values{
					"Action":              {"StopDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "available",
		},
		{
			name: "start_already_available_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement1_SnapshotAttributePersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "modify_and_describe_reflects_change",
			wantContains: "restore",
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			})
			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
				"DBClusterIdentifier":         {"my-cluster"},
			})
			modResp := doRequest(t, h, url.Values{
				"Action":                       {"ModifyDBClusterSnapshotAttribute"},
				"Version":                      {"2014-10-31"},
				"DBClusterSnapshotIdentifier":  {"my-snap"},
				"AttributeName":                {"restore"},
				"ValuesToAdd.AttributeValue.1": {"123456789012"},
			})
			require.Equal(t, http.StatusOK, modResp.Code)

			descResp := doRequest(t, h, url.Values{
				"Action":                      {"DescribeDBClusterSnapshotAttributes"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
			})
			assert.Equal(t, tt.wantStatus, descResp.Code)
			assert.Contains(t, descResp.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement1_CreateClusterNewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_cluster_with_deletion_protection",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"protected-cluster"},
				"DeletionProtection":  {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "protected-cluster",
		},
		{
			name: "create_cluster_with_backup_retention",
			vals: url.Values{
				"Action":                {"CreateDBCluster"},
				"Version":               {"2014-10-31"},
				"DBClusterIdentifier":   {"backup-cluster"},
				"BackupRetentionPeriod": {"7"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "backup-cluster",
		},
		{
			name: "create_cluster_with_storage_encrypted",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"encrypted-cluster"},
				"StorageEncrypted":    {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "encrypted-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement1_DescribeEventCategoriesFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals            url.Values
		name            string
		wantContains    string
		wantNotContains string
		wantStatus      int
	}{
		{
			name: "no_source_type_filter",
			vals: url.Values{
				"Action":  {"DescribeEventCategories"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db-cluster",
		},
		{
			name: "filter_by_db_instance",
			vals: url.Values{
				"Action":     {"DescribeEventCategories"},
				"Version":    {"2014-10-31"},
				"SourceType": {"db-instance"},
			},
			wantStatus:      http.StatusOK,
			wantContains:    "db-instance",
			wantNotContains: "db-cluster-snapshot",
		},
		{
			name: "filter_by_snapshot",
			vals: url.Values{
				"Action":     {"DescribeEventCategories"},
				"Version":    {"2014-10-31"},
				"SourceType": {"db-cluster-snapshot"},
			},
			wantStatus:      http.StatusOK,
			wantContains:    "db-cluster-snapshot",
			wantNotContains: "db-instance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
			if tt.wantNotContains != "" {
				assert.NotContains(t, rr.Body.String(), tt.wantNotContains)
			}
		})
	}
}

func TestRefinement1_ModifyGlobalClusterRename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "rename_global_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"old-global"},
				})
			},
			vals: url.Values{
				"Action":                     {"ModifyGlobalCluster"},
				"Version":                    {"2014-10-31"},
				"GlobalClusterIdentifier":    {"old-global"},
				"NewGlobalClusterIdentifier": {"new-global"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "new-global",
		},
		{
			name: "modify_global_cluster_deletion_protection",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"my-global"},
				})
			},
			vals: url.Values{
				"Action":                  {"ModifyGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"my-global"},
				"DeletionProtection":      {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-global",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement1_DescribeDBInstancesByCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *docdb.Handler)
		vals       url.Values
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "filter_by_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				for _, cid := range []string{"cluster-a", "cluster-b"} {
					doRequest(t, h, url.Values{
						"Action":              {"CreateDBCluster"},
						"Version":             {"2014-10-31"},
						"DBClusterIdentifier": {cid},
						"Engine":              {"docdb"},
					})
				}
				for _, id := range []string{"inst-a1", "inst-a2", "inst-b1"} {
					clusterID := "cluster-a"
					if id == "inst-b1" {
						clusterID = "cluster-b"
					}
					doRequest(t, h, url.Values{
						"Action":               {"CreateDBInstance"},
						"Version":              {"2014-10-31"},
						"DBInstanceIdentifier": {id},
						"DBClusterIdentifier":  {clusterID},
					})
				}
			},
			vals: url.Values{
				"Action":              {"DescribeDBInstances"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"cluster-a"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			body := rr.Body.String()

			type instance struct {
				XMLName xml.Name `xml:"DBInstance"`
			}
			type result struct {
				Instances []instance `xml:"DescribeDBInstancesResult>DBInstances>DBInstance"`
			}
			var res result
			_ = xml.Unmarshal([]byte(body), &res)
			assert.Len(t, res.Instances, tt.wantCount)
		})
	}
}

func TestRefinement1_DescribeDBClusterSnapshotsByType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "filter_by_manual_snapshot_type",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"my-snap"},
					"DBClusterIdentifier":         {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":       {"DescribeDBClusterSnapshots"},
				"Version":      {"2014-10-31"},
				"SnapshotType": {"manual"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-snap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement1_GlobalClusterWithEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_global_cluster_with_engine",
			vals: url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"my-global"},
				"Engine":                  {"docdb"},
				"EngineVersion":           {"5.0.0"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "5.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement1_EventSubscriptionSourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_subscription_with_source_type",
			vals: url.Values{
				"Action":                          {"CreateEventSubscription"},
				"Version":                         {"2014-10-31"},
				"SubscriptionName":                {"my-sub"},
				"SourceType":                      {"db-cluster"},
				"EventCategories.EventCategory.1": {"backup"},
				"EventCategories.EventCategory.2": {"failover"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement1_SnapshotHasSnapshotType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "snapshot_has_manual_type",
			wantContains: "manual",
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"my-snap"},
				"DBClusterIdentifier":         {"my-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement2_DeleteClusterProtections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *docdb.Handler)
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "deletion_protection_enabled",
			setup: func(h *docdb.Handler) {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"protected-cluster"},
					"DeletionProtection":  {"true"},
				})
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name: "active_instance_blocks_delete",
			setup: func(h *docdb.Handler) {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"cluster-with-inst"},
				})
				doRequest(t, h, url.Values{
					"Action":               {"CreateDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {"inst1"},
					"DBClusterIdentifier":  {"cluster-with-inst"},
					"DBInstanceClass":      {"db.r5.large"},
					"Engine":               {"docdb"},
				})
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			clusterID := "protected-cluster"
			if tt.name == "active_instance_blocks_delete" {
				clusterID = "cluster-with-inst"
			}
			rr := doRequest(t, h, url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {clusterID},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement2_CreateInstanceInheritsCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
	}{
		{
			name:         "instance_inherits_storage_encrypted",
			wantContains: "<StorageEncrypted>true</StorageEncrypted>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"enc-cluster"},
				"StorageEncrypted":    {"true"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"enc-inst"},
				"DBClusterIdentifier":  {"enc-cluster"},
				"DBInstanceClass":      {"db.r5.large"},
				"Engine":               {"docdb"},
			})
			assert.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement2_FailoverStoppedCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "failover_stopped_cluster_rejected",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			})
			doRequest(t, h, url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"FailoverDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement2_CopySnapshotDuplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "duplicate_snapshot_copy_rejected",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"src-cluster"},
			})
			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"src-snap"},
				"DBClusterIdentifier":         {"src-cluster"},
			})
			doRequest(t, h, url.Values{
				"Action":                            {"CopyDBClusterSnapshot"},
				"Version":                           {"2014-10-31"},
				"SourceDBClusterSnapshotIdentifier": {"src-snap"},
				"TargetDBClusterSnapshotIdentifier": {"dst-snap"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":                            {"CopyDBClusterSnapshot"},
				"Version":                           {"2014-10-31"},
				"SourceDBClusterSnapshotIdentifier": {"src-snap"},
				"TargetDBClusterSnapshotIdentifier": {"dst-snap"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement2_DeleteParameterGroupInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "parameter_group_in_use_rejected",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBParameterGroupStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
				"DBParameterGroupFamily":      {"docdb4.0"},
				"Description":                 {"test pg"},
			})
			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBCluster"},
				"Version":                     {"2014-10-31"},
				"DBClusterIdentifier":         {"pg-cluster"},
				"DBClusterParameterGroupName": {"my-pg"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":                      {"DeleteDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"my-pg"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRefinement2_DeleteSubnetGroupInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "subnet_group_in_use_rejected",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBSubnetGroupStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                   {"CreateDBSubnetGroup"},
				"Version":                  {"2014-10-31"},
				"DBSubnetGroupName":        {"my-sg"},
				"DBSubnetGroupDescription": {"test sg"},
				"SubnetIds.member.1":       {"subnet-aaa"},
			})
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"sg-cluster"},
				"DBSubnetGroupName":   {"my-sg"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":            {"DeleteDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"my-sg"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- AWS-accuracy audit tests ----

func TestAudit_CreateCluster_VpcSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		vpcSGIDs     []string
		wantStatus   int
	}{
		{
			name:         "single_vpc_sg",
			vpcSGIDs:     []string{"sg-aaaa1111"},
			wantContains: "sg-aaaa1111",
			wantStatus:   200,
		},
		{
			name:         "multiple_vpc_sgs",
			vpcSGIDs:     []string{"sg-aaaa1111", "sg-bbbb2222"},
			wantContains: "sg-aaaa1111",
			wantStatus:   200,
		},
		{
			name:         "no_vpc_sgs",
			vpcSGIDs:     nil,
			wantContains: "CreateDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"vpc-test-cluster"},
			}
			for i, sgID := range tt.vpcSGIDs {
				vals.Set(fmt.Sprintf("VpcSecurityGroupIds.member.%d", i+1), sgID)
			}

			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_CreateCluster_IAMDatabaseAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		paramVal     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "iam_auth_enabled",
			paramVal:     "true",
			wantContains: "IAMDatabaseAuthenticationEnabled",
			wantStatus:   200,
		},
		{
			name:         "iam_auth_disabled",
			paramVal:     "false",
			wantContains: "CreateDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":                          {"CreateDBCluster"},
				"Version":                         {"2014-10-31"},
				"DBClusterIdentifier":             {"iam-auth-cluster"},
				"EnableIAMDatabaseAuthentication": {tt.paramVal},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_CreateCluster_KmsKeyId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		kmsKeyID     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "kms_key_set",
			kmsKeyID:     "arn:aws:kms:us-east-1:000000000000:key/test-key-id",
			wantContains: "test-key-id",
			wantStatus:   200,
		},
		{
			name:         "no_kms_key",
			kmsKeyID:     "",
			wantContains: "CreateDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"kms-cluster"},
			}
			if tt.kmsKeyID != "" {
				vals.Set("KmsKeyId", tt.kmsKeyID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_CreateCluster_CloudwatchLogsExports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		logTypes     []string
		wantStatus   int
	}{
		{
			name:         "profiler_log_enabled",
			logTypes:     []string{"profiler"},
			wantContains: "profiler",
			wantStatus:   200,
		},
		{
			name:         "multiple_logs",
			logTypes:     []string{"profiler", "audit"},
			wantContains: "profiler",
			wantStatus:   200,
		},
		{
			name:         "no_logs",
			logTypes:     nil,
			wantContains: "CreateDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"logs-cluster"},
			}
			for i, lt := range tt.logTypes {
				vals.Set(fmt.Sprintf("EnableCloudwatchLogsExports.member.%d", i+1), lt)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_DeleteCluster_FinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(*testing.T, *docdb.Handler)
		vals               url.Values
		name               string
		wantContains       string
		wantStatus         int
		wantSnapshotExists bool
	}{
		{
			name: "skip_final_snapshot",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster"},
				"SkipFinalSnapshot":   {"true"},
			},
			wantStatus:         200,
			wantContains:       "DeleteDBClusterResponse",
			wantSnapshotExists: false,
		},
		{
			name: "create_final_snapshot",
			vals: url.Values{
				"Action":                           {"DeleteDBCluster"},
				"Version":                          {"2014-10-31"},
				"DBClusterIdentifier":              {"del-cluster"},
				"SkipFinalSnapshot":                {"false"},
				"FinalDBClusterSnapshotIdentifier": {"final-snap"},
			},
			wantStatus:         200,
			wantContains:       "DeleteDBClusterResponse",
			wantSnapshotExists: true,
		},
		{
			name: "final_snapshot_already_exists_fails",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"final-snap"},
					"DBClusterIdentifier":         {"del-cluster"},
				})
			},
			vals: url.Values{
				"Action":                           {"DeleteDBCluster"},
				"Version":                          {"2014-10-31"},
				"DBClusterIdentifier":              {"del-cluster"},
				"SkipFinalSnapshot":                {"false"},
				"FinalDBClusterSnapshotIdentifier": {"final-snap"},
			},
			wantStatus:   400,
			wantContains: "DBClusterSnapshotAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster"},
			})
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)

			if tt.wantSnapshotExists {
				snaps, err := h.Backend.DescribeDBClusterSnapshots(context.Background(), "final-snap", "", "")
				require.NoError(t, err)
				assert.Len(t, snaps, 1)
			}
		})
	}
}

func TestAudit_ModifyCluster_EngineVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
		wantContains  string
		wantStatus    int
	}{
		{
			name:          "upgrade_to_5_0",
			engineVersion: "5.0.0",
			wantContains:  "5.0.0",
			wantStatus:    200,
		},
		{
			name:          "no_version_change",
			engineVersion: "",
			wantContains:  "ModifyDBClusterResponse",
			wantStatus:    200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-ev-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-ev-cluster"},
			}
			if tt.engineVersion != "" {
				vals.Set("EngineVersion", tt.engineVersion)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_ModifyCluster_CloudwatchLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		enableLogs   []string
		disableLogs  []string
		wantStatus   int
	}{
		{
			name:         "enable_profiler_log",
			enableLogs:   []string{"profiler"},
			wantContains: "profiler",
			wantStatus:   200,
		},
		{
			name:         "enable_then_disable",
			enableLogs:   []string{"profiler"},
			disableLogs:  []string{"profiler"},
			wantContains: "ModifyDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-logs-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-logs-cluster"},
			}
			for i, lt := range tt.enableLogs {
				vals.Set(fmt.Sprintf("CloudwatchLogsExportConfiguration.EnableLogTypes.member.%d", i+1), lt)
			}
			for i, lt := range tt.disableLogs {
				vals.Set(fmt.Sprintf("CloudwatchLogsExportConfiguration.DisableLogTypes.member.%d", i+1), lt)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_ModifyCluster_Port(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		port         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "change_port",
			port:         "27018",
			wantContains: "ModifyDBClusterResponse",
			wantStatus:   200,
		},
		{
			name:         "no_port_change",
			port:         "",
			wantContains: "ModifyDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-port-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-port-cluster"},
			}
			if tt.port != "" {
				vals.Set("Port", tt.port)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_CreateInstance_CACertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		caCertID     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "ca_cert_set",
			caCertID:     "rds-ca-rsa2048-g1",
			wantContains: "rds-ca-rsa2048-g1",
			wantStatus:   200,
		},
		{
			name:         "no_ca_cert",
			caCertID:     "",
			wantContains: "CreateDBInstanceResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"ca-cert-inst"},
				"Engine":               {"docdb"},
			}
			if tt.caCertID != "" {
				vals.Set("CACertificateIdentifier", tt.caCertID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_CreateInstance_CopyTagsToSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		copyTagsToSnapshot string
		wantCopyTags       bool
		wantStatus         int
	}{
		{
			name:               "copy_tags_enabled",
			copyTagsToSnapshot: "true",
			wantCopyTags:       true,
			wantStatus:         200,
		},
		{
			name:               "copy_tags_disabled",
			copyTagsToSnapshot: "false",
			wantCopyTags:       false,
			wantStatus:         200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"copy-tags-inst"},
				"CopyTagsToSnapshot":   {tt.copyTagsToSnapshot},
				"Engine":               {"docdb"},
			})
			require.Equal(t, tt.wantStatus, rr.Code)

			instances, err := h.Backend.DescribeDBInstances(context.Background(), "copy-tags-inst", "")
			require.NoError(t, err)
			require.Len(t, instances, 1)
			assert.Equal(t, tt.wantCopyTags, instances[0].CopyTagsToSnapshot)
		})
	}
}

func TestAudit_ModifyInstance_CACertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		caCertID     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "update_ca_cert",
			caCertID:     "rds-ca-rsa2048-g1",
			wantContains: "rds-ca-rsa2048-g1",
			wantStatus:   200,
		},
		{
			name:         "no_ca_cert_change",
			caCertID:     "",
			wantContains: "ModifyDBInstanceResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"modify-ca-inst"},
				"Engine":               {"docdb"},
			})
			vals := url.Values{
				"Action":               {"ModifyDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"modify-ca-inst"},
			}
			if tt.caCertID != "" {
				vals.Set("CACertificateIdentifier", tt.caCertID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_ModifyInstance_PromotionTier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		promotionTier string
		wantTier      int
		wantStatus    int
	}{
		{
			name:          "set_tier_0",
			promotionTier: "0",
			wantTier:      0,
			wantStatus:    200,
		},
		{
			name:          "set_tier_15",
			promotionTier: "15",
			wantTier:      15,
			wantStatus:    200,
		},
		{
			name:          "no_tier_change",
			promotionTier: "",
			wantTier:      1,
			wantStatus:    200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"tier-inst"},
				"Engine":               {"docdb"},
			})
			vals := url.Values{
				"Action":               {"ModifyDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"tier-inst"},
			}
			if tt.promotionTier != "" {
				vals.Set("PromotionTier", tt.promotionTier)
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, tt.wantStatus, rr.Code)

			instances, err := h.Backend.DescribeDBInstances(context.Background(), "tier-inst", "")
			require.NoError(t, err)
			require.Len(t, instances, 1)
			assert.Equal(t, tt.wantTier, instances[0].PromotionTier)
		})
	}
}

func TestAudit_DeleteCluster_DeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		wantContains       string
		wantStatus         int
		deletionProtection bool
	}{
		{
			name:               "deletion_protection_blocks_delete",
			deletionProtection: true,
			wantStatus:         400,
			wantContains:       "InvalidDBClusterStateFault",
		},
		{
			name:               "no_deletion_protection_allows_delete",
			deletionProtection: false,
			wantStatus:         200,
			wantContains:       "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend
			b.AddDBClusterInternal(&docdb.DBCluster{
				DBClusterIdentifier: "prot-cluster",
				Status:              "available",
				DeletionProtection:  tt.deletionProtection,
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"prot-cluster"},
				"SkipFinalSnapshot":   {"true"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_DeleteCluster_WithInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
		addInstance  bool
	}{
		{
			name:         "fails_with_instances",
			addInstance:  true,
			wantStatus:   400,
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name:         "succeeds_without_instances",
			addInstance:  false,
			wantStatus:   200,
			wantContains: "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend
			b.AddDBClusterInternal(&docdb.DBCluster{
				DBClusterIdentifier: "inst-cluster",
				Status:              "available",
			})
			if tt.addInstance {
				b.AddDBInstanceInternal(&docdb.DBInstance{
					DBInstanceIdentifier: "inst-to-block",
					DBClusterIdentifier:  "inst-cluster",
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"inst-cluster"},
				"SkipFinalSnapshot":   {"true"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_ClusterVpcSGPersistedToBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantFirst string
		sgIDs     []string
		wantLen   int
	}{
		{
			name:      "two_sgs_stored",
			sgIDs:     []string{"sg-aaa", "sg-bbb"},
			wantLen:   2,
			wantFirst: "sg-aaa",
		},
		{
			name:    "no_sgs",
			sgIDs:   nil,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"sg-test-cluster"},
			}
			for i, sgID := range tt.sgIDs {
				vals.Set(fmt.Sprintf("VpcSecurityGroupIds.member.%d", i+1), sgID)
			}
			doRequest(t, h, vals)

			clusters, err := h.Backend.DescribeDBClusters(context.Background(), "sg-test-cluster")
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			assert.Len(t, clusters[0].VpcSecurityGroupIDs, tt.wantLen)
			if tt.wantFirst != "" {
				assert.Equal(t, tt.wantFirst, clusters[0].VpcSecurityGroupIDs[0])
			}
		})
	}
}

func TestAudit_ModifyCluster_VpcSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newSGIDs []string
		wantLen  int
	}{
		{
			name:     "replace_with_two_sgs",
			newSGIDs: []string{"sg-new1", "sg-new2"},
			wantLen:  2,
		},
		{
			name:     "no_sg_update",
			newSGIDs: nil,
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-sg-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-sg-cluster"},
			}
			for i, sgID := range tt.newSGIDs {
				vals.Set(fmt.Sprintf("VpcSecurityGroupIds.member.%d", i+1), sgID)
			}
			doRequest(t, h, vals)

			clusters, err := h.Backend.DescribeDBClusters(context.Background(), "modify-sg-cluster")
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			assert.Len(t, clusters[0].VpcSecurityGroupIDs, tt.wantLen)
		})
	}
}

func TestAudit_ModifyCluster_CloudwatchEnableDisable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		enableLogs   []string
		disableLogs  []string
		wantLogCount int
	}{
		{
			name:         "enable_two_log_types",
			enableLogs:   []string{"profiler", "audit"},
			wantLogCount: 2,
		},
		{
			name:         "enable_then_disable_one",
			enableLogs:   []string{"profiler", "audit"},
			disableLogs:  []string{"profiler"},
			wantLogCount: 1,
		},
		{
			name:         "disable_non_enabled_is_noop",
			disableLogs:  []string{"profiler"},
			wantLogCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"cw-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"cw-cluster"},
			}
			for i, lt := range tt.enableLogs {
				vals.Set(fmt.Sprintf("CloudwatchLogsExportConfiguration.EnableLogTypes.member.%d", i+1), lt)
			}
			for i, lt := range tt.disableLogs {
				vals.Set(fmt.Sprintf("CloudwatchLogsExportConfiguration.DisableLogTypes.member.%d", i+1), lt)
			}
			doRequest(t, h, vals)

			clusters, err := h.Backend.DescribeDBClusters(context.Background(), "cw-cluster")
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			assert.Len(t, clusters[0].EnabledCloudwatchLogsExports, tt.wantLogCount)
		})
	}
}

func TestAudit_ClusterResponseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
	}{
		{
			name: "all_expected_fields_present",
			wantContains: []string{
				"DBClusterArn",
				"DBClusterIdentifier",
				"Engine",
				"Status",
				"Endpoint",
				"ReaderEndpoint",
				"Port",
				"EngineVersion",
				"VpcSecurityGroups",
				"DBClusterMembers",
				"EnabledCloudwatchLogsExports",
				"IAMDatabaseAuthenticationEnabled",
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
				"DBClusterIdentifier": {"field-check-cluster"},
				"Engine":              {"docdb"},
				"MasterUsername":      {"admin"},
			})
			require.Equal(t, 200, rr.Code)
			body := rr.Body.String()
			for _, field := range tt.wantContains {
				assert.Contains(t, body, field, "field %q missing from response", field)
			}
		})
	}
}

func TestAudit_InstanceResponseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
	}{
		{
			name: "all_expected_fields_present",
			wantContains: []string{
				"DBInstanceArn",
				"DBInstanceIdentifier",
				"Engine",
				"DBInstanceStatus",
				"DBInstanceClass",
				"EngineVersion",
				"EnabledCloudwatchLogsExports",
				"CopyTagsToSnapshot",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"field-check-inst"},
				"Engine":               {"docdb"},
			})
			require.Equal(t, 200, rr.Code)
			body := rr.Body.String()
			for _, field := range tt.wantContains {
				assert.Contains(t, body, field, "field %q missing from response", field)
			}
		})
	}
}

func TestAudit_StopCluster_AlreadyStopped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "stop_already_stopped_cluster_fails",
			wantStatus:   400,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend
			b.AddDBClusterInternal(&docdb.DBCluster{
				DBClusterIdentifier: "stopped-cluster",
				Status:              "stopped",
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"stopped-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_StartCluster_AlreadyAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "start_already_available_cluster_fails",
			wantStatus:   400,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"avail-cluster"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"avail-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_FailoverCluster_StoppedFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "failover_stopped_cluster_fails",
			wantStatus:   400,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend
			b.AddDBClusterInternal(&docdb.DBCluster{
				DBClusterIdentifier: "stopped-fo-cluster",
				Status:              "stopped",
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"FailoverDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"stopped-fo-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_ClusterInheritedDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		wantEngine          string
		wantEngineVersion   string
		wantPort            int
		wantBackupRetention int
	}{
		{
			name:                "defaults_applied",
			wantEngine:          "docdb",
			wantEngineVersion:   "4.0.0",
			wantPort:            27017,
			wantBackupRetention: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			cluster, err := b.CreateDBCluster(
				context.Background(),
				"defaults-cluster", "", "", "admin", "", "", "", "",
				0, false, false, 0, "", "", nil, nil, nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEngine, cluster.Engine)
			assert.Equal(t, tt.wantEngineVersion, cluster.EngineVersion)
			assert.Equal(t, tt.wantPort, cluster.Port)
			assert.Equal(t, tt.wantBackupRetention, cluster.BackupRetentionPeriod)
		})
	}
}

func TestAudit_InstanceInheritsClusterProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterID      string
		wantEngineVer  string
		wantStorageEnc bool
	}{
		{
			name:           "instance_inherits_from_cluster",
			clusterID:      "parent-cluster",
			wantEngineVer:  "5.0.0",
			wantStorageEnc: true,
		},
		{
			name:           "instance_no_cluster_uses_defaults",
			clusterID:      "",
			wantEngineVer:  "4.0.0",
			wantStorageEnc: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.clusterID != "" {
				b.AddDBClusterInternal(&docdb.DBCluster{
					DBClusterIdentifier: tt.clusterID,
					EngineVersion:       "5.0.0",
					StorageEncrypted:    true,
				})
			}
			inst, err := b.CreateDBInstance(
				context.Background(),
				"inherit-inst", tt.clusterID, "", "docdb", 1, nil, nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEngineVer, inst.EngineVersion)
			assert.Equal(t, tt.wantStorageEnc, inst.StorageEncrypted)
		})
	}
}

func TestAudit_CopyCluster_SnapshotRetainsMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "copy_snapshot_retains_engine_version",
			wantContains: "4.0.0",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"snap-src-cluster"},
				"EngineVersion":       {"4.0.0"},
			})
			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"src-snap"},
				"DBClusterIdentifier":         {"snap-src-cluster"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":                            {"CopyDBClusterSnapshot"},
				"Version":                           {"2014-10-31"},
				"SourceDBClusterSnapshotIdentifier": {"src-snap"},
				"TargetDBClusterSnapshotIdentifier": {"dst-snap"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_RestoreCluster_FromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantContains   string
		wantStatus     int
		snapshotExists bool
		targetExists   bool
	}{
		{
			name:           "restore_from_valid_snapshot",
			snapshotExists: true,
			targetExists:   false,
			wantStatus:     200,
			wantContains:   "RestoreDBClusterFromSnapshotResponse",
		},
		{
			name:           "restore_with_nonexistent_snapshot_fails",
			snapshotExists: false,
			targetExists:   false,
			wantStatus:     400,
			wantContains:   "DBClusterSnapshotNotFoundFault",
		},
		{
			name:           "restore_when_target_exists_fails",
			snapshotExists: true,
			targetExists:   true,
			wantStatus:     400,
			wantContains:   "DBClusterAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.snapshotExists {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"orig-cluster"},
				})
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"restore-snap"},
					"DBClusterIdentifier":         {"orig-cluster"},
				})
			}
			if tt.targetExists {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"restored-cluster"},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":                      {"RestoreDBClusterFromSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"restore-snap"},
				"DBClusterIdentifier":         {"restored-cluster"},
				"Engine":                      {"docdb"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_RestoreCluster_ToPointInTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
		sourceExists bool
		targetExists bool
	}{
		{
			name:         "restore_from_source",
			sourceExists: true,
			targetExists: false,
			wantStatus:   200,
			wantContains: "RestoreDBClusterToPointInTimeResponse",
		},
		{
			name:         "source_not_found_fails",
			sourceExists: false,
			targetExists: false,
			wantStatus:   400,
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name:         "target_already_exists_fails",
			sourceExists: true,
			targetExists: true,
			wantStatus:   400,
			wantContains: "DBClusterAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.sourceExists {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"pitr-source"},
				})
			}
			if tt.targetExists {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"pitr-target"},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":                    {"RestoreDBClusterToPointInTime"},
				"Version":                   {"2014-10-31"},
				"SourceDBClusterIdentifier": {"pitr-source"},
				"DBClusterIdentifier":       {"pitr-target"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_GlobalCluster_FailoverSwitchover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		action       string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "failover_sets_failing_over_status",
			action:       "FailoverGlobalCluster",
			wantStatus:   200,
			wantContains: "failing-over",
		},
		{
			name:         "switchover_sets_switching_over_status",
			action:       "SwitchoverGlobalCluster",
			wantStatus:   200,
			wantContains: "switching-over",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"fo-gc"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":                    {tt.action},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"fo-gc"},
				"TargetDbClusterIdentifier": {"some-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_GlobalCluster_ModifyRenameAndDeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		newID              string
		deletionProtection string
		wantContains       string
		wantStatus         int
	}{
		{
			name:         "rename_global_cluster",
			newID:        "renamed-gc",
			wantContains: "renamed-gc",
			wantStatus:   200,
		},
		{
			name:               "set_deletion_protection",
			newID:              "",
			deletionProtection: "true",
			wantContains:       "ModifyGlobalClusterResponse",
			wantStatus:         200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"mod-gc"},
			})
			vals := url.Values{
				"Action":                  {"ModifyGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"mod-gc"},
			}
			if tt.newID != "" {
				vals.Set("NewGlobalClusterIdentifier", tt.newID)
			}
			if tt.deletionProtection != "" {
				vals.Set("DeletionProtection", tt.deletionProtection)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_SubnetGroup_ModifyDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		newDescription string
		wantContains   string
		wantStatus     int
	}{
		{
			name:           "update_description",
			newDescription: "updated description",
			wantContains:   "ModifyDBSubnetGroupResponse",
			wantStatus:     200,
		},
		{
			name:           "no_description_change",
			newDescription: "",
			wantContains:   "ModifyDBSubnetGroupResponse",
			wantStatus:     200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                   {"CreateDBSubnetGroup"},
				"Version":                  {"2014-10-31"},
				"DBSubnetGroupName":        {"mod-sg"},
				"DBSubnetGroupDescription": {"original"},
				"SubnetIds.member.1":       {"subnet-111"},
			})
			vals := url.Values{
				"Action":            {"ModifyDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"mod-sg"},
			}
			if tt.newDescription != "" {
				vals.Set("DBSubnetGroupDescription", tt.newDescription)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_SnapshotAttributes_AddRemove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		operation    string
		attrName     string
		wantContains string
		valuesToAdd  []string
		valuesToRm   []string
		wantStatus   int
	}{
		{
			name:         "add_restore_attribute",
			operation:    "ModifyDBClusterSnapshotAttribute",
			attrName:     "restore",
			valuesToAdd:  []string{"111111111111"},
			wantContains: "111111111111",
			wantStatus:   200,
		},
		{
			name:         "remove_restore_attribute",
			operation:    "ModifyDBClusterSnapshotAttribute",
			attrName:     "restore",
			valuesToAdd:  []string{"111111111111"},
			valuesToRm:   []string{"111111111111"},
			wantContains: "ModifyDBClusterSnapshotAttributeResponse",
			wantStatus:   200,
		},
		{
			name:         "describe_snapshot_attributes",
			operation:    "DescribeDBClusterSnapshotAttributes",
			attrName:     "",
			wantContains: "DescribeDBClusterSnapshotAttributesResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"attr-cluster"},
			})
			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"attr-snap"},
				"DBClusterIdentifier":         {"attr-cluster"},
			})

			if tt.operation == "ModifyDBClusterSnapshotAttribute" {
				vals := url.Values{
					"Action":                      {"ModifyDBClusterSnapshotAttribute"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"attr-snap"},
					"AttributeName":               {tt.attrName},
				}
				for i, v := range tt.valuesToAdd {
					vals.Set(fmt.Sprintf("ValuesToAdd.AttributeValue.%d", i+1), v)
				}
				for i, v := range tt.valuesToRm {
					vals.Set(fmt.Sprintf("ValuesToRemove.AttributeValue.%d", i+1), v)
				}
				rr := doRequest(t, h, vals)
				assert.Equal(t, tt.wantStatus, rr.Code)
				assert.Contains(t, rr.Body.String(), tt.wantContains)
			} else {
				rr := doRequest(t, h, url.Values{
					"Action":                      {"DescribeDBClusterSnapshotAttributes"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"attr-snap"},
				})
				assert.Equal(t, tt.wantStatus, rr.Code)
				assert.Contains(t, rr.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestAudit_EventSubscription_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_subscription_topic",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"mod-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:old-topic"},
				})
			},
			vals: url.Values{
				"Action":           {"ModifyEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"mod-sub"},
				"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:new-topic"},
			},
			wantStatus:   200,
			wantContains: "new-topic",
		},
		{
			name: "remove_source_identifier",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":               {"CreateEventSubscription"},
					"Version":              {"2014-10-31"},
					"SubscriptionName":     {"src-id-sub"},
					"SourceIds.SourceId.1": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":           {"RemoveSourceIdentifierFromSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"src-id-sub"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   200,
			wantContains: "RemoveSourceIdentifierFromSubscriptionResponse",
		},
		{
			name: "describe_event_subscriptions",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"desc-sub"},
				})
			},
			vals: url.Values{
				"Action":           {"DescribeEventSubscriptions"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"desc-sub"},
			},
			wantStatus:   200,
			wantContains: "desc-sub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_DescribeEventCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sourceType   string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "all_categories",
			sourceType:   "",
			wantContains: "db-cluster",
			wantStatus:   200,
		},
		{
			name:         "db_cluster_categories",
			sourceType:   "db-cluster",
			wantContains: "failover",
			wantStatus:   200,
		},
		{
			name:         "db_instance_categories",
			sourceType:   "db-instance",
			wantContains: "recovery",
			wantStatus:   200,
		},
		{
			name:         "snapshot_categories",
			sourceType:   "db-cluster-snapshot",
			wantContains: "restoration",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":  {"DescribeEventCategories"},
				"Version": {"2014-10-31"},
			}
			if tt.sourceType != "" {
				vals.Set("SourceType", tt.sourceType)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit_Pagination_Clusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxRecords  string
		marker      string
		wantCount   int
		wantHasMore bool
	}{
		{
			name:        "no_limit_returns_all",
			maxRecords:  "",
			wantCount:   5,
			wantHasMore: false,
		},
		{
			name:        "limit_to_2",
			maxRecords:  "2",
			wantCount:   2,
			wantHasMore: true,
		},
		{
			name:        "page_2_with_marker",
			maxRecords:  "2",
			marker:      "2",
			wantCount:   2,
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range 5 {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {fmt.Sprintf("cluster-%d", i)},
				})
			}
			vals := url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			}
			if tt.maxRecords != "" {
				vals.Set("MaxRecords", tt.maxRecords)
			}
			if tt.marker != "" {
				vals.Set("Marker", tt.marker)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, 200, rr.Code)
			body := rr.Body.String()
			if tt.wantHasMore {
				assert.Contains(t, body, "<Marker>")
			}
		})
	}
}

func TestAudit_SubnetGroup_DeleteInUseFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
		useInCluster bool
	}{
		{
			name:         "in_use_by_cluster_fails",
			useInCluster: true,
			wantStatus:   400,
			wantContains: "InvalidDBSubnetGroupStateFault",
		},
		{
			name:         "not_in_use_succeeds",
			useInCluster: false,
			wantStatus:   200,
			wantContains: "DeleteDBSubnetGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                   {"CreateDBSubnetGroup"},
				"Version":                  {"2014-10-31"},
				"DBSubnetGroupName":        {"inuse-sg"},
				"DBSubnetGroupDescription": {"test"},
				"SubnetIds.member.1":       {"subnet-aaa"},
			})
			if tt.useInCluster {
				h.Backend.AddDBClusterInternal(&docdb.DBCluster{
					DBClusterIdentifier: "using-cluster",
					DBSubnetGroupName:   "inuse-sg",
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":            {"DeleteDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"inuse-sg"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit2_MasterUserPassword_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		password     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "valid_password",
			password:     "ValidPass1",
			wantContains: "CreateDBClusterResponse",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "too_short",
			password:     "short",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "too_long",
			password:     strings.Repeat("a", 101),
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "contains_slash",
			password:     "password/here",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "contains_at_sign",
			password:     "password@here",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "no_password_ok",
			password:     "",
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"pw-cluster"},
				"Engine":              {"docdb"},
			}
			if tt.password != "" {
				vals.Set("MasterUserPassword", tt.password)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit2_EngineVersion_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
		wantContains  string
		wantStatus    int
	}{
		{
			name:          "valid_3_6",
			engineVersion: "3.6.0",
			wantStatus:    http.StatusOK,
			wantContains:  "3.6.0",
		},
		{
			name:          "valid_4_0",
			engineVersion: "4.0.0",
			wantStatus:    http.StatusOK,
			wantContains:  "4.0.0",
		},
		{
			name:          "valid_5_0",
			engineVersion: "5.0.0",
			wantStatus:    http.StatusOK,
			wantContains:  "5.0.0",
		},
		{
			name:          "invalid_version",
			engineVersion: "6.0.0",
			wantStatus:    http.StatusBadRequest,
			wantContains:  "InvalidParameterValue",
		},
		{
			name:          "empty_defaults_to_4_0",
			engineVersion: "",
			wantStatus:    http.StatusOK,
			wantContains:  "4.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"ev-cluster"},
				"Engine":              {"docdb"},
			}
			if tt.engineVersion != "" {
				vals.Set("EngineVersion", tt.engineVersion)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit2_CreateInstance_RequiresCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantContains  string
		createCluster bool
		wantStatus    int
	}{
		{
			name:          "cluster_exists",
			createCluster: true,
			wantStatus:    http.StatusOK,
			wantContains:  "CreateDBInstanceResponse",
		},
		{
			name:          "cluster_missing",
			createCluster: false,
			wantStatus:    http.StatusBadRequest,
			wantContains:  "DBClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.createCluster {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
					"Engine":              {"docdb"},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-inst"},
				"DBClusterIdentifier":  {"my-cluster"},
				"Engine":               {"docdb"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit2_PromotionTier_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tier         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "tier_0_valid",
			tier:         "0",
			wantStatus:   http.StatusOK,
			wantContains: "<PromotionTier>0</PromotionTier>",
		},
		{
			name:         "tier_15_valid",
			tier:         "15",
			wantStatus:   http.StatusOK,
			wantContains: "<PromotionTier>15</PromotionTier>",
		},
		{
			name:         "tier_16_invalid",
			tier:         "16",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "default_tier_1",
			tier:         "",
			wantStatus:   http.StatusOK,
			wantContains: "<PromotionTier>1</PromotionTier>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"pt-cluster"},
				"Engine":              {"docdb"},
			})
			vals := url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"pt-inst"},
				"DBClusterIdentifier":  {"pt-cluster"},
				"Engine":               {"docdb"},
			}
			if tt.tier != "" {
				vals.Set("PromotionTier", tt.tier)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit2_DBClusterMembers_Populated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		instanceCount int
		wantMembers   int
	}{
		{
			name:          "no_instances",
			instanceCount: 0,
			wantMembers:   0,
		},
		{
			name:          "one_instance",
			instanceCount: 1,
			wantMembers:   1,
		},
		{
			name:          "two_instances",
			instanceCount: 2,
			wantMembers:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"member-cluster"},
				"Engine":              {"docdb"},
			})
			for i := range tt.instanceCount {
				doRequest(t, h, url.Values{
					"Action":               {"CreateDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {fmt.Sprintf("member-inst-%d", i)},
					"DBClusterIdentifier":  {"member-cluster"},
					"Engine":               {"docdb"},
				})
			}

			rr := doRequest(t, h, url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"member-cluster"},
			})
			assert.Equal(t, http.StatusOK, rr.Code)

			type member struct {
				XMLName xml.Name `xml:"DBClusterMember"`
			}
			type result struct {
				Members []member `xml:"DescribeDBClustersResult>DBClusters>DBCluster>DBClusterMembers>DBClusterMember"`
			}
			var res result
			require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &res))
			assert.Len(t, res.Members, tt.wantMembers)
		})
	}
}

func TestAudit2_TagValidation_OnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		tagCount     int
		keyLen       int
		valueLen     int
		wantStatus   int
	}{
		{
			name:         "valid_single_tag",
			tagCount:     1,
			keyLen:       3,
			valueLen:     4,
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
		{
			name:         "key_128_chars_ok",
			tagCount:     1,
			keyLen:       128,
			valueLen:     1,
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
		{
			name:         "key_129_chars_fails",
			tagCount:     1,
			keyLen:       129,
			valueLen:     1,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "value_256_chars_ok",
			tagCount:     1,
			keyLen:       3,
			valueLen:     256,
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
		{
			name:         "value_257_chars_fails",
			tagCount:     1,
			keyLen:       3,
			valueLen:     257,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "50_tags_ok",
			tagCount:     50,
			keyLen:       3,
			valueLen:     1,
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
		{
			name:         "51_tags_fails",
			tagCount:     51,
			keyLen:       3,
			valueLen:     1,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"tag-cluster"},
				"Engine":              {"docdb"},
			}
			for i := range tt.tagCount {
				vals.Set(fmt.Sprintf("Tags.Tag.%d.Key", i+1), fmt.Sprintf("%s%d", strings.Repeat("k", tt.keyLen-1), i))
				vals.Set(fmt.Sprintf("Tags.Tag.%d.Value", i+1), strings.Repeat("v", tt.valueLen))
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit2_AddTagsToResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		tagCount     int
		keyLen       int
		valueLen     int
		wantStatus   int
	}{
		{
			name:         "valid_tag",
			tagCount:     1,
			keyLen:       3,
			valueLen:     4,
			wantStatus:   http.StatusOK,
			wantContains: "AddTagsToResourceResponse",
		},
		{
			name:         "key_too_long",
			tagCount:     1,
			keyLen:       129,
			valueLen:     1,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "value_too_long",
			tagCount:     1,
			keyLen:       3,
			valueLen:     257,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "51_tags_fails",
			tagCount:     51,
			keyLen:       3,
			valueLen:     1,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"tag-cluster"},
				"Engine":              {"docdb"},
			})
			clusters, err := h.Backend.DescribeDBClusters(context.Background(), "tag-cluster")
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			clusterARN := clusters[0].DBClusterArn

			vals := url.Values{
				"Action":       {"AddTagsToResource"},
				"Version":      {"2014-10-31"},
				"ResourceName": {clusterARN},
			}
			for i := range tt.tagCount {
				vals.Set(fmt.Sprintf("Tags.Tag.%d.Key", i+1), fmt.Sprintf("%s%d", strings.Repeat("k", tt.keyLen-1), i))
				vals.Set(fmt.Sprintf("Tags.Tag.%d.Value", i+1), strings.Repeat("v", tt.valueLen))
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestAudit2_BackupRetentionPeriod_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		retention    string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "valid_7",
			retention:    "7",
			wantStatus:   http.StatusOK,
			wantContains: "<BackupRetentionPeriod>7</BackupRetentionPeriod>",
		},
		{
			name:         "valid_35",
			retention:    "35",
			wantStatus:   http.StatusOK,
			wantContains: "<BackupRetentionPeriod>35</BackupRetentionPeriod>",
		},
		{
			name:         "too_large_36",
			retention:    "36",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "default_1",
			retention:    "",
			wantStatus:   http.StatusOK,
			wantContains: "<BackupRetentionPeriod>1</BackupRetentionPeriod>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"brp-cluster"},
				"Engine":              {"docdb"},
			}
			if tt.retention != "" {
				vals.Set("BackupRetentionPeriod", tt.retention)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}
