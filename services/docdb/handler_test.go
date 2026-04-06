package docdb_test

import (
	"encoding/xml"
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
name  string
setup func(b *docdb.InMemoryBackend)
want  int
}{
{
name: "empty_backend_reset",
setup: func(_ *docdb.InMemoryBackend) {},
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
tt.setup(b)
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
name    string
wantErr error
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

got, err := b.DescribeDBClusters("")
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

got, err := b.DescribeDBInstances("")
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

got, err := b.DescribeDBSubnetGroups("")
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

got, err := b.DescribeDBClusterParameterGroups("")
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

got, err := b.DescribeDBClusterSnapshots("", "")
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

got := b.DescribeGlobalClusters("")

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
b.AddTagsToResource("arn:aws:rds:us-east-1:000000000000:cluster:test", tt.tags)

got := b.ListTagsForResource("arn:aws:rds:us-east-1:000000000000:cluster:test")

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

clusters, err := h.Backend.DescribeDBClusters(tt.id)
require.NoError(t, err)
require.Len(t, clusters, 1)

assert.Equal(t, tt.wantLen, len(clusters[0].Tags))
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

instances, err := h.Backend.DescribeDBInstances(tt.id)
require.NoError(t, err)
require.Len(t, instances, 1)

assert.Equal(t, tt.wantLen, len(instances[0].Tags))
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

got, err := b.DescribeDBClusterSnapshots("", tt.clusterID)
require.NoError(t, err)

assert.Len(t, got, tt.wantCount)
})
}
}

func TestRefinement1_DescribeGlobalClusters_RealData(t *testing.T) {
t.Parallel()

tests := []struct {
name      string
gcIDs     []string
filterID  string
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
err := b.ApplyPendingMaintenanceAction("arn:aws:rds:us-east-1:000000000000:cluster:c1", "system-update", tt.optInType)

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
name    string
seedFn  func(b *docdb.InMemoryBackend)
checkFn func(t *testing.T, b *docdb.InMemoryBackend)
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
b.AddDBClusterParameterGroupInternal(&docdb.DBClusterParameterGroup{DBClusterParameterGroupName: "seed-pg"})
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

clusters, err := b2.DescribeDBClusters(tt.wantCluster)
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
_, err := b.DeleteDBCluster(tt.id)

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

for i := 0; i < tt.resetTimes; i++ {
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
