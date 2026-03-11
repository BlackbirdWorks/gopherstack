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
		name         string
		action       string
		vals         url.Values
		wantStatus   int
		wantContains string
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
		name         string
		vals         url.Values
		wantStatus   int
		wantContains string
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
		name         string
		vals         url.Values
		wantStatus   int
		wantContains string
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
		name         string
		vals         url.Values
		wantStatus   int
		wantContains string
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
		name         string
		vals         url.Values
		wantStatus   int
		wantContains string
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
		name         string
		vals         url.Values
		wantStatus   int
		wantContains string
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
		name         string
		vals         url.Values
		wantStatus   int
		wantContains string
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
		name         string
		vals         url.Values
		wantStatus   int
		wantContains string
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
		name         string
		vals         url.Values
		wantStatus   int
		wantContains string
		seedName     string
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
