package neptune_test

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

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func newTestHandler(t *testing.T) *neptune.Handler {
	t.Helper()
	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")

	return neptune.NewHandler(backend)
}

func doRequest(t *testing.T, h *neptune.Handler, vals url.Values) *httptest.ResponseRecorder {
	t.Helper()
	body := vals.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "aws-sdk-go-v2/1.0 api/neptune#1.0")
	rr := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rr)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rr
}

func createCluster(t *testing.T, h *neptune.Handler, id string) {
	t.Helper()
	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {id},
	})
}

func createInstance(t *testing.T, h *neptune.Handler, instanceID, clusterID string) {
	t.Helper()
	doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {instanceID},
		"DBClusterIdentifier":  {clusterID},
		"DBInstanceClass":      {"db.r5.large"},
	})
}

func TestHandler_CreateDescribeDeleteDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		setup        func(*neptune.Handler)
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
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-cluster",
		},
		{
			name: "describe_clusters",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "test-cluster")
			},
			vals: url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClustersResponse",
		},
		{
			name: "delete_cluster",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "test-cluster")
			},
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
		{
			name: "modify_cluster_not_found",
			vals: url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"mod-cluster"},
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
				tt.setup(h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_StopStartFailoverDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "stop_cluster",
			vals: url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"stop-cluster"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "start_cluster",
			vals: url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"stop-cluster"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "failover_cluster",
			vals: url.Values{
				"Action":              {"FailoverDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"stop-cluster"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createCluster(t, h, "stop-cluster")
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
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
				"DBInstanceIdentifier": {"test-instance"},
				"DBClusterIdentifier":  {"inst-cluster"},
				"DBInstanceClass":      {"db.r5.large"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-instance",
		},
		{
			name: "describe_instances",
			vals: url.Values{
				"Action":  {"DescribeDBInstances"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBInstancesResponse",
		},
		{
			name: "modify_instance",
			vals: url.Values{
				"Action":               {"ModifyDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"test-instance"},
				"DBInstanceClass":      {"db.r5.xlarge"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db.r5.xlarge",
		},
		{
			name: "reboot_instance",
			vals: url.Values{
				"Action":               {"RebootDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"test-instance"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RebootDBInstanceResponse",
		},
		{
			name: "delete_instance",
			vals: url.Values{
				"Action":               {"DeleteDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"test-instance"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBInstanceResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createCluster(t, h, "inst-cluster")
			if tt.name != "create_instance" {
				createInstance(t, h, "test-instance", "inst-cluster")
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DBSubnetGroups(t *testing.T) {
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
				"DBSubnetGroupName":        {"test-sg"},
				"DBSubnetGroupDescription": {"test subnet group"},
				"SubnetIds.member.1":       {"subnet-00000000"},
				"SubnetIds.member.2":       {"subnet-11111111"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-sg",
		},
		{
			name: "describe_subnet_groups",
			vals: url.Values{
				"Action":  {"DescribeDBSubnetGroups"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBSubnetGroupsResponse",
		},
		{
			name: "delete_subnet_group",
			vals: url.Values{
				"Action":            {"DeleteDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"test-sg"},
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
					"DBSubnetGroupName":        {"test-sg"},
					"DBSubnetGroupDescription": {"test subnet group"},
				})
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DBClusterParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_parameter_group",
			vals: url.Values{
				"Action":                      {"CreateDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"test-pg"},
				"DBParameterGroupFamily":      {"neptune1.3"},
				"Description":                 {"test parameter group"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-pg",
		},
		{
			name: "describe_parameter_groups",
			vals: url.Values{
				"Action":  {"DescribeDBClusterParameterGroups"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClusterParameterGroupsResponse",
		},
		{
			name: "modify_parameter_group",
			vals: url.Values{
				"Action":                      {"ModifyDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"test-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-pg",
		},
		{
			name: "delete_parameter_group",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"test-pg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterParameterGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.name != "create_parameter_group" {
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterParameterGroup"},
					"Version":                     {"2014-10-31"},
					"DBClusterParameterGroupName": {"test-pg"},
					"DBParameterGroupFamily":      {"neptune1.3"},
					"Description":                 {"test pg"},
				})
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DBClusterSnapshots(t *testing.T) {
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
				"DBClusterSnapshotIdentifier": {"test-snap"},
				"DBClusterIdentifier":         {"snap-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-snap",
		},
		{
			name: "describe_snapshots",
			vals: url.Values{
				"Action":  {"DescribeDBClusterSnapshots"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClusterSnapshotsResponse",
		},
		{
			name: "delete_snapshot",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"test-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterSnapshotResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createCluster(t, h, "snap-cluster")
			if tt.name != "create_snapshot" {
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"test-snap"},
					"DBClusterIdentifier":         {"snap-cluster"},
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

	arn := "arn:aws:neptune:us-east-1:000000000000:cluster:tag-cluster"

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
				"ResourceName":     {arn},
				"Tags.Tag.1.Key":   {"Env"},
				"Tags.Tag.1.Value": {"test"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "AddTagsToResourceResponse",
		},
		{
			name: "list_tags",
			vals: url.Values{
				"Action":       {"ListTagsForResource"},
				"Version":      {"2014-10-31"},
				"ResourceName": {arn},
			},
			wantStatus:   http.StatusOK,
			wantContains: "ListTagsForResourceResponse",
		},
		{
			name: "remove_tags",
			vals: url.Values{
				"Action":           {"RemoveTagsFromResource"},
				"Version":          {"2014-10-31"},
				"ResourceName":     {arn},
				"TagKeys.member.1": {"Env"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RemoveTagsFromResourceResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.name == "list_tags" || tt.name == "remove_tags" {
				doRequest(t, h, url.Values{
					"Action":           {"AddTagsToResource"},
					"Version":          {"2014-10-31"},
					"ResourceName":     {arn},
					"Tags.Tag.1.Key":   {"Env"},
					"Tags.Tag.1.Value": {"test"},
				})
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeEngineVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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
			wantContains: "neptune",
		},
		{
			name: "describe_orderable_options",
			vals: url.Values{
				"Action":  {"DescribeOrderableDBInstanceOptions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db.r5.large",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
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
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name: "instance_not_found",
			vals: url.Values{
				"Action":               {"DeleteDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBInstanceNotFound",
		},
		{
			name: "subnet_group_not_found",
			vals: url.Values{
				"Action":            {"DeleteDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBSubnetGroupNotFoundFault",
		},
		{
			name: "parameter_group_not_found",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterParameterGroupNotFoundFault",
		},
		{
			name: "snapshot_not_found",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
		{
			name: "invalid_cluster_identifier",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {""},
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

func TestHandler_DuplicateErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "cluster_already_exists",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"dup-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterAlreadyExistsFault",
		},
		{
			name: "instance_already_exists",
			vals: url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"dup-instance"},
				"DBClusterIdentifier":  {"dup-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBInstanceAlreadyExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createCluster(t, h, "dup-cluster")
			createInstance(t, h, "dup-instance", "dup-cluster")
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

	tests := []struct {
		name      string
		method    string
		path      string
		ua        string
		ct        string
		body      string
		wantMatch bool
	}{
		{
			name:      "valid_neptune_request",
			method:    http.MethodPost,
			path:      "/",
			ua:        "aws-sdk-go-v2/1.0 api/neptune#1.0",
			ct:        "application/x-www-form-urlencoded",
			body:      "Action=DescribeDBClusters&Version=2014-10-31",
			wantMatch: true,
		},
		{
			name:      "wrong_method",
			method:    http.MethodGet,
			path:      "/",
			ua:        "aws-sdk-go-v2/1.0 api/neptune#1.0",
			ct:        "application/x-www-form-urlencoded",
			body:      "Action=DescribeDBClusters&Version=2014-10-31",
			wantMatch: false,
		},
		{
			name:      "dashboard_path",
			method:    http.MethodPost,
			path:      "/dashboard/neptune",
			ua:        "aws-sdk-go-v2/1.0 api/neptune#1.0",
			ct:        "application/x-www-form-urlencoded",
			body:      "Action=DescribeDBClusters&Version=2014-10-31",
			wantMatch: false,
		},
		{
			name:      "wrong_user_agent",
			method:    http.MethodPost,
			path:      "/",
			ua:        "aws-sdk-go-v2/1.0 api/rds#1.0",
			ct:        "application/x-www-form-urlencoded",
			body:      "Action=DescribeDBClusters&Version=2014-10-31",
			wantMatch: false,
		},
		{
			name:      "wrong_version",
			method:    http.MethodPost,
			path:      "/",
			ua:        "aws-sdk-go-v2/1.0 api/neptune#1.0",
			ct:        "application/x-www-form-urlencoded",
			body:      "Action=DescribeDBClusters&Version=2012-12-01",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.ct)
			req.Header.Set("User-Agent", tt.ua)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)
			got := matcher(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestHandler_Metadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	t.Run("name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "Neptune", h.Name())
	})

	t.Run("chaos_service_name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "neptune", h.ChaosServiceName())
	})

	t.Run("supported_operations", func(t *testing.T) {
		t.Parallel()
		ops := h.GetSupportedOperations()
		assert.NotEmpty(t, ops)
		assert.Contains(t, ops, "CreateDBCluster")
		assert.Contains(t, ops, "DescribeDBClusters")
	})

	t.Run("extract_operation", func(t *testing.T) {
		t.Parallel()
		body := url.Values{
			"Action":  {"CreateDBCluster"},
			"Version": {"2014-10-31"},
		}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rr := httptest.NewRecorder()
		e := echo.New()
		c := e.NewContext(req, rr)
		op := h.ExtractOperation(c)
		assert.Equal(t, "CreateDBCluster", op)
	})

	t.Run("xml_header", func(t *testing.T) {
		t.Parallel()
		rr := doRequest(t, h, url.Values{
			"Action":  {"DescribeDBClusters"},
			"Version": {"2014-10-31"},
		})
		require.Equal(t, http.StatusOK, rr.Code)
		body, err := io.ReadAll(rr.Body)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(string(body), xml.Header))
	})
}

func TestHandler_DescribeDBClusters_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, id := range []string{"cluster-1", "cluster-2", "cluster-3"} {
		doRequest(t, h, url.Values{
			"Action":              {"CreateDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {id},
		})
	}

	tests := []struct {
		vals       url.Values
		name       string
		wantCode   int
		wantMarker bool
	}{
		{
			name: "all clusters",
			vals: url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "paginated with MaxRecords=1",
			vals: url.Values{
				"Action":     {"DescribeDBClusters"},
				"Version":    {"2014-10-31"},
				"MaxRecords": {"1"},
			},
			wantCode:   http.StatusOK,
			wantMarker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantCode, rr.Code)
			assert.Contains(t, rr.Body.String(), "DescribeDBClustersResponse")

			if tt.wantMarker {
				assert.Contains(t, rr.Body.String(), "<Marker>")
			}
		})
	}
}

func TestHandler_DescribeDBInstances_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "test-cluster")

	for _, id := range []string{"inst-1", "inst-2"} {
		createInstance(t, h, id, "test-cluster")
	}

	tests := []struct {
		vals       url.Values
		name       string
		wantCode   int
		wantMarker bool
	}{
		{
			name: "all instances",
			vals: url.Values{
				"Action":  {"DescribeDBInstances"},
				"Version": {"2014-10-31"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "paginated with MaxRecords=1",
			vals: url.Values{
				"Action":     {"DescribeDBInstances"},
				"Version":    {"2014-10-31"},
				"MaxRecords": {"1"},
			},
			wantCode:   http.StatusOK,
			wantMarker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantCode, rr.Code)

			if tt.wantMarker {
				assert.Contains(t, rr.Body.String(), "<Marker>")
			}
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	body := "Action=DescribeDBClusters&Version=2014-10-31&DBClusterIdentifier=my-cluster"
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "aws-sdk-go-v2/1.0 api/neptune#1.0")
	c := e.NewContext(req, httptest.NewRecorder())
	resource := h.ExtractResource(c)
	assert.Equal(t, "my-cluster", resource)
}

func TestNeptune_Provider(t *testing.T) {
	t.Parallel()

	p := &neptune.Provider{}
	assert.Equal(t, "Neptune", p.Name())
}

func TestHandler_DescribeSubnetGroupsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"sg-1", "sg-2", "sg-3"} {
		doRequest(t, h, url.Values{
			"Action":                   {"CreateDBSubnetGroup"},
			"Version":                  {"2014-10-31"},
			"DBSubnetGroupName":        {name},
			"DBSubnetGroupDescription": {"test"},
		})
	}

	rr := doRequest(t, h, url.Values{
		"Action":     {"DescribeDBSubnetGroups"},
		"Version":    {"2014-10-31"},
		"MaxRecords": {"1"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<Marker>")
}

func TestHandler_DescribeClusterSnapshotsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap-cluster")

	for _, id := range []string{"snap-1", "snap-2"} {
		doRequest(t, h, url.Values{
			"Action":                      {"CreateDBClusterSnapshot"},
			"Version":                     {"2014-10-31"},
			"DBClusterSnapshotIdentifier": {id},
			"DBClusterIdentifier":         {"snap-cluster"},
		})
	}

	rr := doRequest(t, h, url.Values{
		"Action":     {"DescribeDBClusterSnapshots"},
		"Version":    {"2014-10-31"},
		"MaxRecords": {"1"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<Marker>")
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Contains(t, ops, "CreateDBCluster")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	assert.NotEmpty(t, regions)
}

func TestHandler_ModifyReboot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals     url.Values
		name     string
		wantBody string
		wantCode int
	}{
		{
			name: "modify cluster",
			vals: url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"mod-cluster"},
			},
			wantCode: http.StatusOK,
			wantBody: "ModifyDBClusterResponse",
		},
		{
			name: "stop cluster",
			vals: url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"mod-cluster"},
			},
			wantCode: http.StatusOK,
			wantBody: "StopDBClusterResponse",
		},
		{
			name: "start cluster",
			vals: url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"mod-cluster"},
			},
			wantCode: http.StatusOK,
			wantBody: "StartDBClusterResponse",
		},
		{
			name: "failover cluster",
			vals: url.Values{
				"Action":              {"FailoverDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"mod-cluster"},
			},
			wantCode: http.StatusOK,
			wantBody: "FailoverDBClusterResponse",
		},
		{
			name: "modify instance",
			vals: url.Values{
				"Action":               {"ModifyDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"mod-inst"},
				"DBInstanceClass":      {"db.r5.large"},
			},
			wantCode: http.StatusOK,
			wantBody: "ModifyDBInstanceResponse",
		},
		{
			name: "reboot instance",
			vals: url.Values{
				"Action":               {"RebootDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"mod-inst"},
			},
			wantCode: http.StatusOK,
			wantBody: "RebootDBInstanceResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "mod-cluster")
			createInstance(t, h, "mod-inst", "mod-cluster")

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantCode, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantBody)
		})
	}
}

func TestHandler_DeleteClusterAndInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals     url.Values
		name     string
		wantBody string
		wantCode int
	}{
		{
			name: "delete instance",
			vals: url.Values{
				"Action":               {"DeleteDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"del-inst"},
			},
			wantCode: http.StatusOK,
			wantBody: "DeleteDBInstanceResponse",
		},
		{
			name: "delete cluster",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster"},
			},
			wantCode: http.StatusOK,
			wantBody: "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "del-cluster")
			createInstance(t, h, "del-inst", "del-cluster")

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantCode, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantBody)
		})
	}
}

func TestHandler_DescribeGlobalClusters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DescribeGlobalClustersResponse")
}

func TestHandler_DeleteClusterSnapshot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap2-cluster")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap2"},
		"DBClusterIdentifier":         {"snap2-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap2"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DeleteDBClusterSnapshotResponse")
}
