package docdb_test

import (
	"context"
	"encoding/xml"
	"maps"
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

func TestHandler_ReadOnlySmokeOperations(t *testing.T) {
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
			wantContains: "DBParameterGroupNotFound",
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

func TestReset(t *testing.T) {
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

func TestHandlerReset(t *testing.T) {
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

func TestProviderInit_NilCtx(t *testing.T) {
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

func TestSeedHelpers(t *testing.T) {
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

func TestExportCountHelpers(t *testing.T) {
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

func TestPersistenceRoundTrip(t *testing.T) {
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

			data := b1.Snapshot(t.Context())
			require.NotEmpty(t, data)

			b2 := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			err := b2.Restore(t.Context(), data)
			require.NoError(t, err)

			clusters, err := b2.DescribeDBClusters(context.Background(), tt.wantCluster)
			require.NoError(t, err)
			require.Len(t, clusters, 1)

			assert.Equal(t, tt.wantCluster, clusters[0].DBClusterIdentifier)
		})
	}
}

func TestMultipleResetCycle(t *testing.T) {
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

func b2CreateCluster(t *testing.T, h *docdb.Handler, id string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {id},
		"Engine":              {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create cluster %s: %s", id, rr.Body.String())
}

func b2CreateInstance(t *testing.T, h *docdb.Handler, instanceID, clusterID string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {instanceID},
		"DBClusterIdentifier":  {clusterID},
		"DBInstanceClass":      {"db.t3.medium"},
		"Engine":               {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create instance %s: %s", instanceID, rr.Body.String())
}

func b2CreateSubnetGroup(t *testing.T, h *docdb.Handler, name string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {name},
		"DBSubnetGroupDescription": {"test"},
		"SubnetIds.SubnetId.1":     {"subnet-aaa"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create subnet group %s: %s", name, rr.Body.String())
}

func b2CreateSnapshot(t *testing.T, h *docdb.Handler, snapshotID, clusterID string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {snapshotID},
		"DBClusterIdentifier":         {clusterID},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create snapshot %s: %s", snapshotID, rr.Body.String())
}

func b2CreateParamGroup(t *testing.T, h *docdb.Handler, name string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {name},
		"DBParameterGroupFamily":      {"docdb4.0"},
		"Description":                 {"test group"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create param group %s: %s", name, rr.Body.String())
}

func pbCreateCluster(t *testing.T, h *docdb.Handler, clusterID string, extraVals url.Values) {
	t.Helper()
	vals := url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {clusterID},
		"Engine":              {"docdb"},
	}
	maps.Copy(vals, extraVals)
	rr := doRequest(t, h, vals)
	require.Equal(t, http.StatusOK, rr.Code, "create cluster %s: %s", clusterID, rr.Body.String())
}

func pbCreateInstance(t *testing.T, h *docdb.Handler, instanceID, clusterID string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {instanceID},
		"DBClusterIdentifier":  {clusterID},
		"DBInstanceClass":      {"db.t3.medium"},
		"Engine":               {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create instance %s: %s", instanceID, rr.Body.String())
}

func pbExtractErrorCode(t *testing.T, body string) string {
	t.Helper()
	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &errResp))

	return errResp.Error.Code
}
