package neptune_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/neptune"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				"SkipFinalSnapshot":   {"true"},
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
			wantContains: "DBParameterGroupNotFound",
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

// TestBackendReset verifies that Reset() clears all maps.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	backend.AddClusterInternal("c1")
	backend.AddClusterInternal("c2")

	require.Equal(t, 2, neptune.ClusterCount(backend))

	backend.Reset()

	assert.Equal(t, 0, neptune.ClusterCount(backend))
	assert.Equal(t, 0, neptune.InstanceCount(backend))
	assert.Equal(t, 0, neptune.SubnetGroupCount(backend))
	assert.Equal(t, 0, neptune.ClusterParameterGroupCount(backend))
	assert.Equal(t, 0, neptune.ClusterSnapshotCount(backend))
	assert.Equal(t, 0, neptune.ParameterGroupCount(backend))
	assert.Equal(t, 0, neptune.ClusterEndpointCount(backend))
	assert.Equal(t, 0, neptune.EventSubscriptionCount(backend))
	assert.Equal(t, 0, neptune.GlobalClusterCount(backend))
	assert.Equal(t, 0, neptune.TagCount(backend))
}

// TestHandlerReset verifies that Handler.Reset() delegates to the backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "c1")
	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_ = neptune.NewHandler(backend)
	// Reset via handler
	createCluster(t, h, "del-me")
	h.Reset()

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusters"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.NotContains(t, rr.Body.String(), "del-me")
}

// TestAccountID verifies AccountID is returned from the backend.
func TestAccountID(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", backend.AccountID())
	assert.Equal(t, "eu-west-1", backend.Region())
}

// TestProviderInit_NilCtx verifies ErrNilAppContext is returned when nil ctx is passed.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &neptune.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, neptune.ErrNilAppContext)
}

// TestProviderInit_ValidCtx verifies Init succeeds with a valid context.
func TestProviderInit_ValidCtx(t *testing.T) {
	t.Parallel()

	p := &neptune.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestGetSupportedOperations_AllOps verifies the expected ops count and sorting.
func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	require.NotEmpty(t, ops)

	// Verify all 10 new ops are present
	expectedNew := []string{
		"AddRoleToDBCluster",
		"AddSourceIdentifierToSubscription",
		"ApplyPendingMaintenanceAction",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CopyDBParameterGroup",
		"CreateDBClusterEndpoint",
		"CreateDBParameterGroup",
		"CreateEventSubscription",
		"CreateGlobalCluster",
	}
	for _, op := range expectedNew {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}

	// Verify sorted
	sorted := make([]string, len(ops))
	copy(sorted, ops)
	sort.Strings(sorted)
	assert.Equal(t, sorted, ops, "GetSupportedOperations should return sorted list")

	assert.Len(t, ops, neptune.HandlerOpsLen(h))
}

// TestPersistenceRoundTrip verifies Snapshot/Restore preserves all state.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	backend.AddClusterInternal("persist-cluster")
	backend.AddSnapshotInternal("persist-snap", "persist-cluster")
	backend.AddClusterParameterGroupInternal("persist-pg", "neptune1.3")
	backend.AddParameterGroupInternal("persist-dpg", "neptune1.3")
	backend.AddEventSubscriptionInternal("persist-sub", "arn:aws:sns:us-east-1:000000000000:test")

	data := backend.Snapshot(t.Context())
	require.NotEmpty(t, data)

	// Verify it's valid JSON
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	restored := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), data))

	assert.Equal(t, 1, neptune.ClusterCount(restored))
	assert.Equal(t, 1, neptune.ClusterSnapshotCount(restored))
	assert.Equal(t, 1, neptune.ClusterParameterGroupCount(restored))
	assert.Equal(t, 1, neptune.ParameterGroupCount(restored))
	assert.Equal(t, 1, neptune.EventSubscriptionCount(restored))
}

// TestSeedHelpers verifies all AddXInternal seed helpers work correctly.
func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	backend.AddClusterInternal("s-cluster")
	backend.AddSnapshotInternal("s-snap", "s-cluster")
	backend.AddClusterParameterGroupInternal("s-cpg", "neptune1.3")
	backend.AddParameterGroupInternal("s-pg", "neptune1.3")
	backend.AddEventSubscriptionInternal("s-sub", "arn:aws:sns:us-east-1:000000000000:test")

	assert.Equal(t, 1, neptune.ClusterCount(backend))
	assert.Equal(t, 1, neptune.ClusterSnapshotCount(backend))
	assert.Equal(t, 1, neptune.ClusterParameterGroupCount(backend))
	assert.Equal(t, 1, neptune.ParameterGroupCount(backend))
	assert.Equal(t, 1, neptune.EventSubscriptionCount(backend))
}

// TestPersistence_EmptyRestore verifies Restore with empty maps is safe.
func TestPersistence_EmptyRestore(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	data := backend.Snapshot(t.Context())
	require.NotEmpty(t, data)

	fresh := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), data))
	assert.Equal(t, 0, neptune.ClusterCount(fresh))
}

// TestPersistence_InvalidJSON verifies Restore returns error on bad data.
func TestPersistence_InvalidJSON(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	err := backend.Restore(t.Context(), []byte(`not-json`))
	require.Error(t, err)
}

// TestPersistence_Handler tests handler Snapshot and Restore.
func TestPersistence_Handler(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterInternal("persist-cluster")

	// Snapshot via handler
	data := h.Snapshot(t.Context())
	require.NotEmpty(t, data)

	// Restore into fresh handler
	backend2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := neptune.NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), data))
	assert.Equal(t, 1, neptune.ClusterCount(backend2))
}
