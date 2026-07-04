package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

func createTestCluster(t *testing.T, h *medialive.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{
		"name":        "test-cluster",
		"ClusterType": "ON_PREMISES",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["Id"].(string)
}

func TestCluster_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "create returns cluster with ARN and ACTIVE state",
			body: map[string]any{
				"name":        "my-cluster",
				"ClusterType": "ON_PREMISES",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["Arn"], "arn:aws:medialive:us-east-1:000000000000:cluster:")
				assert.Equal(t, "ACTIVE", resp["State"])
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, "my-cluster", resp["Name"])
				assert.Equal(t, "ON_PREMISES", resp["ClusterType"])
			},
		},
		{
			name:     "create missing Name returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/clusters", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestCluster_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterID := createTestCluster(t, h)

	assert.Equal(t, 1, medialive.ClusterCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, http.MethodGet, "/prod/clusters/"+clusterID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, clusterID, descResp["Id"])
	assert.Equal(t, "ACTIVE", descResp["State"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/prod/clusters/"+clusterID, map[string]any{
		"name": "updated-cluster",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated-cluster", updateResp["Name"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/clusters", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	clusters := listResp["Clusters"].([]any)
	assert.Len(t, clusters, 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/clusters/"+clusterID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var delResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &delResp))
	assert.Equal(t, "DELETED", delResp["State"])

	assert.Equal(t, 0, medialive.ClusterCount(h.Backend.(*medialive.InMemoryBackend)))
}

func TestCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe", http.MethodGet, "/prod/clusters/missing"},
		{"update", http.MethodPut, "/prod/clusters/missing"},
		{"delete", http.MethodDelete, "/prod/clusters/missing"},
		{"list-alerts", http.MethodGet, "/prod/clusters/missing/alerts"},
		{"node-reg-script", http.MethodPost, "/prod/clusters/missing/nodeRegistrationScript"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestClusterAlerts_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterID := createTestCluster(t, h)

	rec := doRequest(t, h, http.MethodGet, "/prod/clusters/"+clusterID+"/alerts", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	alerts := resp["Alerts"].([]any)
	assert.Empty(t, alerts)
}

func TestNodeRegistrationScript(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterID := createTestCluster(t, h)

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/clusters/"+clusterID+"/nodeRegistrationScript",
		nil,
	)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	script, ok := resp["NodeRegistrationScript"].(string)
	assert.True(t, ok)
	assert.NotEmpty(t, script)
}

func TestNode_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterID := createTestCluster(t, h)

	// Create node
	rec := doRequest(t, h, http.MethodPost, "/prod/clusters/"+clusterID+"/nodes", map[string]any{
		"name": "my-node",
		"Role": "ACTIVE",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	nodeID := createResp["Id"].(string)
	assert.NotEmpty(t, nodeID)
	assert.Equal(t, "ACTIVE", createResp["State"])
	assert.Equal(t, "CONNECTED", createResp["ConnectionState"])
	assert.Contains(t, createResp["Arn"], "arn:aws:medialive:us-east-1:000000000000:node:")
	assert.Equal(t, clusterID, createResp["ClusterId"])

	assert.Equal(t, 1, medialive.NodeCount(h.Backend.(*medialive.InMemoryBackend), clusterID))

	// Describe node
	rec = doRequest(t, h, http.MethodGet, "/prod/clusters/"+clusterID+"/nodes/"+nodeID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, nodeID, descResp["Id"])

	// Update node
	rec = doRequest(
		t,
		h,
		http.MethodPut,
		"/prod/clusters/"+clusterID+"/nodes/"+nodeID,
		map[string]any{
			"name": "updated-node",
			"Role": "BACKUP",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated-node", updateResp["Name"])
	assert.Equal(t, "BACKUP", updateResp["Role"])

	// UpdateNodeState
	rec = doRequest(
		t,
		h,
		http.MethodPut,
		"/prod/clusters/"+clusterID+"/nodes/"+nodeID+"/state",
		map[string]any{
			"State": "DRAINING",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	var stateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stateResp))
	assert.Equal(t, "DRAINING", stateResp["State"])

	// List nodes
	rec = doRequest(t, h, http.MethodGet, "/prod/clusters/"+clusterID+"/nodes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	nodes := listResp["Nodes"].([]any)
	assert.Len(t, nodes, 1)

	// Delete node
	rec = doRequest(t, h, http.MethodDelete, "/prod/clusters/"+clusterID+"/nodes/"+nodeID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 0, medialive.NodeCount(h.Backend.(*medialive.InMemoryBackend), clusterID))
}

func TestNode_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterID := createTestCluster(t, h)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe missing cluster", http.MethodGet, "/prod/clusters/missing/nodes/n1"},
		{"describe missing node", http.MethodGet, "/prod/clusters/" + clusterID + "/nodes/missing"},
		{"update missing node", http.MethodPut, "/prod/clusters/" + clusterID + "/nodes/missing"},
		{
			"update-state missing node",
			http.MethodPut,
			"/prod/clusters/" + clusterID + "/nodes/missing/state",
		},
		{
			"delete missing node",
			http.MethodDelete,
			"/prod/clusters/" + clusterID + "/nodes/missing",
		},
		{"list nodes missing cluster", http.MethodGet, "/prod/clusters/missing/nodes"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, map[string]any{})
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestListClusterAlerts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		forceState    string
		wantAlertCode string
		wantStatus    int
		setupCluster  bool
		wantEmpty     bool
	}{
		{
			name:         "cluster not found returns 404",
			setupCluster: false,
			wantStatus:   http.StatusNotFound,
		},
		{
			name:         "active cluster returns empty alerts",
			setupCluster: true,
			forceState:   "",
			wantStatus:   http.StatusOK,
			wantEmpty:    true,
		},
		{
			name:          "non-active cluster returns synthetic alert",
			setupCluster:  true,
			forceState:    "DELETING",
			wantStatus:    http.StatusOK,
			wantAlertCode: "CLUSTER_NOT_READY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clusterID := "missing-cluster-id"

			if tt.setupCluster {
				clusterID = createTestCluster(t, h)
				if tt.forceState != "" {
					medialive.ForceClusterState(
						h.Backend.(*medialive.InMemoryBackend),
						clusterID,
						tt.forceState,
					)
				}
			}

			rec := doRequest(t, h, http.MethodGet, "/prod/clusters/"+clusterID+"/alerts", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				alerts := resp["Alerts"].([]any)

				if tt.wantEmpty {
					assert.Empty(t, alerts)
				} else {
					require.NotEmpty(t, alerts)
					first := alerts[0].(map[string]any)
					assert.Equal(t, tt.wantAlertCode, first["AlertCode"])
				}
			}
		})
	}
}
