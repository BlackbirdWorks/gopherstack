package dax_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- CreateCluster ----

func TestHandlerCreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		check      func(t *testing.T, resp map[string]any)
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       validClusterBody("test-cluster"),
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				cluster := resp["Cluster"].(map[string]any)
				assert.Equal(t, "test-cluster", cluster["ClusterName"])
				assert.Equal(t, "available", cluster["Status"])
				assert.Equal(t, dax.EncryptionTypeNone, cluster["ClusterEndpointEncryptionType"])
			},
		},
		{
			name: "with TLS encryption",
			body: func() map[string]any {
				b := validClusterBody("tls-cluster")
				b["ClusterEndpointEncryptionType"] = "TLS"

				return b
			}(),
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				cluster := resp["Cluster"].(map[string]any)
				assert.Equal(t, "TLS", cluster["ClusterEndpointEncryptionType"])
			},
		},
		{
			name: "invalid node type",
			body: map[string]any{
				"ClusterName":       "bad",
				"NodeType":          "invalid.type",
				"IamRoleArn":        "arn:aws:iam::123456789012:role/r",
				"ReplicationFactor": 1,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "replication factor too high",
			body: map[string]any{
				"ClusterName":       "big",
				"NodeType":          "dax.r5.large",
				"IamRoleArn":        "arn:aws:iam::123456789012:role/r",
				"ReplicationFactor": 11,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := daxRequest(t, h, "CreateCluster", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

func TestHandlerCreateCluster_Duplicate(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	body := validClusterBody("dup-cluster")
	rec := daxRequest(t, h, "CreateCluster", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := daxRequest(t, h, "CreateCluster", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errResp))
	assert.Equal(t, "ClusterAlreadyExistsFault", errResp["__type"])
}

// ---- DescribeClusters ----

func TestHandlerDescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, h *dax.Handler)
		body      map[string]any
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			setup:     func(_ *testing.T, _ *dax.Handler) {},
			body:      map[string]any{},
			wantCount: 0,
		},
		{
			name: "after create",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("c1"))
			},
			body:      map[string]any{},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, "DescribeClusters", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			clusters := resp["Clusters"].([]any)
			assert.Len(t, clusters, tt.wantCount)
		})
	}
}

// ---- UpdateCluster ----

func TestHandlerUpdateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dax.Handler)
		body       map[string]any
		check      func(t *testing.T, resp map[string]any)
		name       string
		wantStatus int
	}{
		{
			name: "update description",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("upd-cluster"))
			},
			body: map[string]any{
				"ClusterName": "upd-cluster",
				"Description": "Updated",
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				cluster := resp["Cluster"].(map[string]any)
				assert.Equal(t, "Updated", cluster["Description"])
			},
		},
		{
			name: "update notification topic",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("notif-cluster"))
			},
			body: map[string]any{
				"ClusterName":          "notif-cluster",
				"NotificationTopicArn": "arn:aws:sns:us-east-1:123456789012:topic",
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				cluster := resp["Cluster"].(map[string]any)
				nc := cluster["NotificationConfiguration"].(map[string]any)
				assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:topic", nc["TopicArn"])
			},
		},
		{
			name:       "not found",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{"ClusterName": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, "UpdateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

// ---- DeleteCluster ----

func TestHandlerDeleteCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dax.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("del-cluster"))
			},
			body:       map[string]any{"ClusterName": "del-cluster"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{"ClusterName": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, "DeleteCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- IncreaseReplicationFactor / DecreaseReplicationFactor ----

func TestHandlerReplicationFactor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dax.Handler)
		body       map[string]any
		check      func(t *testing.T, resp map[string]any)
		name       string
		operation  string
		wantStatus int
	}{
		{
			name:      "increase 1 to 3",
			operation: "IncreaseReplicationFactor",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("grow"))
			},
			body:       map[string]any{"ClusterName": "grow", "NewReplicationFactor": 3},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				cluster := resp["Cluster"].(map[string]any)
				assert.InDelta(t, float64(3), cluster["TotalNodes"], 0)
			},
		},
		{
			name:      "decrease 3 to 1",
			operation: "DecreaseReplicationFactor",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				body := validClusterBody("shrink")
				body["ReplicationFactor"] = 3
				daxRequest(t, h, "CreateCluster", body)
			},
			body:       map[string]any{"ClusterName": "shrink", "NewReplicationFactor": 1},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				cluster := resp["Cluster"].(map[string]any)
				assert.InDelta(t, float64(1), cluster["TotalNodes"], 0)
			},
		},
		{
			name:       "increase cluster not found",
			operation:  "IncreaseReplicationFactor",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{"ClusterName": "no-such", "NewReplicationFactor": 2},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, tt.operation, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

// ---- RebootNode ----

func TestHandlerRebootNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dax.Handler)
		body       map[string]any
		check      func(t *testing.T, resp map[string]any)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("reboot-cluster"))
			},
			body:       map[string]any{"ClusterName": "reboot-cluster", "NodeId": "reboot-cluster-0000"},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				cluster := resp["Cluster"].(map[string]any)
				nodes := cluster["Nodes"].([]any)
				require.Len(t, nodes, 1)
				node := nodes[0].(map[string]any)
				assert.Equal(t, dax.StatusRebooting, node["NodeStatus"])
			},
		},
		{
			name:       "cluster not found",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{"ClusterName": "no-such", "NodeId": "n0"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, "RebootNode", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}
