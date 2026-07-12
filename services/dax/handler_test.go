package dax_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// newTestHandler creates a handler backed by a fresh in-memory backend.
func newTestHandler() *dax.Handler {
	return dax.NewHandler(dax.NewInMemoryBackend("123456789012", "us-east-1"))
}

// daxRequest sends a JSON request to the handler with the given target and body.
func daxRequest(t *testing.T, h *dax.Handler, target string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer

	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}

	req := httptest.NewRequest(http.MethodPost, "/", &buf)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonDAXV3."+target)

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func validClusterBody(name string) map[string]any {
	return map[string]any{
		"ClusterName":       name,
		"NodeType":          "dax.r5.large",
		"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
		"ReplicationFactor": 1,
	}
}

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

// ---- Tags ----

func TestHandlerTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *dax.Handler) string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name: "tag cluster",
			setup: func(t *testing.T, h *dax.Handler) string {
				t.Helper()
				rec := daxRequest(t, h, "CreateCluster", validClusterBody("tagged-cluster"))
				var resp map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["Cluster"].(map[string]any)["ClusterArn"].(string)
			},
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "resource not found",
			setup: func(_ *testing.T, _ *dax.Handler) string {
				return "arn:aws:dax:us-east-1:123456789012:cache/no-such"
			},
			tags:       []map[string]string{{"Key": "k", "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			arn := tt.setup(t, h)

			rec := daxRequest(t, h, "TagResource", map[string]any{
				"ResourceName": arn,
				"Tags":         tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandlerListTags(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	createRec := daxRequest(t, h, "CreateCluster", validClusterBody("tagged-cluster"))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	clusterArn := createResp["Cluster"].(map[string]any)["ClusterArn"].(string)

	tagRec := daxRequest(t, h, "TagResource", map[string]any{
		"ResourceName": clusterArn,
		"Tags":         []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	listRec := daxRequest(t, h, "ListTags", map[string]any{"ResourceName": clusterArn})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].([]any)
	assert.NotEmpty(t, tags)
}

// ---- Parameter Groups ----

func TestHandlerParameterGroups(t *testing.T) {
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
			name:       "create",
			operation:  "CreateParameterGroup",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{"ParameterGroupName": "my-pg", "Description": "My parameter group"},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				pg := resp["ParameterGroup"].(map[string]any)
				assert.Equal(t, "my-pg", pg["ParameterGroupName"])
			},
		},
		{
			name:       "describe all",
			operation:  "DescribeParameterGroups",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				groups := resp["ParameterGroups"].([]any)
				assert.NotEmpty(t, groups)
			},
		},
		{
			name:      "update",
			operation: "UpdateParameterGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "upd-pg"})
			},
			body: map[string]any{
				"ParameterGroupName": "upd-pg",
				"ParameterNameValues": []map[string]string{
					{"ParameterName": "query-ttl-millis", "ParameterValue": "60000"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "delete",
			operation: "DeleteParameterGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "pg-del"})
			},
			body:       map[string]any{"ParameterGroupName": "pg-del"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe parameters",
			operation:  "DescribeParameters",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{"ParameterGroupName": dax.DefaultParameterGroupName},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				params := resp["Parameters"].([]any)
				assert.NotEmpty(t, params)
			},
		},
		{
			name:       "describe default parameters",
			operation:  "DescribeDefaultParameters",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				params := resp["Parameters"].([]any)
				assert.Len(t, params, 2)
			},
		},
		{
			name:      "reset parameter group",
			operation: "ResetParameterGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "reset-pg"})
			},
			body:       map[string]any{"ParameterGroupName": "reset-pg"},
			wantStatus: http.StatusOK,
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

// ---- Subnet Groups ----

func TestHandlerSubnetGroups(t *testing.T) {
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
			name:      "create",
			operation: "CreateSubnetGroup",
			setup:     func(_ *testing.T, _ *dax.Handler) {},
			body: map[string]any{
				"SubnetGroupName": "my-sg",
				"Description":     "My subnet group",
				"SubnetIds":       []string{"subnet-abc12345"},
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				sg := resp["SubnetGroup"].(map[string]any)
				assert.Equal(t, "my-sg", sg["SubnetGroupName"])
				subnets := sg["Subnets"].([]any)
				require.Len(t, subnets, 1)
				subnet := subnets[0].(map[string]any)
				assert.Equal(t, "subnet-abc12345", subnet["SubnetIdentifier"])
				assert.Equal(t, "us-east-1a", subnet["SubnetAvailabilityZone"])
			},
		},
		{
			name:       "describe all",
			operation:  "DescribeSubnetGroups",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				groups := resp["SubnetGroups"].([]any)
				assert.NotEmpty(t, groups)
			},
		},
		{
			name:      "update",
			operation: "UpdateSubnetGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "upd-sg",
					"SubnetIds":       []string{"subnet-11111111"},
				})
			},
			body: map[string]any{
				"SubnetGroupName": "upd-sg",
				"Description":     "Updated description",
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				sg := resp["SubnetGroup"].(map[string]any)
				assert.Equal(t, "Updated description", sg["Description"])
			},
		},
		{
			name:      "delete",
			operation: "DeleteSubnetGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "sg-del",
					"SubnetIds":       []string{"subnet-11111111"},
				})
			},
			body:       map[string]any{"SubnetGroupName": "sg-del"},
			wantStatus: http.StatusOK,
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

// ---- DescribeEvents ----

func TestHandlerDescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dax.Handler)
		body       map[string]any
		check      func(t *testing.T, resp map[string]any)
		name       string
		wantStatus int
	}{
		{
			name: "events after create",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("evt-cluster"))
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				events := resp["Events"].([]any)
				assert.NotEmpty(t, events)
			},
		},
		{
			name:       "empty when no activity",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				events := resp["Events"].([]any)
				assert.Empty(t, events)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, "DescribeEvents", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

// ---- Error mapping ----

func TestHandlerErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		setup     func(t *testing.T, h *dax.Handler)
		body      map[string]any
		wantCode  string
	}{
		{
			name:      "ClusterAlreadyExistsFault",
			operation: "CreateCluster",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("dup"))
			},
			body:     validClusterBody("dup"),
			wantCode: "ClusterAlreadyExistsFault",
		},
		{
			name:      "ClusterNotFoundFault on delete",
			operation: "DeleteCluster",
			setup:     func(_ *testing.T, _ *dax.Handler) {},
			body:      map[string]any{"ClusterName": "no-such"},
			wantCode:  "ClusterNotFoundFault",
		},
		{
			name:      "ParameterGroupNotFoundFault",
			operation: "DescribeParameters",
			setup:     func(_ *testing.T, _ *dax.Handler) {},
			body:      map[string]any{"ParameterGroupName": "missing"},
			wantCode:  "ParameterGroupNotFoundFault",
		},
		{
			name:      "SubnetGroupNotFoundFault",
			operation: "DescribeSubnetGroups",
			setup:     func(_ *testing.T, _ *dax.Handler) {},
			body:      map[string]any{"SubnetGroupNames": []string{"missing"}},
			wantCode:  "SubnetGroupNotFoundFault",
		},
		{
			name:      "InvalidParameterValueException for bad node type",
			operation: "CreateCluster",
			setup:     func(_ *testing.T, _ *dax.Handler) {},
			body: map[string]any{
				"ClusterName":       "x",
				"NodeType":          "bad.type",
				"IamRoleArn":        "arn:aws:iam::123456789012:role/r",
				"ReplicationFactor": 1,
			},
			wantCode: "InvalidParameterValueException",
		},
		{
			name:      "SubnetGroupAlreadyExistsFault",
			operation: "CreateSubnetGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "dup-sg",
					"SubnetIds":       []string{"subnet-11111111"},
				})
			},
			body: map[string]any{
				"SubnetGroupName": "dup-sg",
				"SubnetIds":       []string{"subnet-11111111"},
			},
			wantCode: "SubnetGroupAlreadyExistsFault",
		},
		{
			name:      "ParameterGroupAlreadyExistsFault",
			operation: "CreateParameterGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "dup-pg"})
			},
			body:     map[string]any{"ParameterGroupName": "dup-pg"},
			wantCode: "ParameterGroupAlreadyExistsFault",
		},
		{
			name:      "InvalidAction for unknown operation",
			operation: "UnknownAction",
			setup:     func(_ *testing.T, _ *dax.Handler) {},
			body:      map[string]any{},
			wantCode:  "InvalidAction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, tt.operation, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp["__type"])
		})
	}
}

// ---- Reset / GetSupportedOperations ----

func TestHandlerReset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	daxRequest(t, h, "CreateCluster", validClusterBody("r-cluster"))

	h.Reset()

	rec := daxRequest(t, h, "DescribeClusters", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	clusters := resp["Clusters"].([]any)
	assert.Empty(t, clusters)
}

func TestHandlerGetSupportedOperations(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateCluster",
		"DescribeClusters",
		"UpdateCluster",
		"DeleteCluster",
		"IncreaseReplicationFactor",
		"DecreaseReplicationFactor",
		"RebootNode",
		"TagResource",
		"ListTags",
		"CreateParameterGroup",
		"DescribeParameterGroups",
		"UpdateParameterGroup",
		"DeleteParameterGroup",
		"DescribeParameters",
		"DescribeDefaultParameters",
		"ResetParameterGroup",
		"CreateSubnetGroup",
		"DescribeSubnetGroups",
		"UpdateSubnetGroup",
		"DeleteSubnetGroup",
		"DescribeEvents",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op)
	}
}
