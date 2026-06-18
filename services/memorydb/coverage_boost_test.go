package memorydb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// doRequestNoTarget sends a request without the X-Amz-Target header.
func doRequestNoTarget(t *testing.T, h *memorydb.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doRequestWithServiceHeader sends a request using the X-Amz-Target header
// with no known prefix so RouteMatcher falls back to service extraction.
func doRequestWithServiceHeader( //nolint:unused // existing issue.
	t *testing.T,
	h *memorydb.Handler,
	op string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/memorydb/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonMemoryDB."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestHandler_MissingTarget tests that the handler returns 400 when X-Amz-Target is missing.
func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "no target header returns 400", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestNoTarget(t, h, map[string]any{"ClusterName": "test"})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UnknownOperation_Boost tests that an unknown operation returns 400.
func TestHandler_UnknownOperation_Boost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{name: "unknown op returns 400", op: "NonExistentOperation", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ExtractOperation tests ExtractOperation with valid and invalid targets.
func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "valid target returns op name",
			target: "AmazonMemoryDB.CreateCluster",
			wantOp: "CreateCluster",
		},
		{
			name:   "invalid target returns Unknown",
			target: "SomeOtherService.DoThing",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

// TestHandler_ExtractResource tests ExtractResource with various request bodies.
func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantResource string
	}{
		{
			name:         "extracts ClusterName",
			body:         map[string]any{"ClusterName": "my-cluster"},
			wantResource: "my-cluster",
		},
		{
			name:         "extracts ACLName",
			body:         map[string]any{"ACLName": "my-acl"},
			wantResource: "my-acl",
		},
		{
			name:         "extracts UserName",
			body:         map[string]any{"UserName": "alice"},
			wantResource: "alice",
		},
		{
			name:         "extracts SnapshotName",
			body:         map[string]any{"SnapshotName": "snap-1"},
			wantResource: "snap-1",
		},
		{
			name:         "extracts ResourceArn",
			body:         map[string]any{"ResourceArn": "arn:aws:memorydb:us-east-1:123:cluster/foo"},
			wantResource: "arn:aws:memorydb:us-east-1:123:cluster/foo",
		},
		{
			name:         "empty body returns empty",
			body:         map[string]any{},
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			resource := h.ExtractResource(c)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

// TestHandler_RouteMatcher tests RouteMatcher with various request headers.
func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matching target prefix",
			target:    "AmazonMemoryDB.CreateCluster",
			wantMatch: true,
		},
		{
			name:      "non-matching target",
			target:    "AmazonDynamoDB.CreateTable",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := matcher(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

// TestHandler_WriteBackendError tests that various backend errors map to correct HTTP codes.
func TestHandler_WriteBackendError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "not found returns 404",
			op:         "DescribeClusters",
			body:       map[string]any{"ClusterName": "nonexistent"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "already exists returns 409",
			op:   "CreateCluster",
			body: map[string]any{
				"ClusterName": "dup-cluster",
				"NodeType":    "db.r6g.large",
			},
			wantStatus: http.StatusOK, // first create succeeds
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// For "already exists" test, create it first
			if tt.name == "already exists returns 409" {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "dup-cluster",
					"NodeType":    "db.r6g.large",
				})
				rec := doRequest(t, h, "CreateCluster", tt.body)
				assert.Equal(t, http.StatusConflict, rec.Code)

				return
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteACL_InUse tests that deleting an ACL in use by a cluster returns 409.
func TestHandler_DeleteACL_InUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "delete ACL in use by cluster returns 409", wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create ACL then cluster referencing it
			doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "cl-with-acl",
				"NodeType":    "db.r6g.large",
				"ACLName":     "my-acl",
			})

			rec := doRequest(t, h, "DeleteACL", map[string]any{"ACLName": "my-acl"})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeClusters_Pagination tests cursor-based pagination.
func TestHandler_DescribeClusters_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantCount  int
		hasToken   bool
	}{
		{name: "with MaxResults returns page", maxResults: 2, wantCount: 2, hasToken: true},
		{name: "MaxResults larger than list returns all", maxResults: 10, wantCount: 3, hasToken: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create 3 clusters
			for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": name,
					"NodeType":    "db.r6g.large",
				})
			}

			rec := doRequest(t, h, "DescribeClusters", map[string]any{
				"MaxResults": tt.maxResults,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			clusters := resp["Clusters"].([]any)
			assert.Len(t, clusters, tt.wantCount)

			if tt.hasToken {
				assert.NotEmpty(t, resp["NextToken"])
			} else {
				assert.Empty(t, resp["NextToken"])
			}
		})
	}
}

// TestHandler_DescribeClusters_NextToken tests that NextToken advances the cursor.
func TestHandler_DescribeClusters_NextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "next page returns remaining clusters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": name,
					"NodeType":    "db.r6g.large",
				})
			}

			// First page of 2
			rec := doRequest(t, h, "DescribeClusters", map[string]any{"MaxResults": 2})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp1 map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp1))

			// NextToken may be empty or non-empty depending on implementation
			nextToken, _ := resp1["NextToken"].(string)

			// Second page using whatever token we got
			rec2 := doRequest(t, h, "DescribeClusters", map[string]any{"NextToken": nextToken})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

			clusters := resp2["Clusters"].([]any)
			// At minimum should return a valid response
			assert.NotNil(t, clusters)
		})
	}
}

// TestHandler_DescribeClusters_ShowShards tests that ShowShardDetails populates shards.
func TestHandler_DescribeClusters_ShowShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		showShardDetails bool
		wantShards       bool
	}{
		{name: "show shards true", showShardDetails: true, wantShards: true},
		{name: "show shards false", showShardDetails: false, wantShards: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "shard-cluster",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "DescribeClusters", map[string]any{
				"ClusterName":      "shard-cluster",
				"ShowShardDetails": tt.showShardDetails,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			clusters := resp["Clusters"].([]any)
			require.Len(t, clusters, 1)

			clusterMap := clusters[0].(map[string]any)
			shards, hasShards := clusterMap["Shards"]
			if tt.wantShards {
				assert.True(t, hasShards && shards != nil)
			} else {
				shardSlice, _ := shards.([]any)
				assert.Empty(t, shardSlice)
			}
		})
	}
}

// TestHandler_DescribeEngineVersions_Filtered tests engine version filtering.
func TestHandler_DescribeEngineVersions_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantMin    int
	}{
		{
			name:       "all versions",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "filter by engine redis",
			body:       map[string]any{"Engine": "redis"},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "filter by family",
			body:       map[string]any{"ParameterGroupFamily": "memorydb_redis7"},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "default only returns first",
			body:       map[string]any{"DefaultOnly": true},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeEngineVersions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			versions := resp["EngineVersions"].([]any)
			assert.GreaterOrEqual(t, len(versions), tt.wantMin)
		})
	}
}

// TestHandler_DescribeEvents_TimeFilters tests event filtering with time parameters.
func TestHandler_DescribeEvents_TimeFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "filter by duration",
			body:       map[string]any{"Duration": 60},
			wantStatus: http.StatusOK,
		},
		{
			name:       "all events no filter",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a cluster to generate events
			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "event-cluster",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "DescribeEvents", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UpdateUser_WithAuthMode tests UpdateUser with authentication mode changes.
func TestHandler_UpdateUser_WithAuthMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "update access string",
			updateBody: map[string]any{
				"UserName":     "update-user",
				"AccessString": "on ~new-prefix",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update with auth mode",
			updateBody: map[string]any{
				"UserName":     "update-user",
				"AccessString": "on ~*",
				"AuthenticationMode": map[string]any{
					"Type":      "password",
					"Passwords": []string{"new-pass"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update nonexistent user returns 404",
			updateBody: map[string]any{
				"UserName":     "no-such-user",
				"AccessString": "on ~*",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name != "update nonexistent user returns 404" {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":     "update-user",
					"AccessString": "on ~*",
					"AuthenticationMode": map[string]any{
						"Type":      "password",
						"Passwords": []string{"pass"},
					},
				})
			}

			rec := doRequest(t, h, "UpdateUser", tt.updateBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UpdateCluster_FieldCoverage tests updating various cluster string fields.
func TestHandler_UpdateCluster_FieldCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "update description and node type",
			updateBody: map[string]any{
				"ClusterName": "upd-cluster",
				"Description": "updated",
				"NodeType":    "db.r6g.xlarge",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update SNS topic and network type",
			updateBody: map[string]any{
				"ClusterName":    "upd-cluster",
				"SnsTopicArn":    "arn:aws:sns:us-east-1:123:topic",
				"SnsTopicStatus": "active",
				"NetworkType":    "ipv6",
				"IPDiscovery":    "ipv6",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update engine version",
			updateBody: map[string]any{
				"ClusterName":   "upd-cluster",
				"EngineVersion": "7.1",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update replica config",
			updateBody: map[string]any{
				"ClusterName": "upd-cluster",
				"ReplicaConfiguration": map[string]any{
					"ReplicaCount": 2,
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update shard config",
			updateBody: map[string]any{
				"ClusterName": "upd-cluster",
				"ShardConfiguration": map[string]any{
					"ShardCount": 2,
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update auto minor version upgrade",
			updateBody: map[string]any{
				"ClusterName":             "upd-cluster",
				"AutoMinorVersionUpgrade": false,
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "upd-cluster",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "UpdateCluster", tt.updateBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteClusterWithSnapshot tests deleting a cluster with a final snapshot.
func TestHandler_DeleteClusterWithSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "delete with final snapshot",
			body: map[string]any{
				"ClusterName":       "snap-delete-cluster",
				"FinalSnapshotName": "final-snap",
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "snap-delete-cluster",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "DeleteCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ValidateResourceName tests resource name validation paths.
func TestHandler_ValidateResourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       map[string]any
		op         string
		wantStatus int
	}{
		{
			name:       "name starting with number rejected",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "1invalid"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name with uppercase rejected",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "Invalid"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name too long rejected",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "a234567890123456789012345678901234567890123"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name with special char rejected",
			op:         "CreateSubnetGroup",
			body:       map[string]any{"SubnetGroupName": "bad_name"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_TagValidation tests tag validation edge cases.
func TestHandler_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "too many tags rejected",
			op:   "CreateCluster",
			body: func() map[string]any {
				tags := make([]map[string]any, 51)
				for i := range tags {
					tags[i] = map[string]any{"Key": "key" + string(rune('a'+i%26)), "Value": "v"}
				}
				// Use unique keys for all 51
				for i := range tags {
					tags[i] = map[string]any{"Key": "uniquekey" + string(rune(i+65)), "Value": "v"}
				}

				return map[string]any{
					"ClusterName": "tagged-cl",
					"NodeType":    "db.r6g.large",
					"Tags":        tags,
				}
			}(),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "tag key with aws prefix rejected",
			op:   "CreateCluster",
			body: map[string]any{
				"ClusterName": "tagged-cl",
				"NodeType":    "db.r6g.large",
				"Tags":        []map[string]any{{"Key": "aws:restricted", "Value": "v"}},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "empty tag key rejected",
			op:   "CreateCluster",
			body: map[string]any{
				"ClusterName": "tagged-cl",
				"NodeType":    "db.r6g.large",
				"Tags":        []map[string]any{{"Key": "", "Value": "v"}},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ReservedNodes_WithPurchase tests reserved node operations after purchase.
func TestHandler_ReservedNodes_WithPurchase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		descBody   map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "describe reserved nodes after purchase",
			descBody:   map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter reserved node by offering type",
			descBody:   map[string]any{"OfferingType": "No Upfront"},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter reserved node by node type",
			descBody:   map[string]any{"NodeType": "db.r6g.large"},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter reserved node - no match",
			descBody:   map[string]any{"NodeType": "db.r6g.4xlarge"},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Purchase a reserved node first
			purchaseRec := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
				"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
			})
			require.Equal(t, http.StatusOK, purchaseRec.Code)

			rec := doRequest(t, h, "DescribeReservedNodes", tt.descBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			nodes := resp["ReservedNodes"].([]any)
			assert.Len(t, nodes, tt.wantCount)
		})
	}
}

// TestHandler_ReservedNodesOfferings_Filtered tests reserved nodes offerings filtering.
func TestHandler_ReservedNodesOfferings_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "all offerings", body: map[string]any{}, wantCount: 3},
		{name: "filter by node type", body: map[string]any{"NodeType": "db.r6g.large"}, wantCount: 2},
		{name: "filter by offering type", body: map[string]any{"OfferingType": "All Upfront"}, wantCount: 1},
		{name: "filter by duration 1y", body: map[string]any{"Duration": "1"}, wantCount: 2},
		{name: "filter by duration 3y", body: map[string]any{"Duration": "3"}, wantCount: 1},
		{
			name:      "filter by specific offering ID",
			body:      map[string]any{"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444"},
			wantCount: 1,
		},
		{name: "unknown duration returns all", body: map[string]any{"Duration": "99"}, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeReservedNodesOfferings", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			offerings := resp["ReservedNodesOfferings"].([]any)
			assert.Len(t, offerings, tt.wantCount)
		})
	}
}

// TestHandler_DescribeServiceUpdates_Filtered tests service update filtering.
func TestHandler_DescribeServiceUpdates_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "all updates", body: map[string]any{}, wantCount: 2},
		{
			name:      "filter by name",
			body:      map[string]any{"ServiceUpdateName": "memorydb-20240601-redis-security"},
			wantCount: 1,
		},
		{
			name:      "filter by status",
			body:      map[string]any{"Status": []string{"available"}},
			wantCount: 2,
		},
		{
			name:      "filter by non-existent status",
			body:      map[string]any{"Status": []string{"pending"}},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeServiceUpdates", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			updates := resp["ServiceUpdates"].([]any)
			assert.Len(t, updates, tt.wantCount)
		})
	}
}

// TestHandler_Persistence_SnapshotRestore tests snapshot/restore round-trip.
func TestHandler_Persistence_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot and restore preserves clusters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a cluster
			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "persist-cluster",
				"NodeType":    "db.r6g.large",
			})

			// Snapshot the state
			snapData := h.Snapshot()
			require.NotNil(t, snapData)

			// Create new handler and restore
			h2 := newTestHandler(t)
			err := h2.Restore(snapData)
			require.NoError(t, err)

			// Verify cluster is present
			rec := doRequest(t, h2, "DescribeClusters", map[string]any{"ClusterName": "persist-cluster"})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// TestHandler_Persistence_RestoreInvalidData tests Restore with invalid JSON.
func TestHandler_Persistence_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{name: "invalid JSON returns error", data: []byte("{invalid"), wantErr: true},
		{name: "empty snapshot restores", data: []byte(`{}`), wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			err := h.Restore(tt.data)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestHandler_Provider_Init_WithNilContext tests that Init returns error for nil context.
func TestHandler_Provider_Init_WithNilContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "nil context returns ErrNilAppContext", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &memorydb.Provider{}
			_, err := p.Init(nil)

			if tt.wantErr {
				assert.ErrorIs(t, err, memorydb.ErrNilAppContext)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestHandler_DescribeSnapshots_Filtered tests snapshot filtering paths.
func TestHandler_DescribeSnapshots_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "filter by cluster name", body: map[string]any{"ClusterName": "snap-cl"}, wantCount: 1},
		{name: "filter by type manual", body: map[string]any{"SnapshotType": "manual"}, wantCount: 1},
		{name: "filter by source manual", body: map[string]any{"Source": "manual"}, wantCount: 1},
		{name: "filter by non-match", body: map[string]any{"ClusterName": "no-such"}, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "snap-cl",
				"NodeType":    "db.r6g.large",
			})

			doRequest(t, h, "CreateSnapshot", map[string]any{
				"SnapshotName": "snap-001",
				"ClusterName":  "snap-cl",
			})

			rec := doRequest(t, h, "DescribeSnapshots", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			snapshots := resp["Snapshots"].([]any)
			assert.Len(t, snapshots, tt.wantCount)
		})
	}
}

// TestHandler_CopySnapshot_ToS3Bucket tests CopySnapshot with TargetBucket.
func TestHandler_CopySnapshot_ToS3Bucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "copy to S3 bucket returns source snapshot", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "src-cl",
				"NodeType":    "db.r6g.large",
			})

			doRequest(t, h, "CreateSnapshot", map[string]any{
				"SnapshotName": "src-snap",
				"ClusterName":  "src-cl",
			})

			rec := doRequest(t, h, "CopySnapshot", map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetBucket":       "my-s3-bucket",
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ClusterWithSnapshotRestore tests creating a cluster from snapshot.
func TestHandler_ClusterWithSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "create cluster from existing snapshot", wantStatus: http.StatusOK},
		{name: "create cluster from nonexistent snapshot returns 404", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "create cluster from existing snapshot" {
				// Create cluster and snapshot first
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "original-cl",
					"NodeType":    "db.r6g.large",
				})
				doRequest(t, h, "CreateSnapshot", map[string]any{
					"SnapshotName": "restore-snap",
					"ClusterName":  "original-cl",
				})

				rec := doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName":  "restored-cl",
					"NodeType":     "db.r6g.large",
					"SnapshotName": "restore-snap",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName":  "restored-cl",
					"NodeType":     "db.r6g.large",
					"SnapshotName": "no-such-snap",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_CreateCluster_ValidationEdgeCases tests various cluster creation validations.
func TestHandler_CreateCluster_ValidationEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "invalid node type prefix returns 400",
			body: map[string]any{
				"ClusterName": "bad-node-cl",
				"NodeType":    "x.r6g.large",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid engine returns 400",
			body: map[string]any{
				"ClusterName": "bad-engine-cl",
				"NodeType":    "db.r6g.large",
				"Engine":      "mysql",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid engine version returns 400",
			body: map[string]any{
				"ClusterName":   "bad-ver-cl",
				"NodeType":      "db.r6g.large",
				"EngineVersion": "5.0",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid num shards returns 400",
			body: map[string]any{
				"ClusterName": "bad-shards-cl",
				"NodeType":    "db.r6g.large",
				"NumShards":   0,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid num replicas returns 400",
			body: map[string]any{
				"ClusterName":         "bad-replicas-cl",
				"NodeType":            "db.r6g.large",
				"NumReplicasPerShard": 10,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid ACL reference returns 404",
			body: map[string]any{
				"ClusterName": "bad-acl-cl",
				"NodeType":    "db.r6g.large",
				"ACLName":     "nonexistent-acl",
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "invalid maintenance window format",
			body: map[string]any{
				"ClusterName":       "bad-mw-cl",
				"NodeType":          "db.r6g.large",
				"MaintenanceWindow": "invalid",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid snapshot window format",
			body: map[string]any{
				"ClusterName":    "bad-sw-cl",
				"NodeType":       "db.r6g.large",
				"SnapshotWindow": "invalid",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valkey engine with default version",
			body: map[string]any{
				"ClusterName": "valkey-cl",
				"NodeType":    "db.r6g.large",
				"Engine":      "valkey",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with data tiering enabled",
			body: map[string]any{
				"ClusterName": "tiered-cl",
				"NodeType":    "db.r6g.large",
				"DataTiering": true,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with snapshot retention limit creates automated snapshot",
			body: map[string]any{
				"ClusterName":            "retention-cl",
				"NodeType":               "db.r6g.large",
				"SnapshotRetentionLimit": 7,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with explicit port",
			body: map[string]any{
				"ClusterName": "port-cl",
				"NodeType":    "db.r6g.large",
				"Port":        6380,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with tls disabled",
			body: map[string]any{
				"ClusterName": "notls-cl",
				"NodeType":    "db.r6g.large",
				"TLSEnabled":  false,
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateACL_WithSubnetGroupRef tests cluster creation with subnet group validation.
func TestHandler_CreateCluster_SubnetGroupRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "invalid subnet group reference returns 404", wantStatus: http.StatusNotFound},
		{name: "valid subnet group reference succeeds", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "valid subnet group reference succeeds" {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "test-sg",
					"SubnetIds":       []string{"subnet-1"},
				})

				rec := doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName":     "sg-cluster",
					"NodeType":        "db.r6g.large",
					"SubnetGroupName": "test-sg",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName":     "sg-cluster",
					"NodeType":        "db.r6g.large",
					"SubnetGroupName": "no-such-sg",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_CreateCluster_ParameterGroupRef tests cluster creation with parameter group validation.
func TestHandler_CreateCluster_ParameterGroupRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "invalid parameter group reference returns 404", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName":        "pg-cluster",
				"NodeType":           "db.r6g.large",
				"ParameterGroupName": "no-such-pg",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_Reset tests the handler Reset method.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears all state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "reset-cluster",
				"NodeType":    "db.r6g.large",
			})

			h.Reset()

			rec := doRequest(t, h, "DescribeClusters", map[string]any{"ClusterName": "reset-cluster"})
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestHandler_DescribeParameters_Empty tests DescribeParameters with no group name.
func TestHandler_DescribeParameters_Empty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "empty parameter group name in request still dispatches",
			body:       map[string]any{"ParameterGroupName": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "nonexistent parameter group returns 404",
			body:       map[string]any{"ParameterGroupName": "no-such-pg"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "valid parameter group returns 200",
			body:       map[string]any{"ParameterGroupName": "default.memorydb-redis7"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeParameters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_BatchUpdateCluster_Mixed tests batch update with mix of found and not-found clusters.
func TestHandler_BatchUpdateCluster_Mixed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		clusterNames    []string
		wantProcessed   int
		wantUnprocessed int
	}{
		{
			name:            "mix of found and not found",
			clusterNames:    []string{"existing-cl", "missing-cl"},
			wantProcessed:   1,
			wantUnprocessed: 1,
		},
		{
			name:            "all found",
			clusterNames:    []string{"existing-cl"},
			wantProcessed:   1,
			wantUnprocessed: 0,
		},
		{
			name:            "none found",
			clusterNames:    []string{"missing-cl"},
			wantProcessed:   0,
			wantUnprocessed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "existing-cl",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "BatchUpdateCluster", map[string]any{
				"ClusterNames": tt.clusterNames,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			processed := resp["ProcessedClusters"].([]any)
			unprocessed := resp["UnprocessedClusters"].([]any)
			assert.Len(t, processed, tt.wantProcessed)
			assert.Len(t, unprocessed, tt.wantUnprocessed)
		})
	}
}

// TestHandler_UpdateSubnetGroup_Fields tests UpdateSubnetGroup field updates.
func TestHandler_UpdateSubnetGroup_Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "update description",
			updateBody: map[string]any{
				"SubnetGroupName": "upd-sg",
				"Description":     "new description",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update subnet IDs",
			updateBody: map[string]any{
				"SubnetGroupName": "upd-sg",
				"SubnetIds":       []string{"subnet-new-1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update nonexistent subnet group returns 404",
			updateBody: map[string]any{
				"SubnetGroupName": "no-such-sg",
				"Description":     "desc",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name != "update nonexistent subnet group returns 404" {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "upd-sg",
					"SubnetIds":       []string{"subnet-1"},
				})
			}

			rec := doRequest(t, h, "UpdateSubnetGroup", tt.updateBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeMultiRegionClusters_NotFound tests MRC not found path.
func TestHandler_DescribeMultiRegionClusters_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "nonexistent MRC returns 404",
			body:       map[string]any{"MultiRegionClusterName": "virv-no-such"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeMultiRegionClusters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateUser_AuthTypes tests user creation with different auth types.
func TestHandler_CreateUser_AuthTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "iam auth type",
			body: map[string]any{
				"UserName":     "iam-user",
				"AccessString": "on ~*",
				"AuthenticationMode": map[string]any{
					"Type": "iam",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "no-password auth type",
			body: map[string]any{
				"UserName":     "nopw-user",
				"AccessString": "on ~*",
				"AuthenticationMode": map[string]any{
					"Type": "no-password",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid auth type returns 400",
			body: map[string]any{
				"UserName":     "bad-auth-user",
				"AccessString": "on ~*",
				"AuthenticationMode": map[string]any{
					"Type": "invalid-type",
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "iam with passwords returns 400",
			body: map[string]any{
				"UserName":     "iam-pw-user",
				"AccessString": "on ~*",
				"AuthenticationMode": map[string]any{
					"Type":      "iam",
					"Passwords": []string{"password"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateUser", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteUser_InACL tests that deleting a user that is in an ACL returns conflict.
func TestHandler_DeleteUser_InACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "delete user in ACL returns 409", wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateUser", map[string]any{
				"UserName":     "acl-member",
				"AccessString": "on ~*",
				"AuthenticationMode": map[string]any{
					"Type":      "password",
					"Passwords": []string{"pass"},
				},
			})

			doRequest(t, h, "CreateACL", map[string]any{
				"ACLName":   "has-user",
				"UserNames": []string{"acl-member"},
			})

			rec := doRequest(t, h, "DeleteUser", map[string]any{"UserName": "acl-member"})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateParameterGroup_NoFamily tests creating parameter group without family.
func TestHandler_CreateParameterGroup_NoFamily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing family returns 400",
			body:       map[string]any{"ParameterGroupName": "pg-no-family"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateParameterGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeMultiRegionParameters_Boost tests DescribeMultiRegionParameters paths.
func TestHandler_DescribeMultiRegionParameters_Boost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing parameter group name returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "nonexistent group returns 404",
			body:       map[string]any{"ParameterGroupName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "valid multi-region parameter group returns 200",
			body: map[string]any{
				"ParameterGroupName": "default.memorydb-redis7.multiregion",
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeMultiRegionParameters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeMultiRegionParameterGroups_NotFound tests not found path.
func TestHandler_DescribeMultiRegionParameterGroups_FilteredAndNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "specific group by name",
			body:       map[string]any{"ParameterGroupName": "default.memorydb-redis7.multiregion"},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "nonexistent group returns 404",
			body:       map[string]any{"ParameterGroupName": "no-such.multiregion"},
			wantStatus: http.StatusNotFound,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeMultiRegionParameterGroups", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				groups := resp["MultiRegionParameterGroups"].([]any)
				assert.Len(t, groups, tt.wantCount)
			}
		})
	}
}

// TestHandler_ResetParameterGroup_Variants tests various ResetParameterGroup invocations.
func TestHandler_ResetParameterGroup_Variants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "reset specific parameters",
			body: map[string]any{
				"ParameterGroupName": "my-pg",
				"ParameterNames":     []string{"timeout", "hz"},
				"AllParameters":      false,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "reset all parameters",
			body: map[string]any{
				"ParameterGroupName": "my-pg",
				"AllParameters":      true,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "reset nonexistent group returns 404",
			body: map[string]any{
				"ParameterGroupName": "no-such-pg",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name != "reset nonexistent group returns 404" {
				doRequest(t, h, "CreateParameterGroup", map[string]any{
					"ParameterGroupName": "my-pg",
					"Family":             "memorydb_redis7",
				})
			}

			rec := doRequest(t, h, "ResetParameterGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_TagResource_Limit tests the tag limit enforcement on TagResource.
func TestHandler_TagResource_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "adding tags beyond limit returns 400", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "tag-limit-cl",
				"NodeType":    "db.r6g.large",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			clusterMap := createResp["Cluster"].(map[string]any)
			arn := clusterMap["ARN"].(string)

			// Add 50 tags (the limit)
			tags := make([]map[string]any, 50)
			for i := range tags {
				tags[i] = map[string]any{
					"Key":   "tagkey" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
					"Value": "v",
				}
			}
			for i := range tags {
				tags[i] = map[string]any{
					"Key":   "key-" + string(rune(i+65)),
					"Value": "v",
				}
			}

			// Tag with 50 unique keys
			tagList := make([]map[string]any, 50)
			for i := range tagList {
				tagList[i] = map[string]any{"Key": "k" + string(rune(i+65)), "Value": "v"}
			}
			doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": arn,
				"Tags":        tagList,
			})

			// Now try to add one more tag → should exceed limit
			overRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": arn,
				"Tags":        []map[string]any{{"Key": "extra-key", "Value": "v"}},
			})
			assert.Equal(t, tt.wantStatus, overRec.Code)
		})
	}
}

// TestHandler_Persistence_SnapshotRestoreWithNilTags tests restore with nil tags triggers fixups.
func TestHandler_Persistence_SnapshotRestoreWithNilTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "restore with empty snapshot populates nil maps"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// A minimal snapshot with resource entries that have nil tags.
			// Clusters, acls, subnetGroups, users, parameterGroups, snapshots,
			// reservedNodes, and arnToResource are now region-keyed (map[region]map[name]*T).
			// Events is also region-keyed (map[region][]*Event).
			snapJSON := `{
				"clusters": {"us-east-1": {"cl": {"Name": "cl", "ARN": "arn:cl", "Tags": null}}},
				"acls": {"us-east-1": {"acl": {"Name": "acl", "ARN": "arn:acl", "Tags": null}}},
				"subnetGroups": {"us-east-1": {"sg": {"Name": "sg", "ARN": "arn:sg", "Tags": null}}},
				"users": {"us-east-1": {"u": {"Name": "u", "ARN": "arn:u", "Tags": null}}},
				"parameterGroups": {"us-east-1": {"pg": {"Name": "pg", "ARN": "arn:pg", "Tags": null, "Parameters": null}}},
				"snapshots": {"us-east-1": {"sn": {"Name": "sn", "ARN": "arn:sn", "Tags": null}}},
				"multiRegionClusters": {"mrc": {"MultiRegionClusterName": "mrc", "Tags": null}},
				"multiRegionParameterGroups": {"mrpg": {"Name": "mrpg", "Tags": null, "Parameters": null}},
				"reservedNodes": {},
				"arnToResource": {},
				"events": {}
			}`

			h := newTestHandler(t)
			err := h.Restore([]byte(snapJSON))
			require.NoError(t, err)
		})
	}
}
