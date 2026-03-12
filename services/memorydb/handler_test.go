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

func newTestHandler(t *testing.T) *memorydb.Handler {
	t.Helper()

	b := memorydb.NewInMemoryBackend()
	h := memorydb.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doRequest(t *testing.T, h *memorydb.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonMemoryDB."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantStatus  int
		wantCluster bool
	}{
		{
			name: "creates cluster",
			body: map[string]any{
				"ClusterName": "my-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantStatus:  http.StatusOK,
			wantCluster: true,
		},
		{
			name:       "missing cluster name",
			body:       map[string]any{"NodeType": "db.r6g.large"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing node type",
			body:       map[string]any{"ClusterName": "my-cluster"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing target header",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "missing target header" {
				e := echo.New()
				bodyBytes, _ := json.Marshal(tt.body)
				req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			rec := doRequest(t, h, "CreateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCluster {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				clusterVal, ok := resp["Cluster"]
				require.True(t, ok, "response should contain Cluster field")

				clusterMap, ok := clusterVal.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.body["ClusterName"], clusterMap["Name"])
				assert.NotEmpty(t, clusterMap["ARN"])
			}
		})
	}
}

func TestHandler_DescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "describe all clusters",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "cluster-a",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "cluster-b",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "describe by name not found",
			body:       map[string]any{"ClusterName": "no-such-cluster"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DescribeClusters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				clusters, ok := resp["Clusters"].([]any)
				require.True(t, ok)
				assert.Len(t, clusters, tt.wantCount)
			}
		})
	}
}

func TestHandler_DeleteCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "deletes existing cluster",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent cluster",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusOK {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "del-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			}

			clusterName := "del-cluster"
			if tt.wantStatus == http.StatusNotFound {
				clusterName = "no-cluster"
			}

			rec := doRequest(t, h, "DeleteCluster", map[string]any{"ClusterName": clusterName})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ACL_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setup      func(*memorydb.Handler)
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "create ACL",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "my-acl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create ACL missing name",
			op:         "CreateACL",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe ACLs",
			op:   "DescribeACLs",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "acl-x"})
			},
			body:       map[string]any{"ACLName": "acl-x"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe ACL not found",
			op:         "DescribeACLs",
			body:       map[string]any{"ACLName": "no-such-acl"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete ACL",
			op:   "DeleteACL",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "del-acl"})
			},
			body:       map[string]any{"ACLName": "del-acl"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags   map[string]string
		name       string
		wantStatus int
	}{
		{
			name:       "list tag after create",
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"Env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "tag-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"Tags":        []map[string]any{{"Key": "Env", "Value": "test"}},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

			clusterMap := createResp["Cluster"].(map[string]any)
			clusterARN := clusterMap["ARN"].(string)

			listRec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": clusterARN})
			assert.Equal(t, tt.wantStatus, listRec.Code)

			if tt.wantStatus == http.StatusOK {
				var tagsResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))
				tagList := tagsResp["TagList"].([]any)
				require.NotEmpty(t, tagList)
			}
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "unknown op returns bad request",
			op:         "NotARealOperation",
			wantStatus: http.StatusBadRequest,
		},
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
