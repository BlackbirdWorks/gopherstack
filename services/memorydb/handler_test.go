package memorydb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func doCreateCluster(t *testing.T, h *memorydb.Handler, body map[string]any) (map[string]any, int) {
	t.Helper()
	rec := doRequest(t, h, "CreateCluster", body)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp, rec.Code
}

func doDescribeUsers(t *testing.T, h *memorydb.Handler, userName string) []any {
	t.Helper()
	body := map[string]any{}
	if userName != "" {
		body["UserName"] = userName
	}
	rec := doRequest(t, h, "DescribeUsers", body)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	users, _ := resp["Users"].([]any)

	return users
}

func doDescribeACLs(t *testing.T, h *memorydb.Handler, aclName string) []any {
	t.Helper()
	body := map[string]any{}
	if aclName != "" {
		body["ACLName"] = aclName
	}
	rec := doRequest(t, h, "DescribeACLs", body)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	acls, _ := resp["ACLs"].([]any)

	return acls
}

func doDescribeSnapshots(t *testing.T, h *memorydb.Handler, snapshotName string) []any {
	t.Helper()
	body := map[string]any{}
	if snapshotName != "" {
		body["SnapshotName"] = snapshotName
	}
	rec := doRequest(t, h, "DescribeSnapshots", body)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	snaps, _ := resp["Snapshots"].([]any)

	return snaps
}

func doDescribeParameterGroups(t *testing.T, h *memorydb.Handler, pgName string) []any {
	t.Helper()
	body := map[string]any{}
	if pgName != "" {
		body["ParameterGroupName"] = pgName
	}
	rec := doRequest(t, h, "DescribeParameterGroups", body)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pgs, _ := resp["ParameterGroups"].([]any)

	return pgs
}

func doDescribeParameters(t *testing.T, h *memorydb.Handler, pgName string) []any {
	t.Helper()
	rec := doRequest(t, h, "DescribeParameters", map[string]any{"ParameterGroupName": pgName})
	require.Equal(t, http.StatusOK, rec.Code, "DescribeParameters failed: %s", rec.Body)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	params, _ := resp["Parameters"].([]any)

	return params
}

// minimalClusterBody returns a minimal valid CreateCluster request body.
func minimalClusterBody(name string) map[string]any {
	return map[string]any{
		"ClusterName": name,
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	}
}

func newTestBackend() *memorydb.InMemoryBackend {
	return memorydb.NewInMemoryBackend(testAccountID, testRegion)
}

func newTestHandler(t *testing.T) *memorydb.Handler {
	t.Helper()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
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

// doRequestRaw sends a raw body (without JSON marshalling) to the handler.
func doRequestRaw(t *testing.T, h *memorydb.Handler, op string, raw []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonMemoryDB."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
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

// TestHandler_Name tests the Name method.
func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "MemoryDB", h.Name())
}

// TestHandler_Infrastructure tests routing infrastructure methods.
func TestHandler_Infrastructure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// These infrastructure methods return constant values
	assert.Equal(t, "memorydb", h.ChaosServiceName())
	assert.NotNil(t, h.ChaosOperations())
	assert.NotNil(t, h.ChaosRegions())

	// RouteMatcher
	rm := h.RouteMatcher()
	assert.NotNil(t, rm)
	assert.Equal(t, 87, h.MatchPriority())

	// GetSupportedOperations
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateCluster")
	assert.Contains(t, ops, "BatchUpdateCluster")
	assert.Contains(t, ops, "CopySnapshot")
	assert.Contains(t, ops, "CreateMultiRegionCluster")
	assert.Contains(t, ops, "CreateSnapshot")
	assert.Contains(t, ops, "DescribeEngineVersions")
	assert.Contains(t, ops, "DescribeEvents")
	assert.Contains(t, ops, "DescribeMultiRegionClusters")
	assert.Contains(t, ops, "DescribeMultiRegionParameterGroups")
}

// doRequestAsync is doRequest without the require.* assertions doRequest
// makes internally on marshal/handler failure -- require.FailNow from a
// non-test goroutine is unsafe (testifylint: go-require), so concurrent
// callers use this instead and assert on the result back in the main
// goroutine.
func doRequestAsync(h *memorydb.Handler, op string, body any) (*httptest.ResponseRecorder, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonMemoryDB."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err = h.Handler()(c); err != nil {
		return nil, err
	}

	return rec, nil
}

// -- Timestamp wire-shape regression tests ---------------------------------
//
// Real aws-sdk-go-v2/service/memorydb (awsjson1.1) serializes every TStamp
// shape as a JSON number of epoch seconds, never an RFC3339 string. A field
// emitted as a string where the SDK expects a number breaks client-side
// deserialization outright (see aws-sdk-go-v2's deserializers.go: "expected
// TStamp to be a JSON Number, got string instead").

func snapshotNameForIndex(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"

	return "race-snap-" + string(letters[i%len(letters)]) + string(letters[(i/len(letters))%len(letters)])
}

func collectSnapshotARNs(t *testing.T, h *memorydb.Handler) []string {
	t.Helper()

	rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"ClusterName": "race-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	snaps, _ := resp["Snapshots"].([]any)

	arns := make([]string, 0, len(snaps))

	for _, raw := range snaps {
		s, _ := raw.(map[string]any)
		if a, ok := s["ARN"].(string); ok {
			arns = append(arns, a)
		}
	}

	return arns
}

func tagEntry(key, value string) map[string]any {
	return map[string]any{"Key": key, "Value": value}
}

func tagsPayload(pairs ...map[string]any) []any {
	result := make([]any, 0, len(pairs))
	for _, p := range pairs {
		result = append(result, p)
	}

	return result
}

// -- Tag validation on TagResource -----------------------------------------------

// TestRefinement1_HandlerReset verifies that Handler.Reset() delegates to backend.
func TestHandlerResetDelegatesToBackend(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	h := memorydb.NewHandler(b)
	b.AddClusterInternal("cluster-x", "db.r6g.large")

	h.Reset()

	assert.Equal(t, 0, memorydb.ClusterCount(b))
}

// TestRefinement1_GetSupportedOperations verifies 35 sorted operations are returned.
func TestGetSupportedOperations(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	h := memorydb.NewHandler(b)

	ops := h.GetSupportedOperations()

	assert.Len(t, ops, 46)
	assert.Contains(t, ops, "DescribeSnapshots")
	assert.Contains(t, ops, "BatchUpdateCluster")
	assert.Contains(t, ops, "CreateMultiRegionCluster")
	assert.Contains(t, ops, "DescribeParameters")
	assert.Contains(t, ops, "FailoverShard")
	assert.Contains(t, ops, "UpdateMultiRegionCluster")
	assert.Contains(t, ops, "DescribeReservedNodes")
	assert.Contains(t, ops, "DescribeReservedNodesOfferings")
	assert.Contains(t, ops, "PurchaseReservedNodesOffering")
	assert.Contains(t, ops, "DescribeMultiRegionParameters")
}

// TestRefinement1_WriteBackendErrorValidation verifies ErrValidation maps to 400.
func TestWriteBackendErrorValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateParameterGroup with missing Family -> should return 400 via ErrValidation.
	rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "no-fam",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_ExtractResourceSnapshotName verifies SnapshotName extraction.
func TestExtractResourceSnapshotName(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	h := memorydb.NewHandler(b)

	// The handler must parse SnapshotName when ExtractResource is called.
	// We verify indirectly that DescribeSnapshots picks up SnapshotName from body.
	b.AddSnapshotInternal("ext-snap", "cl")

	rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"SnapshotName": "ext-snap"})

	assert.Equal(t, http.StatusOK, rec.Code)
}

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

// helper: create a cluster and return the parsed response body.
func createCluster(t *testing.T, h *memorydb.Handler, body map[string]any) map[string]any {
	t.Helper()

	rec := doRequest(t, h, "CreateCluster", body)
	require.Equal(t, http.StatusOK, rec.Code, "create cluster failed: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// helper: create a cluster and return the Cluster object from the response.
func createClusterObj(t *testing.T, h *memorydb.Handler, body map[string]any) map[string]any {
	t.Helper()
	resp := createCluster(t, h, body)
	cl, ok := resp["Cluster"].(map[string]any)
	require.True(t, ok, "response has no Cluster field")

	return cl
}

// helper: create a snapshot and return the parsed response body.
func createSnapshot(t *testing.T, h *memorydb.Handler, body map[string]any) {
	t.Helper()

	rec := doRequest(t, h, "CreateSnapshot", body)
	require.Equal(t, http.StatusOK, rec.Code, "create snapshot failed: %s", rec.Body)
}

// -- Engine field (Gap 2) -------------------------------------------------------

func TestPaginationAcrossResourceTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*memorydb.Handler)
		body      map[string]any
		name      string
		op        string
		wantCount int
		wantToken bool
	}{
		{
			name: "describe clusters pagination",
			op:   "DescribeClusters",
			setup: func(h *memorydb.Handler) {
				for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
					createCluster(t, h, map[string]any{
						"ClusterName": name,
						"NodeType":    "db.r6g.large",
						"ACLName":     "open-access",
					})
				}
			},
			body:      map[string]any{"MaxResults": 2},
			wantCount: 2,
			wantToken: true,
		},
		{
			name: "describe ACLs pagination",
			op:   "DescribeACLs",
			setup: func(h *memorydb.Handler) {
				for _, name := range []string{"acl-a", "acl-b", "acl-c"} {
					doRequest(t, h, "CreateACL", map[string]any{"ACLName": name})
				}
			},
			body:      map[string]any{"MaxResults": 2},
			wantCount: 2,
			wantToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doRequest(t, h, tt.op, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			// Find the list field.
			var items []any
			for _, key := range []string{"Clusters", "ACLs", "Users", "Snapshots", "ParameterGroups", "SubnetGroups"} {
				if v, ok := resp[key].([]any); ok {
					items = v

					break
				}
			}

			assert.Len(t, items, tt.wantCount)

			if tt.wantToken {
				assert.NotEmpty(t, resp["NextToken"])
			}
		})
	}
}

// -- Describe snapshots pagination --------------------------------------------
