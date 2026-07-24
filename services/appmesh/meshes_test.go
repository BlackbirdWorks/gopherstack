package appmesh_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// ─── Mesh CRUD (handler) ───

func TestAppMesh_MeshCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// CreateMesh
	rec := doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "my-mesh"})
	assert.Equal(t, http.StatusOK, rec.Code)
	mesh := getBody(t, rec)
	assert.Equal(t, "my-mesh", mesh["meshName"])
	assert.Equal(t, "ACTIVE", mesh["status"].(map[string]any)["status"])
	meta := mesh["metadata"].(map[string]any)
	assert.NotEmpty(t, meta["arn"])
	assert.NotEmpty(t, meta["uid"])
	assert.Equal(t, int64(1), int64(meta["version"].(float64)))
	assert.Contains(t, meta["arn"].(string), "arn:aws:appmesh:us-east-1:000000000000:mesh/my-mesh")

	// DescribeMesh
	rec = doRequest(t, h, http.MethodGet, "/meshes/my-mesh", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	mesh = getBody(t, rec)
	assert.Equal(t, "my-mesh", mesh["meshName"])

	// ListMeshes
	rec = doRequest(t, h, http.MethodGet, "/meshes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	meshes := body["meshes"].([]any)
	assert.Len(t, meshes, 1)

	// UpdateMesh
	rec = doRequest(t, h, http.MethodPut, "/meshes/my-mesh",
		map[string]any{"spec": map[string]any{"egressFilter": map[string]any{"type": "ALLOW_ALL"}}})
	assert.Equal(t, http.StatusOK, rec.Code)
	mesh = getBody(t, rec)
	assert.Equal(t, int64(2), int64(mesh["metadata"].(map[string]any)["version"].(float64)))

	// DeleteMesh
	rec = doRequest(t, h, http.MethodDelete, "/meshes/my-mesh", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm deleted
	rec = doRequest(t, h, http.MethodGet, "/meshes/my-mesh", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAppMesh_MeshCreateDuplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "dup"})
	rec := doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "dup"})
	assert.Equal(t, http.StatusConflict, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "ConflictException", body["code"])
}

func TestAppMesh_MeshDeleteWithResources(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "mesh1"})
	doRequest(t, h, http.MethodPut, "/meshes/mesh1/virtualNodes",
		map[string]any{"virtualNodeName": "vn1"})

	rec := doRequest(t, h, http.MethodDelete, "/meshes/mesh1", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "ResourceInUseException", body["code"])
}

// TestAppMesh_ListLimitQueryParam verifies that the client-supplied "limit"
// query param (the real wire name for max page size on App Mesh List*
// operations — see ListMeshesInput.Limit in aws-sdk-go-v2) is honored, not
// silently ignored in favor of a fixed default page size.
func TestAppMesh_ListLimitQueryParam(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for _, name := range []string{"a", "b", "c"} {
		doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": name})
	}

	rec := doRequest(t, h, http.MethodGet, "/meshes?limit=2", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	meshes := body["meshes"].([]any)
	assert.Len(t, meshes, 2, "limit=2 must cap the page at 2 items")
	assert.NotEmpty(t, body["nextToken"], "a capped page must return a nextToken")

	// Second page via nextToken picks up the remainder.
	rec = doRequest(t, h, http.MethodGet,
		fmt.Sprintf("/meshes?limit=2&nextToken=%s", body["nextToken"]), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	assert.Len(t, body["meshes"].([]any), 1)
	assert.Empty(t, body["nextToken"])
}

// TestAppMesh_MeshNameTooLong verifies meshName is rejected with
// BadRequestException once it exceeds the real ResourceName shape's 255-char
// max (botocore service-2.json: {"type": "string", "max": 255, "min": 1}).
func TestAppMesh_MeshNameTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	longName := strings.Repeat("a", 256)
	rec := doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": longName})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "BadRequestException", body["code"])

	// The boundary (255 chars) must still be accepted.
	okName := strings.Repeat("b", 255)
	rec = doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": okName})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─── Mesh backend tests ───

// TestBackend_PaginationViaListMeshes exercises paginateStrings edge cases
// through ListMeshes.
func TestBackend_PaginationViaListMeshes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		nextToken  string
		meshNames  []string
		wantCount  int
		maxResults int32
		wantNext   bool
	}{
		{
			name:       "no pagination needed",
			meshNames:  []string{"a", "b", "c"},
			maxResults: 10,
			nextToken:  "",
			wantCount:  3,
			wantNext:   false,
		},
		{
			name:       "first page of 2",
			meshNames:  []string{"a", "b", "c"},
			maxResults: 2,
			nextToken:  "",
			wantCount:  2,
			wantNext:   true,
		},
		{
			name:       "second page via nextToken",
			meshNames:  []string{"a", "b", "c"},
			maxResults: 2,
			nextToken:  "b",
			wantCount:  1,
			wantNext:   false,
		},
		{
			name:       "nextToken past all items",
			meshNames:  []string{"a", "b"},
			maxResults: 10,
			nextToken:  "z",
			wantCount:  0,
			wantNext:   false,
		},
		{
			name:       "empty list",
			meshNames:  []string{},
			maxResults: 10,
			nextToken:  "",
			wantCount:  0,
			wantNext:   false,
		},
		{
			name:       "zero maxResults returns all",
			meshNames:  []string{"a", "b", "c"},
			maxResults: 0,
			nextToken:  "",
			wantCount:  3,
			wantNext:   false,
		},
		{
			name:       "nextToken equal to existing item is skipped",
			meshNames:  []string{"a", "b", "c"},
			maxResults: 10,
			nextToken:  "a",
			wantCount:  2,
			wantNext:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			for _, name := range tt.meshNames {
				_, err := b.CreateMesh(name, nil, nil)
				require.NoError(t, err)
			}
			items, next, err := b.ListMeshes(tt.maxResults, tt.nextToken)
			require.NoError(t, err)
			assert.Len(t, items, tt.wantCount)
			if tt.wantNext {
				assert.NotEmpty(t, next)
			} else {
				assert.Empty(t, next)
			}
		})
	}
}

// TestBackend_SpecRoundTrip verifies a mesh spec round-trips through
// CreateMesh/DescribeMesh unchanged. Renamed from the misleading
// TestAppMesh_SpecOrEmptyInvalidJSON (it never exercised invalid JSON, and
// is a pure backend test with no HTTP handler involved).
func TestBackend_SpecRoundTrip(t *testing.T) {
	t.Parallel()
	b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateMesh("m1", json.RawMessage(`{"key":"val"}`), nil)
	require.NoError(t, err)

	m, err := b.DescribeMesh("m1")
	require.NoError(t, err)
	assert.JSONEq(t, `{"key":"val"}`, string(m.Spec))
}
