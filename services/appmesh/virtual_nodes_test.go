package appmesh_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ─── VirtualNode CRUD (handler) ───

func TestAppMesh_VirtualNodeCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})

	// Create
	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualNodes",
		map[string]any{"virtualNodeName": "vn1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	vn := getBody(t, rec)
	assert.Equal(t, "vn1", vn["virtualNodeName"])
	assert.Contains(t, vn["metadata"].(map[string]any)["arn"].(string), "virtualNode/vn1")

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualNodes/vn1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualNodes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	assert.Len(t, body["virtualNodes"].([]any), 1)

	// Update
	rec = doRequest(t, h, http.MethodPut, "/meshes/m1/virtualNodes/vn1",
		map[string]any{"spec": map[string]any{}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualNodes/vn1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm deleted
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualNodes/vn1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
