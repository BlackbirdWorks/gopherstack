package appmesh_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// ─── Tag operations (handler) ───

func TestAppMesh_TagOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{
		"meshName": "tagged-mesh",
		"tags":     []map[string]string{{"key": "env", "value": "test"}},
	})

	// Get mesh ARN
	rec := doRequest(t, h, http.MethodGet, "/meshes/tagged-mesh", nil)
	arn := getBody(t, rec)["metadata"].(map[string]any)["arn"].(string)

	// ListTags
	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", arn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	tags := body["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["key"])

	// TagResource
	rec = doRequest(t, h, http.MethodPut, "/tag",
		map[string]any{"resourceArn": arn, "tags": []map[string]string{{"key": "team", "value": "platform"}}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify tag added
	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", arn), nil)
	body = getBody(t, rec)
	assert.Len(t, body["tags"].([]any), 2)

	// UntagResource
	rec = doRequest(t, h, http.MethodPut, "/untag",
		map[string]any{"resourceArn": arn, "tagKeys": []string{"env"}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify tag removed
	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", arn), nil)
	body = getBody(t, rec)
	assert.Len(t, body["tags"].([]any), 1)
	assert.Equal(t, "team", body["tags"].([]any)[0].(map[string]any)["key"])
}

// ─── Tags: ListTagsForResource missing resourceArn param ───

func TestAppMesh_ListTagsMissingArn(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := doRequest(t, h, http.MethodGet, "/tags", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "BadRequestException", body["code"])
}

// ─── Tags backend tests ───

// TestBackend_ArnLookupAllResourceTypes exercises arnInVirtualNodes /
// arnInVirtualRouters / etc. (via TagResource) for every resource type.
func TestBackend_ArnLookupAllResourceTypes(t *testing.T) {
	t.Parallel()
	b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")

	// Create resources needed for each resource type.
	mesh, err := b.CreateMesh("m1", nil, nil)
	require.NoError(t, err)

	vn, err := b.CreateVirtualNode("m1", "vn1", nil, nil)
	require.NoError(t, err)

	vr, err := b.CreateVirtualRouter("m1", "vr1", nil, nil)
	require.NoError(t, err)

	r, err := b.CreateRoute("m1", "vr1", "r1", nil, nil)
	require.NoError(t, err)

	vs, err := b.CreateVirtualService("m1", "vs1", nil, nil)
	require.NoError(t, err)

	vg, err := b.CreateVirtualGateway("m1", "gw1", nil, nil)
	require.NoError(t, err)

	gr, err := b.CreateGatewayRoute("m1", "gw1", "gr1", nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name string
		arn  string
	}{
		{"mesh arn", mesh.Meta.Arn},
		{"virtual node arn", vn.Meta.Arn},
		{"virtual router arn", vr.Meta.Arn},
		{"route arn", r.Meta.Arn},
		{"virtual service arn", vs.Meta.Arn},
		{"virtual gateway arn", vg.Meta.Arn},
		{"gateway route arn", gr.Meta.Arn},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// TagResource exercises arnExists which calls all arnIn* helpers.
			tagErr := b.TagResource(
				tt.arn,
				map[string]string{"test": "value"},
			)
			assert.NoError(t, tagErr, "TagResource should succeed for valid ARN: %s", tt.arn)
		})
	}
}
