package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "my-image",
		"RoleArn":   "arn:aws:iam::000000000000:role/test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ImageArn"], "my-image")
}

func TestHandler_DescribeImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-1", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "DescribeImage", map[string]any{"ImageName": "img-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "img-1", resp["ImageName"])
	assert.Equal(t, "CREATED", resp["ImageStatus"])
}

func TestHandler_DeleteImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-del", "RoleArn": "arn:test"})
	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{"ImageName": "img-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeImage", map[string]any{"ImageName": "img-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListImages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-a", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-b", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "ListImages", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["Images"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// ImageVersion
// ---------------------------------------------------------------------------

func TestHandler_CreateImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-ver", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "img-ver"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ImageVersionArn"], "img-ver")
}

func TestHandler_DescribeImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-v", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "img-v"})

	rec := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{"ImageName": "img-v", "Version": 1})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InEpsilon(t, float64(1), resp["Version"], 0.001)
	assert.Equal(t, "CREATED", resp["ImageVersionStatus"])
}

func TestHandler_ListImageVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-lv", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "img-lv"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "img-lv"})

	rec := doSageMakerRequest(t, h, "ListImageVersions", map[string]any{"ImageName": "img-lv"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ImageVersions"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// CompilationJob
// ---------------------------------------------------------------------------

func TestDeleteImage_WithVersions_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "img-has-ver",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-has-ver",
	})

	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{
		"ImageName": "img-has-ver",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDeleteImage_AfterVersionsRemoved_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "img-ver-cleanup",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-ver-cleanup",
	})
	doSageMakerRequest(t, h, "DeleteImageVersion", map[string]any{
		"ImageName": "img-ver-cleanup",
		"Version":   1,
	})

	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{
		"ImageName": "img-ver-cleanup",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// MonitoringSchedule: stop/start state guards
// ---------------------------------------------------------------------------

func TestHandler_ListAliases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "alias-img"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "alias-img"})
	doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName": "alias-img", "AliasesToAdd": []string{"latest", "stable"},
	})

	rec := doSageMakerRequest(t, h, "ListAliases", map[string]any{"ImageName": "alias-img"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	aliases, _ := out["SageMakerImageVersionAliases"].([]any)
	assert.ElementsMatch(t, []any{"latest", "stable"}, aliases)
}

func TestHandler_ListAliases_ImageNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAliases", map[string]any{"ImageName": "no-such-image"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

func TestHandler_UpdateImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName":   "my-image",
		"RoleArn":     "arn:test",
		"Description": "original",
	})

	rec := doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName":   "my-image",
		"DisplayName": "My Image",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeImage", map[string]any{
		"ImageName": "my-image",
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "updated", resp["Description"])
	assert.Equal(t, "My Image", resp["DisplayName"])

	// DeleteProperties clears Description.
	doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName":        "my-image",
		"DeleteProperties": []string{"Description"},
	})

	describeRec2 := doSageMakerRequest(t, h, "DescribeImage", map[string]any{
		"ImageName": "my-image",
	})

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(describeRec2.Body.Bytes(), &resp2))
	assert.Empty(t, resp2["Description"])
}

func TestHandler_UpdateImage_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "my-image",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "my-image",
	})

	rec := doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName":    "my-image",
		"Version":      1,
		"MLFramework":  "PyTorch 2.0",
		"AliasesToAdd": []string{"latest", "stable"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{
		"ImageName": "my-image",
		"Version":   1,
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "PyTorch 2.0", resp["MLFramework"])
	aliases, ok := resp["Aliases"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases, 2)

	// Remove one alias.
	doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName":       "my-image",
		"Version":         1,
		"AliasesToDelete": []string{"latest"},
	})

	describeRec2 := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{
		"ImageName": "my-image",
		"Version":   1,
	})

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(describeRec2.Body.Bytes(), &resp2))
	aliases2, ok := resp2["Aliases"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases2, 1)
	assert.Equal(t, "stable", aliases2[0])
}

func TestHandler_UpdateImageVersion_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName": "does-not-exist",
		"Version":   1,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
