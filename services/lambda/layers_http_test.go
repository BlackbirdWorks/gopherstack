package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- GetLayerVersionByArn HTTP tests ---

func TestGetLayerVersionByArn(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// Publish a layer
	out, err := bk.PublishLayerVersion(&lambda.PublishLayerVersionInput{
		LayerName:          "arn-test-layer",
		CompatibleRuntimes: []string{"python3.12"},
		Content:            &lambda.LayerVersionContentInput{},
	})
	require.NoError(t, err)

	// Get by ARN using /2018-10-31/layers-by-arn?Arn={arn}
	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers-by-arn?Arn="+url.QueryEscape(out.LayerVersionArn), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "arn-test-layer")
}

// ============================================================
// Layer CRUD + versions
// ============================================================

func TestLayer_PublishAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	pubRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/test-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="},"Description":"v1","CompatibleRuntimes":["python3.12","python3.11"]}`)
	require.Equal(t, http.StatusCreated, pubRec.Code)

	var pub lambda.PublishLayerVersionOutput
	require.NoError(t, json.NewDecoder(pubRec.Body).Decode(&pub))
	assert.Equal(t, int64(1), pub.Version)
	assert.Equal(t, "v1", pub.Description)
	assert.Equal(t, []string{"python3.12", "python3.11"}, pub.CompatibleRuntimes)
	assert.NotEmpty(t, pub.LayerVersionArn)
	assert.NotEmpty(t, pub.LayerArn)

	// Get version
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/test-layer/versions/1", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var got lambda.GetLayerVersionOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&got))
	assert.Equal(t, int64(1), got.Version)
}

func TestLayer_MultipleVersions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	for i := range 3 {
		rec := callInMemoryHandler(t, h, http.MethodPost,
			"/2018-10-31/layers/multi-layer/versions",
			fmt.Sprintf(`{"Content":{"ZipFile":"UEsDBAA="},"Description":"v%d"}`, i+1))
		require.Equal(t, http.StatusCreated, rec.Code)

		var pub lambda.PublishLayerVersionOutput
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&pub))
		assert.Equal(t, int64(i+1), pub.Version)
	}
}

func TestLayer_ListVersions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/list-ver-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/list-ver-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/list-ver-layer/versions", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListLayerVersionsOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.LayerVersions, 2)
}

func TestLayer_DeleteVersion(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/del-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2018-10-31/layers/del-layer/versions/1", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/del-layer/versions/1", "")
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestLayer_ListLayers(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/layer-a/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/layer-b/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2018-10-31/layers", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListLayersOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.GreaterOrEqual(t, len(out.Layers), 2)
}

func TestLayer_GetVersionByArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	pubRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/arn-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)
	require.Equal(t, http.StatusCreated, pubRec.Code)

	var pub lambda.PublishLayerVersionOutput
	require.NoError(t, json.NewDecoder(pubRec.Body).Decode(&pub))

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers-by-arn?Arn="+pub.LayerVersionArn, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var got lambda.GetLayerVersionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.Equal(t, pub.LayerVersionArn, got.LayerVersionArn)
}

func TestLayer_GetVersionByArn_MissingArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2018-10-31/layers-by-arn", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestListLayers_CompatibleRuntimeFilter verifies CompatibleRuntime filtering.
func TestListLayers_CompatibleRuntimeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		compatibleRuntime string
		wantCount         int
	}{
		{
			name:      "no filter returns all layers",
			wantCount: 2,
		},
		{
			name:              "filter python3.12 returns one",
			compatibleRuntime: "python3.12",
			wantCount:         1,
		},
		{
			name:              "filter nodejs20.x returns one",
			compatibleRuntime: "nodejs20.x",
			wantCount:         1,
		},
		{
			name:              "filter no-match returns none",
			compatibleRuntime: "ruby3.3",
			wantCount:         0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)

			// Publish two layers with different runtimes.
			_, err := bk.PublishLayerVersion(&lambda.PublishLayerVersionInput{
				LayerName:          "python-layer",
				CompatibleRuntimes: []string{"python3.12"},
				Content:            &lambda.LayerVersionContentInput{},
			})
			require.NoError(t, err)

			_, err = bk.PublishLayerVersion(&lambda.PublishLayerVersionInput{
				LayerName:          "node-layer",
				CompatibleRuntimes: []string{"nodejs20.x"},
				Content:            &lambda.LayerVersionContentInput{},
			})
			require.NoError(t, err)

			listPath := "/2018-10-31/layers"
			if tc.compatibleRuntime != "" {
				listPath += "?CompatibleRuntime=" + tc.compatibleRuntime
			}

			rec := callInMemoryHandler(t, h, http.MethodGet, listPath, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			layers, _ := out["Layers"].([]any)
			assert.Len(t, layers, tc.wantCount, "layer count mismatch for runtime=%q", tc.compatibleRuntime)
		})
	}
}
