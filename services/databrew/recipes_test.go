package databrew_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// ---- Recipe backend ----

func TestCreateRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	steps := []databrew.RecipeStep{{Action: map[string]any{"Operation": "TRIM"}}}
	r, err := b.CreateRecipe(
		context.Background(),
		"my-recipe",
		"trim recipe",
		steps,
		map[string]string{"team": "data"},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-recipe", r.Name)
	assert.Equal(t, "0.1", r.RecipeVersion)
	assert.Len(t, r.Steps, 1)
	assert.Equal(t, "data", r.Tags["team"])
}

func TestCreateRecipe_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "", "desc", nil, nil)
	require.Error(t, err)
}

func TestCreateRecipe_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.Error(t, err)
}

func TestDescribeRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "r1", "desc", nil, nil)
	require.NoError(t, err)
	r, err := b.DescribeRecipe(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, "r1", r.Name)
}

func TestDescribeRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeRecipe(context.Background(), "nope")
	require.Error(t, err)
}

func TestListRecipes(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "r1", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r2", "", nil, nil)
	require.NoError(t, err)
	list, _ := b.ListRecipes(context.Background(), 100, "")
	assert.Len(t, list, 2)
}

func TestPublishRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "pub-r", "initial", nil, nil)
	require.NoError(t, err)
	err = b.PublishRecipe(context.Background(), "pub-r", "published desc")
	require.NoError(t, err)
	r, err := b.DescribeRecipe(context.Background(), "pub-r")
	require.NoError(t, err)
	assert.Equal(t, "1.0", r.RecipeVersion)
	assert.Equal(t, "published desc", r.Description)
}

func TestPublishRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.PublishRecipe(context.Background(), "no-such", "")
	require.Error(t, err)
}

func TestUpdateRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "upd-r", "old desc", nil, nil)
	require.NoError(t, err)
	steps := []databrew.RecipeStep{{Action: map[string]any{"Operation": "UPPER_CASE"}}}
	err = b.UpdateRecipe(context.Background(), "upd-r", "new desc", steps)
	require.NoError(t, err)
	r, err := b.DescribeRecipe(context.Background(), "upd-r")
	require.NoError(t, err)
	assert.Equal(t, "new desc", r.Description)
	assert.Len(t, r.Steps, 1)
}

func TestUpdateRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateRecipe(context.Background(), "no-such", "", nil)
	require.Error(t, err)
}

func TestDeleteRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "del-r", "", nil, nil)
	require.NoError(t, err)
	err = b.DeleteRecipe(context.Background(), "del-r")
	require.NoError(t, err)
	_, err = b.DescribeRecipe(context.Background(), "del-r")
	require.Error(t, err)
}

func TestDeleteRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteRecipe(context.Background(), "no-such")
	require.Error(t, err)
}

// ---- Recipe handler ----

func TestHandlerCreateRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{
		"Name": "my-recipe", "Description": "test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-recipe", resp["Name"])
}

func TestHandlerDescribeRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "r1"})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes/r1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerListRecipes(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Recipes"])
}

func TestHandlerPublishRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "pub-r"})
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes/pub-r/publishRecipe", map[string]any{
		"Description": "published",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerUpdateRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "upd-r"})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/recipes/upd-r", map[string]any{
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "del-r"})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/recipes/del-r", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Recipe versions handler ----

func TestHandlerListRecipeVersions(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "ver-r"})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes/ver-r/recipeVersions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Recipes"])
}

func TestHandlerListRecipeVersions_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes/no-such/recipeVersions", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerDeleteRecipeVersion(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "rv-r"})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/recipes/rv-r/recipeVersion/1.0", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerBatchDeleteRecipeVersion(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "bdrv-r"})
	// Real AWS path: POST /recipes/{Name}/batchDeleteRecipeVersion (a recipe
	// sub-op), not a bare /recipeVersions endpoint -- see
	// aws-sdk-go-v2/service/databrew's serializers.go SplitURI call for
	// awsRestjson1_serializeOpBatchDeleteRecipeVersion.
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes/bdrv-r/batchDeleteRecipeVersion", map[string]any{
		"RecipeVersions": []string{"1.0"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerBatchDeleteRecipeVersion_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes/no-such/batchDeleteRecipeVersion", map[string]any{
		"RecipeVersions": []string{"1.0"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Recipe wire-shape / routing regression coverage ----

// TestRecipeReference verifies CreateRecipeJob reads RecipeReference and DescribeJob returns it.
func TestRecipeReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes",
		map[string]any{"Name": "my-recipe", "Steps": []any{}})

	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name":            "rj1",
		"RecipeReference": map[string]any{"Name": "my-recipe", "RecipeVersion": "LATEST_WORKING"},
		"RoleArn":         "arn:aws:iam::123456789012:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/rj1", nil)
	require.Equal(t, http.StatusOK, desc.Code)

	var job map[string]any
	require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &job))

	assert.Nil(t, job["RecipeName"], "RecipeName must not appear in JSON — AWS SDK uses RecipeReference")
	ref, ok := job["RecipeReference"].(map[string]any)
	require.True(t, ok, "RecipeReference must be an object")
	assert.Equal(t, "my-recipe", ref["Name"])
}

// TestRecipeVersionOps verifies ListRecipeVersions, BatchDeleteRecipeVersion,
// and DeleteRecipeVersion routing.
func TestRecipeVersionOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
		wantOK bool
	}{
		{
			name:   "ListRecipeVersions GET",
			method: http.MethodGet,
			path:   "/databrew/v1/recipes/my-recipe/recipeVersions",
			wantOp: "ListRecipeVersions",
			wantOK: true,
		},
		{
			name:   "BatchDeleteRecipeVersion POST",
			method: http.MethodPost,
			path:   "/databrew/v1/recipes/my-recipe/recipeVersions",
			wantOp: "BatchDeleteRecipeVersion",
			wantOK: true,
		},
		{
			name:   "DeleteRecipeVersion DELETE",
			method: http.MethodDelete,
			path:   "/databrew/v1/recipes/my-recipe/recipeVersion/1.0",
			wantOp: "DeleteRecipeVersion",
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes",
				map[string]any{"Name": "my-recipe", "Steps": []any{}})

			if tc.wantOK {
				rec := databrewReq(t, h, tc.method, tc.path, map[string]any{
					"RecipeVersions": []string{"1.0"},
				})
				assert.Equal(t, http.StatusOK, rec.Code, "path %s method %s", tc.path, tc.method)
			}
		})
	}
}

// TestExtractOperation_RecipeVersions verifies op routing for recipe version paths.
func TestExtractOperation_RecipeVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "list recipe versions",
			method: http.MethodGet,
			path:   "/databrew/v1/recipes/r/recipeVersions",
			wantOp: "ListRecipeVersions",
		},
		{
			name:   "batch delete recipe versions",
			method: http.MethodPost,
			path:   "/databrew/v1/recipes/r/recipeVersions",
			wantOp: "BatchDeleteRecipeVersion",
		},
		{
			name:   "delete single recipe version",
			method: http.MethodDelete,
			path:   "/databrew/v1/recipes/r/recipeVersion/1.0",
			wantOp: "DeleteRecipeVersion",
		},
		{
			name:   "publish recipe",
			method: http.MethodPost,
			path:   "/databrew/v1/recipes/r/publishRecipe",
			wantOp: "PublishRecipe",
		},
	}

	h := newTestHandler()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := extractOp(t, h, tc.method, tc.path)
			assert.Equal(t, tc.wantOp, got)
		})
	}
}
