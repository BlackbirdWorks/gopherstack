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
	assert.Equal(t, "LATEST_WORKING", r.RecipeVersion)
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
	r, err := b.DescribeRecipe(context.Background(), "r1", "")
	require.NoError(t, err)
	assert.Equal(t, "r1", r.Name)
}

func TestDescribeRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeRecipe(context.Background(), "nope", "")
	require.Error(t, err)
}

// TestListRecipes_LatestWorking verifies the RecipeVersion=LATEST_WORKING
// filter returns every recipe's working draft, regardless of publish state.
func TestListRecipes_LatestWorking(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "r1", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r2", "", nil, nil)
	require.NoError(t, err)
	list, _ := b.ListRecipes(context.Background(), 100, "", "LATEST_WORKING")
	assert.Len(t, list, 2)
}

// TestListRecipes_DefaultHidesUnpublished locks in the real AWS default:
// "If RecipeVersion is omitted, ListRecipes returns all of the
// LATEST_PUBLISHED recipe versions" -- a recipe with no published version
// does not appear in a default (no filter) listing.
func TestListRecipes_DefaultHidesUnpublished(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "unpub", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "pub", "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishRecipe(context.Background(), "pub", ""))

	list, _ := b.ListRecipes(context.Background(), 100, "", "")
	require.Len(t, list, 1)
	assert.Equal(t, "pub", list[0].Name)
	assert.Equal(t, "1.0", list[0].RecipeVersion)

	// LATEST_PUBLISHED is an explicit synonym for the default.
	list, _ = b.ListRecipes(context.Background(), 100, "", "LATEST_PUBLISHED")
	require.Len(t, list, 1)
	assert.Equal(t, "pub", list[0].Name)
}

func TestPublishRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "pub-r", "initial", nil, nil)
	require.NoError(t, err)
	err = b.PublishRecipe(context.Background(), "pub-r", "published desc")
	require.NoError(t, err)
	r, err := b.DescribeRecipe(context.Background(), "pub-r", "")
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
	r, err := b.DescribeRecipe(context.Background(), "upd-r", "")
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
	_, err = b.DescribeRecipe(context.Background(), "del-r", "")
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

// TestHandlerListRecipeVersions_UnknownRecipe verifies the handler surfaces
// ListRecipeVersions's real behavior for an unknown recipe: 200 with an
// empty list, not a 404 (see TestListRecipeVersions_UnknownRecipe).
func TestHandlerListRecipeVersions_UnknownRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes/no-such/recipeVersions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Recipes"])
}

func TestHandlerDeleteRecipeVersion(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "rv-r"})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes/rv-r/publishRecipe", nil)
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/recipes/rv-r/recipeVersion/1.0", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerDeleteRecipeVersion_NotFound verifies deleting a version that
// was never published (no publish call preceded it) 404s.
func TestHandlerDeleteRecipeVersion_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "rv-r2"})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/recipes/rv-r2/recipeVersion/1.0", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
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
			// DeleteRecipeVersion 404s for a version that was never
			// published; publish first so "1.0" exists for every subtest.
			databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes/my-recipe/publishRecipe", nil)

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

// ---- Recipe version history ----

// TestPublishRecipe_IncrementsVersion verifies each PublishRecipe call
// appends a new numbered version (1.0, 2.0, ...) rather than overwriting a
// single tracked version, and that earlier published snapshots remain
// independently retrievable by their own RecipeVersion after later
// publishes and working-draft edits.
func TestPublishRecipe_IncrementsVersion(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ctx := context.Background()
	_, err := b.CreateRecipe(ctx, "multi-pub-r", "v1 desc", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishRecipe(ctx, "multi-pub-r", ""))

	steps := []databrew.RecipeStep{{Action: map[string]any{"Operation": "TRIM"}}}
	require.NoError(t, b.UpdateRecipe(ctx, "multi-pub-r", "v2 desc", steps))
	require.NoError(t, b.PublishRecipe(ctx, "multi-pub-r", ""))

	v1, err := b.DescribeRecipe(ctx, "multi-pub-r", "1.0")
	require.NoError(t, err)
	assert.Equal(t, "v1 desc", v1.Description)
	assert.Empty(t, v1.Steps)

	v2, err := b.DescribeRecipe(ctx, "multi-pub-r", "2.0")
	require.NoError(t, err)
	assert.Equal(t, "v2 desc", v2.Description)
	require.Len(t, v2.Steps, 1)

	latest, err := b.DescribeRecipe(ctx, "multi-pub-r", "")
	require.NoError(t, err)
	assert.Equal(t, "2.0", latest.RecipeVersion,
		"no-version-filter DescribeRecipe returns the latest published version")

	versions, _, err := b.ListRecipeVersions(ctx, "multi-pub-r", 100, "")
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, "1.0", versions[0].RecipeVersion)
	assert.Equal(t, "2.0", versions[1].RecipeVersion)
}

// TestDescribeRecipe_LatestWorkingVsPublished verifies LATEST_WORKING and
// LATEST_PUBLISHED resolve to different snapshots once the working draft
// has diverged from the last published version.
func TestDescribeRecipe_LatestWorkingVsPublished(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ctx := context.Background()
	_, err := b.CreateRecipe(ctx, "diverge-r", "published desc", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishRecipe(ctx, "diverge-r", ""))
	require.NoError(t, b.UpdateRecipe(ctx, "diverge-r", "draft desc", nil))

	working, err := b.DescribeRecipe(ctx, "diverge-r", "LATEST_WORKING")
	require.NoError(t, err)
	assert.Equal(t, "LATEST_WORKING", working.RecipeVersion)
	assert.Equal(t, "draft desc", working.Description)

	published, err := b.DescribeRecipe(ctx, "diverge-r", "LATEST_PUBLISHED")
	require.NoError(t, err)
	assert.Equal(t, "1.0", published.RecipeVersion)
	assert.Equal(t, "published desc", published.Description)
}

// TestDescribeRecipe_LatestPublished_NeverPublished verifies
// RecipeVersion=LATEST_PUBLISHED 404s (rather than falling back to the
// working draft) when the recipe has never been published -- unlike the
// no-filter default, this is an explicit request for a published version.
func TestDescribeRecipe_LatestPublished_NeverPublished(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "never-pub-r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.DescribeRecipe(context.Background(), "never-pub-r", "LATEST_PUBLISHED")
	require.ErrorIs(t, err, databrew.ErrNotFound)
}

// TestDeleteRecipe_CascadesVersions verifies deleting a recipe removes its
// entire published version history, leaving no ghost rows behind.
func TestDeleteRecipe_CascadesVersions(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ctx := context.Background()
	_, err := b.CreateRecipe(ctx, "cascade-r", "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishRecipe(ctx, "cascade-r", ""))
	require.NoError(t, b.DeleteRecipe(ctx, "cascade-r"))

	// Re-creating the same name must not resurrect the old version history.
	_, err = b.CreateRecipe(ctx, "cascade-r", "", nil, nil)
	require.NoError(t, err)
	versions, _, err := b.ListRecipeVersions(ctx, "cascade-r", 100, "")
	require.NoError(t, err)
	assert.Empty(t, versions)
}

// TestBatchDeleteRecipeVersion_PartialFailure verifies a version that
// doesn't exist is reported as a partial failure in the response's Errors
// list, while the overall call still succeeds -- per
// aws-sdk-go-v2/service/databrew's documented "request will complete
// successfully, but with partial failures" behavior for a missing version.
func TestBatchDeleteRecipeVersion_PartialFailure(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ctx := context.Background()
	_, err := b.CreateRecipe(ctx, "bdrv-partial-r", "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishRecipe(ctx, "bdrv-partial-r", ""))

	errs, err := b.BatchDeleteRecipeVersion(ctx, "bdrv-partial-r", []string{"1.0", "9.0"})
	require.NoError(t, err, "a missing version is a partial failure, not a whole-request error")
	require.Len(t, errs, 1)
	assert.Equal(t, "9.0", errs[0].RecipeVersion)
	assert.Equal(t, "ResourceNotFoundException", errs[0].ErrorCode)

	_, err = b.DescribeRecipe(ctx, "bdrv-partial-r", "1.0")
	assert.ErrorIs(t, err, databrew.ErrNotFound, "1.0 must actually be deleted")
}

// TestBatchDeleteRecipeVersion_WholeRequestRejection verifies the
// whole-request rejection cases (empty list, duplicate entries,
// syntactically invalid identifier) return a ValidationException instead of
// a partial-failure entry.
func TestBatchDeleteRecipeVersion_WholeRequestRejection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		versions []string
	}{
		{name: "empty list", versions: []string{}},
		{name: "duplicate entries", versions: []string{"1.0", "1.0"}},
		{name: "invalid identifier", versions: []string{"not-a-version"}},
		{name: "LATEST_PUBLISHED unsupported", versions: []string{"LATEST_PUBLISHED"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			ctx := context.Background()
			_, err := b.CreateRecipe(ctx, "bdrv-reject-r", "", nil, nil)
			require.NoError(t, err)

			_, err = b.BatchDeleteRecipeVersion(ctx, "bdrv-reject-r", tc.versions)
			require.Error(t, err)
			assert.ErrorIs(t, err, databrew.ErrValidation)
		})
	}
}

// TestDeleteRecipeVersion_LatestWorking verifies LATEST_WORKING can only be
// deleted (removing the recipe entirely) when no published versions exist,
// and is rejected once a published version is present.
func TestDeleteRecipeVersion_LatestWorking(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ctx := context.Background()

	_, err := b.CreateRecipe(ctx, "del-working-r", "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeleteRecipeVersion(ctx, "del-working-r", "LATEST_WORKING"))
	_, err = b.DescribeRecipe(ctx, "del-working-r", "")
	require.ErrorIs(t, err, databrew.ErrNotFound, "deleting the only version removes the recipe")

	_, err = b.CreateRecipe(ctx, "del-working-r2", "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishRecipe(ctx, "del-working-r2", ""))
	err = b.DeleteRecipeVersion(ctx, "del-working-r2", "LATEST_WORKING")
	require.Error(t, err, "LATEST_WORKING must not be deletable while a published version exists")
	assert.ErrorIs(t, err, databrew.ErrValidation)
}

// TestListRecipeVersions_UnknownRecipe verifies ListRecipeVersions returns an
// empty list rather than erroring for an unknown recipe: its own SDK
// deserializeOpError switch types only ValidationException, not
// ResourceNotFoundException (aws-sdk-go-v2/service/databrew@v1.42.4
// deserializers.go), so a not-found sentinel here would decode client-side
// as an untyped smithy.GenericAPIError instead of a real exception type.
func TestListRecipeVersions_UnknownRecipe(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	versions, next, err := b.ListRecipeVersions(context.Background(), "no-such-r", 100, "")
	require.NoError(t, err)
	assert.Empty(t, versions)
	assert.Empty(t, next)
}
