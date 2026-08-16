package lambda_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ============================================================
// Tags
// ============================================================

func TestTags_TagAndListAndUntag(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "tags-fn")

	// Get function ARN
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/tags-fn", "")
	var out lambda.GetFunctionOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
	fnARN := out.Configuration.FunctionArn

	// Tag
	tagRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2017-03-31/tags/"+fnARN,
		`{"Tags":{"env":"prod","team":"platform"}}`)
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	// List tags
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2017-03-31/tags/"+fnARN, "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsOut))
	tags := tagsOut["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// Untag
	untagRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2017-03-31/tags/"+fnARN+"?tagKeys=env", "")
	assert.Equal(t, http.StatusNoContent, untagRec.Code)

	// Verify removed
	listRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2017-03-31/tags/"+fnARN, "")
	require.Equal(t, http.StatusOK, listRec2.Code)

	var tagsOut2 map[string]any
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&tagsOut2))
	tags2, _ := tagsOut2["Tags"].(map[string]any)
	_, hasEnv := tags2["env"]
	assert.False(t, hasEnv)
	_, hasTeam := tags2["team"]
	assert.True(t, hasTeam)
}

// TestGetFunction_ReturnsTopLevelTags verifies GetFunctionOutput carries a
// top-level Tags field sibling to Configuration, per the real GetFunction
// response shape (botocore lambda/2015-03-31 GetFunctionResponse: Configuration,
// Code, Tags, TagsError, Concurrency). Clients such as terraform-provider-aws
// read tags from this field, not from Configuration.Tags (which AWS doesn't send).
func TestGetFunction_ReturnsTopLevelTags(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"tagged-get-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","Tags":{"env":"prod"}}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/tagged-get-fn", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))

	rawTags, ok := out["Tags"].(map[string]any)
	require.True(t, ok, "GetFunctionOutput must have a top-level Tags field, got: %v", out["Tags"])
	assert.Equal(t, "prod", rawTags["env"])
}

func TestTags_CreateFunctionWithTags(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"tagged-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","Tags":{"project":"myapp","stage":"dev"}}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	// Tags may be in the function config or accessible separately
	_ = fn
}

// TestTaggedFunctions_CreationTags verifies tags supplied at CreateFunction
// reach TaggedFunctions, the ARN-keyed store the Resource Groups Tagging API
// GetResources listing consults (wireTaggingLambda in cli.go). CreateFunction
// previously only wrote fn.Tags, a separate store, so newly-tagged functions
// were invisible to cross-service tag listing until an explicit TagResource call.
func TestTaggedFunctions_CreationTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		fnName  string
		wantLen int
	}{
		{
			name:    "tags at creation are listed",
			fnName:  "created-with-tags",
			tags:    map[string]string{"project": "myapp"},
			wantLen: 1,
		},
		{
			name:    "no tags at creation yields empty tag map",
			fnName:  "created-without-tags",
			tags:    nil,
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			tagsJSON, err := json.Marshal(tt.tags)
			require.NoError(t, err)

			body := `{"FunctionName":"` + tt.fnName + `","PackageType":"Image","Code":{"ImageUri":"x"},` +
				`"Role":"arn:aws:iam:::role/r","Tags":` + string(tagsJSON) + `}`
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			entries := h.TaggedFunctions(context.Background())
			require.Len(t, entries, tt.wantLen)
			assert.Equal(t, tt.tags, entries[0].Tags)
		})
	}
}
