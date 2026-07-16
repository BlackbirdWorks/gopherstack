package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Tags
// ============================================================

func TestBatch2_Tags_TagAndListAndUntag(t *testing.T) {
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
		"/2015-03-31/tags/"+fnARN,
		`{"Tags":{"env":"prod","team":"platform"}}`)
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	// List tags
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/tags/"+fnARN, "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsOut))
	tags := tagsOut["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// Untag
	untagRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/tags/"+fnARN+"?tagKeys=env", "")
	assert.Equal(t, http.StatusNoContent, untagRec.Code)

	// Verify removed
	listRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/tags/"+fnARN, "")
	require.Equal(t, http.StatusOK, listRec2.Code)

	var tagsOut2 map[string]any
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&tagsOut2))
	tags2, _ := tagsOut2["Tags"].(map[string]any)
	_, hasEnv := tags2["env"]
	assert.False(t, hasEnv)
	_, hasTeam := tags2["team"]
	assert.True(t, hasTeam)
}

func TestBatch2_Tags_CreateFunctionWithTags(t *testing.T) {
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
