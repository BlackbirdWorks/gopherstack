package translate_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name": "tagged-term", "MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	termProps := unmarshalJSON(t, rec.Body.Bytes())["TerminologyProperties"].(map[string]any)
	termARN := termProps["Arn"].(string)

	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": termARN,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
			{"Key": "team", "Value": "translate"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": termARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	tags, _ := m["Tags"].([]any)
	assert.Len(t, tags, 2)
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name": "untag-term", "MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
		"Tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	termARN := unmarshalJSON(t, rec.Body.Bytes())["TerminologyProperties"].(map[string]any)["Arn"].(string)

	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": termARN,
		"TagKeys":     []string{"remove"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": termARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	tags, _ := m["Tags"].([]any)
	assert.Len(t, tags, 1)
}

// TestTagResource_NotFound verifies that tagging a nonexistent resource ARN
// returns ResourceNotFoundException.
func TestTagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:translate:us-east-1:000000000000:terminology/nonexistent",
		"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestUntagResource_NotFound verifies that untagging a nonexistent resource ARN
// returns ResourceNotFoundException.
func TestUntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": "arn:aws:translate:us-east-1:000000000000:terminology/nonexistent",
		"TagKeys":     []string{"k"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestListTagsForResource_NotFound verifies that listing tags for a nonexistent
// resource ARN returns ResourceNotFoundException.
func TestListTagsForResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": "arn:aws:translate:us-east-1:000000000000:terminology/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestTagResource_DeletedTerminologyARN verifies that tagging fails after
// the terminology has been deleted (ARN no longer valid).
func TestTagResource_DeletedTerminologyARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name": "to-delete-tag", "MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	termARN := unmarshalJSON(t, rec.Body.Bytes())["TerminologyProperties"].(map[string]any)["Arn"].(string)

	rec = doRequest(t, h, "DeleteTerminology", map[string]any{"Name": "to-delete-tag"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": termARN,
		"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestTagOperations verifies TagResource, UntagResource, and ListTagsForResource.
func TestTagOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	impRec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "tag-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   b64("en,es"),
			"Format": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, impRec.Code)

	impResp := unmarshalJSON(t, impRec.Body.Bytes())
	resourceARN := impResp["TerminologyProperties"].(map[string]any)["Arn"].(string)

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": resourceARN,
		"Tags":        []any{map[string]any{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	listResp := unmarshalJSON(t, listRec.Body.Bytes())
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)

	found := false
	for _, raw := range tags {
		tag := raw.(map[string]any)
		if tag["Key"] == "env" && tag["Value"] == "test" {
			found = true
		}
	}
	assert.True(t, found, "added tag must appear in ListTagsForResource")

	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": resourceARN,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec2 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": resourceARN})
	listResp2 := unmarshalJSON(t, listRec2.Body.Bytes())
	tags2 := listResp2["Tags"].([]any)

	for _, raw := range tags2 {
		tag := raw.(map[string]any)
		assert.NotEqual(t, "env", tag["Key"], "untagged key must be absent")
	}
}
