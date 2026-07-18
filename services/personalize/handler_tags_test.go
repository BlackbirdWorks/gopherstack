package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_TagRoundTrip(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateDatasetGroup", map[string]any{
		"name": "tagged-group",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	dgArn := personalizeUnmarshal(t, rec)["datasetGroupArn"].(string)

	// Tag
	rec = personalizeDo(t, h, "TagResource", map[string]any{
		"resourceArn": dgArn,
		"tags": []any{
			map[string]any{"tagKey": "env", "tagValue": "test"},
			map[string]any{"tagKey": "team", "tagValue": "ml"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = personalizeDo(t, h, "ListTagsForResource", map[string]any{"resourceArn": dgArn})
	require.Equal(t, http.StatusOK, rec.Code)
	m := personalizeUnmarshal(t, rec)
	tags := m["tags"].([]any)
	assert.Len(t, tags, 2)
	// Verify Key+Value shape
	for _, tag := range tags {
		t2 := tag.(map[string]any)
		assert.Contains(t, t2, "tagKey")
		assert.Contains(t, t2, "tagValue")
	}

	// Untag
	rec = personalizeDo(t, h, "UntagResource", map[string]any{
		"resourceArn": dgArn,
		"tagKeys":     []any{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "ListTagsForResource", map[string]any{"resourceArn": dgArn})
	m = personalizeUnmarshal(t, rec)
	tags = m["tags"].([]any)
	assert.Len(t, tags, 1)
	remaining := tags[0].(map[string]any)
	assert.Equal(t, "team", remaining["tagKey"])
	assert.Equal(t, "ml", remaining["tagValue"])
}

// --- FeatureTransformation real lookup ---
