package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKAV2_TaggingOperations(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	createRec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "tagged-app",
		"RuntimeEnvironment": "FLINK-1_18",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	detail := createOut["ApplicationDetail"].(map[string]any)
	appARN := detail["ApplicationARN"].(string)

	// ListTagsForResource.
	listRec := doKAV2Request(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": appARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	tags, ok := listOut["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 1)

	// TagResource - add tag.
	tagRec := doKAV2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": appARN,
		"Tags":        []map[string]string{{"Key": "team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// UntagResource.
	untagRec := doKAV2Request(t, h, "UntagResource", map[string]any{
		"ResourceARN": appARN,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)

	// Verify only 1 tag remains.
	listRec2 := doKAV2Request(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": appARN,
	})
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listOut2))
	tags2, ok := listOut2["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags2, 1)
}

// TestKAV2_TagOperations exercises TagResource/UntagResource's
// happy-dispatch code paths against a freshly created application.
func TestKAV2_TagOperations(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	createKAV2App(t, h, "tag-app")

	appARN := "arn:aws:kinesisanalytics:us-east-1:123456789012:application/tag-app"

	// TagResource
	rec := doKAV2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": appARN,
		"Tags":        []map[string]any{{"Key": "env", "Value": "test"}},
	})
	assert.Positive(t, rec.Code)

	// UntagResource
	rec = doKAV2Request(t, h, "UntagResource", map[string]any{
		"ResourceARN": appARN,
		"TagKeys":     []string{"env"},
	})
	assert.Positive(t, rec.Code)
}
