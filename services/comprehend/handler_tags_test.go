package comprehend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Tag round-trip field shapes ---

func TestTagsFieldShapes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	created := request(t, h, "CreateEntityRecognizer", map[string]any{
		"RecognizerName": "tag-test",
		"LanguageCode":   "en",
		"Tags":           []any{map[string]any{"Key": "env", "Value": "prod"}},
	})
	arn := created["EntityRecognizerArn"].(string)

	// List initial tag
	listResp := request(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	assert.Equal(t, arn, listResp["ResourceArn"], "ListTagsForResource must echo ResourceArn")
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok, "Tags must be a list")
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "prod", tag["Value"])

	// Add a second tag
	request(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        []any{map[string]any{"Key": "team", "Value": "nlp"}},
	})

	listResp2 := request(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	assert.Len(t, listResp2["Tags"], 2)

	// Remove one tag
	request(t, h, "UntagResource", map[string]any{"ResourceArn": arn, "TagKeys": []any{"env"}})

	listResp3 := request(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	assert.Len(t, listResp3["Tags"], 1)
}
