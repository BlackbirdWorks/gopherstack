package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	m, err := h.Backend.CreateModel(context.Background(),
		"tagged-model",
		"arn:aws:iam::000000000000:role/test",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Add tags. AddTagsOutput.Tags (api_op_AddTags.go) is the full current tag
	// set for the resource, not an empty ack — assert it here so a future
	// regression to an empty-body response fails this test.
	rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
		"ResourceArn": m.ModelARN,
		"Tags":        []map[string]string{{"Key": "Env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var addResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &addResp))

	addTags, ok := addResp["Tags"].([]any)
	require.True(t, ok, "AddTags response body: %s", rec.Body.String())
	require.Len(t, addTags, 1)

	// List tags.
	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": m.ModelARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags, ok := resp["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)

	// Delete tags.
	rec = doSageMakerRequest(t, h, "DeleteTags", map[string]any{
		"ResourceArn": m.ModelARN,
		"TagKeys":     []string{"Env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted.
	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": m.ModelARN,
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags, ok = resp["Tags"].([]any)
	require.True(t, ok)
	assert.Empty(t, tags)
}

func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		target string
	}{
		{
			name:   "add tags to nonexistent resource",
			target: "AddTags",
			body: map[string]any{
				"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/nonexistent",
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			},
		},
		{
			name:   "list tags for nonexistent resource",
			target: "ListTags",
			body: map[string]any{
				"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/nonexistent",
			},
		},
		{
			name:   "delete tags from nonexistent resource",
			target: "DeleteTags",
			body: map[string]any{
				"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/nonexistent",
				"TagKeys":     []string{"k"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, tt.target, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestDeleteTags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "delete tags on non-existent resource returns 400",
			body: map[string]any{
				"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/does-not-exist",
				"TagKeys":     []string{"env"},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "DeleteTags", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_AddTags_RequiresTags verifies that AddTags rejects a request
// with no Tags. AddTagsInput.Tags (api_op_AddTags.go) is "This member is
// required" — an empty/absent Tags slice must not silently succeed.
func TestHandler_AddTags_RequiresTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
		"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/my-model",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_DeleteTags_RequiresTagKeys verifies that DeleteTags rejects a
// request with no TagKeys. DeleteTagsInput.TagKeys (api_op_DeleteTags.go) is
// "This member is required" — an empty/absent TagKeys slice must not
// silently succeed.
func TestHandler_DeleteTags_RequiresTagKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DeleteTags", map[string]any{
		"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/my-model",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ListTags_MaxResults verifies ListTagsInput.MaxResults
// (api_op_ListTags.go, default 100) caps the page size, which was previously
// silently ignored (always sagemakerDefaultPageSize).
func TestHandler_ListTags_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	m, err := h.Backend.CreateModel(context.Background(), "many-tags-model",
		"arn:aws:iam::000000000000:role/test", nil, nil, nil)
	require.NoError(t, err)

	tags := make([]map[string]string, 0, 5)
	for i := range 5 {
		tags = append(tags, map[string]string{"Key": string(rune('a' + i)), "Value": "v"})
	}

	rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
		"ResourceArn": m.ModelARN,
		"Tags":        tags,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": m.ModelARN,
		"MaxResults":  2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	page, ok := resp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, page, 2)
	assert.NotEmpty(t, resp["NextToken"])
}
