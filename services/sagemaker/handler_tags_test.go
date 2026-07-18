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

	// Add tags.
	rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
		"ResourceArn": m.ModelARN,
		"Tags":        []map[string]string{{"Key": "Env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

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
