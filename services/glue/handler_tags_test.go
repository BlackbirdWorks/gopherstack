package glue_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagValidation tests tag validation constraints on TagResource and resource creation.
func TestTagValidation(t *testing.T) {
	t.Parallel()

	longKey := strings.Repeat("k", 129)
	longValue := strings.Repeat("v", 257)
	maxTags := make(map[string]string, 51)
	for i := range 51 {
		maxTags[strings.Repeat("a", 1)+string(rune('a'+i%26))+string(rune('a'+i/26))] = "val"
	}

	tests := []struct {
		setup    func(h interface{ createJob() })
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "tag_key_too_long",
			tags:     map[string]string{longKey: "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag_value_too_long",
			tags:     map[string]string{"k": longValue},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "too_many_tags",
			tags:     maxTags,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid_tags",
			tags:     map[string]string{"env": "prod", "team": "data"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": "tagdb-" + tt.name},
				"Tags":          tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestTagResource_Validation tests TagResource validation.
func TestTagResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "key_too_long",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value_too_long",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid_tag",
			tags:     map[string]string{"key": "val"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			doGlueRequest(t, h2, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": "tagresdb2"},
			})
			rec := doGlueRequest(t, h2, "GetDatabase", map[string]any{"Name": "tagresdb2"})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doGlueRequest(t, h2, "TagResource", map[string]any{
				"ResourceArn": "arn:aws:glue:us-east-1:000000000000:database/tagresdb2",
				"TagsToAdd":   tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
