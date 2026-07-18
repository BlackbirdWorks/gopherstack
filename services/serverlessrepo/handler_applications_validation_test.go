package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateApplication_NameLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		appName  string
		wantCode int
	}{
		{name: "too long", appName: strings.Repeat("a", 141), wantCode: http.StatusBadRequest},
		{name: "max length accepted", appName: strings.Repeat("a", 140), wantCode: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":        tt.appName,
				"description": "desc",
				"author":      "author",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["message"], "name must be at most")
			}
		})
	}
}

func TestCreateApplication_InvalidName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
	}{
		{name: "spaces_in_name", appName: "my app"},
		{name: "underscore_in_name", appName: "my_app"},
		{name: "slash_in_name", appName: "my/app"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":        tt.appName,
				"description": "desc",
				"author":      "author",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code, "invalid name should be rejected")
		})
	}
}

func TestCreateApplication_ValidName_WithHyphens(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-valid-app-123",
		"description": "desc",
		"author":      "author",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestCreateApplication_AuthorLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		author   string
		wantCode int
	}{
		{name: "too long", author: strings.Repeat("a", 128), wantCode: http.StatusBadRequest},
		{name: "max length accepted", author: strings.Repeat("a", 127), wantCode: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":        "my-app",
				"description": "desc",
				"author":      tt.author,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["message"], "author must be at most")
			}
		})
	}
}

func TestCreateApplication_DescriptionLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
		wantCode    int
	}{
		{name: "too long", description: strings.Repeat("a", 257), wantCode: http.StatusBadRequest},
		{name: "max length accepted", description: strings.Repeat("a", 256), wantCode: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":        "my-app",
				"description": tt.description,
				"author":      "author",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["message"], "description must be at most")
			}
		})
	}
}

func TestCreateApplication_InvalidSemanticVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
	}{
		{name: "no_dots", version: "1"},
		{name: "one_dot", version: "1.0"},
		{name: "alpha", version: "v1.0.0"},
		{name: "empty_parts", version: ".0.0"},
		{name: "trailing_dot", version: "1.0."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":            "my-app",
				"description":     "desc",
				"author":          "author",
				"semanticVersion": tt.version,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestCreateApplication_ValidSemanticVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		label   string
		appName string
		version string
	}{
		{label: "basic", appName: "app-sv-basic", version: "1.0.0"},
		{label: "prerelease", appName: "app-sv-pre", version: "1.0.0-alpha.1"},
		{label: "build_metadata", appName: "app-sv-build", version: "1.0.0+build.1"},
		{label: "large_numbers", appName: "app-sv-large", version: "10.20.30"},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":            tt.appName,
				"description":     "desc",
				"author":          "author",
				"semanticVersion": tt.version,
			})
			assert.Equal(t, http.StatusCreated, rec.Code)
		})
	}
}

func TestCreateApplication_LabelsValidation(t *testing.T) {
	t.Parallel()

	maxLabels := make([]string, 10)
	for i := range maxLabels {
		maxLabels[i] = "label"
	}

	tooManyLabels := make([]string, 11)
	for i := range tooManyLabels {
		tooManyLabels[i] = "label"
	}

	tests := []struct {
		name        string
		wantMessage string
		labels      []string
		wantCode    int
	}{
		{
			name:        "too many labels",
			labels:      tooManyLabels,
			wantCode:    http.StatusBadRequest,
			wantMessage: "at most 10 labels",
		},
		{
			name:     "exactly 10 labels accepted",
			labels:   maxLabels,
			wantCode: http.StatusCreated,
		},
		{
			name:        "label too long",
			labels:      []string{strings.Repeat("x", 128)},
			wantCode:    http.StatusBadRequest,
			wantMessage: "at most 127 characters",
		},
		{
			name:     "empty label",
			labels:   []string{"ok", ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":        "my-app",
				"description": "desc",
				"author":      "author",
				"labels":      tt.labels,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantMessage != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["message"], tt.wantMessage)
			}
		})
	}
}

func TestUpdateApplication_DescriptionTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/my-app", map[string]any{
		"description": strings.Repeat("x", 257),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdateApplication_AuthorTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/my-app", map[string]any{
		"author": strings.Repeat("x", 128),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdateApplication_LabelsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		labels   []string
		wantCode int
	}{
		{
			name:     "too_many_labels",
			labels:   []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "label_too_long",
			labels:   []string{strings.Repeat("x", 128)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_label",
			labels:   []string{"ok", ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "max_10_labels",
			labels:   []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
			wantCode: http.StatusOK,
		},
		{
			name:     "clear_labels",
			labels:   []string{},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/my-app", map[string]any{
				"labels": tt.labels,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
