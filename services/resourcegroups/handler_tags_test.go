package resourcegroups_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroupsHandler_GetTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *resourcegroups.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *resourcegroups.Handler) string {
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
					"Name": "tagged-group",
					"Tags": map[string]string{"env": "test"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					Group struct {
						GroupArn string `json:"GroupArn"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.Group.GroupArn
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *resourcegroups.Handler) string {
				return "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			arn := tt.setup(h)
			rec := doResourceTagsRequest(t, h, http.MethodGet, arn, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestResourceGroupsHandler_Tag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "success",
			tags:     map[string]string{"team": "platform"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			tags:     map[string]string{"k": "v"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			var groupARN string
			if tt.wantCode == http.StatusOK {
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					Group struct {
						GroupArn string `json:"GroupArn"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				groupARN = out.Group.GroupArn
			} else {
				groupARN = "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			}
			rec := doResourceTagsRequest(t, h, http.MethodPut, groupARN, map[string]any{"Tags": tt.tags})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestResourceGroupsHandler_Untag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keys     []string
		wantCode int
	}{
		{
			name:     "success",
			keys:     []string{"env"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			keys:     []string{"env"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			var groupARN string
			if tt.wantCode == http.StatusOK {
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
					"Name": "my-group",
					"Tags": map[string]string{"env": "test"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					Group struct {
						GroupArn string `json:"GroupArn"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				groupARN = out.Group.GroupArn
			} else {
				groupARN = "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			}
			rec := doResourceTagsRequest(t, h, http.MethodPatch, groupARN, map[string]any{"Keys": tt.keys})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestUntagViaDeleteVerb covers that DELETE /resources/{Arn}/tags must work
// as the Untag operation.
func TestUntagViaDeleteVerb(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name": "delete-tag-group",
		"Tags": map[string]string{"env": "prod", "team": "platform"},
	})

	b := h.Backend
	g, err := b.GetGroup(context.Background(), "delete-tag-group")
	require.NoError(t, err)

	// DELETE /resources/{Arn}/tags with JSON body.
	e := echo.New()
	bodyBytes, _ := json.Marshal(map[string]any{"Keys": []string{"env"}})
	req := httptest.NewRequest(
		http.MethodDelete,
		"/resources/"+g.ARN+"/tags",
		strings.NewReader(string(bodyBytes)),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err = h.Handler()(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	// Verify "env" tag was removed.
	tagMap, err := b.GetTagsByARN(context.Background(), g.ARN)
	require.NoError(t, err)
	assert.NotContains(t, tagMap, "env")
	assert.Contains(t, tagMap, "team")
}

// TestReservedTagNamespace covers that tag keys with "aws:" prefix must be
// rejected on CreateGroup.
func TestReservedTagNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid_tag",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "reserved_aws_prefix",
			tags:     map[string]string{"aws:custom": "val"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "reserved_aws_prefix_upper",
			tags:     map[string]string{"AWS:custom": "val"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "reserved_aws_mixed_case",
			tags:     map[string]string{"Aws:custom": "val"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			body := map[string]any{
				"Name": "tag-group-" + tt.name,
				"Tags": tt.tags,
			}
			rec := doResourceGroupsRequest(t, h, "CreateGroup", body)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestReservedTagNamespaceOnAddTags verifies tag key validation on AddTagsByARN.
func TestReservedTagNamespaceOnAddTags(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "tag-test-group"})

	b := h.Backend
	g, err := b.GetGroup(context.Background(), "tag-test-group")
	require.NoError(t, err)

	_, err = b.AddTagsByARN(context.Background(), g.ARN, map[string]string{"aws:reserved": "val"})
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrValidation)
}
