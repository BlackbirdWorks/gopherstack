package glacier_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		addTags   string
	}{
		{
			name:      "add_list_remove_tags",
			vaultName: "tag-vault",
			addTags:   `{"Tags":{"env":"test","team":"infra"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Add tags
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/-/vaults/"+tt.vaultName+"/tags?operation=add",
				strings.NewReader(tt.addTags),
			)
			rec2 := httptest.NewRecorder()
			c := e.NewContext(req, rec2)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNoContent, rec2.Code)

			// List tags
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/tags", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var tagsResp map[string]any
			err = json.Unmarshal(rec.Body.Bytes(), &tagsResp)
			require.NoError(t, err)

			tags := tagsResp["Tags"].(map[string]any)
			assert.Equal(t, "test", tags["env"])
			assert.Equal(t, "infra", tags["team"])

			// Remove tags
			e2 := echo.New()
			req2 := httptest.NewRequest(http.MethodPost, "/-/vaults/"+tt.vaultName+"/tags?operation=remove",
				strings.NewReader(`{"TagKeys":["team"]}`))
			rec3 := httptest.NewRecorder()
			c2 := e2.NewContext(req2, rec3)
			err = h.Handler()(c2)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNoContent, rec3.Code)
		})
	}
}

func TestTags_KeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]string
		name       string
		wantStatus int
	}{
		{
			name:       "valid_tag",
			tags:       map[string]string{"env": "prod"},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "valid_tag_with_allowed_chars",
			tags:       map[string]string{"my.tag:key/value": "ok"},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "aws_prefix_rejected",
			tags:       map[string]string{"aws:internal": "x"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_char_in_key",
			tags:       map[string]string{"bad\x01key": "v"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_key_rejected",
			tags:       map[string]string{"": "v"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "tag-val-vault")

			tagsJSON, _ := json.Marshal(tt.tags)
			body := fmt.Sprintf(`{"Tags":%s}`, tagsJSON)
			rec := doRequest(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/tag-val-vault/tags?operation=add", body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// -------------------------------------------------------------------------
// Issue 15: ListVaults limit 1-50 validation
// -------------------------------------------------------------------------

func TestTags_ExactLimitAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagCount int
		wantOK   bool
	}{
		{name: "exactly_10_tags_accepted", tagCount: 10, wantOK: true},
		{name: "11_tags_rejected", tagCount: 11, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "tag-limit-vault-"+tt.name)

			tags := make(map[string]string, tt.tagCount)
			for i := range tt.tagCount {
				tags[fmt.Sprintf("key-%02d", i)] = fmt.Sprintf("val-%02d", i)
			}

			body, err := json.Marshal(map[string]any{"Tags": tags})
			require.NoError(t, err)

			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/tag-limit-vault-"+tt.name+"/tags?operation=add",
				string(body), nil)

			if tt.wantOK {
				assert.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestTags_ReservedPrefixRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tagKey  string
		wantBad bool
	}{
		{name: "aws_prefix_rejected", tagKey: "aws:reserved", wantBad: true},
		{name: "normal_key_accepted", tagKey: "mykey", wantBad: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "tag-prefix-"+tt.name)

			body := `{"Tags":{"` + tt.tagKey + `":"value"}}`
			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/tag-prefix-"+tt.name+"/tags?operation=add", body, nil)

			if tt.wantBad {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusNoContent, rec.Code)
			}
		})
	}
}

func TestTags_RemoveNonExistentKeyIsNoop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "remove_missing_key_returns_204"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "tag-remove-vault")

			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/tag-remove-vault/tags?operation=remove",
				`{"TagKeys":["nonexistent-key"]}`, nil)
			assert.Equal(t, http.StatusNoContent, rec.Code, tt.name)
		})
	}
}

func TestTags_ListTagsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list_tags_returns_empty_map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "empty-tags-vault")

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/empty-tags-vault/tags", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tags, ok := resp["Tags"].(map[string]any)
			assert.True(t, ok, tt.name)
			assert.Empty(t, tags)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. ListJobs filter combinations
// ─────────────────────────────────────────────────────────────────────────────
