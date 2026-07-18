package mediaconvert_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

func TestMediaConvert_Preset_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	presetName := "test-preset"

	// Create
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/presets", map[string]any{
		"name":        presetName,
		"description": "a test preset",
		"category":    "Standard",
		"settings":    map[string]any{"codec": "H.264"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	pData, ok := createResp["preset"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, presetName, pData["name"])
	assert.Equal(t, "CUSTOM", pData["type"])
	assert.NotEmpty(t, pData["arn"])

	// Get
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/presets/"+presetName, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), presetName)

	// List
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/presets", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), presetName)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/presets/"+presetName, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify deleted
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/presets/"+presetName, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_Preset_TableTests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *mediaconvert.Handler)
		name       string
		method     string
		path       string
		wantInBody string
		wantStatus int
	}{
		{
			name:       "create_preset_missing_name",
			method:     http.MethodPost,
			path:       "/2017-08-29/presets",
			body:       map[string]any{"description": "no name"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_preset_duplicate",
			setup: func(h *mediaconvert.Handler) {
				doRequest(t, h, http.MethodPost, "/2017-08-29/presets", map[string]any{"name": "dup-preset"})
			},
			method:     http.MethodPost,
			path:       "/2017-08-29/presets",
			body:       map[string]any{"name": "dup-preset"},
			wantStatus: http.StatusConflict,
		},
		{
			name:       "get_preset_not_found",
			method:     http.MethodGet,
			path:       "/2017-08-29/presets/nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_preset_not_found",
			method:     http.MethodDelete,
			path:       "/2017-08-29/presets/nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_presets_empty",
			method:     http.MethodGet,
			path:       "/2017-08-29/presets",
			wantStatus: http.StatusOK,
			wantInBody: `"presets"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantInBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantInBody)
			}
		})
	}
}

// TestCreatePreset_WithTags verifies tags are stored at creation time.
func TestCreatePreset_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/presets", map[string]any{
		"name": "tagged-preset",
		"tags": map[string]string{"project": "alpha"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	p := resp["preset"].(map[string]any)
	tags := p["tags"].(map[string]any)
	assert.Equal(t, "alpha", tags["project"])
}

// TestCreatePreset_SettingsDeepCopy ensures modifying returned settings does not affect stored data.
func TestCreatePreset_SettingsDeepCopy(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	settings := map[string]any{"codec": "H.264", "nested": map[string]any{"bitrate": 4000}}

	p, err := b.CreatePreset("deep-copy-test", "", "", settings, nil)
	require.NoError(t, err)

	// Mutate the returned copy.
	p.Settings["codec"] = "H.265"
	p.Settings["injected"] = "EVIL"

	// Original should be unaffected.
	p2, err := b.GetPreset("deep-copy-test")
	require.NoError(t, err)
	assert.Equal(t, "H.264", p2.Settings["codec"])
	assert.NotContains(t, p2.Settings, "injected")
}

// TestListPresets_SortedByName verifies list is sorted.
func TestListPresets_SortedByName(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	for _, name := range []string{"z-preset", "a-preset", "m-preset"} {
		_, err := b.CreatePreset(name, "", "", nil, nil)
		require.NoError(t, err)
	}

	presets := b.ListPresets()
	require.Len(t, presets, 3)
	assert.Equal(t, "a-preset", presets[0].Name)
	assert.Equal(t, "m-preset", presets[1].Name)
	assert.Equal(t, "z-preset", presets[2].Name)
}

// TestCreatePreset_EmptyName verifies name validation at backend level.
func TestCreatePreset_EmptyName(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreatePreset("", "", "", nil, nil)
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestDeletePreset_CleansUpTags verifies tags are removed on deletion.
func TestDeletePreset_CleansUpTags(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	p, err := b.CreatePreset("tagged-preset", "", "", nil, map[string]string{"k": "v"})
	require.NoError(t, err)

	require.NoError(t, b.DeletePreset("tagged-preset"))
	assert.Empty(t, b.GetTags(p.Arn))
}

// TestUpdatePreset_Success verifies PUT /presets/{name} returns 200.
func TestUpdatePreset_Success(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreatePreset("my-preset", "original", "Cat1", nil, nil)
	require.NoError(t, err)

	h := mediaconvert.NewHandler(b)
	rec := doRequest(t, h, http.MethodPut, "/2017-08-29/presets/my-preset",
		map[string]any{"description": "updated desc", "category": "NewCat"})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	preset, ok := out["preset"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "updated desc", preset["description"])
	assert.Equal(t, "NewCat", preset["category"])
}

// TestUpdatePreset_NotFound returns 404 for unknown preset.
func TestUpdatePreset_NotFound(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPut, "/2017-08-29/presets/no-such-preset", map[string]any{})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestUpdatePreset_Direct validates the backend method directly.
func TestUpdatePreset_Direct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		desc        string
		category    string
		wantDesc    string
		wantCat     string
		wantErr     bool
		setupPreset bool
	}{
		{
			name:        "update_description",
			setupPreset: true,
			desc:        "new desc",
			category:    "",
			wantDesc:    "new desc",
			wantCat:     "Old",
		},
		{
			name:        "update_category",
			setupPreset: true,
			desc:        "",
			category:    "NewCat",
			wantDesc:    "orig",
			wantCat:     "NewCat",
		},
		{
			name:        "not_found",
			setupPreset: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

			if tt.setupPreset {
				_, err := b.CreatePreset("p1", "orig", "Old", nil, nil)
				require.NoError(t, err)
			}

			p, err := b.UpdatePreset("p1", tt.desc, tt.category, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "not found")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, p.Description)
			assert.Equal(t, tt.wantCat, p.Category)
		})
	}
}

// TestUpdatePreset_SettingsUpdated verifies settings are updated.
func TestUpdatePreset_SettingsUpdated(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreatePreset("p-settings", "", "", nil, nil)
	require.NoError(t, err)

	settings := map[string]any{"codec": "AAC"}
	p, err := b.UpdatePreset("p-settings", "", "", settings)
	require.NoError(t, err)
	assert.Equal(t, "AAC", p.Settings["codec"])
}

// TestUpdatePreset_LastUpdatedChanges verifies LastUpdated changes.
func TestUpdatePreset_LastUpdatedChanges(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	original, err := b.CreatePreset("lu-preset", "", "", nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdatePreset("lu-preset", "new", "", nil)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, updated.LastUpdated, original.LastUpdated)
}

// TestCreatePreset_DeepNestedSettingsNoPanic verifies no panic/infinite
// recursion for a deeply-nested settings document.
func TestCreatePreset_DeepNestedSettingsNoPanic(t *testing.T) {
	t.Parallel()

	// Build a deeply nested map 25 levels deep.
	var buildNested func(depth int) map[string]any
	buildNested = func(depth int) map[string]any {
		if depth == 0 {
			return map[string]any{"leaf": "value"}
		}

		return map[string]any{"child": buildNested(depth - 1)}
	}

	deepMap := buildNested(25)

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	// Should not panic; some deep values may be nil due to depth cap.
	p, err := b.CreatePreset("deep-preset", "", "", deepMap, nil)
	require.NoError(t, err)
	require.NotNil(t, p)
}

// TestCreatePreset_ShallowSettingsCloned verifies shallow maps still cloned correctly.
func TestCreatePreset_ShallowSettingsCloned(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	settings := map[string]any{
		"codec":   "H.264",
		"bitrate": 4000,
		"nested":  map[string]any{"level": 1},
	}

	p, err := b.CreatePreset("shallow-test", "", "", settings, nil)
	require.NoError(t, err)
	assert.Equal(t, "H.264", p.Settings["codec"])
	nested := p.Settings["nested"].(map[string]any)
	assert.Equal(t, 1, nested["level"])
}
