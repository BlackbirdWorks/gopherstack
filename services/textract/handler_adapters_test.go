package textract_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestHandler_AdapterLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		adapterName string
		description string
		wantStatus  int
	}{
		{
			name:        "full adapter lifecycle",
			adapterName: "my-adapter",
			description: "test adapter",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "missing adapter name returns error",
			adapterName: "",
			description: "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// CreateAdapter
			createBody := map[string]any{
				"AdapterName":  tt.adapterName,
				"Description":  tt.description,
				"FeatureTypes": []string{"QUERIES"},
			}
			createRec := doTextractRequest(t, h, "CreateAdapter", createBody)
			assert.Equal(t, tt.wantStatus, createRec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			adapterID := createResp["AdapterId"]
			assert.NotEmpty(t, adapterID)

			// GetAdapter
			getBody := map[string]string{"AdapterId": adapterID}
			getRec := doTextractRequest(t, h, "GetAdapter", getBody)
			assert.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.adapterName, getResp["AdapterName"])
			assert.Equal(t, tt.description, getResp["Description"])

			// DeleteAdapter
			deleteBody := map[string]string{"AdapterId": adapterID}
			deleteRec := doTextractRequest(t, h, "DeleteAdapter", deleteBody)
			assert.Equal(t, http.StatusOK, deleteRec.Code)

			// GetAdapter after delete returns error
			getRec2 := doTextractRequest(t, h, "GetAdapter", getBody)
			assert.Equal(t, http.StatusBadRequest, getRec2.Code)
		})
	}
}

func TestHandler_GetAdapter_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"AdapterId": "nonexistent-adapter"}
	rec := doTextractRequest(t, h, "GetAdapter", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetAdapter_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}
	rec := doTextractRequest(t, h, "GetAdapter", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_GetAdapter_IncludesTags verifies that GetAdapter (single adapter
// fetch) does include Tags, unlike the ListAdapters summary shape.
func TestHandler_GetAdapter_IncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "tags-adapter",
		"FeatureTypes": []string{"FORMS"},
		"Tags":         map[string]string{"key": "value"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	getRec := doTextractRequest(t, h, "GetAdapter", map[string]any{
		"AdapterId": createResp["AdapterId"],
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	tags, hasTags := getResp["Tags"].(map[string]any)
	assert.True(t, hasTags, "GetAdapter must include Tags")
	assert.Equal(t, "value", tags["key"])
}

func TestHandler_DeleteAdapter_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"AdapterId": "nonexistent-adapter"}
	rec := doTextractRequest(t, h, "DeleteAdapter", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteAdapter_CascadesVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create adapter
	createResp := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "cascade-test",
		"FeatureTypes": []string{"QUERIES"},
	})
	require.Equal(t, http.StatusOK, createResp.Code)

	var cr map[string]string
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &cr))
	adapterID := cr["AdapterId"]

	// Create a version
	versionResp := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, versionResp.Code)

	var vr map[string]string
	require.NoError(t, json.Unmarshal(versionResp.Body.Bytes(), &vr))
	adapterVersion := vr["AdapterVersion"]

	// Delete adapter should cascade to versions
	deleteResp := doTextractRequest(t, h, "DeleteAdapter", map[string]string{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)

	// Version should now be gone
	getVersionResp := doTextractRequest(t, h, "GetAdapterVersion", map[string]string{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	assert.Equal(t, http.StatusBadRequest, getVersionResp.Code)
}

// TestHandler_CreateAdapter_InvalidFeatureTypeTABLES verifies CreateAdapter
// rejects TABLES (only FORMS and QUERIES are valid adapter feature types).
func TestHandler_CreateAdapter_InvalidFeatureTypeTABLES(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "bad-adapter",
		"FeatureTypes": []string{"TABLES"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code, "TABLES is not valid for adapters")
}

// TestHandler_CreateAdapter_ClientRequestTokenDedup verifies CreateAdapter's
// ClientRequestToken dedup returns the same AdapterId.
func TestHandler_CreateAdapter_ClientRequestTokenDedup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"AdapterName":        "dedup-adapter",
		"FeatureTypes":       []string{"FORMS"},
		"ClientRequestToken": "adapter-token-xyz",
	}

	rec1 := doTextractRequest(t, h, "CreateAdapter", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	rec2 := doTextractRequest(t, h, "CreateAdapter", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	assert.Equal(t, resp1["AdapterId"], resp2["AdapterId"], "same token should return same AdapterId")
}

// TestHandler_UpdateAdapter_PreservesNameAndFeatureTypes verifies that
// UpdateAdapter does not modify AdapterName or FeatureTypes; only Description
// and AutoUpdate are mutable.
func TestHandler_UpdateAdapter_PreservesNameAndFeatureTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "immutable-name",
		"FeatureTypes": []string{"FORMS"},
		"Description":  "original",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]
	require.NotEmpty(t, adapterID)

	updateRec := doTextractRequest(t, h, "UpdateAdapter", map[string]any{
		"AdapterId":   adapterID,
		"Description": "updated description",
		"AutoUpdate":  "ENABLED",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))

	assert.Equal(t, "immutable-name", updateResp["AdapterName"],
		"AdapterName must be unchanged after UpdateAdapter")

	featureTypes, _ := updateResp["FeatureTypes"].([]any)
	require.Len(t, featureTypes, 1, "FeatureTypes count must be unchanged")
	assert.Equal(t, "FORMS", featureTypes[0],
		"FeatureTypes must be unchanged after UpdateAdapter")

	assert.Equal(t, "updated description", updateResp["Description"])
	assert.Equal(t, "ENABLED", updateResp["AutoUpdate"])
}

// TestHandler_CreateAdapter_FeatureTypeValidation ensures at least one valid
// feature type is required, and only FORMS/QUERIES are accepted.
func TestHandler_CreateAdapter_FeatureTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		featureTypes []string
		wantStatus   int
	}{
		{
			// TABLES is not allowed for adapters (only FORMS and QUERIES per AWS spec).
			name:         "TABLES returns 400 for adapter",
			featureTypes: []string{"TABLES"},
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "valid FORMS",
			featureTypes: []string{"FORMS"},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "valid QUERIES",
			featureTypes: []string{"QUERIES"},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "multiple valid types FORMS+QUERIES",
			featureTypes: []string{"FORMS", "QUERIES"},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "empty slice returns 400",
			featureTypes: []string{},
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "invalid type returns 400",
			featureTypes: []string{"INVALID_TYPE"},
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
				"AdapterName":  "test-adapter",
				"FeatureTypes": tt.featureTypes,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateAdapter_AutoUpdateValidation validates the AutoUpdate field.
func TestHandler_CreateAdapter_AutoUpdateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		autoUpdate string
		wantStatus int
	}{
		{name: "ENABLED", autoUpdate: "ENABLED", wantStatus: http.StatusOK},
		{name: "DISABLED", autoUpdate: "DISABLED", wantStatus: http.StatusOK},
		{name: "empty defaults to DISABLED", autoUpdate: "", wantStatus: http.StatusOK},
		{name: "invalid returns 400", autoUpdate: "INVALID", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
				"AdapterName":  "adapter",
				"FeatureTypes": []string{"FORMS"},
				"AutoUpdate":   tt.autoUpdate,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UpdateAdapter_HappyPath tests updating adapter description and AutoUpdate.
func TestHandler_UpdateAdapter_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "my-adapter",
		"FeatureTypes": []string{"FORMS"},
		"Description":  "original",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]

	updateRec := doTextractRequest(t, h, "UpdateAdapter", map[string]any{
		"AdapterId":   adapterID,
		"Description": "updated description",
		"AutoUpdate":  "ENABLED",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated description", updateResp["Description"])
	assert.Equal(t, "ENABLED", updateResp["AutoUpdate"])
}

// TestHandler_UpdateAdapter_NotFound ensures not-found returns 400.
func TestHandler_UpdateAdapter_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "UpdateAdapter", map[string]any{
		"AdapterId":  "nonexistent",
		"AutoUpdate": "DISABLED",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp["__type"])
}

// TestHandler_ListAdapters_HappyPath tests ListAdapters with multiple entries.
func TestHandler_ListAdapters_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		rec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
			"AdapterName":  name,
			"FeatureTypes": []string{"FORMS"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doTextractRequest(t, h, "ListAdapters", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	adapters, ok := resp["Adapters"].([]any)
	assert.True(t, ok)
	assert.Len(t, adapters, 3)
}

// TestHandler_ListAdapters_SummaryShape verifies that ListAdapters returns
// FeatureTypes in each adapter summary and omits Tags: real AWS ListAdapters
// returns AdapterOverview items with FeatureTypes but no Tags; Tags are only
// accessible via GetAdapter or ListTagsForResource.
func TestHandler_ListAdapters_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "tagged-adapter",
		"FeatureTypes": []string{"FORMS", "QUERIES"},
		"Tags":         map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	listRec := doTextractRequest(t, h, "ListAdapters", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	adapters, ok := resp["Adapters"].([]any)
	require.True(t, ok)
	require.Len(t, adapters, 1)

	summary, ok := adapters[0].(map[string]any)
	require.True(t, ok)

	fts, hasFT := summary["FeatureTypes"].([]any)
	assert.True(t, hasFT, "ListAdapters summary must include FeatureTypes")
	assert.Len(t, fts, 2, "FeatureTypes must reflect adapter's feature types")

	_, hasTags := summary["Tags"]
	assert.False(t, hasTags, "ListAdapters summary must not include Tags")
}

// TestHandler_Snapshot_Restore_WithAdapters proves Snapshot/Restore round-trips
// adapters through the Handler.
func TestHandler_Snapshot_Restore_WithAdapters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		adapterCount int
	}{
		{name: "empty backend", adapterCount: 0},
		{name: "backend with adapters", adapterCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for range tt.adapterCount {
				doTextractRequest(t, h, "CreateAdapter", map[string]any{
					"AdapterName":  "test-adapter",
					"FeatureTypes": []string{"QUERIES"},
				})
			}

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			h2 := newTestHandler(t)
			require.NoError(t, h2.Restore(t.Context(), snap))

			assert.Equal(t, tt.adapterCount, textract.AdapterCount(h2.Backend.(*textract.InMemoryBackend)))
		})
	}
}
