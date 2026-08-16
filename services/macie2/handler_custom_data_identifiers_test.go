package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestMacie2_CustomDataIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *macie2.Handler) string
		pathFn   func(id string) string
		check    func(t *testing.T, body []byte)
		name     string
		method   string
		wantCode int
	}{
		{
			name:   "CreateCustomDataIdentifier returns id",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":  "ssn-detector",
				"regex": `\d{3}-\d{2}-\d{4}`,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["customDataIdentifierId"])
			},
		},
		{
			name:   "GetCustomDataIdentifier returns full detail",
			method: http.MethodGet,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":        "phone-detector",
					"regex":       `\d{3}-\d{3}-\d{4}`,
					"description": "detects phone numbers",
					"keywords":    []string{"phone", "tel"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["customDataIdentifierId"]
			},
			pathFn:   func(id string) string { return "/custom-data-identifiers/" + id },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "phone-detector", resp["name"])
				assert.Equal(t, "detects phone numbers", resp["description"])
				assert.NotEmpty(t, resp["arn"])
				assert.InDelta(t, float64(50), resp["maximumMatchDistance"], 0.001)
			},
		},
		{
			name:   "TestCustomDataIdentifier returns match count",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers/test" },
			body: map[string]any{
				"regex":      `\d{3}-\d{2}-\d{4}`,
				"sampleText": "my ssn is 123-45-6789 and another 987-65-4321",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.InDelta(t, float64(2), resp["matchCount"], 0.001)
			},
		},
		{
			name:     "ListCustomDataIdentifiers returns items key",
			method:   http.MethodPost,
			pathFn:   func(_ string) string { return "/custom-data-identifiers/list" },
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "items")
			},
		},
		{
			name:   "DeleteCustomDataIdentifier marks as deleted",
			method: http.MethodDelete,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":  "del-detector",
					"regex": `.+`,
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["customDataIdentifierId"]
			},
			pathFn:   func(id string) string { return "/custom-data-identifiers/" + id },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := ""
			if tt.setup != nil {
				id = tt.setup(h)
			}

			path := tt.pathFn(id)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_CustomDataIdentifierWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		setup     func(h *macie2.Handler) string
		pathFn    func(id string) string
		name      string
		method    string
		wantError string
		wantCode  int
	}{
		{
			name:   "CreateCustomDataIdentifier with invalid regex returns 400",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":  "bad-regex",
				"regex": `[invalid`,
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:   "CreateCustomDataIdentifier with maxMatchDistance > 300 returns 400",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":                 "too-far",
				"regex":                `\d+`,
				"maximumMatchDistance": 301,
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:   "CreateCustomDataIdentifier with maxMatchDistance 0 returns 400",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":                 "zero-dist",
				"regex":                `\d+`,
				"maximumMatchDistance": 0,
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:   "CreateCustomDataIdentifier with maxMatchDistance 300 succeeds",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":                 "max-dist",
				"regex":                `\d+`,
				"maximumMatchDistance": 300,
			},
			wantCode: http.StatusOK,
		},
		{
			// Real AWS soft-deletes custom data identifiers: Delete never
			// removes them, so Get keeps succeeding (with deleted:true) --
			// see TestGetCustomDataIdentifierOnDeletedCDI for the body
			// assertion. Only a genuinely unknown ID 404s.
			name: "GetCustomDataIdentifier on deleted CDI returns 200",
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":  "to-delete",
					"regex": `\d+`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				id := resp["customDataIdentifierId"]

				doRequest(t, h, http.MethodDelete, "/custom-data-identifiers/"+id, nil)

				return id
			},
			method:   http.MethodGet,
			pathFn:   func(id string) string { return "/custom-data-identifiers/" + id },
			wantCode: http.StatusOK,
		},
		{
			name:      "GetCustomDataIdentifier on unknown ID returns 404",
			method:    http.MethodGet,
			pathFn:    func(_ string) string { return "/custom-data-identifiers/nonexistent" },
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := ""
			if tt.setup != nil {
				id = tt.setup(h)
			}

			path := tt.pathFn(id)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestCustomDataIdentifierDeletedField locks in real
// GetCustomDataIdentifierOutput wire shape: the "deleted" field is always
// present (false for a live identifier) and flips to true -- without a 404
// -- once the identifier has been soft-deleted.
func TestCustomDataIdentifierDeletedField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
		"name":  "deleted-field",
		"regex": `\d+`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["customDataIdentifierId"]

	rec2 := doRequest(t, h, http.MethodGet, "/custom-data-identifiers/"+id, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	deleted, hasDeleted := resp["deleted"]
	require.True(t, hasDeleted, "GetCustomDataIdentifier must include a 'deleted' field")
	assert.Equal(t, false, deleted)

	rec3 := doRequest(t, h, http.MethodDelete, "/custom-data-identifiers/"+id, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	rec4 := doRequest(t, h, http.MethodGet, "/custom-data-identifiers/"+id, nil)
	require.Equal(t, http.StatusOK, rec4.Code, "Get on a soft-deleted identifier must not 404")

	var afterDelete map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &afterDelete))
	assert.Equal(t, true, afterDelete["deleted"])

	// BatchGetCustomDataIdentifiers must also return the soft-deleted
	// identifier (with deleted:true) rather than silently excluding it.
	batchRec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers/get", map[string]any{
		"ids": []string{id},
	})
	require.Equal(t, http.StatusOK, batchRec.Code)

	var batchResp map[string]any
	require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &batchResp))
	items, ok := batchResp["customDataIdentifiers"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, item["deleted"])
	assert.Empty(t, batchResp["notFoundIdentifierIds"])
}

// TestCustomDataIdentifierSeverityLevels locks the severityLevels gap fix:
// CreateCustomDataIdentifier accepts severityLevels and
// GetCustomDataIdentifier echoes them back (real GetCustomDataIdentifierOutput
// carries this field; it was previously dropped entirely).
func TestCustomDataIdentifierSeverityLevels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
		"name":  "severity-levels",
		"regex": `\d+`,
		"severityLevels": []map[string]any{
			{"severity": "LOW", "occurrencesThreshold": 1},
			{"severity": "HIGH", "occurrencesThreshold": 100},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	rec2 := doRequest(t, h, http.MethodGet, "/custom-data-identifiers/"+created["customDataIdentifierId"], nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	levels, ok := resp["severityLevels"].([]any)
	require.True(t, ok)
	require.Len(t, levels, 2)

	first, ok := levels[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "LOW", first["severity"])
	assert.InDelta(t, float64(1), first["occurrencesThreshold"], 0.0001)
}

// 6-10. Empty-state: arrays must be [] not null

func TestListCustomDataIDsEmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["items"]
	require.True(t, ok, "response must contain items key")
	assert.NotNil(t, v, "items must be [] not null when empty")

	arr, isArr := v.([]any)
	require.True(t, isArr, "items must be an array")
	assert.Empty(t, arr)
}
