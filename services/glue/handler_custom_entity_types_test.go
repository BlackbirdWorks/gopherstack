package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestBatch2_CustomEntityType_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doGlueRequest(t, h, "CreateCustomEntityType", map[string]any{
		"Name":        "my-cet",
		"RegexString": "\\d+",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-cet")

	// Get via GetCustomEntityType
	rec = doGlueRequest(t, h, "GetCustomEntityType", map[string]any{"Name": "my-cet"})
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doGlueRequest(t, h, "ListCustomEntityTypes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-cet")

	// Delete
	rec = doGlueRequest(t, h, "DeleteCustomEntityType", map[string]any{"Name": "my-cet"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestBatch2Accuracy_CreateCustomEntityType_Validation verifies that
// CreateCustomEntityType requires both Name and RegexString.
func TestBatch2Accuracy_CreateCustomEntityType_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantError string
		wantCode  int
	}{
		{
			name:      "empty_name_rejected",
			body:      map[string]any{"Name": "", "RegexString": "\\d+"},
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidInputException",
		},
		{
			name:      "empty_regex_rejected",
			body:      map[string]any{"Name": "my-cet", "RegexString": ""},
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidInputException",
		},
		{
			name:      "missing_regex_rejected",
			body:      map[string]any{"Name": "my-cet2"},
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidInputException",
		},
		{
			name:     "valid_name_and_regex_accepted",
			body:     map[string]any{"Name": "valid-cet", "RegexString": "\\d+"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateCustomEntityType", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestBatch3_CustomEntityType_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, body string)
		name     string
		op       string
		wantCode int
	}{
		{
			name: "create-with-regex",
			op:   "CreateCustomEntityType",
			body: map[string]any{
				"Name":         "SSN",
				"RegexString":  `\d{3}-\d{2}-\d{4}`,
				"ContextWords": []string{"social", "security"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "get",
			op:   "GetCustomEntityType",
			body: map[string]any{"Name": "SSN"},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "SSN")
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-missing",
			op:       "GetCustomEntityType",
			body:     map[string]any{"Name": "NO-CET"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list",
			op:       "ListCustomEntityTypes",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "SSN")
			},
		},
		{
			name:     "batch-get",
			op:       "BatchGetCustomEntityTypes",
			body:     map[string]any{"TypeNames": []string{"SSN", "NO-CET"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteCustomEntityType",
			body:     map[string]any{"Name": "SSN"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateCustomEntityType", map[string]any{
				"Name":         "SSN",
				"RegexString":  `\d{3}-\d{2}-\d{4}`,
				"ContextWords": []string{"social", "security"},
			})

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestBatch3_CustomEntityType_RegexAndContextWords_Persist(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateCustomEntityType", map[string]any{
		"Name":         "CreditCard",
		"RegexString":  `\d{4}-\d{4}-\d{4}-\d{4}`,
		"ContextWords": []string{"credit", "card", "payment"},
	})

	rec := doGlueRequest(t, h, "GetCustomEntityType", map[string]any{"Name": "CreditCard"})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, `\d{4}-\d{4}-\d{4}-\d{4}`, out["RegexString"])
	ctxWords, ok := out["ContextWords"].([]any)
	require.True(t, ok)
	assert.Len(t, ctxWords, 3)
}

// TestCustomEntityType_ErrorPropagation verifies error propagation for custom entity types.
func TestCustomEntityType_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *glue.Handler)
		input    map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "create_ok",
			action:   "CreateCustomEntityType",
			input:    map[string]any{"Name": "my-cet", "RegexString": `\d+`},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteCustomEntityType",
			input:    map[string]any{"Name": "no-such-cet"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "delete_after_create_ok",
			action: "DeleteCustomEntityType",
			setup: func(t *testing.T, h *glue.Handler) {
				t.Helper()
				rec := doGlueRequest(t, h, "CreateCustomEntityType", map[string]any{
					"Name": "to-delete", "RegexString": `\w+`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:    map[string]any{"Name": "to-delete"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(t, h)
			}

			rec := doGlueRequest(t, h, tc.action, tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestAccuracy_GetCustomEntityType_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetCustomEntityType", map[string]any{
		"Name": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_GetCustomEntityType_Found(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	b.AddCustomEntityTypeInternal(&glue.CustomEntityType{
		Name:        "MY_TYPE",
		RegexString: `\d+`,
	})
	h := glue.NewHandler(b)

	rec := doGlueRequest(t, h, "GetCustomEntityType", map[string]any{
		"Name": "MY_TYPE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Name        string `json:"Name"`
		RegexString string `json:"RegexString"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "MY_TYPE", out.Name)
	assert.Equal(t, `\d+`, out.RegexString)
}
