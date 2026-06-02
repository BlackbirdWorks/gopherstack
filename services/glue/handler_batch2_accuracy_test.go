package glue_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch2Accuracy_Blueprint_GetNotFound verifies GetBlueprint returns
// EntityNotFoundException for a missing blueprint.
func TestBatch2Accuracy_Blueprint_GetNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bpName    string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "missing_blueprint_returns_404_error",
			bpName:    "no-such-bp",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "existing_blueprint_returns_ok",
			bpName:   "real-bp",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.create {
				rec := doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": tt.bpName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "GetBlueprint", map[string]any{"Name": tt.bpName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBatch2Accuracy_Blueprint_UpdateNotFound verifies UpdateBlueprint returns
// EntityNotFoundException for a missing blueprint (not upsert).
func TestBatch2Accuracy_Blueprint_UpdateNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "update_missing_blueprint_fails",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "update_existing_blueprint_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bpName := "upd-bp-" + tt.name
			if tt.create {
				doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": bpName})
			}

			rec := doGlueRequest(t, h, "UpdateBlueprint", map[string]any{"Name": bpName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBatch2Accuracy_Blueprint_DeleteNotFound verifies DeleteBlueprint returns
// EntityNotFoundException for a missing blueprint.
func TestBatch2Accuracy_Blueprint_DeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "delete_missing_blueprint_fails",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "delete_existing_blueprint_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bpName := "del-bp-" + tt.name
			if tt.create {
				doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": bpName})
			}

			rec := doGlueRequest(t, h, "DeleteBlueprint", map[string]any{"Name": bpName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBatch2Accuracy_StartBlueprintRun_NotFound verifies StartBlueprintRun returns
// EntityNotFoundException when the blueprint does not exist.
func TestBatch2Accuracy_StartBlueprintRun_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "start_run_missing_blueprint_fails",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "start_run_existing_blueprint_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bpName := "run-bp-" + tt.name
			if tt.create {
				doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": bpName})
			}

			rec := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{"BlueprintName": bpName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBatch2Accuracy_CreateBlueprint_NameRequired verifies CreateBlueprint
// rejects an empty Name with ValidationException.
func TestBatch2Accuracy_CreateBlueprint_NameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bpName    string
		wantError string
		wantCode  int
	}{
		{
			name:      "empty_name_rejected",
			bpName:    "",
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:     "valid_name_accepted",
			bpName:   "valid-blueprint",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": tt.bpName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
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
			wantError: "ValidationException",
		},
		{
			name:      "empty_regex_rejected",
			body:      map[string]any{"Name": "my-cet", "RegexString": ""},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:      "missing_regex_rejected",
			body:      map[string]any{"Name": "my-cet2"},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
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

// TestBatch2Accuracy_CreateUsageProfile_NameRequired verifies CreateUsageProfile
// rejects an empty Name with ValidationException.
func TestBatch2Accuracy_CreateUsageProfile_NameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		profName  string
		wantError string
		wantCode  int
	}{
		{
			name:      "empty_name_rejected",
			profName:  "",
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:     "valid_name_accepted",
			profName: "my-profile",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateUsageProfile", map[string]any{
				"Name": tt.profName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}
