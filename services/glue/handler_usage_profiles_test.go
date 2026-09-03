package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteUsageProfile_NotFound verifies that DeleteUsageProfile raises
// InvalidInputException when the profile does not exist: its error switch
// (glue@v1.152.0 deserializers.go) has no EntityNotFoundException case,
// unlike GetUsageProfile/UpdateUsageProfile's.
func TestDeleteUsageProfile_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		profName  string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "delete_missing_profile_returns_invalid_input",
			profName:  "ghost-profile",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidInputException",
		},
		{
			name:     "delete_existing_profile_succeeds",
			profName: "real-profile",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.create {
				rec := doGlueRequest(t, h, "CreateUsageProfile", map[string]any{"Name": tt.profName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "DeleteUsageProfile", map[string]any{"Name": tt.profName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestUsageProfile_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// List empty
	rec := doGlueRequest(t, h, "ListUsageProfiles", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create
	rec = doGlueRequest(t, h, "CreateUsageProfile", map[string]any{
		"Name":        "my-profile",
		"Description": "test profile",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get
	rec = doGlueRequest(t, h, "GetUsageProfile", map[string]any{"Name": "my-profile"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-profile")

	// List shows it
	rec = doGlueRequest(t, h, "ListUsageProfiles", map[string]any{})
	assert.Contains(t, rec.Body.String(), "my-profile")

	// Update
	rec = doGlueRequest(t, h, "UpdateUsageProfile", map[string]any{"Name": "my-profile"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doGlueRequest(t, h, "DeleteUsageProfile", map[string]any{"Name": "my-profile"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCreateUsageProfile_NameRequired verifies CreateUsageProfile
// rejects an empty Name with InvalidInputException.
func TestCreateUsageProfile_NameRequired(t *testing.T) {
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
			wantError: "InvalidInputException",
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

// TestUsageProfile_ErrorPropagation verifies 404 on unknown usage profile.
func TestUsageProfile_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "get_not_found",
			action:   "GetUsageProfile",
			input:    map[string]any{"Name": "no-such-profile"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_empty_name_ok",
			action:   "GetUsageProfile",
			input:    map[string]any{"Name": ""},
			wantCode: http.StatusOK,
		},
		{
			name:     "create_ok",
			action:   "CreateUsageProfile",
			input:    map[string]any{"Name": "my-profile"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteUsageProfile",
			input:    map[string]any{"Name": "any-profile"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tc.action, tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestGetUsageProfile_FullFields tests that GetUsageProfile returns all profile fields.
func TestGetUsageProfile_FullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doGlueRequest(t, h, "CreateUsageProfile", map[string]any{
		"Name":        "full-profile",
		"Description": "test description",
		"Tags":        map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	getRec := doGlueRequest(t, h, "GetUsageProfile", map[string]any{
		"Name": "full-profile",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		Tags           map[string]string `json:"Tags"`
		Name           string            `json:"Name"`
		Description    string            `json:"Description"`
		CreatedOn      float64           `json:"CreatedOn"`
		LastModifiedOn float64           `json:"LastModifiedOn"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	assert.Equal(t, "full-profile", out.Name)
	assert.Equal(t, "test description", out.Description)
	assert.NotZero(t, out.CreatedOn)
	assert.NotZero(t, out.LastModifiedOn)
}
