package fis_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestFISHandler_GetSafetyLever(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// The safety lever ID is the account ID.
	rec := doRequest(t, h, http.MethodGet, "/safetyLevers/000000000000", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		SafetyLever struct {
			ID    string `json:"id"`
			State struct {
				Status string `json:"status"`
			} `json:"state"`
		} `json:"safetyLever"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "000000000000", resp.SafetyLever.ID)
	assert.Equal(t, "disengaged", resp.SafetyLever.State.Status)
}

func TestFISHandler_GetSafetyLever_AnyIDReturnsLever(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Real AWS returns the account's single lever for any ID path segment.
	rec := doRequest(t, h, http.MethodGet, "/safetyLevers/999999999999", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestFISHandler_UpdateSafetyLeverState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        map[string]any
		wantStatus   string
		wantHTTPCode int
	}{
		{
			name: "engage_lever",
			input: map[string]any{
				"updateSafetyLeverStateInput": map[string]any{
					"status": "engaged",
					"reason": "testing safety lever",
				},
			},
			wantStatus:   "engaged",
			wantHTTPCode: http.StatusOK,
		},
		{
			name: "disengage_lever",
			input: map[string]any{
				"updateSafetyLeverStateInput": map[string]any{
					"status": "disengaged",
					"reason": "resuming operations",
				},
			},
			wantStatus:   "disengaged",
			wantHTTPCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPatch, "/safetyLevers/000000000000", tt.input)
			require.Equal(t, tt.wantHTTPCode, rec.Code)

			var resp struct {
				SafetyLever struct {
					State struct {
						Status string `json:"status"`
					} `json:"state"`
				} `json:"safetyLever"`
			}

			mustJSON(t, rec, &resp)
			assert.Equal(t, tt.wantStatus, resp.SafetyLever.State.Status)
		})
	}
}

func TestFISHandler_UpdateSafetyLeverState_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPatch, "/safetyLevers/000000000000",
		bytes.NewBufferString("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetSafetyLever_DefaultAlias(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/safetyLevers/default", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "GET /safetyLevers/default should work")

	var resp struct {
		SafetyLever struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"safetyLever"`
	}

	mustJSON(t, rec, &resp)
	assert.NotEmpty(t, resp.SafetyLever.ID)
}

func TestUpdateSafetyLever_DefaultAlias(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"updateSafetyLeverStateInput": map[string]any{
			"status": "engaged",
			"reason": "testing default alias",
		},
	}

	rec := doRequest(t, h, http.MethodPatch, "/safetyLevers/default", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		SafetyLever struct {
			State struct {
				Status string `json:"status"`
			} `json:"state"`
		} `json:"safetyLever"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "engaged", resp.SafetyLever.State.Status)
}

// ----------------------------------------
// Issue #26 — __type in error responses
// ----------------------------------------

func TestPersistenceRestoreNilSafetyLever(t *testing.T) {
	t.Parallel()

	// Restore from a snapshot that lacks a safetyLever field should
	// reconstruct the lever rather than leaving it nil.
	raw := []byte(`{"templates":{},"experiments":{},"targetAccountConfigs":{},
		"safetyLever":null,"accountID":"000000000000","region":"us-east-1"}`)

	b := fis.NewTestBackend()
	require.NoError(t, b.Restore(t.Context(), raw))

	// GetSafetyLever should succeed (lever was auto-rebuilt).
	lever, err := b.GetSafetyLever("000000000000")
	require.NoError(t, err)
	assert.Equal(t, "disengaged", lever.State.Status)
}

// ----------------------------------------
// TargetAccountConfiguration CRUD round-trip
// ----------------------------------------

func TestSafetyLever_PreservedAcrossPersistence(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Engage the lever via HTTP PATCH.
	rec := doRequest(
		t, h, http.MethodPatch, "/safetyLevers/000000000000",
		map[string]any{
			"updateSafetyLeverStateInput": map[string]any{
				"status": "engaged",
				"reason": "test lock",
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	b := h.Backend.(*fis.ExportedInMemoryBackend)
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := fis.NewTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	lever, err := b2.GetSafetyLever("000000000000")
	require.NoError(t, err)
	assert.Equal(t, "engaged", lever.State.Status)
	assert.Equal(t, "test lock", lever.State.Reason)
}

// ----------------------------------------
// DeleteTemplate cascades via HTTP
// ----------------------------------------

// TestFISGetSafetyLeverAnyID verifies that GetSafetyLever returns the account's
// lever for any path segment, not just the exact stored account ID.
func TestFISGetSafetyLeverAnyID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "exact account ID", id: "000000000000"},
		{name: "default alias", id: "default"},
		{name: "different account ID", id: "999999999999"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/safetyLevers/"+tc.id, nil)
			assert.Equal(t, http.StatusOK, rec.Code, "id=%q", tc.id)

			var resp struct {
				SafetyLever struct {
					State struct{ Status string } `json:"state"`
				} `json:"safetyLever"`
			}

			mustJSON(t, rec, &resp)
			assert.Equal(t, "disengaged", resp.SafetyLever.State.Status)
		})
	}
}
