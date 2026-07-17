package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMacie2_Session(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *macie2.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "GetMacieSession when not enabled returns 403",
			method:   http.MethodGet,
			path:     "/macie",
			wantCode: http.StatusForbidden,
		},
		{
			name:   "EnableMacie returns 200",
			method: http.MethodPost,
			path:   "/macie",
			body: map[string]string{
				"findingPublishingFrequency": "FIFTEEN_MINUTES",
				"status":                     "ENABLED",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "GetMacieSession after enable returns session details",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/macie", map[string]string{
					"findingPublishingFrequency": "ONE_HOUR",
				})
			},
			method:   http.MethodGet,
			path:     "/macie",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ONE_HOUR", resp["findingPublishingFrequency"])
				assert.Equal(t, "ENABLED", resp["status"])
				assert.NotEmpty(t, resp["serviceRole"])
				assert.NotEmpty(t, resp["createdAt"])
			},
		},
		{
			name: "UpdateMacieSession changes frequency",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/macie", nil)
			},
			method:   http.MethodPatch,
			path:     "/macie",
			body:     map[string]string{"findingPublishingFrequency": "SIX_HOURS"},
			wantCode: http.StatusOK,
		},
		{
			name: "DisableMacie returns 200",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/macie", nil)
			},
			method:   http.MethodDelete,
			path:     "/macie",
			wantCode: http.StatusOK,
		},
		{
			name:     "DisableMacie when not enabled returns 200 (idempotent)",
			method:   http.MethodDelete,
			path:     "/macie",
			wantCode: http.StatusOK,
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

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_FrequencyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		freq      string
		wantError string
		wantCode  int
	}{
		{
			name:     "EnableMacie with FIFTEEN_MINUTES succeeds",
			freq:     "FIFTEEN_MINUTES",
			wantCode: http.StatusOK,
		},
		{
			name:     "EnableMacie with ONE_HOUR succeeds",
			freq:     "ONE_HOUR",
			wantCode: http.StatusOK,
		},
		{
			name:     "EnableMacie with SIX_HOURS succeeds",
			freq:     "SIX_HOURS",
			wantCode: http.StatusOK,
		},
		{
			name:      "EnableMacie with invalid frequency returns 400",
			freq:      "HOURLY",
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:      "EnableMacie with lowercase frequency returns 400",
			freq:      "one_hour",
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/macie",
				map[string]string{"findingPublishingFrequency": tt.freq})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestMacie2_UpdateSessionNotEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		freq      string
		wantError string
		wantCode  int
		enabled   bool
	}{
		{
			name:      "UpdateMacieSession when not enabled returns 403",
			enabled:   false,
			freq:      "ONE_HOUR",
			wantCode:  http.StatusForbidden,
			wantError: "AccessDeniedException",
		},
		{
			name:     "UpdateMacieSession when enabled succeeds",
			enabled:  true,
			freq:     "ONE_HOUR",
			wantCode: http.StatusOK,
		},
		{
			name:      "UpdateMacieSession with invalid frequency returns 400",
			enabled:   true,
			freq:      "DAILY",
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.enabled {
				doRequest(t, h, http.MethodPost, "/macie", nil)
			}

			rec := doRequest(t, h, http.MethodPatch, "/macie",
				map[string]string{"findingPublishingFrequency": tt.freq})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestSessionTimestampsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/macie", map[string]string{
		"findingPublishingFrequency": "ONE_HOUR",
	})

	rec := doRequest(t, h, http.MethodGet, "/macie", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	for _, field := range []string{"createdAt", "updatedAt"} {
		raw, ok := resp[field]
		require.True(t, ok, "GetMacieSession must include %s", field)
		ts, ok := raw.(string)
		require.True(t, ok, "%s must be an ISO8601 string, got %T", field, raw)
		_, err := time.Parse(time.RFC3339, ts)
		assert.NoError(t, err, "%s must be a valid RFC3339 timestamp, got %q", field, ts)
	}
}

func TestSessionUpdatedAtAdvances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/macie", nil)

	rec1 := doRequest(t, h, http.MethodGet, "/macie", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var before map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &before))
	createdAt := before["createdAt"].(string)

	time.Sleep(1001 * time.Millisecond)

	doRequest(t, h, http.MethodPatch, "/macie",
		map[string]string{"findingPublishingFrequency": "SIX_HOURS"})

	rec2 := doRequest(t, h, http.MethodGet, "/macie", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &after))

	assert.Equal(t, createdAt, after["createdAt"].(string), "createdAt must not change after update")
	assert.NotEqual(t, after["createdAt"], after["updatedAt"],
		"updatedAt must differ from createdAt after UpdateMacieSession")
}
