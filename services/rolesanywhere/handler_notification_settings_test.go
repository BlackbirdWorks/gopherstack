package rolesanywhere_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Notification Settings HTTP ----

func TestHandler_NotificationSettings_PutReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"put and reset notification settings", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create trust anchor first.
			recTA := doREST(t, h, http.MethodPost, "/trustanchors", map[string]any{
				"name":   "notif-http-anchor",
				"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
			})
			require.Equal(t, http.StatusCreated, recTA.Code)

			var taResp map[string]any
			require.NoError(t, json.Unmarshal(recTA.Body.Bytes(), &taResp))
			ta := taResp["trustAnchor"].(map[string]any)
			taID := ta["trustAnchorId"].(string)

			// Put notification settings.
			recPut := doREST(t, h, http.MethodPatch, "/put-notifications-settings", map[string]any{
				"trustAnchorId": taID,
				"notificationSettings": []map[string]any{
					{"event": "CA_CERTIFICATE_EXPIRY", "enabled": true},
				},
			})
			assert.Equal(t, tt.wantStatus, recPut.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(recPut.Body.Bytes(), &putResp))
			assert.Contains(t, putResp, "trustAnchor")

			// Reset notification settings.
			recReset := doREST(
				t,
				h,
				http.MethodPatch,
				"/reset-notifications-settings",
				map[string]any{
					"trustAnchorId": taID,
					"notificationSettingKeys": []map[string]any{
						{"event": "CA_CERTIFICATE_EXPIRY"},
					},
				},
			)
			assert.Equal(t, tt.wantStatus, recReset.Code)
		})
	}
}

func TestHandler_NotificationSettings_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			"put notifications invalid json → 400",
			"/put-notifications-settings",
			http.StatusBadRequest,
		},
		{
			"reset notifications invalid json → 400",
			"/reset-notifications-settings",
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPatch,
				tt.path,
				bytes.NewReader([]byte(`{invalid`)),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_NotificationSettings_TrustAnchorNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "put notifications nonexistent anchor → 404",
			path: "/put-notifications-settings",
			body: map[string]any{
				"trustAnchorId":        "no-such-anchor",
				"notificationSettings": []any{},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "reset notifications nonexistent anchor → 404",
			path: "/reset-notifications-settings",
			body: map[string]any{
				"trustAnchorId":           "no-such-anchor",
				"notificationSettingKeys": []any{},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(t, h, http.MethodPatch, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
