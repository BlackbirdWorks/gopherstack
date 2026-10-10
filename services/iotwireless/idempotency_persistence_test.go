package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestClientRequestTokenSurvivesRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantSameID bool
	}{
		{
			name:       "same_params_replay",
			body:       `{"Name":"p","ClientRequestToken":"tok"}`,
			wantStatus: http.StatusCreated,
			wantSameID: true,
		},
		{
			name:       "different_params_conflict",
			body:       `{"Name":"other","ClientRequestToken":"tok"}`,
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodPost, "/service-profiles", `{"Name":"p","ClientRequestToken":"tok"}`)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

			var first map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &first))

			snap := h.Backend.(*iotwireless.InMemoryBackend).Snapshot(t.Context())
			require.NotEmpty(t, snap)

			restored := newTestHandlerHTTP()
			require.NoError(t, restored.Backend.(*iotwireless.InMemoryBackend).Restore(t.Context(), snap))

			rec = doIoTWRequest(t, restored, http.MethodPost, "/service-profiles", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantSameID {
				var again map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &again))
				assert.Equal(t, first["Id"], again["Id"])
			}
		})
	}
}
