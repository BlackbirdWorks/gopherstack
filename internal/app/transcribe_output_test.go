package app

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	transcribebackend "github.com/blackbirdworks/gopherstack/services/transcribe"
)

func TestRegisterTranscribeEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantCode int
	}{
		{
			name:     "unknown job",
			path:     "/_gopherstack/transcribe/output/us-east-1/transcription/nope/tok/asrOutput.json",
			wantCode: http.StatusForbidden,
		},
		{name: "short path", path: "/_gopherstack/transcribe/output/us-east-1/x", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := transcribebackend.NewHandler(transcribebackend.NewInMemoryBackend())
			h.EnableRegions()

			e := echo.New()
			registerTranscribeEndpoints(e, []service.Registerable{h})

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.path, nil))
			require.Equal(t, tt.wantCode, rec.Code)
			assert.Empty(t, rec.Body.String())
		})
	}
}
