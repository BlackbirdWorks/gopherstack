package transcribe_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/transcribe"
)

func newTestTranscribeHandler(t *testing.T) *transcribe.Handler {
	t.Helper()

	return transcribe.NewHandler(transcribe.NewInMemoryBackend(), slog.Default())
}

func doTranscribeRequest(t *testing.T, h *transcribe.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Transcribe."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestTranscribe_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Name",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)
				assert.Equal(t, "Transcribe", h.Name())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "StartTranscriptionJob")
				assert.Contains(t, ops, "GetTranscriptionJob")
				assert.Contains(t, ops, "ListTranscriptionJobs")
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)
				assert.Equal(t, 100, h.MatchPriority())
			},
		},
		{
			name: "RouteMatcher",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)
				e := echo.New()

				// Matching request
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "Transcribe.StartTranscriptionJob")
				c := e.NewContext(req, httptest.NewRecorder())
				assert.True(t, h.RouteMatcher()(c))

				// Non-matching request
				req2 := httptest.NewRequest(http.MethodPost, "/", nil)
				req2.Header.Set("X-Amz-Target", "OtherService.Action")
				c2 := e.NewContext(req2, httptest.NewRecorder())
				assert.False(t, h.RouteMatcher()(c2))
			},
		},
		{
			name: "ExtractOperation",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)
				e := echo.New()

				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "Transcribe.StartTranscriptionJob")
				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, "StartTranscriptionJob", h.ExtractOperation(c))
			},
		},
		{
			name: "ExtractResource",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)
				e := echo.New()

				body := `{"TranscriptionJobName":"my-job"}`
				req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
				req.Header.Set("X-Amz-Target", "Transcribe.GetTranscriptionJob")
				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, "my-job", h.ExtractResource(c))
			},
		},
		{
			name: "StartTranscriptionJob",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)

				rec := doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
					"TranscriptionJobName": "test-job",
					"LanguageCode":         "en-US",
					"Media": map[string]any{
						"MediaFileUri": "s3://my-bucket/audio.mp3",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				job, ok := resp["TranscriptionJob"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "test-job", job["TranscriptionJobName"])
				assert.Equal(t, "COMPLETED", job["TranscriptionJobStatus"])
			},
		},
		{
			name: "StartTranscriptionJob_AlreadyExists",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)

				doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
					"TranscriptionJobName": "dup-job",
					"LanguageCode":         "en-US",
				})

				rec := doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
					"TranscriptionJobName": "dup-job",
					"LanguageCode":         "en-US",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "GetTranscriptionJob",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)

				doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
					"TranscriptionJobName": "get-job",
					"LanguageCode":         "en-US",
				})

				rec := doTranscribeRequest(t, h, "GetTranscriptionJob", map[string]any{
					"TranscriptionJobName": "get-job",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				job, ok := resp["TranscriptionJob"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "get-job", job["TranscriptionJobName"])
			},
		},
		{
			name: "GetTranscriptionJob_NotFound",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)

				rec := doTranscribeRequest(t, h, "GetTranscriptionJob", map[string]any{
					"TranscriptionJobName": "no-such-job",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "ListTranscriptionJobs",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)

				doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
					"TranscriptionJobName": "list-job-1",
					"LanguageCode":         "en-US",
				})
				doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
					"TranscriptionJobName": "list-job-2",
					"LanguageCode":         "en-US",
				})

				rec := doTranscribeRequest(t, h, "ListTranscriptionJobs", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				summaries, ok := resp["TranscriptionJobSummaries"].([]any)
				require.True(t, ok)
				assert.GreaterOrEqual(t, len(summaries), 2)
			},
		},
		{
			name: "UnknownAction",
			run: func(t *testing.T) {
				h := newTestTranscribeHandler(t)

				rec := doTranscribeRequest(t, h, "UnknownAction", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "Provider_Init",
			run: func(t *testing.T) {
				p := &transcribe.Provider{}
				assert.Equal(t, "Transcribe", p.Name())

				svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
				require.NoError(t, err)
				assert.NotNil(t, svc)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
