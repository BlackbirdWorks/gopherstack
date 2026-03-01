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

func TestTranscribe_Name(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)
	assert.Equal(t, "Transcribe", h.Name())
}

func TestTranscribe_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "StartTranscriptionJob")
	assert.Contains(t, ops, "GetTranscriptionJob")
	assert.Contains(t, ops, "ListTranscriptionJobs")
}

func TestTranscribe_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestTranscribe_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matching_request",
			target: "Transcribe.StartTranscriptionJob",
			want:   true,
		},
		{
			name:   "non_matching_request",
			target: "OtherService.Action",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestTranscribeHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestTranscribe_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Transcribe.StartTranscriptionJob")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "StartTranscriptionJob", h.ExtractOperation(c))
}

func TestTranscribe_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)
	e := echo.New()
	body := `{"TranscriptionJobName":"my-job"}`
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	req.Header.Set("X-Amz-Target", "Transcribe.GetTranscriptionJob")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-job", h.ExtractResource(c))
}

type transcribeSetupAction struct {
	action string
	body   map[string]any
}

func TestTranscribe_HandlerActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupActions []transcribeSetupAction
		action       string
		body         map[string]any
		wantCode     int
		wantContains []string
	}{
		{
			name:   "StartTranscriptionJob",
			action: "StartTranscriptionJob",
			body: map[string]any{
				"TranscriptionJobName": "test-job",
				"LanguageCode":         "en-US",
				"Media": map[string]any{
					"MediaFileUri": "s3://my-bucket/audio.mp3",
				},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"test-job", "COMPLETED"},
		},
		{
			name: "StartTranscriptionJob_AlreadyExists",
			setupActions: []transcribeSetupAction{
				{action: "StartTranscriptionJob", body: map[string]any{
					"TranscriptionJobName": "dup-job",
					"LanguageCode":         "en-US",
				}},
			},
			action: "StartTranscriptionJob",
			body: map[string]any{
				"TranscriptionJobName": "dup-job",
				"LanguageCode":         "en-US",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "GetTranscriptionJob",
			setupActions: []transcribeSetupAction{
				{action: "StartTranscriptionJob", body: map[string]any{
					"TranscriptionJobName": "get-job",
					"LanguageCode":         "en-US",
				}},
			},
			action: "GetTranscriptionJob",
			body: map[string]any{
				"TranscriptionJobName": "get-job",
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"get-job"},
		},
		{
			name:   "GetTranscriptionJob_NotFound",
			action: "GetTranscriptionJob",
			body: map[string]any{
				"TranscriptionJobName": "no-such-job",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "ListTranscriptionJobs",
			setupActions: []transcribeSetupAction{
				{action: "StartTranscriptionJob", body: map[string]any{
					"TranscriptionJobName": "list-job-1",
					"LanguageCode":         "en-US",
				}},
				{action: "StartTranscriptionJob", body: map[string]any{
					"TranscriptionJobName": "list-job-2",
					"LanguageCode":         "en-US",
				}},
			},
			action:       "ListTranscriptionJobs",
			body:         map[string]any{},
			wantCode:     http.StatusOK,
			wantContains: []string{"list-job-1", "list-job-2"},
		},
		{
			name:     "UnknownAction",
			action:   "UnknownAction",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestTranscribeHandler(t)

			for _, sa := range tt.setupActions {
				doTranscribeRequest(t, h, sa.action, sa.body)
			}

			rec := doTranscribeRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestTranscribe_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &transcribe.Provider{}
	assert.Equal(t, "Transcribe", p.Name())

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}
