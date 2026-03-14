package transcribe_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

func newTestTranscribeHandler(t *testing.T) *transcribe.Handler {
	t.Helper()

	return transcribe.NewHandler(transcribe.NewInMemoryBackend())
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
	body   map[string]any
	action string
}

func TestTranscribe_HandlerActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		action       string
		setupActions []transcribeSetupAction
		wantContains []string
		wantCode     int
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

func TestTranscribe_DeleteTranscriptionJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *transcribe.Handler) {
				t.Helper()
				_, err := h.Backend.StartTranscriptionJob("job-to-delete", "en-US", "s3://bucket/file.mp4")
				require.NoError(t, err)
			},
			body:     map[string]any{"TranscriptionJobName": "job-to-delete"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			setup:    func(_ *testing.T, _ *transcribe.Handler) {},
			body:     map[string]any{"TranscriptionJobName": "missing-job"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "missing_name",
			setup:    func(_ *testing.T, _ *transcribe.Handler) {},
			body:     map[string]any{},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestTranscribeHandler(t)
			tt.setup(t, h)

			rec := doTranscribeRequest(t, h, "DeleteTranscriptionJob", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestTranscribe_ListTranscriptionJobsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		count         int
		wantNextToken bool
	}{
		{
			name:          "single_page",
			count:         5,
			wantNextToken: false,
		},
		{
			name:          "multi_page",
			count:         105, // exceeds transcribeDefaultPageSize=100
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestTranscribeHandler(t)

			for i := range tt.count {
				_, err := h.Backend.StartTranscriptionJob(
					fmt.Sprintf("job-%04d", i),
					"en-US",
					fmt.Sprintf("s3://bucket/file%d.mp4", i),
				)
				require.NoError(t, err)
			}

			rec := doTranscribeRequest(t, h, "ListTranscriptionJobs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, summariesOK := resp["TranscriptionJobSummaries"].([]any)
			require.True(t, summariesOK)

			if tt.wantNextToken {
				assert.Len(t, summaries, 100)
				nextToken, tokenOK := resp["NextToken"].(string)
				require.True(t, tokenOK, "NextToken should be present")
				assert.NotEmpty(t, nextToken)

				// Second page using the token.
				rec2 := doTranscribeRequest(t, h, "ListTranscriptionJobs", map[string]any{"NextToken": nextToken})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

				summaries2, summaries2OK := resp2["TranscriptionJobSummaries"].([]any)
				require.True(t, summaries2OK)
				assert.Len(t, summaries2, tt.count-100)
				assert.Empty(t, resp2["NextToken"])
			} else {
				assert.Len(t, summaries, tt.count)
				assert.Empty(t, resp["NextToken"])
			}
		})
	}
}
