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
					"MediaFileURI": "s3://my-bucket/audio.mp3",
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
			wantCode: http.StatusConflict,
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
				_, err := h.Backend.StartTranscriptionJob(
					&transcribe.TranscriptionJob{
						JobName:      "job-to-delete",
						LanguageCode: "en-US",
						Media:        transcribe.Media{MediaFileURI: "s3://bucket/file.mp4"},
					},
				)
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
			wantCode: http.StatusBadRequest,
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
				_, err := h.Backend.StartTranscriptionJob(&transcribe.TranscriptionJob{
					JobName:      fmt.Sprintf("job-%04d", i),
					LanguageCode: "en-US",
					Media:        transcribe.Media{MediaFileURI: fmt.Sprintf("s3://bucket/file%d.mp4", i)},
				})
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

func TestTranscribe_CreateCallAnalyticsCategory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name:  "success",
			setup: func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body: map[string]any{
				"CategoryName": "my-category",
				"InputType":    "POST_CALL",
			},
			wantCode: http.StatusOK,
			wantKey:  "my-category",
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, b *transcribe.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateCallAnalyticsCategory(
					&transcribe.CallAnalyticsCategory{CategoryName: "dup-cat", InputType: "POST_CALL"},
				)
				require.NoError(t, err)
			},
			body: map[string]any{
				"CategoryName": "dup-cat",
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "missing_name",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "CreateCallAnalyticsCategory", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}

func TestTranscribe_DeleteCallAnalyticsCategory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *transcribe.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateCallAnalyticsCategory(
					&transcribe.CallAnalyticsCategory{CategoryName: "cat-to-delete", InputType: "POST_CALL"},
				)
				require.NoError(t, err)
			},
			body:     map[string]any{"CategoryName": "cat-to-delete"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{"CategoryName": "missing-cat"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "DeleteCallAnalyticsCategory", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestTranscribe_CreateLanguageModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name:  "success",
			setup: func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body: map[string]any{
				"ModelName":     "my-model",
				"BaseModelName": "WideBand",
				"LanguageCode":  "en-US",
			},
			wantCode: http.StatusOK,
			wantKey:  "my-model",
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, b *transcribe.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLanguageModel(
					&transcribe.LanguageModel{ModelName: "dup-model", BaseModelName: "WideBand", LanguageCode: "en-US"},
				)
				require.NoError(t, err)
			},
			body: map[string]any{
				"ModelName":     "dup-model",
				"BaseModelName": "WideBand",
				"LanguageCode":  "en-US",
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "missing_name",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "CreateLanguageModel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}

func TestTranscribe_DeleteLanguageModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *transcribe.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLanguageModel(
					&transcribe.LanguageModel{
						ModelName:     "model-to-delete",
						BaseModelName: "WideBand",
						LanguageCode:  "en-US",
					},
				)
				require.NoError(t, err)
			},
			body:     map[string]any{"ModelName": "model-to-delete"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{"ModelName": "missing-model"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "DeleteLanguageModel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestTranscribe_CreateMedicalVocabulary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name:  "success",
			setup: func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body: map[string]any{
				"VocabularyName":    "my-med-vocab",
				"LanguageCode":      "en-US",
				"VocabularyFileURI": "s3://bucket/med-vocab.txt",
			},
			wantCode: http.StatusOK,
			wantKey:  "my-med-vocab",
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, b *transcribe.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateMedicalVocabulary("dup-med-vocab", "en-US", "s3://bucket/f.txt", nil)
				require.NoError(t, err)
			},
			body: map[string]any{
				"VocabularyName":    "dup-med-vocab",
				"LanguageCode":      "en-US",
				"VocabularyFileURI": "s3://bucket/f.txt",
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "missing_name",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "CreateMedicalVocabulary", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}

func TestTranscribe_CreateVocabulary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name:  "success",
			setup: func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body: map[string]any{
				"VocabularyName": "my-vocab",
				"LanguageCode":   "en-US",
			},
			wantCode: http.StatusOK,
			wantKey:  "my-vocab",
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, b *transcribe.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateVocabulary(
					&transcribe.Vocabulary{
						VocabularyName: "dup-vocab",
						LanguageCode:   "en-US",
						Phrases:        []string{"test"},
					},
				)
				require.NoError(t, err)
			},
			body: map[string]any{
				"VocabularyName": "dup-vocab",
				"LanguageCode":   "en-US",
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "missing_name",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "CreateVocabulary", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}

func TestTranscribe_CreateVocabularyFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name:  "success",
			setup: func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body: map[string]any{
				"VocabularyFilterName": "my-filter",
				"LanguageCode":         "en-US",
				"Words":                []string{"badword"},
			},
			wantCode: http.StatusOK,
			wantKey:  "my-filter",
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, b *transcribe.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateVocabularyFilter(
					&transcribe.VocabularyFilter{
						VocabularyFilterName: "dup-filter",
						LanguageCode:         "en-US",
						Words:                []string{"test"},
					},
				)
				require.NoError(t, err)
			},
			body: map[string]any{
				"VocabularyFilterName": "dup-filter",
				"LanguageCode":         "en-US",
				"Words":                []string{"word"},
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "missing_name",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "CreateVocabularyFilter", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}

func TestTranscribe_DeleteCallAnalyticsJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, b *transcribe.InMemoryBackend) {
				b.AddCallAnalyticsJobInternal(&transcribe.CallAnalyticsJob{
					CallAnalyticsJobName:   "ca-job-del",
					CallAnalyticsJobStatus: "COMPLETED",
				})
			},
			body:     map[string]any{"CallAnalyticsJobName": "ca-job-del"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{"CallAnalyticsJobName": "no-such-ca-job"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "DeleteCallAnalyticsJob", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestTranscribe_DeleteMedicalScribeJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, b *transcribe.InMemoryBackend) {
				b.AddMedicalScribeJobInternal(&transcribe.MedicalScribeJob{
					MedicalScribeJobName:   "ms-job-del",
					MedicalScribeJobStatus: "COMPLETED",
				})
			},
			body:     map[string]any{"MedicalScribeJobName": "ms-job-del"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{"MedicalScribeJobName": "no-such-ms-job"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "DeleteMedicalScribeJob", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestTranscribe_DeleteMedicalTranscriptionJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, b *transcribe.InMemoryBackend) {
				b.AddMedicalTranscriptionJobInternal(&transcribe.MedicalTranscriptionJob{
					MedicalTranscriptionJobName: "mt-job-del",
					TranscriptionJobStatus:      "COMPLETED",
				})
			},
			body:     map[string]any{"MedicalTranscriptionJobName": "mt-job-del"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{"MedicalTranscriptionJobName": "no-such-mt-job"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "DeleteMedicalTranscriptionJob", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ── Timestamp wire shape: epoch-seconds numbers, not RFC3339 strings ───────────
//
// The real aws-sdk-go-v2 transcribe deserializer parses CreationTime/StartTime/
// CompletionTime/CreateTime/LastModifiedTime via smithytime.ParseEpochSeconds,
// which requires a JSON number. A JSON string in that position makes the real
// SDK reject the response outright, so every timestamp field must round-trip
// as encoding/json.Number, never as a quoted string.

func TestTranscribe_TimestampFields_AreJSONNumbers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		body       map[string]any
		timeFields []string
	}{
		{
			name:   "GetTranscriptionJob",
			action: "StartTranscriptionJob",
			body: map[string]any{
				"TranscriptionJobName": "ts-epoch-job",
				"LanguageCode":         "en-US",
				"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
			},
			timeFields: []string{"CreationTime", "StartTime", "CompletionTime"},
		},
		{
			name:   "StartCallAnalyticsJob",
			action: "StartCallAnalyticsJob",
			body: map[string]any{
				"CallAnalyticsJobName": "ca-epoch-job",
				"LanguageCode":         "en-US",
				"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
			},
			timeFields: []string{"CreationTime", "StartTime", "CompletionTime"},
		},
		{
			name:   "StartMedicalScribeJob",
			action: "StartMedicalScribeJob",
			body: map[string]any{
				"MedicalScribeJobName": "ms-epoch-job",
				"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
				"DataAccessRoleArn":    "arn:aws:iam::123456789012:role/Scribe",
				"OutputBucketName":     "scribe-out",
			},
			timeFields: []string{"CreationTime", "StartTime", "CompletionTime"},
		},
		{
			name:   "StartMedicalTranscriptionJob",
			action: "StartMedicalTranscriptionJob",
			body: map[string]any{
				"MedicalTranscriptionJobName": "mt-epoch-job",
				"LanguageCode":                "en-US",
				"Specialty":                   "PRIMARYCARE",
				"Type":                        "DICTATION",
				"Media":                       map[string]any{"MediaFileUri": "s3://b/f"},
			},
			timeFields: []string{"CreationTime", "StartTime", "CompletionTime"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)

			rec := doTranscribeRequest(t, h, tt.action, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			// The response wraps the resource under a single top-level key; find it.
			var inner map[string]json.RawMessage
			for _, v := range raw {
				_ = json.Unmarshal(v, &inner)

				if inner != nil {
					break
				}
			}
			require.NotNil(t, inner, "expected a single wrapped resource object in %s", rec.Body.String())

			for _, field := range tt.timeFields {
				val, ok := inner[field]
				require.True(t, ok, "expected field %s in response %s", field, rec.Body.String())

				var num json.Number
				err := json.Unmarshal(val, &num)
				assert.NoError(t, err, "field %s must decode as a JSON number (epoch seconds), got %s", field, val)
			}
		})
	}
}

func TestTranscribe_DescribeLanguageModel_TimestampFieldsAreJSONNumbers(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	doTranscribeRequest(t, h, "CreateLanguageModel", map[string]any{
		"ModelName":     "lm-epoch-describe",
		"BaseModelName": "WideBand",
		"LanguageCode":  "en-US",
	})

	rec := doTranscribeRequest(t, h, "DescribeLanguageModel", map[string]any{
		"ModelName": "lm-epoch-describe",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		LanguageModel map[string]json.RawMessage `json:"LanguageModel"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	for _, field := range []string{"CreateTime", "LastModifiedTime"} {
		val, ok := out.LanguageModel[field]
		require.True(t, ok, "expected field %s in response %s", field, rec.Body.String())

		var num json.Number
		err := json.Unmarshal(val, &num)
		assert.NoError(t, err, "field %s must decode as a JSON number (epoch seconds), got %s", field, val)
	}
}
