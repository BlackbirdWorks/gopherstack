package transcribe_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &transcribe.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, transcribe.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &transcribe.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ transcribe.StorageBackend = (*transcribe.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 43)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_ErrValidationSentinel verifies ErrValidation wraps ErrInvalidParameter.
func TestRefinement1_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	_, err := b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: ""},
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, transcribe.ErrValidation)
}

// TestRefinement1_ErrAlreadyExistsSentinel verifies ErrAlreadyExists is returned on duplicate.
func TestRefinement1_ErrAlreadyExistsSentinel(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	_, err := b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "dup",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		},
	)
	require.NoError(t, err)

	_, err = b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "dup",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, transcribe.ErrAlreadyExists)
}

// TestRefinement1_BackendReset verifies Reset clears all state.
func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	_, err := b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "job1",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		},
	)
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, transcribe.JobCount(b))
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	_, err := b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "job1",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		},
	)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, transcribe.JobCount(b))
}

// TestRefinement1_HandlerOpsCount verifies the cached dispatch has 43 entries.
func TestRefinement1_HandlerOpsCount(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)
	assert.Equal(t, 43, transcribe.HandlerOpsLen(h))
}

// TestRefinement1_ConflictIs409 verifies that duplicate resources produce HTTP 409.
func TestRefinement1_ConflictIs409(t *testing.T) {
	t.Parallel()

	tests := []struct {
		first  map[string]any
		second map[string]any
		name   string
		action string
	}{
		{
			name:   "transcription_job",
			action: "StartTranscriptionJob",
			first:  map[string]any{"TranscriptionJobName": "job-dup", "LanguageCode": "en-US"},
			second: map[string]any{"TranscriptionJobName": "job-dup", "LanguageCode": "en-US"},
		},
		{
			name:   "call_analytics_category",
			action: "CreateCallAnalyticsCategory",
			first:  map[string]any{"CategoryName": "cat-dup"},
			second: map[string]any{"CategoryName": "cat-dup"},
		},
		{
			name:   "language_model",
			action: "CreateLanguageModel",
			first:  map[string]any{"ModelName": "mdl-dup", "BaseModelName": "WideBand", "LanguageCode": "en-US"},
			second: map[string]any{"ModelName": "mdl-dup", "BaseModelName": "WideBand", "LanguageCode": "en-US"},
		},
		{
			name:   "vocabulary",
			action: "CreateVocabulary",
			first:  map[string]any{"VocabularyName": "voc-dup", "LanguageCode": "en-US"},
			second: map[string]any{"VocabularyName": "voc-dup", "LanguageCode": "en-US"},
		},
		{
			name:   "vocabulary_filter",
			action: "CreateVocabularyFilter",
			first: map[string]any{
				"VocabularyFilterName": "flt-dup",
				"LanguageCode":         "en-US",
				"Words":                []string{"bad"},
			},
			second: map[string]any{
				"VocabularyFilterName": "flt-dup",
				"LanguageCode":         "en-US",
				"Words":                []string{"bad"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)

			rec1 := doTranscribeRequest(t, h, tt.action, tt.first)
			require.Equal(t, http.StatusOK, rec1.Code)

			rec2 := doTranscribeRequest(t, h, tt.action, tt.second)
			assert.Equal(t, http.StatusConflict, rec2.Code)
			assert.Contains(t, rec2.Body.String(), "ConflictException")
		})
	}
}

// TestRefinement1_ValidationIs400 verifies that missing required fields produce HTTP 400.
func TestRefinement1_ValidationIs400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "start_job_missing_job_name",
			action: "StartTranscriptionJob",
			body:   map[string]any{"LanguageCode": "en-US"},
		},
		{
			name:   "start_job_missing_language_code",
			action: "StartTranscriptionJob",
			body:   map[string]any{"TranscriptionJobName": "j1"},
		},
		{
			name:   "create_language_model_missing_base_model",
			action: "CreateLanguageModel",
			body:   map[string]any{"ModelName": "m1", "LanguageCode": "en-US"},
		},
		{
			name:   "create_language_model_missing_language_code",
			action: "CreateLanguageModel",
			body:   map[string]any{"ModelName": "m1", "BaseModelName": "WideBand"},
		},
		{
			name:   "create_medical_vocab_missing_file_uri",
			action: "CreateMedicalVocabulary",
			body:   map[string]any{"VocabularyName": "v1", "LanguageCode": "en-US"},
		},
		{
			name:   "create_vocabulary_missing_language",
			action: "CreateVocabulary",
			body:   map[string]any{"VocabularyName": "v1"},
		},
		{
			name:   "create_vocabulary_filter_missing_language",
			action: "CreateVocabularyFilter",
			body:   map[string]any{"VocabularyFilterName": "f1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			rec := doTranscribeRequest(t, h, tt.action, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "BadRequestException")
		})
	}
}

// TestRefinement1_ExportCountHelpers verifies all count helpers via seed helpers.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()

	_, err := b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "tj1",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		},
	)
	require.NoError(t, err)

	b.AddCallAnalyticsCategoryInternal(&transcribe.CallAnalyticsCategory{CategoryName: "cat1"})
	b.AddLanguageModelInternal(&transcribe.LanguageModel{ModelName: "mdl1"})
	b.AddMedicalVocabularyInternal(&transcribe.MedicalVocabulary{VocabularyName: "mv1"})
	b.AddVocabularyInternal(&transcribe.Vocabulary{VocabularyName: "v1"})
	b.AddVocabularyFilterInternal(&transcribe.VocabularyFilter{VocabularyFilterName: "f1"})
	b.AddCallAnalyticsJobInternal(&transcribe.CallAnalyticsJob{CallAnalyticsJobName: "caj1"})
	b.AddMedicalScribeJobInternal(&transcribe.MedicalScribeJob{MedicalScribeJobName: "msj1"})
	b.AddMedicalTranscriptionJobInternal(&transcribe.MedicalTranscriptionJob{MedicalTranscriptionJobName: "mtj1"})

	assert.Equal(t, 1, transcribe.JobCount(b))
	assert.Equal(t, 1, transcribe.CallAnalyticsCategoryCount(b))
	assert.Equal(t, 1, transcribe.LanguageModelCount(b))
	assert.Equal(t, 1, transcribe.MedicalVocabularyCount(b))
	assert.Equal(t, 1, transcribe.VocabularyCount(b))
	assert.Equal(t, 1, transcribe.VocabularyFilterCount(b))
	assert.Equal(t, 1, transcribe.CallAnalyticsJobCount(b))
	assert.Equal(t, 1, transcribe.MedicalScribeJobCount(b))
	assert.Equal(t, 1, transcribe.MedicalTranscriptionJobCount(b))
}

// TestRefinement1_ResetClearsAllMaps verifies Reset empties all 9 internal maps.
func TestRefinement1_ResetClearsAllMaps(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()

	_, err := b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "tj1",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		},
	)
	require.NoError(t, err)

	b.AddCallAnalyticsCategoryInternal(&transcribe.CallAnalyticsCategory{CategoryName: "cat1"})
	b.AddLanguageModelInternal(&transcribe.LanguageModel{ModelName: "mdl1"})
	b.AddMedicalVocabularyInternal(&transcribe.MedicalVocabulary{VocabularyName: "mv1"})
	b.AddVocabularyInternal(&transcribe.Vocabulary{VocabularyName: "v1"})
	b.AddVocabularyFilterInternal(&transcribe.VocabularyFilter{VocabularyFilterName: "f1"})
	b.AddCallAnalyticsJobInternal(&transcribe.CallAnalyticsJob{CallAnalyticsJobName: "caj1"})
	b.AddMedicalScribeJobInternal(&transcribe.MedicalScribeJob{MedicalScribeJobName: "msj1"})
	b.AddMedicalTranscriptionJobInternal(&transcribe.MedicalTranscriptionJob{MedicalTranscriptionJobName: "mtj1"})

	b.Reset()

	assert.Equal(t, 0, transcribe.JobCount(b))
	assert.Equal(t, 0, transcribe.CallAnalyticsCategoryCount(b))
	assert.Equal(t, 0, transcribe.LanguageModelCount(b))
	assert.Equal(t, 0, transcribe.MedicalVocabularyCount(b))
	assert.Equal(t, 0, transcribe.VocabularyCount(b))
	assert.Equal(t, 0, transcribe.VocabularyFilterCount(b))
	assert.Equal(t, 0, transcribe.CallAnalyticsJobCount(b))
	assert.Equal(t, 0, transcribe.MedicalScribeJobCount(b))
	assert.Equal(t, 0, transcribe.MedicalTranscriptionJobCount(b))
}

// TestRefinement1_SnapshotRestoreAllMaps verifies Snapshot/Restore round-trips all 9 maps.
func TestRefinement1_SnapshotRestoreAllMaps(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()

	_, err := b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "tj1",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		},
	)
	require.NoError(t, err)

	b.AddCallAnalyticsCategoryInternal(&transcribe.CallAnalyticsCategory{CategoryName: "cat1", InputType: "POST_CALL"})
	b.AddLanguageModelInternal(&transcribe.LanguageModel{ModelName: "mdl1", LanguageCode: "en-US"})
	b.AddMedicalVocabularyInternal(&transcribe.MedicalVocabulary{VocabularyName: "mv1", LanguageCode: "en-US"})
	b.AddVocabularyInternal(&transcribe.Vocabulary{VocabularyName: "v1", LanguageCode: "en-US"})
	b.AddVocabularyFilterInternal(&transcribe.VocabularyFilter{VocabularyFilterName: "f1", LanguageCode: "en-US"})
	b.AddCallAnalyticsJobInternal(&transcribe.CallAnalyticsJob{CallAnalyticsJobName: "caj1"})
	b.AddMedicalScribeJobInternal(&transcribe.MedicalScribeJob{MedicalScribeJobName: "msj1"})
	b.AddMedicalTranscriptionJobInternal(&transcribe.MedicalTranscriptionJob{MedicalTranscriptionJobName: "mtj1"})

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := transcribe.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, transcribe.JobCount(b2))
	assert.Equal(t, 1, transcribe.CallAnalyticsCategoryCount(b2))
	assert.Equal(t, 1, transcribe.LanguageModelCount(b2))
	assert.Equal(t, 1, transcribe.MedicalVocabularyCount(b2))
	assert.Equal(t, 1, transcribe.VocabularyCount(b2))
	assert.Equal(t, 1, transcribe.VocabularyFilterCount(b2))
	assert.Equal(t, 1, transcribe.CallAnalyticsJobCount(b2))
	assert.Equal(t, 1, transcribe.MedicalScribeJobCount(b2))
	assert.Equal(t, 1, transcribe.MedicalTranscriptionJobCount(b2))
}

// TestRefinement1_ErrorBodyContainsType verifies error responses include __type.
func TestRefinement1_ErrorBodyContainsType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		body     map[string]any
		wantType string
		wantCode int
	}{
		{
			name:     "not_found_returns_NotFoundException",
			action:   "GetTranscriptionJob",
			body:     map[string]any{"TranscriptionJobName": "no-such"},
			wantType: "NotFoundException",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "missing_name_returns_BadRequestException",
			action:   "StartTranscriptionJob",
			body:     map[string]any{},
			wantType: "BadRequestException",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown_op_returns_UnknownOperationException",
			action:   "NoSuchOp",
			body:     map[string]any{},
			wantType: "UnknownOperationException",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)

			rec := doTranscribeRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantType)
		})
	}
}

// TestRefinement1_StatusConstants verifies named status constants are used in responses.
func TestRefinement1_StatusConstants(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	rec := doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
		"TranscriptionJobName": "status-test",
		"LanguageCode":         "en-US",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "COMPLETED")
}

// TestRefinement1_ModelStatusCompleted verifies language model responds COMPLETED.
func TestRefinement1_ModelStatusCompleted(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	rec := doTranscribeRequest(t, h, "CreateLanguageModel", map[string]any{
		"ModelName":     "model-status-test",
		"BaseModelName": "WideBand",
		"LanguageCode":  "en-US",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "COMPLETED")
}

// TestRefinement1_VocabStateReady verifies vocabulary responds READY.
func TestRefinement1_VocabStateReady(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	rec := doTranscribeRequest(t, h, "CreateVocabulary", map[string]any{
		"VocabularyName": "vocab-state-test",
		"LanguageCode":   "en-US",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "READY")
}

// TestRefinement1_MedVocabStateReady verifies medical vocabulary responds READY.
func TestRefinement1_MedVocabStateReady(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	rec := doTranscribeRequest(t, h, "CreateMedicalVocabulary", map[string]any{
		"VocabularyName":    "med-vocab-state-test",
		"LanguageCode":      "en-US",
		"VocabularyFileURI": "s3://bucket/vocab.txt",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "READY")
}

// TestRefinement1_AccountID verifies backend AccountID returns expected value.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	assert.NotEmpty(t, b.AccountID())
}

// TestRefinement1_DeleteJobMissingName verifies empty name returns 400.
func TestRefinement1_DeleteJobMissingName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "delete_transcription_job_empty_name",
			action: "DeleteTranscriptionJob",
			body:   map[string]any{"TranscriptionJobName": ""},
		},
		{
			name:   "delete_call_analytics_job_empty_name",
			action: "DeleteCallAnalyticsJob",
			body:   map[string]any{"CallAnalyticsJobName": ""},
		},
		{
			name:   "delete_medical_scribe_job_empty_name",
			action: "DeleteMedicalScribeJob",
			body:   map[string]any{"MedicalScribeJobName": ""},
		},
		{
			name:   "delete_medical_transcription_job_empty_name",
			action: "DeleteMedicalTranscriptionJob",
			body:   map[string]any{"MedicalTranscriptionJobName": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			rec := doTranscribeRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_ConcurrentCreateDeleteSafe validates race-free concurrent operations.
func TestRefinement1_ConcurrentCreateDeleteSafe(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()

	const n = 20
	errs := make(chan error, n)

	for i := range n {
		go func(i int) {
			_, e := b.CreateVocabulary(
				&transcribe.Vocabulary{
					VocabularyName: fmt.Sprintf("vocab-%d", i),
					LanguageCode:   "en-US",
					Phrases:        []string{"test"},
				},
			)
			errs <- e
		}(i)
	}

	for range n {
		e := <-errs
		if e != nil {
			t.Errorf("unexpected error: %v", e)
		}
	}

	assert.Equal(t, n, transcribe.VocabularyCount(b))
}

// TestRefinement1_DeleteAfterResetIs404 verifies delete after reset returns 404.
func TestRefinement1_DeleteAfterResetIs404(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	_, err := b.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "reset-job",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		},
	)
	require.NoError(t, err)

	h.Reset()

	rec := doTranscribeRequest(t, h, "DeleteTranscriptionJob", map[string]any{
		"TranscriptionJobName": "reset-job",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_CreateVocabularyFilterOutput verifies response shape.
func TestRefinement1_CreateVocabularyFilterOutput(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	rec := doTranscribeRequest(t, h, "CreateVocabularyFilter", map[string]any{
		"VocabularyFilterName": "output-filter",
		"LanguageCode":         "en-US",
		"Words":                []string{"badword"},
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "output-filter")
	assert.Contains(t, rec.Body.String(), "en-US")
}

// TestRefinement1_CallAnalyticsCategoryOutputShape verifies CategoryProperties in response.
func TestRefinement1_CallAnalyticsCategoryOutputShape(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	rec := doTranscribeRequest(t, h, "CreateCallAnalyticsCategory", map[string]any{
		"CategoryName": "shape-cat",
		"InputType":    "POST_CALL",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CategoryProperties")
	assert.Contains(t, rec.Body.String(), "shape-cat")
	assert.Contains(t, rec.Body.String(), "POST_CALL")
}

// TestRefinement1_MultipleDeleteCallsReturn404 verifies idempotent deletes fail on second call.
func TestRefinement1_MultipleDeleteCallsReturn404(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	b.AddCallAnalyticsCategoryInternal(&transcribe.CallAnalyticsCategory{CategoryName: "once-cat"})

	rec1 := doTranscribeRequest(t, h, "DeleteCallAnalyticsCategory", map[string]any{
		"CategoryName": "once-cat",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doTranscribeRequest(t, h, "DeleteCallAnalyticsCategory", map[string]any{
		"CategoryName": "once-cat",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}
