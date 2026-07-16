package transcribe_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// TestProviderInit verifies Provider.Init's nil-context guard and its normal
// success path across the AppContext shapes callers construct it with.
func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &transcribe.Provider{}
	assert.Equal(t, "Transcribe", p.Name())

	t.Run("nil_context_returns_error", func(t *testing.T) {
		t.Parallel()

		_, err := p.Init(nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, transcribe.ErrNilAppContext)
	})

	t.Run("logger_only_context_succeeds", func(t *testing.T) {
		t.Parallel()

		svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
		require.NoError(t, err)
		assert.NotNil(t, svc)
	})

	t.Run("janitor_ctx_context_succeeds", func(t *testing.T) {
		t.Parallel()

		reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
		require.NoError(t, err)
		assert.NotNil(t, reg)
	})
}

func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ transcribe.StorageBackend = (*transcribe.InMemoryBackend)(nil)
}

func TestAccountID(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	assert.NotEmpty(t, b.AccountID())
}

// seedOneOfEachResource populates one entry in every store.Table the backend
// registers (see store_setup.go), so tests can assert Reset/count behavior
// across the full resource surface without hand-rolling nine setup calls each.
func seedOneOfEachResource(t *testing.T, b *transcribe.InMemoryBackend) {
	t.Helper()

	_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
		JobName:      "tj1",
		LanguageCode: "en-US",
		Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
	})
	require.NoError(t, err)

	b.AddCallAnalyticsCategoryInternal(&transcribe.CallAnalyticsCategory{CategoryName: "cat1"})
	b.AddLanguageModelInternal(&transcribe.LanguageModel{ModelName: "mdl1"})
	b.AddMedicalVocabularyInternal(&transcribe.MedicalVocabulary{VocabularyName: "mv1"})
	b.AddVocabularyInternal(&transcribe.Vocabulary{VocabularyName: "v1"})
	b.AddVocabularyFilterInternal(&transcribe.VocabularyFilter{VocabularyFilterName: "f1"})
	b.AddCallAnalyticsJobInternal(&transcribe.CallAnalyticsJob{CallAnalyticsJobName: "caj1"})
	b.AddMedicalScribeJobInternal(&transcribe.MedicalScribeJob{MedicalScribeJobName: "msj1"})
	b.AddMedicalTranscriptionJobInternal(&transcribe.MedicalTranscriptionJob{MedicalTranscriptionJobName: "mtj1"})
}

func assertAllResourceCounts(t *testing.T, b *transcribe.InMemoryBackend, want int) {
	t.Helper()

	assert.Equal(t, want, transcribe.JobCount(b))
	assert.Equal(t, want, transcribe.CallAnalyticsCategoryCount(b))
	assert.Equal(t, want, transcribe.LanguageModelCount(b))
	assert.Equal(t, want, transcribe.MedicalVocabularyCount(b))
	assert.Equal(t, want, transcribe.VocabularyCount(b))
	assert.Equal(t, want, transcribe.VocabularyFilterCount(b))
	assert.Equal(t, want, transcribe.CallAnalyticsJobCount(b))
	assert.Equal(t, want, transcribe.MedicalScribeJobCount(b))
	assert.Equal(t, want, transcribe.MedicalTranscriptionJobCount(b))
}

// TestExportCountHelpers verifies every export_test.go count helper reports 1
// after seeding one entry per resource table.
func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	seedOneOfEachResource(t, b)
	assertAllResourceCounts(t, b, 1)
}

// TestReset_ClearsAllState verifies Reset empties every resource table -- both
// via the backend directly and via Handler.Reset delegating to it.
func TestReset_ClearsAllState(t *testing.T) {
	t.Parallel()

	t.Run("backend_reset_clears_all_tables", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		seedOneOfEachResource(t, b)

		b.Reset()

		assertAllResourceCounts(t, b, 0)
	})

	t.Run("handler_reset_delegates_to_backend", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		h := transcribe.NewHandler(b)

		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job1",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		})
		require.NoError(t, err)

		h.Reset()

		assert.Equal(t, 0, transcribe.JobCount(b))
	})
}
