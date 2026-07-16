package transcribe_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

func TestCreateMedicalVocabulary(t *testing.T) {
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

// TestMedicalVocabularyState_Ready verifies medical vocabulary responds READY.
func TestMedicalVocabularyState_Ready(t *testing.T) {
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
