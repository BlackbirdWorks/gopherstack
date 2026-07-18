package transcribe_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

func TestCreateVocabularyFilter_Words(t *testing.T) {
	t.Parallel()

	t.Run("words_required", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
			VocabularyFilterName: "my-filter-no-words",
			LanguageCode:         "en-US",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("words_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		vf, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
			VocabularyFilterName: "my-filter",
			LanguageCode:         "en-US",
			Words:                []string{"profanity", "slur"},
		})
		require.NoError(t, err)
		assert.Len(t, vf.Words, 2)
	})
}

func TestCreateVocabularyFilter(t *testing.T) {
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

// TestCreateVocabularyFilter_OutputShape verifies response shape.
func TestCreateVocabularyFilter_OutputShape(t *testing.T) {
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
