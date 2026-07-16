package transcribe_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

func TestCreateVocabulary_Phrases(t *testing.T) {
	t.Parallel()

	t.Run("phrases_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		v, err := b.CreateVocabulary(&transcribe.Vocabulary{
			VocabularyName: "my-vocab",
			LanguageCode:   "en-US",
			Phrases:        []string{"AWS Lambda", "DynamoDB", "gopherstack"},
		})
		require.NoError(t, err)
		assert.Len(t, v.Phrases, 3)
	})

	t.Run("vocabulary_file_uri_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		v, err := b.CreateVocabulary(&transcribe.Vocabulary{
			VocabularyName:    "my-vocab-file",
			LanguageCode:      "en-US",
			VocabularyFileURI: "s3://bucket/vocab.txt",
		})
		require.NoError(t, err)
		assert.Equal(t, "s3://bucket/vocab.txt", v.VocabularyFileURI)
	})

	t.Run("both_phrases_and_file_uri_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateVocabulary(&transcribe.Vocabulary{
			VocabularyName:    "my-vocab-both",
			LanguageCode:      "en-US",
			Phrases:           []string{"word"},
			VocabularyFileURI: "s3://bucket/vocab.txt",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("too_many_phrases_rejected", func(t *testing.T) {
		t.Parallel()

		phrases := make([]string, 257)
		for i := range phrases {
			phrases[i] = "phrase"
		}

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateVocabulary(&transcribe.Vocabulary{
			VocabularyName: "my-vocab-too-many",
			LanguageCode:   "en-US",
			Phrases:        phrases,
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

func TestHTTP_CreateVocabulary_WithPhrases(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerWithBackend(t)
	rec := doTranscribeRequest(t, h, "CreateVocabulary", map[string]any{
		"VocabularyName": "phrases-vocab",
		"LanguageCode":   "en-US",
		"Phrases":        []string{"AWS Lambda", "DynamoDB"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "phrases-vocab")
}

func TestCreateVocabulary(t *testing.T) {
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

// TestVocabularyState_Ready verifies vocabulary responds READY.
func TestVocabularyState_Ready(t *testing.T) {
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

// TestConcurrentCreateSafe validates race-free concurrent CreateVocabulary calls.
func TestConcurrentCreateSafe(t *testing.T) {
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

// TestGetVocabulary_DownloadUri verifies DownloadUri is returned by GetVocabulary.
func TestGetVocabulary_DownloadUri(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	createRec := doTranscribeRequest(t, h, "CreateVocabulary", map[string]any{
		"VocabularyName":    "dl-uri-vocab",
		"LanguageCode":      "en-US",
		"VocabularyFileUri": "s3://my-bucket/vocab.txt",
	})
	require.Equal(t, http.StatusOK, createRec.Code, "create vocab: %s", createRec.Body)

	getRec := doTranscribeRequest(t, h, "GetVocabulary", map[string]any{
		"VocabularyName": "dl-uri-vocab",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &raw))
	assert.NotEmpty(t, raw["DownloadUri"], "DownloadUri must be present in GetVocabulary response")
}

// TestGetVocabulary_LastModifiedTime verifies LastModifiedTime is returned by GetVocabulary.
func TestGetVocabulary_LastModifiedTime(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	createRec := doTranscribeRequest(t, h, "CreateVocabulary", map[string]any{
		"VocabularyName":    "lmt-vocab",
		"LanguageCode":      "en-US",
		"VocabularyFileUri": "s3://my-bucket/vocab.txt",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	getRec := doTranscribeRequest(t, h, "GetVocabulary", map[string]any{
		"VocabularyName": "lmt-vocab",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &raw))
	assert.NotNil(t, raw["LastModifiedTime"], "LastModifiedTime must be present in GetVocabulary response")
}
