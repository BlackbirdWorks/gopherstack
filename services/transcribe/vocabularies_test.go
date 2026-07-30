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

	t.Run("phrase_too_long_rejected", func(t *testing.T) {
		t.Parallel()

		longPhrase := string(make([]byte, 257))

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateVocabulary(&transcribe.Vocabulary{
			VocabularyName: "my-vocab-phrase-too-long",
			LanguageCode:   "en-US",
			Phrases:        []string{"ok", longPhrase},
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

func TestHTTP_UpdateVocabulary(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	_, _ = b.CreateVocabulary(&transcribe.Vocabulary{
		VocabularyName: "test-vocab",
		LanguageCode:   "en-US",
		Phrases:        []string{"old"},
	})

	rec := doTranscribeRequest(t, h, "UpdateVocabulary", map[string]any{
		"VocabularyName": "test-vocab",
		"LanguageCode":   "en-US",
		"Phrases":        []string{"new"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTP_DeleteVocabulary(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	_, _ = b.CreateVocabulary(&transcribe.Vocabulary{
		VocabularyName: "test-vocab-delete",
		LanguageCode:   "en-US",
	})

	rec := doTranscribeRequest(t, h, "DeleteVocabulary", map[string]any{
		"VocabularyName": "test-vocab-delete",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTP_ListVocabularies(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	_, _ = b.CreateVocabulary(&transcribe.Vocabulary{
		VocabularyName: "test-vocab-list",
		LanguageCode:   "en-US",
	})

	rec := doTranscribeRequest(t, h, "ListVocabularies", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "test-vocab-list")
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

func TestUpdateVocabulary(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	_, err := b.CreateVocabulary(&transcribe.Vocabulary{
		VocabularyName: "my-vocab",
		LanguageCode:   "en-US",
		Phrases:        []string{"initial"},
	})
	require.NoError(t, err)

	tests := []struct {
		input   *transcribe.Vocabulary
		name    string
		wantErr bool
	}{
		{
			name: "success_update",
			input: &transcribe.Vocabulary{
				VocabularyName: "my-vocab",
				LanguageCode:   "en-GB",
				Phrases:        []string{"updated"},
			},
			wantErr: false,
		},
		{
			name: "missing_name",
			input: &transcribe.Vocabulary{
				LanguageCode: "en-US",
			},
			wantErr: true,
		},
		{
			name: "not_found",
			input: &transcribe.Vocabulary{
				VocabularyName: "missing-vocab",
				LanguageCode:   "en-US",
			},
			wantErr: true,
		},
		{
			name: "too_many_phrases",
			input: &transcribe.Vocabulary{
				VocabularyName: "my-vocab",
				Phrases:        make([]string, 257),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, updateErr := b.UpdateVocabulary(tt.input)
			if tt.wantErr {
				require.Error(t, updateErr)
			} else {
				require.NoError(t, updateErr)
				v, getErr := b.GetVocabulary(tt.input.VocabularyName)
				require.NoError(t, getErr)
				if tt.input.LanguageCode != "" {
					assert.Equal(t, tt.input.LanguageCode, v.LanguageCode)
				}
				if len(tt.input.Phrases) > 0 {
					assert.Equal(t, tt.input.Phrases, v.Phrases)
				}
			}
		})
	}
}

func TestDeleteVocabulary(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	_, err := b.CreateVocabulary(&transcribe.Vocabulary{
		VocabularyName: "delete-vocab",
		LanguageCode:   "en-US",
	})
	require.NoError(t, err)

	err = b.DeleteVocabulary("")
	require.ErrorIs(t, err, transcribe.ErrValidation)

	err = b.DeleteVocabulary("missing-vocab")
	require.Error(t, err)

	err = b.DeleteVocabulary("delete-vocab")
	require.NoError(t, err)

	_, err = b.GetVocabulary("delete-vocab")
	require.Error(t, err)
}

func TestListVocabularies(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	b.AddVocabularyInternal(&transcribe.Vocabulary{
		VocabularyName:  "vocab-1",
		VocabularyState: "READY",
	})
	b.AddVocabularyInternal(&transcribe.Vocabulary{
		VocabularyName:  "vocab-2",
		VocabularyState: "PENDING",
	})
	b.AddVocabularyInternal(&transcribe.Vocabulary{
		VocabularyName:  "vocab-3",
		VocabularyState: "READY",
	})

	list, _ := b.ListVocabularies("", "", "", 0)
	require.Len(t, list, 3)

	list, _ = b.ListVocabularies("READY", "", "", 0)
	require.Len(t, list, 2)

	list, _ = b.ListVocabularies("PENDING", "", "", 0)
	require.Len(t, list, 1)
	assert.Equal(t, "vocab-2", list[0].VocabularyName)
}

// TestListVocabularies_NameContains verifies the NameContains filter (case-insensitive
// substring match), per the real ListVocabulariesInput field.
func TestListVocabularies_NameContains(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	b.AddVocabularyInternal(&transcribe.Vocabulary{VocabularyName: "medical-terms", VocabularyState: "READY"})
	b.AddVocabularyInternal(&transcribe.Vocabulary{VocabularyName: "legal-terms", VocabularyState: "READY"})
	b.AddVocabularyInternal(&transcribe.Vocabulary{VocabularyName: "sports-vocab", VocabularyState: "READY"})

	list, _ := b.ListVocabularies("", "terms", "", 0)
	require.Len(t, list, 2)

	list, _ = b.ListVocabularies("", "TERMS", "", 0)
	require.Len(t, list, 2, "NameContains must be case-insensitive")

	list, _ = b.ListVocabularies("", "sports", "", 0)
	require.Len(t, list, 1)
	assert.Equal(t, "sports-vocab", list[0].VocabularyName)
}

// TestCreateVocabulary_LastModifiedTimeAndFailureReasonEchoed verifies CreateVocabulary's
// response includes LastModifiedTime and (empty, on success) FailureReason, matching the
// real CreateVocabularyOutput shape which real gopherstack previously omitted.
func TestCreateVocabulary_LastModifiedTimeAndFailureReasonEchoed(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	rec := doTranscribeRequest(t, h, "CreateVocabulary", map[string]any{
		"VocabularyName": "wire-shape-vocab",
		"LanguageCode":   "en-US",
		"Phrases":        []string{"hello"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasLastModified := raw["LastModifiedTime"]
	assert.True(t, hasLastModified, "CreateVocabularyOutput must include LastModifiedTime")

	// FailureReason is expected to be absent (omitempty) on a successful synchronous create.
	_, hasFailureReason := raw["FailureReason"]
	assert.False(t, hasFailureReason, "FailureReason must be omitted (empty) on success")
}

// TestListVocabularies_EchoesStatusFilter verifies the top-level Status field on
// ListVocabulariesOutput echoes the StateEquals request filter, per the real
// ListVocabulariesOutput shape.
func TestListVocabularies_EchoesStatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	rec := doTranscribeRequest(t, h, "ListVocabularies", map[string]any{
		"StateEquals": "READY",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.Equal(t, "READY", raw["Status"])
}
