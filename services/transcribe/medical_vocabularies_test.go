package transcribe_test

import (
	"encoding/json"
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

func TestHTTP_MedicalVocabularyEndpoints(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerWithBackend(t)

	// Create
	rec := doTranscribeRequest(t, h, "CreateMedicalVocabulary", map[string]any{
		"VocabularyName":    "med-vocab-test",
		"LanguageCode":      "en-US",
		"VocabularyFileUri": "s3://bucket/vocab.txt",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get
	getRec := doTranscribeRequest(t, h, "GetMedicalVocabulary", map[string]any{
		"VocabularyName": "med-vocab-test",
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "med-vocab-test")

	// Update
	upRec := doTranscribeRequest(t, h, "UpdateMedicalVocabulary", map[string]any{
		"VocabularyName":    "med-vocab-test",
		"LanguageCode":      "en-US",
		"VocabularyFileUri": "s3://bucket/vocab2.txt",
	})
	require.Equal(t, http.StatusOK, upRec.Code)

	// List
	listRec := doTranscribeRequest(t, h, "ListMedicalVocabularies", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "med-vocab-test")

	// Delete
	delRec := doTranscribeRequest(t, h, "DeleteMedicalVocabulary", map[string]any{
		"VocabularyName": "med-vocab-test",
	})
	require.Equal(t, http.StatusOK, delRec.Code)
}

// TestListMedicalVocabularies_NameContains verifies the NameContains filter
// (case-insensitive substring match), per the real ListMedicalVocabulariesInput field.
func TestListMedicalVocabularies_NameContains(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	b.AddMedicalVocabularyInternal(&transcribe.MedicalVocabulary{VocabularyName: "cardiology-terms"})
	b.AddMedicalVocabularyInternal(&transcribe.MedicalVocabulary{VocabularyName: "oncology-terms"})
	b.AddMedicalVocabularyInternal(&transcribe.MedicalVocabulary{VocabularyName: "general-list"})

	list, _ := b.ListMedicalVocabularies("", "terms", "")
	require.Len(t, list, 2)

	list, _ = b.ListMedicalVocabularies("", "GENERAL", "")
	require.Len(t, list, 1, "NameContains must be case-insensitive")
	assert.Equal(t, "general-list", list[0].VocabularyName)
}

// TestMedicalVocabulary_LastModifiedTimeAndFailureReason verifies Create/Update
// surface LastModifiedTime and Get surfaces FailureReason, matching the real
// CreateMedicalVocabularyOutput/UpdateMedicalVocabularyOutput/GetMedicalVocabularyOutput
// shapes.
func TestMedicalVocabulary_LastModifiedTimeAndFailureReason(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	createRec := doTranscribeRequest(t, h, "CreateMedicalVocabulary", map[string]any{
		"VocabularyName":    "wire-shape-medvocab",
		"LanguageCode":      "en-US",
		"VocabularyFileUri": "s3://bucket/vocab.txt",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	var createRaw map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createRaw))
	assert.Contains(t, createRaw, "LastModifiedTime")

	getRec := doTranscribeRequest(t, h, "GetMedicalVocabulary", map[string]any{
		"VocabularyName": "wire-shape-medvocab",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getRaw map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getRaw))
	_, hasFailureReason := getRaw["FailureReason"]
	assert.False(t, hasFailureReason, "FailureReason must be omitted (empty) on success")

	updateRec := doTranscribeRequest(t, h, "UpdateMedicalVocabulary", map[string]any{
		"VocabularyName":    "wire-shape-medvocab",
		"VocabularyFileUri": "s3://bucket/vocab2.txt",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateRaw map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateRaw))
	assert.Contains(t, updateRaw, "LastModifiedTime")
}
