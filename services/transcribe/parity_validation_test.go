package transcribe_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// TestTranscribe_BadParams_NotInternalFailure asserts bad params return
// BadRequestException, not InternalFailureException.
func TestTranscribe_BadParams_NotInternalFailure(t *testing.T) {
	t.Parallel()

	const wantType = "BadRequestException"

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name:     "StartTranscriptionJob_missing_name",
			action:   "StartTranscriptionJob",
			body:     map[string]any{"LanguageCode": "en-US"},
			wantCode: http.StatusBadRequest,
			wantType: wantType,
		},
		{
			name:   "StartTranscriptionJob_invalid_language_code",
			action: "StartTranscriptionJob",
			body: map[string]any{
				"TranscriptionJobName": "bad-lang-job",
				"LanguageCode":         "xx-INVALID",
			},
			wantCode: http.StatusBadRequest,
			wantType: wantType,
		},
		{
			name:   "StartTranscriptionJob_invalid_media_format",
			action: "StartTranscriptionJob",
			body: map[string]any{
				"TranscriptionJobName": "bad-format-job",
				"LanguageCode":         "en-US",
				"MediaFormat":          "bmp",
			},
			wantCode: http.StatusBadRequest,
			wantType: wantType,
		},
		{
			name:   "StartTranscriptionJob_invalid_sample_rate",
			action: "StartTranscriptionJob",
			body: map[string]any{
				"TranscriptionJobName": "bad-rate-job",
				"LanguageCode":         "en-US",
				"MediaSampleRateHertz": 100,
			},
			wantCode: http.StatusBadRequest,
			wantType: wantType,
		},
		{
			name:     "StartCallAnalyticsJob_missing_name",
			action:   "StartCallAnalyticsJob",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: wantType,
		},
		{
			name:     "DeleteTranscriptionJob_missing_name",
			action:   "DeleteTranscriptionJob",
			body:     map[string]any{"TranscriptionJobName": ""},
			wantCode: http.StatusBadRequest,
			wantType: wantType,
		},
		{
			name:   "CreateVocabulary_missing_name",
			action: "CreateVocabulary",
			body: map[string]any{
				"LanguageCode":      "en-US",
				"VocabularyFileUri": "s3://bucket/vocab.txt",
			},
			wantCode: http.StatusBadRequest,
			wantType: wantType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
			rec := doTranscribeRequest(t, h, tt.action, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.Equal(t, tt.wantType, resp["__type"], "expected %s, not InternalFailureException", tt.wantType)
		})
	}
}
