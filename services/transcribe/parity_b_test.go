package transcribe_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CallAnalyticsJobIncludesTranscriptURI verifies that completed
// CallAnalytics jobs include a Transcript.TranscriptFileUri in the response.
// Real AWS always populates this field for COMPLETED jobs; the emulator
// previously omitted it, causing callers to get an empty transcript URI.
func TestParity_CallAnalyticsJobIncludesTranscriptURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		media    map[string]any
		langCode string
	}{
		{
			name:     "s3_media_uri",
			langCode: "en-US",
			media:    map[string]any{"MediaFileUri": "s3://my-bucket/call.mp3"},
		},
		{
			name:     "wav_format",
			langCode: "es-US",
			media:    map[string]any{"MediaFileUri": "s3://calls-bucket/recording.wav"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestTranscribeHandler(t)
			jobName := "parity-ca-job-" + tt.name

			startRec := doTranscribeRequest(t, h, "StartCallAnalyticsJob", map[string]any{
				"CallAnalyticsJobName": jobName,
				"LanguageCode":         tt.langCode,
				"Media":                tt.media,
			})
			require.Equal(t, http.StatusOK, startRec.Code)

			getRec := doTranscribeRequest(t, h, "GetCallAnalyticsJob", map[string]any{
				"CallAnalyticsJobName": jobName,
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			job, ok := resp["CallAnalyticsJob"].(map[string]any)
			require.True(t, ok, "CallAnalyticsJob must be present")
			assert.Equal(t, "COMPLETED", job["CallAnalyticsJobStatus"])

			transcript, ok := job["Transcript"].(map[string]any)
			require.True(t, ok, "Transcript must be present in COMPLETED job response")

			uri, _ := transcript["TranscriptFileUri"].(string)
			assert.NotEmpty(t, uri, "TranscriptFileUri must be non-empty")
			assert.True(t, strings.HasPrefix(uri, "s3://"),
				"TranscriptFileUri must be an S3 URI, got: %s", uri)
			assert.Contains(t, uri, jobName,
				"TranscriptFileUri must include the job name")
		})
	}
}
