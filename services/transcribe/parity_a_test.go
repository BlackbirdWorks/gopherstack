package transcribe_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_MedicalTranscriptionJobTranscriptURI verifies that
// GetMedicalTranscriptionJob and StartMedicalTranscriptionJob return a
// populated Transcript.TranscriptFileURI, matching real AWS behaviour.
// The previous implementation omitted the Transcript field entirely, causing
// clients that read the output location to get an empty string or nil pointer.
func TestParity_MedicalTranscriptionJobTranscriptURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		outputBucket  string
		outputKey     string
		wantURIPrefix string
		wantURISuffix string
	}{
		{
			name:          "custom_bucket_no_key_uses_job_name",
			outputBucket:  "my-transcripts",
			outputKey:     "",
			wantURIPrefix: "s3://my-transcripts/",
			wantURISuffix: ".json",
		},
		{
			name:          "custom_bucket_with_key",
			outputBucket:  "my-transcripts",
			outputKey:     "custom/path/output.json",
			wantURIPrefix: "s3://my-transcripts/custom/path/output.json",
			wantURISuffix: "",
		},
		{
			name:          "no_bucket_uses_synthetic",
			outputBucket:  "",
			outputKey:     "",
			wantURIPrefix: "s3://synthetic-transcripts/",
			wantURISuffix: ".json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestTranscribeHandler(t)
			jobName := "med-job-" + tt.name

			body := map[string]any{
				"MedicalTranscriptionJobName": jobName,
				"LanguageCode":                "en-US",
				"MediaFormat":                 "mp3",
				"Specialty":                   "PRIMARYCARE",
				"Type":                        "DICTATION",
				"Media":                       map[string]any{"MediaFileUri": "s3://input-bucket/audio.mp3"},
			}
			if tt.outputBucket != "" {
				body["OutputBucketName"] = tt.outputBucket
			}
			if tt.outputKey != "" {
				body["OutputKey"] = tt.outputKey
			}

			startRec := doTranscribeRequest(t, h, "StartMedicalTranscriptionJob", body)
			require.Equal(t, http.StatusOK, startRec.Code)

			getRec := doTranscribeRequest(t, h, "GetMedicalTranscriptionJob", map[string]any{
				"MedicalTranscriptionJobName": jobName,
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var out struct {
				MedicalTranscriptionJob struct {
					Transcript struct {
						TranscriptFileURI string `json:"TranscriptFileUri"`
					} `json:"Transcript"`
				} `json:"MedicalTranscriptionJob"`
			}
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

			uri := out.MedicalTranscriptionJob.Transcript.TranscriptFileURI
			assert.NotEmpty(t, uri, "Transcript.TranscriptFileURI must not be empty")
			assert.Contains(t, uri, tt.wantURIPrefix,
				"TranscriptFileUri should contain expected prefix")
			if tt.wantURISuffix != "" {
				assert.Contains(t, uri, tt.wantURISuffix,
					"TranscriptFileUri should contain expected suffix")
			}
		})
	}
}

// TestParity_StartMedicalTranscriptionJob_TranscriptURIInStartResponse verifies
// that StartMedicalTranscriptionJob also returns Transcript in the response body,
// not just GetMedicalTranscriptionJob.
func TestParity_StartMedicalTranscriptionJob_TranscriptURIInStartResponse(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	startRec := doTranscribeRequest(t, h, "StartMedicalTranscriptionJob", map[string]any{
		"MedicalTranscriptionJobName": "med-start-resp-job",
		"LanguageCode":                "en-US",
		"MediaFormat":                 "mp3",
		"Specialty":                   "PRIMARYCARE",
		"Type":                        "DICTATION",
		"OutputBucketName":            "output-bucket",
		"Media":                       map[string]any{"MediaFileUri": "s3://input/audio.mp3"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var out struct {
		MedicalTranscriptionJob struct {
			Transcript struct {
				TranscriptFileURI string `json:"TranscriptFileUri"`
			} `json:"Transcript"`
		} `json:"MedicalTranscriptionJob"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &out))

	uri := out.MedicalTranscriptionJob.Transcript.TranscriptFileURI
	assert.NotEmpty(t, uri, "StartMedicalTranscriptionJob response must include TranscriptFileUri")
	assert.True(t, strings.HasPrefix(uri, "s3://output-bucket/"), "URI should start with s3://output-bucket/")
}
