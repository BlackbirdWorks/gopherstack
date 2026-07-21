package transcribe_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

func TestStartMedicalScribeJob_RequiredFields(t *testing.T) {
	t.Parallel()

	t.Run("valid_job_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		job, err := b.StartMedicalScribeJob(&transcribe.MedicalScribeJob{
			MedicalScribeJobName: "scribe-ok",
			Media:                transcribe.Media{MediaFileURI: "s3://b/f.mp3"},
			DataAccessRoleArn:    "arn:aws:iam::123456789012:role/TranscribeRole",
			OutputBucketName:     "my-output-bucket",
		})
		require.NoError(t, err)
		assert.Equal(t, "COMPLETED", job.MedicalScribeJobStatus)
	})

	t.Run("missing_data_access_role_arn_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartMedicalScribeJob(&transcribe.MedicalScribeJob{
			MedicalScribeJobName: "scribe-no-role",
			Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
			OutputBucketName:     "my-output-bucket",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("missing_output_bucket_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartMedicalScribeJob(&transcribe.MedicalScribeJob{
			MedicalScribeJobName: "scribe-no-bucket",
			Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
			DataAccessRoleArn:    "arn:aws:iam::123456789012:role/TranscribeRole",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

func TestDeleteMedicalScribeJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, b *transcribe.InMemoryBackend) {
				b.AddMedicalScribeJobInternal(&transcribe.MedicalScribeJob{
					MedicalScribeJobName:   "ms-job-del",
					MedicalScribeJobStatus: "COMPLETED",
				})
			},
			body:     map[string]any{"MedicalScribeJobName": "ms-job-del"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{"MedicalScribeJobName": "no-such-ms-job"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "DeleteMedicalScribeJob", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ── GetMedicalScribeJob full field echo ────────────────────────────────────────

func TestGetMedicalScribeJob_FullFieldEcho(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerWithBackend(t)
	doTranscribeRequest(t, h, "StartMedicalScribeJob", map[string]any{
		"MedicalScribeJobName": "scribe-echo-job",
		"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
		"DataAccessRoleArn":    "arn:aws:iam::123456789012:role/ScribeRole",
		"OutputBucketName":     "scribe-output",
	})

	rec := doTranscribeRequest(t, h, "GetMedicalScribeJob", map[string]any{
		"MedicalScribeJobName": "scribe-echo-job",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "scribe-echo-job")
	assert.Contains(t, body, "COMPLETED")
	assert.Contains(t, body, "DataAccessRoleArn")
	assert.Contains(t, body, "scribe-output")
	assert.Contains(t, body, "CreationTime")
}

// ── MedicalScribeJob Tags and ClinicalNoteGenerationSettings ─────────────────

func TestStartMedicalScribeJob_TagsAndClinicalNotes(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerWithBackend(t)
	rec := doTranscribeRequest(t, h, "StartMedicalScribeJob", map[string]any{
		"MedicalScribeJobName":           "scribe-clinical-job",
		"Media":                          map[string]any{"MediaFileUri": "s3://b/f"},
		"DataAccessRoleArn":              "arn:aws:iam::123456789012:role/ScribeRole",
		"OutputBucketName":               "scribe-output",
		"Tags":                           []map[string]string{{"Key": "department", "Value": "cardiology"}},
		"ClinicalNoteGenerationSettings": map[string]any{"NoteTemplate": "SOAP"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "scribe-clinical-job")
}

func TestHTTP_ListMedicalScribeJobs(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerWithBackend(t)
	rec := doTranscribeRequest(t, h, "StartMedicalScribeJob", map[string]any{
		"MedicalScribeJobName": "scribe-list-job",
		"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
		"DataAccessRoleArn":    "arn:aws:iam::123456789012:role/ScribeRole",
		"OutputBucketName":     "scribe-output",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := doTranscribeRequest(t, h, "ListMedicalScribeJobs", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "scribe-list-job")
}
