package transcribe_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// ── GetTranscriptionJob full field echo ───────────────────────────────────────

func TestRefinement2_GetTranscriptionJob_FullFieldEcho(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)

	// Start job with all fields.
	rec := doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
		"TranscriptionJobName": "full-echo-job",
		"LanguageCode":         "en-US",
		"MediaFormat":          "mp3",
		"MediaSampleRateHertz": 16000,
		"Media":                map[string]any{"MediaFileUri": "s3://bucket/audio.mp3"},
		"OutputBucketName":     "my-output",
		"OutputKey":            "prefix/job.json",
		"Settings": map[string]any{
			"ShowSpeakerLabels": true,
			"MaxSpeakerLabels":  3,
		},
		"ContentRedaction": map[string]any{"RedactionType": "PII"},
		"Subtitles":        map[string]any{"Formats": []string{"vtt"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get should echo all fields.
	getRec := doTranscribeRequest(t, h, "GetTranscriptionJob", map[string]any{
		"TranscriptionJobName": "full-echo-job",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	body := getRec.Body.String()
	assert.Contains(t, body, "full-echo-job")
	assert.Contains(t, body, "COMPLETED")
	assert.Contains(t, body, "en-US")
	assert.Contains(t, body, "mp3")
	assert.Contains(t, body, "16000")
	assert.Contains(t, body, "my-output")
	assert.Contains(t, body, "ContentRedaction")
	assert.Contains(t, body, "PII")
	assert.Contains(t, body, "ShowSpeakerLabels")
	assert.Contains(t, body, "CreationTime")
	assert.Contains(t, body, "CompletionTime")
}

// ── GetTranscriptionJob Transcript URI routing ────────────────────────────────

func TestRefinement2_GetTranscriptionJob_TranscriptURIRouting(t *testing.T) {
	t.Parallel()

	t.Run("with_output_bucket", func(t *testing.T) {
		t.Parallel()

		h, _ := newAccuracyHandler(t)
		doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
			"TranscriptionJobName": "bucket-job",
			"LanguageCode":         "en-US",
			"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
			"OutputBucketName":     "my-results",
			"OutputKey":            "my-job.json",
		})

		rec := doTranscribeRequest(t, h, "GetTranscriptionJob", map[string]any{
			"TranscriptionJobName": "bucket-job",
		})
		assert.Contains(t, rec.Body.String(), "s3://my-results/my-job.json")
	})

	t.Run("without_output_bucket_uses_synthetic", func(t *testing.T) {
		t.Parallel()

		h, _ := newAccuracyHandler(t)
		doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
			"TranscriptionJobName": "synthetic-job",
			"LanguageCode":         "en-US",
			"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
		})

		rec := doTranscribeRequest(t, h, "GetTranscriptionJob", map[string]any{
			"TranscriptionJobName": "synthetic-job",
		})
		assert.Contains(t, rec.Body.String(), "s3://synthetic-transcripts/synthetic-job.json")
	})
}

// ── ListTranscriptionJobs includes timestamps ──────────────────────────────────

func TestRefinement2_ListTranscriptionJobs_IncludesTimestamps(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
		"TranscriptionJobName": "ts-job",
		"LanguageCode":         "en-US",
		"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
	})

	rec := doTranscribeRequest(t, h, "ListTranscriptionJobs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreationTime")
}

// ── GetCallAnalyticsJob full field echo ────────────────────────────────────────

func TestRefinement2_GetCallAnalyticsJob_FullFieldEcho(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	doTranscribeRequest(t, h, "StartCallAnalyticsJob", map[string]any{
		"CallAnalyticsJobName": "ca-echo-job",
		"LanguageCode":         "en-US",
		"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
		"DataAccessRoleArn":    "arn:aws:iam::123456789012:role/CARole",
		"ChannelDefinitions": []map[string]any{
			{"ChannelId": 0, "ParticipantRole": "AGENT"},
			{"ChannelId": 1, "ParticipantRole": "CUSTOMER"},
		},
	})

	rec := doTranscribeRequest(t, h, "GetCallAnalyticsJob", map[string]any{
		"CallAnalyticsJobName": "ca-echo-job",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ca-echo-job")
	assert.Contains(t, body, "COMPLETED")
	assert.Contains(t, body, "en-US")
	assert.Contains(t, body, "DataAccessRoleArn")
	assert.Contains(t, body, "ChannelDefinitions")
	assert.Contains(t, body, "AGENT")
	assert.Contains(t, body, "CreationTime")
}

// ── GetMedicalScribeJob full field echo ────────────────────────────────────────

func TestRefinement2_GetMedicalScribeJob_FullFieldEcho(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
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

// ── GetMedicalTranscriptionJob full field echo ─────────────────────────────────

func TestRefinement2_GetMedicalTranscriptionJob_FullFieldEcho(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	doTranscribeRequest(t, h, "StartMedicalTranscriptionJob", map[string]any{
		"MedicalTranscriptionJobName": "med-echo-job",
		"LanguageCode":                "en-US",
		"Media":                       map[string]any{"MediaFileUri": "s3://b/f"},
		"Specialty":                   "PRIMARYCARE",
		"Type":                        "DICTATION",
		"OutputBucketName":            "med-output",
	})

	rec := doTranscribeRequest(t, h, "GetMedicalTranscriptionJob", map[string]any{
		"MedicalTranscriptionJobName": "med-echo-job",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "med-echo-job")
	assert.Contains(t, body, "COMPLETED")
	assert.Contains(t, body, "PRIMARYCARE")
	assert.Contains(t, body, "DICTATION")
	assert.Contains(t, body, "med-output")
	assert.Contains(t, body, "CreationTime")
}

// ── Tags operations ───────────────────────────────────────────────────────────

func TestRefinement2_TagResource_StoresAndReturns(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()

	err := b.TagResource("arn:aws:transcribe:us-east-1:123456789012:transcriptionjob/my-job",
		map[string]string{"env": "prod", "team": "ml"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource("arn:aws:transcribe:us-east-1:123456789012:transcriptionjob/my-job")
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "ml", tags["team"])
}

func TestRefinement2_UntagResource_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	arn := "arn:aws:transcribe:us-east-1:123456789012:transcriptionjob/my-job"

	require.NoError(t, b.TagResource(arn, map[string]string{"env": "prod", "team": "ml", "owner": "alice"}))
	require.NoError(t, b.UntagResource(arn, []string{"env", "owner"}))

	tags, err := b.ListTagsForResource(arn)
	require.NoError(t, err)
	assert.Len(t, tags, 1)
	assert.Equal(t, "ml", tags["team"])
}

func TestRefinement2_ListTagsForResource_UnknownARN_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	tags, err := b.ListTagsForResource("arn:aws:transcribe:us-east-1:123456789012:transcriptionjob/none")
	require.NoError(t, err)
	assert.Empty(t, tags)
}

func TestRefinement2_HTTP_TagResource(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	rec := doTranscribeRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:transcribe:us-east-1:123456789012:transcriptionjob/my-job",
		"Tags":        map[string]string{"env": "prod"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement2_HTTP_ListTagsForResource_ReturnsStoredTags(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	arn := "arn:aws:transcribe:us-east-1:123456789012:transcriptionjob/my-job"

	doTranscribeRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"env": "prod", "team": "ml"},
	})

	rec := doTranscribeRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "prod", resp.Tags["env"])
	assert.Equal(t, "ml", resp.Tags["team"])
}

func TestRefinement2_HTTP_UntagResource(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	arn := "arn:aws:transcribe:us-east-1:123456789012:transcriptionjob/my-job"

	doTranscribeRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"env": "prod", "team": "ml"},
	})

	doTranscribeRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []string{"env"},
	})

	rec := doTranscribeRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	var resp struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotContains(t, resp.Tags, "env")
	assert.Equal(t, "ml", resp.Tags["team"])
}

// ── LanguageModel InputDataConfig echo ────────────────────────────────────────

func TestRefinement2_DescribeLanguageModel_IncludesInputDataConfig(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	doTranscribeRequest(t, h, "CreateLanguageModel", map[string]any{
		"ModelName":     "my-model",
		"BaseModelName": "WideBand",
		"LanguageCode":  "en-US",
		"InputDataConfig": map[string]any{
			"S3Uri":             "s3://bucket/training/",
			"DataAccessRoleArn": "arn:aws:iam::123456789012:role/TranscribeRole",
		},
	})

	rec := doTranscribeRequest(t, h, "DescribeLanguageModel", map[string]any{
		"ModelName": "my-model",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "InputDataConfig")
	assert.Contains(t, body, "s3://bucket/training/")
}

// ── CallAnalyticsJob Tags in input ────────────────────────────────────────────

func TestRefinement2_StartCallAnalyticsJob_TagsInInput(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	rec := doTranscribeRequest(t, h, "StartCallAnalyticsJob", map[string]any{
		"CallAnalyticsJobName": "ca-tagged-job",
		"LanguageCode":         "en-US",
		"Media":                map[string]any{"MediaFileUri": "s3://b/f"},
		"Tags":                 map[string]string{"project": "sales"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Tags should be echoed in response.
	assert.Contains(t, rec.Body.String(), "ca-tagged-job")
}

// ── MedicalScribeJob Tags and ClinicalNoteGenerationSettings ─────────────────

func TestRefinement2_StartMedicalScribeJob_TagsAndClinicalNotes(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	rec := doTranscribeRequest(t, h, "StartMedicalScribeJob", map[string]any{
		"MedicalScribeJobName":           "scribe-clinical-job",
		"Media":                          map[string]any{"MediaFileUri": "s3://b/f"},
		"DataAccessRoleArn":              "arn:aws:iam::123456789012:role/ScribeRole",
		"OutputBucketName":               "scribe-output",
		"Tags":                           map[string]string{"department": "cardiology"},
		"ClinicalNoteGenerationSettings": map[string]any{"NoteTemplate": "SOAP"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "scribe-clinical-job")
}

// ── Backend Reset clears resourceTags ────────────────────────────────────────

func TestRefinement2_Reset_ClearsResourceTags(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	arn := "arn:aws:transcribe:us-east-1:123456789012:transcriptionjob/my-job"
	require.NoError(t, b.TagResource(arn, map[string]string{"k": "v"}))

	b.Reset()

	tags, err := b.ListTagsForResource(arn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}
