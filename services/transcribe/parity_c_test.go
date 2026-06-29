package transcribe_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_MediaFileUri_JSONKey verifies the JSON key is "MediaFileUri" not "MediaFileURI".
func TestParity_MediaFileUri_JSONKey(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	rec := doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
		"TranscriptionJobName": "media-uri-job",
		"LanguageCode":         "en-US",
		"Media":                map[string]any{"MediaFileUri": "s3://my-bucket/audio.mp3"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "start job: %s", rec.Body)

	descRec := doTranscribeRequest(t, h, "GetTranscriptionJob", map[string]any{
		"TranscriptionJobName": "media-uri-job",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))
	job, _ := raw["TranscriptionJob"].(map[string]any)
	require.NotNil(t, job, "TranscriptionJob must be present")

	media, _ := job["Media"].(map[string]any)
	require.NotNil(t, media, "Media must be present")

	_, hasURI := media["MediaFileUri"]
	_, hasWrong := media["MediaFileURI"]

	assert.True(t, hasURI, "Media.MediaFileUri must use lowercase 'ri' suffix")
	assert.False(t, hasWrong, "Media.MediaFileURI must NOT appear (wrong casing)")
}

// TestParity_ChannelId_JSONKey verifies the JSON key is "ChannelId" not "ChannelID".
func TestParity_ChannelId_JSONKey(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	rec := doTranscribeRequest(t, h, "StartCallAnalyticsJob", map[string]any{
		"CallAnalyticsJobName": "chan-id-job",
		"DataAccessRoleArn":    "arn:aws:iam::123456789012:role/transcribe",
		"Media":                map[string]any{"MediaFileUri": "s3://my-bucket/call.mp3"},
		"ChannelDefinitions": []map[string]any{
			{"ChannelId": 0, "ParticipantRole": "AGENT"},
			{"ChannelId": 1, "ParticipantRole": "CUSTOMER"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "start job: %s", rec.Body)

	descRec := doTranscribeRequest(t, h, "GetCallAnalyticsJob", map[string]any{
		"CallAnalyticsJobName": "chan-id-job",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))
	job, _ := raw["CallAnalyticsJob"].(map[string]any)
	require.NotNil(t, job, "CallAnalyticsJob must be present")

	defs, _ := job["ChannelDefinitions"].([]any)
	require.NotEmpty(t, defs, "ChannelDefinitions must be present")

	def0, _ := defs[0].(map[string]any)
	_, hasID := def0["ChannelId"]
	_, hasWrong := def0["ChannelID"]

	assert.True(t, hasID, "ChannelDefinition must use ChannelId (not ChannelID)")
	assert.False(t, hasWrong, "ChannelID must NOT appear (wrong casing)")
}

// TestParity_TranscriptionJob_MediaInResponse verifies Media is present in GetTranscriptionJob response.
func TestParity_TranscriptionJob_MediaInResponse(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	rec := doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
		"TranscriptionJobName": "media-field-job",
		"LanguageCode":         "en-US",
		"Media":                map[string]any{"MediaFileUri": "s3://my-bucket/audio.mp3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTranscribeRequest(t, h, "GetTranscriptionJob", map[string]any{
		"TranscriptionJobName": "media-field-job",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))
	job, _ := raw["TranscriptionJob"].(map[string]any)
	require.NotNil(t, job)

	media, hasMedia := job["Media"]
	assert.True(t, hasMedia, "Media field must be present in GetTranscriptionJob response")
	mediaMap, _ := media.(map[string]any)
	assert.NotEmpty(t, mediaMap["MediaFileUri"], "MediaFileUri must be non-empty")
}

// TestParity_GetVocabulary_DownloadUri verifies DownloadUri is returned by GetVocabulary.
func TestParity_GetVocabulary_DownloadUri(t *testing.T) {
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

// TestParity_GetVocabulary_LastModifiedTime verifies LastModifiedTime is returned by GetVocabulary.
func TestParity_GetVocabulary_LastModifiedTime(t *testing.T) {
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

// TestParity_LanguageModel_TimeFields verifies CreateTime and LastModifiedTime are in DescribeLanguageModel response.
func TestParity_LanguageModel_TimeFields(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	createRec := doTranscribeRequest(t, h, "CreateLanguageModel", map[string]any{
		"ModelName":     "time-field-model",
		"BaseModelName": "NarrowBand",
		"LanguageCode":  "en-US",
		"InputDataConfig": map[string]any{
			"S3Uri":             "s3://my-bucket/data/",
			"DataAccessRoleArn": "arn:aws:iam::123456789012:role/transcribe",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, "create model: %s", createRec.Body)

	descRec := doTranscribeRequest(t, h, "DescribeLanguageModel", map[string]any{
		"ModelName": "time-field-model",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))
	lm, _ := raw["LanguageModel"].(map[string]any)
	require.NotNil(t, lm, "LanguageModel must be present")

	assert.NotNil(t, lm["CreateTime"], "CreateTime must be present in DescribeLanguageModel response")
	assert.NotNil(t, lm["LastModifiedTime"], "LastModifiedTime must be present in DescribeLanguageModel response")
}

// TestParity_LanguageModel_UpgradeAvailability verifies UpgradeAvailability is in response.
func TestParity_LanguageModel_UpgradeAvailability(t *testing.T) {
	t.Parallel()

	h := newTestTranscribeHandler(t)

	createRec := doTranscribeRequest(t, h, "CreateLanguageModel", map[string]any{
		"ModelName":     "upgrade-avail-model",
		"BaseModelName": "NarrowBand",
		"LanguageCode":  "en-US",
		"InputDataConfig": map[string]any{
			"S3Uri":             "s3://my-bucket/data/",
			"DataAccessRoleArn": "arn:aws:iam::123456789012:role/transcribe",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	descRec := doTranscribeRequest(t, h, "DescribeLanguageModel", map[string]any{
		"ModelName": "upgrade-avail-model",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))
	lm, _ := raw["LanguageModel"].(map[string]any)
	require.NotNil(t, lm)

	_, hasField := lm["UpgradeAvailability"]
	assert.True(t, hasField, "UpgradeAvailability must be present in DescribeLanguageModel response")
}
