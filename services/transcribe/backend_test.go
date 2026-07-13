package transcribe_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// ── Creation-time Tags are attached to the resource ARN ─────────────────────
//
// Real AWS treats the Tags parameter on Start*Job/Create* operations as
// immediately-attached resource tags, retrievable via ListTagsForResource
// for that resource's ARN (arn:aws:transcribe:<region>:<account>:<type>/<name>).
// Get/Describe/List outputs for vocabularies, filters, and language models never
// echo Tags directly, so ListTagsForResource is the only way to observe them.

func TestBackend_CreationTags_SyncToResourceARN(t *testing.T) {
	t.Parallel()

	const region = "us-east-1"
	const account = "123456789012"

	tests := []struct {
		create func(b *transcribe.InMemoryBackend) error
		name   string
		arn    string
	}{
		{
			name: "StartTranscriptionJob",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":transcription-job/tagged-ts-job",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
					JobName:      "tagged-ts-job",
					LanguageCode: "en-US",
					Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
					Tags:         map[string]string{"team": "asr"},
				})

				return err
			},
		},
		{
			name: "StartCallAnalyticsJob",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":call-analytics-job/tagged-ca-job",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.StartCallAnalyticsJob(&transcribe.CallAnalyticsJob{
					CallAnalyticsJobName: "tagged-ca-job",
					LanguageCode:         "en-US",
					Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
					Tags:                 map[string]string{"team": "ca"},
				})

				return err
			},
		},
		{
			name: "StartMedicalScribeJob",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":medical-scribe-job/tagged-scribe-job",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.StartMedicalScribeJob(&transcribe.MedicalScribeJob{
					MedicalScribeJobName: "tagged-scribe-job",
					Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
					DataAccessRoleArn:    "arn:aws:iam::123456789012:role/Scribe",
					OutputBucketName:     "scribe-out",
					Tags:                 map[string]string{"team": "scribe"},
				})

				return err
			},
		},
		{
			name: "StartMedicalTranscriptionJob",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":medical-transcription-job/tagged-mt-job",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.StartMedicalTranscriptionJob(&transcribe.MedicalTranscriptionJob{
					MedicalTranscriptionJobName: "tagged-mt-job",
					LanguageCode:                "en-US",
					Specialty:                   "PRIMARYCARE",
					Type:                        "DICTATION",
					Media:                       transcribe.Media{MediaFileURI: "s3://b/f"},
					Tags:                        map[string]string{"team": "medical"},
				})

				return err
			},
		},
		{
			name: "CreateVocabulary",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":vocabulary/tagged-vocab",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.CreateVocabulary(&transcribe.Vocabulary{
					VocabularyName: "tagged-vocab",
					LanguageCode:   "en-US",
					Phrases:        []string{"hello"},
					Tags:           map[string]string{"team": "vocab"},
				})

				return err
			},
		},
		{
			name: "CreateVocabularyFilter",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":vocabulary-filter/tagged-filter",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
					VocabularyFilterName: "tagged-filter",
					LanguageCode:         "en-US",
					Words:                []string{"bleep"},
					Tags:                 map[string]string{"team": "filter"},
				})

				return err
			},
		},
		{
			name: "CreateMedicalVocabulary",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":medical-vocabulary/tagged-med-vocab",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.CreateMedicalVocabulary(
					"tagged-med-vocab", "en-US", "s3://bucket/v.txt", map[string]string{"team": "med-vocab"},
				)

				return err
			},
		},
		{
			name: "CreateLanguageModel",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":language-model/tagged-model",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.CreateLanguageModel(&transcribe.LanguageModel{
					ModelName:     "tagged-model",
					BaseModelName: "WideBand",
					LanguageCode:  "en-US",
					Tags:          map[string]string{"team": "model"},
				})

				return err
			},
		},
		{
			name: "CreateCallAnalyticsCategory",
			arn:  "arn:aws:transcribe:" + region + ":" + account + ":call-analytics-category/tagged-category",
			create: func(b *transcribe.InMemoryBackend) error {
				_, err := b.CreateCallAnalyticsCategory(&transcribe.CallAnalyticsCategory{
					CategoryName: "tagged-category",
					InputType:    "POST_CALL",
					Tags:         map[string]string{"team": "category"},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			require.NoError(t, tt.create(b))

			tags, err := b.ListTagsForResource(tt.arn)
			require.NoError(t, err)
			assert.NotEmpty(t, tags, "expected tags recorded for ARN %s", tt.arn)
		})
	}
}

// ── Deleting a resource forgets its tags ─────────────────────────────────────

func TestBackend_Delete_ForgetsResourceTags(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:transcribe:us-east-1:123456789012:transcription-job/deleted-tagged-job"

	b := transcribe.NewInMemoryBackend()

	_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
		JobName:      "deleted-tagged-job",
		LanguageCode: "en-US",
		Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		Tags:         map[string]string{"team": "asr"},
	})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(arn)
	require.NoError(t, err)
	assert.NotEmpty(t, tags)

	require.NoError(t, b.DeleteTranscriptionJob("deleted-tagged-job"))

	tags, err = b.ListTagsForResource(arn)
	require.NoError(t, err)
	assert.Empty(t, tags, "tags should be forgotten once the resource is deleted")
}

// ── Untagged creation does not fabricate an empty tag entry ─────────────────

func TestBackend_CreationWithoutTags_LeavesResourceTagsEmpty(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:transcribe:us-east-1:123456789012:transcription-job/untagged-job"

	b := transcribe.NewInMemoryBackend()

	_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
		JobName:      "untagged-job",
		LanguageCode: "en-US",
		Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
	})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(arn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}
