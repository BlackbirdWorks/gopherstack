package transcribe_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// ── Gap #1/#2: Full input + output ────────────────────────────────────────────

func TestAccuracy_StartTranscriptionJob_FullInput(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	job, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
		JobName:              "full-input-job",
		LanguageCode:         "en-US",
		MediaFormat:          "mp3",
		MediaSampleRateHertz: 16000,
		Media:                transcribe.Media{MediaFileURI: "s3://bucket/audio.mp3"},
		OutputBucketName:     "my-output-bucket",
		OutputKey:            "results/job.json",
		Settings: &transcribe.TranscriptionSettings{
			ShowSpeakerLabels: true,
			MaxSpeakerLabels:  4,
			ShowAlternatives:  true,
			MaxAlternatives:   3,
		},
		ContentRedaction: &transcribe.ContentRedaction{
			RedactionType:   "PII",
			RedactionOutput: "redacted",
		},
		Subtitles: &transcribe.SubtitlesOutput{
			Formats: []string{"vtt", "srt"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", job.JobStatus)
	assert.Equal(t, "en-US", job.LanguageCode)
	assert.Equal(t, "mp3", job.MediaFormat)
	assert.Equal(t, int32(16000), job.MediaSampleRateHertz)
	assert.Equal(t, "my-output-bucket", job.OutputBucketName)
	assert.NotNil(t, job.ContentRedaction)
	assert.NotNil(t, job.Subtitles)
	assert.Len(t, job.Subtitles.Formats, 2)
	assert.Len(t, job.Subtitles.SubtitleFileURIs, 2)
	assert.NotEmpty(t, job.TranscriptText)
	assert.NotEmpty(t, job.TranscriptJSON)
	assert.False(t, job.CreationTime.IsZero())
	assert.False(t, job.CompletionTime.IsZero())
}

// ── Gap #3: TranscriptionJobName validation ───────────────────────────────────

func TestAccuracy_TranscriptionJobName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		jobName string
		wantErr bool
	}{
		{name: "valid_alphanumeric", jobName: "my-job-123", wantErr: false},
		{name: "valid_dots_underscores", jobName: "my.job_test", wantErr: false},
		{name: "valid_200_chars", jobName: strings.Repeat("a", 200), wantErr: false},
		{name: "empty_name", jobName: "", wantErr: true},
		{name: "space_in_name", jobName: "bad name", wantErr: true},
		{name: "slash_in_name", jobName: "bad/name", wantErr: true},
		{name: "too_long_201_chars", jobName: strings.Repeat("a", 201), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
				JobName:      tc.jobName,
				LanguageCode: "en-US",
				Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
			})

			if tc.wantErr {
				require.ErrorIs(t, err, transcribe.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ── Gap #4: LanguageCode validation ──────────────────────────────────────────

func TestAccuracy_LanguageCode_Validation(t *testing.T) {
	t.Parallel()

	t.Run("valid_language_code_accepted", func(t *testing.T) {
		t.Parallel()

		validCodes := []string{"en-US", "es-US", "fr-FR", "de-DE", "ja-JP", "zh-CN", "ko-KR"}
		b := transcribe.NewInMemoryBackend()

		for i, code := range validCodes {
			_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
				JobName:      "job-valid-lang-" + code + "-" + string(rune('a'+i)),
				LanguageCode: code,
				Media:        transcribe.Media{MediaFileURI: "s3://b/f.mp3"},
			})
			require.NoError(t, err, "expected %s to be valid", code)
		}
	})

	t.Run("invalid_language_code_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-bad-lang",
			LanguageCode: "xx-XX",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("empty_language_code_without_identify_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName: "job-no-lang",
			Media:   transcribe.Media{MediaFileURI: "s3://b/f"},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

// ── Gap #5: IdentifyLanguage / IdentifyMultipleLanguages ─────────────────────

func TestAccuracy_IdentifyLanguage(t *testing.T) {
	t.Parallel()

	t.Run("identify_language_allows_empty_language_code", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		job, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:          "job-identify",
			IdentifyLanguage: true,
			LanguageOptions:  []string{"en-US", "es-US", "fr-FR"},
			Media:            transcribe.Media{MediaFileURI: "s3://b/f"},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, job.LanguageCode)
		assert.Greater(t, job.IdentifiedLanguageScore, float32(0))
	})

	t.Run("identify_multiple_languages_accepts_options", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		job, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:                   "job-identify-multi",
			IdentifyMultipleLanguages: true,
			LanguageOptions:           []string{"en-US", "es-US"},
			Media:                     transcribe.Media{MediaFileURI: "s3://b/f"},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, job.LanguageCode)
	})

	t.Run("too_few_language_options_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:          "job-few-opts",
			IdentifyLanguage: true,
			LanguageOptions:  []string{"en-US"},
			Media:            transcribe.Media{MediaFileURI: "s3://b/f"},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("too_many_language_options_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		opts := make([]string, 11)
		for i := range opts {
			opts[i] = "en-US"
		}

		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:          "job-many-opts",
			IdentifyLanguage: true,
			LanguageOptions:  opts,
			Media:            transcribe.Media{MediaFileURI: "s3://b/f"},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

// ── Gap #6: MediaFormat enum validation ──────────────────────────────────────

func TestAccuracy_MediaFormat_Validation(t *testing.T) {
	t.Parallel()

	validFormats := []string{"mp3", "mp4", "wav", "flac", "ogg", "amr", "webm", "m4a"}

	for _, format := range validFormats {
		t.Run("valid_format_"+format, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
				JobName:      "job-format-" + format,
				LanguageCode: "en-US",
				MediaFormat:  format,
				Media:        transcribe.Media{MediaFileURI: "s3://b/f." + format},
			})
			require.NoError(t, err)
		})
	}

	t.Run("invalid_format_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-bad-format",
			LanguageCode: "en-US",
			MediaFormat:  "avi",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f.avi"},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

// ── Gap #7: MediaSampleRateHertz range ───────────────────────────────────────

func TestAccuracy_MediaSampleRateHertz_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rate    int32
		wantErr bool
	}{
		{name: "valid_8000", rate: 8000, wantErr: false},
		{name: "valid_16000", rate: 16000, wantErr: false},
		{name: "valid_48000", rate: 48000, wantErr: false},
		{name: "zero_accepted", rate: 0, wantErr: false},
		{name: "below_8000_rejected", rate: 7999, wantErr: true},
		{name: "above_48000_rejected", rate: 48001, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
				JobName:              "job-rate-" + tc.name,
				LanguageCode:         "en-US",
				MediaSampleRateHertz: tc.rate,
				Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
			})

			if tc.wantErr {
				require.ErrorIs(t, err, transcribe.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ── Gap #9: Settings validation ───────────────────────────────────────────────

func TestAccuracy_Settings_Validation(t *testing.T) {
	t.Parallel()

	t.Run("valid_settings_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-settings-ok",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
			Settings: &transcribe.TranscriptionSettings{
				ShowSpeakerLabels:      true,
				MaxSpeakerLabels:       4,
				ShowAlternatives:       true,
				MaxAlternatives:        3,
				VocabularyFilterMethod: "mask",
			},
		})
		require.NoError(t, err)
	})

	t.Run("invalid_max_speaker_labels_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-bad-speakers",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
			Settings: &transcribe.TranscriptionSettings{
				ShowSpeakerLabels: true,
				MaxSpeakerLabels:  1,
			},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("invalid_vocabulary_filter_method_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-bad-filter-method",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
			Settings: &transcribe.TranscriptionSettings{
				VocabularyFilterMethod: "invalid-method",
			},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

// ── Gap #12: ContentRedaction validation ─────────────────────────────────────

func TestAccuracy_ContentRedaction_Validation(t *testing.T) {
	t.Parallel()

	t.Run("valid_pii_redaction_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-redact-ok",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
			ContentRedaction: &transcribe.ContentRedaction{
				RedactionType:   "PII",
				RedactionOutput: "redacted_and_unredacted",
			},
		})
		require.NoError(t, err)
	})

	t.Run("missing_redaction_type_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:          "job-redact-no-type",
			LanguageCode:     "en-US",
			Media:            transcribe.Media{MediaFileURI: "s3://b/f"},
			ContentRedaction: &transcribe.ContentRedaction{RedactionOutput: "redacted"},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("invalid_redaction_type_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-bad-redact-type",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
			ContentRedaction: &transcribe.ContentRedaction{
				RedactionType: "NOTPII",
			},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

// ── Gap #13: Subtitles validation ─────────────────────────────────────────────

func TestAccuracy_Subtitles_Validation(t *testing.T) {
	t.Parallel()

	t.Run("valid_subtitle_formats_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		job, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-subtitles-ok",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
			Subtitles:    &transcribe.SubtitlesOutput{Formats: []string{"vtt", "srt"}},
		})
		require.NoError(t, err)
		assert.Len(t, job.Subtitles.SubtitleFileURIs, 2)
		assert.Contains(t, job.Subtitles.SubtitleFileURIs[0], ".vtt")
		assert.Contains(t, job.Subtitles.SubtitleFileURIs[1], ".srt")
	})

	t.Run("invalid_subtitle_format_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
			JobName:      "job-bad-subtitle",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
			Subtitles:    &transcribe.SubtitlesOutput{Formats: []string{"txt"}},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

// ── Gap #16: Transcript JSON generation ──────────────────────────────────────

func TestAccuracy_TranscriptJSON_Generated(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	job, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
		JobName:      "job-transcript-json",
		LanguageCode: "en-US",
		Media:        transcribe.Media{MediaFileURI: "s3://b/f"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, job.TranscriptJSON)
	assert.Contains(t, string(job.TranscriptJSON), "results")
	assert.Contains(t, string(job.TranscriptJSON), "transcripts")
	assert.Contains(t, string(job.TranscriptJSON), "items")
}

// ── Gap #17: OutputBucketName routing ────────────────────────────────────────

func TestAccuracy_OutputBucketName_Routing(t *testing.T) {
	t.Parallel()

	h, b := newAccuracyHandler(t)

	_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
		JobName:          "job-output-bucket",
		LanguageCode:     "en-US",
		Media:            transcribe.Media{MediaFileURI: "s3://b/f"},
		OutputBucketName: "my-results-bucket",
		OutputKey:        "prefix/my-job.json",
	})
	require.NoError(t, err)

	rec := doTranscribeRequest(t, h, "GetTranscriptionJob", map[string]any{
		"TranscriptionJobName": "job-output-bucket",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-results-bucket")
}

func TestAccuracy_JobExecutionSettings_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings *transcribe.JobExecutionSettings
		name     string
		wantErr  bool
	}{
		{name: "unset", settings: nil},
		{name: "role_without_deferred", settings: &transcribe.JobExecutionSettings{DataAccessRoleArn: "role"}},
		{
			name: "deferred_with_role",
			settings: &transcribe.JobExecutionSettings{
				AllowDeferredExecution: true,
				DataAccessRoleArn:      "arn:aws:iam::123456789012:role/transcribe",
			},
		},
		{
			name:     "deferred_without_role",
			settings: &transcribe.JobExecutionSettings{AllowDeferredExecution: true},
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			job, err := transcribe.NewInMemoryBackend().StartTranscriptionJob(&transcribe.TranscriptionJob{
				JobName:              "job-execution-" + tc.name,
				LanguageCode:         "en-US",
				Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
				JobExecutionSettings: tc.settings,
			})
			if tc.wantErr {
				require.ErrorIs(t, err, transcribe.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.settings, job.JobExecutionSettings)
		})
	}
}

// ── Gap #18: Vocabulary Phrases + VocabularyFileUri ──────────────────────────

func TestAccuracy_CreateVocabulary_Phrases(t *testing.T) {
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

// ── Gap #19: VocabularyFilter Words ──────────────────────────────────────────

func TestAccuracy_CreateVocabularyFilter_Words(t *testing.T) {
	t.Parallel()

	t.Run("words_required", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
			VocabularyFilterName: "my-filter-no-words",
			LanguageCode:         "en-US",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("words_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		vf, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
			VocabularyFilterName: "my-filter",
			LanguageCode:         "en-US",
			Words:                []string{"profanity", "slur"},
		})
		require.NoError(t, err)
		assert.Len(t, vf.Words, 2)
	})
}

// ── Gap #20: LanguageModel InputDataConfig ────────────────────────────────────

func TestAccuracy_CreateLanguageModel_InputDataConfig(t *testing.T) {
	t.Parallel()

	t.Run("base_model_name_validated", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateLanguageModel(&transcribe.LanguageModel{
			ModelName:     "my-model",
			BaseModelName: "InvalidBase",
			LanguageCode:  "en-US",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("valid_narrow_band_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		m, err := b.CreateLanguageModel(&transcribe.LanguageModel{
			ModelName:     "narrow-model",
			BaseModelName: "NarrowBand",
			LanguageCode:  "en-US",
			InputDataConfig: &transcribe.InputDataConfig{
				S3Uri:             "s3://bucket/training/",
				DataAccessRoleArn: "arn:aws:iam::123456789012:role/TranscribeRole",
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "NarrowBand", m.BaseModelName)
		assert.NotNil(t, m.InputDataConfig)
	})

	t.Run("input_data_config_s3_uri_required", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateLanguageModel(&transcribe.LanguageModel{
			ModelName:     "model-no-s3",
			BaseModelName: "WideBand",
			LanguageCode:  "en-US",
			InputDataConfig: &transcribe.InputDataConfig{
				DataAccessRoleArn: "arn:aws:iam::123456789012:role/TranscribeRole",
			},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

// ── Gap #21: MedicalTranscriptionJob Specialty + Type ─────────────────────────

func TestAccuracy_StartMedicalTranscriptionJob_SpecialtyType(t *testing.T) {
	t.Parallel()

	t.Run("valid_job_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		job, err := b.StartMedicalTranscriptionJob(&transcribe.MedicalTranscriptionJob{
			MedicalTranscriptionJobName: "med-job-ok",
			LanguageCode:                "en-US",
			Media:                       transcribe.Media{MediaFileURI: "s3://b/f.mp3"},
			Specialty:                   "PRIMARYCARE",
			Type:                        "CONVERSATION",
		})
		require.NoError(t, err)
		assert.Equal(t, "COMPLETED", job.TranscriptionJobStatus)
		assert.Equal(t, "PRIMARYCARE", job.Specialty)
		assert.Equal(t, "CONVERSATION", job.Type)
	})

	t.Run("missing_specialty_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartMedicalTranscriptionJob(&transcribe.MedicalTranscriptionJob{
			MedicalTranscriptionJobName: "med-job-no-specialty",
			LanguageCode:                "en-US",
			Media:                       transcribe.Media{MediaFileURI: "s3://b/f"},
			Type:                        "DICTATION",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("invalid_type_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartMedicalTranscriptionJob(&transcribe.MedicalTranscriptionJob{
			MedicalTranscriptionJobName: "med-job-bad-type",
			LanguageCode:                "en-US",
			Media:                       transcribe.Media{MediaFileURI: "s3://b/f"},
			Specialty:                   "PRIMARYCARE",
			Type:                        "PODCAST",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("phi_content_identification_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		job, err := b.StartMedicalTranscriptionJob(&transcribe.MedicalTranscriptionJob{
			MedicalTranscriptionJobName:      "med-job-phi",
			LanguageCode:                     "en-US",
			Media:                            transcribe.Media{MediaFileURI: "s3://b/f"},
			Specialty:                        "PRIMARYCARE",
			Type:                             "CONVERSATION",
			MedicalContentIdentificationType: "PHI",
		})
		require.NoError(t, err)
		assert.Equal(t, "PHI", job.MedicalContentIdentificationType)
	})
}

// ── Gap #22: MedicalScribeJob required fields ─────────────────────────────────

func TestAccuracy_StartMedicalScribeJob_RequiredFields(t *testing.T) {
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

// ── Gap #23: CallAnalyticsJob ChannelDefinitions ──────────────────────────────

func TestAccuracy_StartCallAnalyticsJob_ChannelDefinitions(t *testing.T) {
	t.Parallel()

	t.Run("valid_two_channels_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		job, err := b.StartCallAnalyticsJob(&transcribe.CallAnalyticsJob{
			CallAnalyticsJobName: "ca-job-ok",
			LanguageCode:         "en-US",
			Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
			ChannelDefinitions: []transcribe.ChannelDefinition{
				{ChannelID: 0, ParticipantRole: "AGENT"},
				{ChannelID: 1, ParticipantRole: "CUSTOMER"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "COMPLETED", job.CallAnalyticsJobStatus)
		assert.Len(t, job.ChannelDefinitions, 2)
	})

	t.Run("only_one_channel_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartCallAnalyticsJob(&transcribe.CallAnalyticsJob{
			CallAnalyticsJobName: "ca-job-one-channel",
			LanguageCode:         "en-US",
			Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
			ChannelDefinitions: []transcribe.ChannelDefinition{
				{ChannelID: 0, ParticipantRole: "AGENT"},
			},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("duplicate_roles_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.StartCallAnalyticsJob(&transcribe.CallAnalyticsJob{
			CallAnalyticsJobName: "ca-job-dup-roles",
			LanguageCode:         "en-US",
			Media:                transcribe.Media{MediaFileURI: "s3://b/f"},
			ChannelDefinitions: []transcribe.ChannelDefinition{
				{ChannelID: 0, ParticipantRole: "AGENT"},
				{ChannelID: 1, ParticipantRole: "AGENT"},
			},
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

// ── Gap #24: CallAnalyticsCategory Rules + InputType ─────────────────────────

func TestAccuracy_CreateCallAnalyticsCategory_Rules(t *testing.T) {
	t.Parallel()

	t.Run("valid_category_with_rules_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		cat, err := b.CreateCallAnalyticsCategory(&transcribe.CallAnalyticsCategory{
			CategoryName: "my-category",
			InputType:    "POST_CALL",
			Rules: []transcribe.CallAnalyticsRule{
				{
					NonTalkTimeFilter: &transcribe.NonTalkTimeFilter{
						Threshold:       30000,
						ParticipantRole: "CUSTOMER",
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Len(t, cat.Rules, 1)
		assert.Equal(t, "POST_CALL", cat.InputType)
	})

	t.Run("invalid_input_type_rejected", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateCallAnalyticsCategory(&transcribe.CallAnalyticsCategory{
			CategoryName: "bad-type",
			InputType:    "INVALID_TYPE",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("update_category_with_new_rules", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateCallAnalyticsCategory(&transcribe.CallAnalyticsCategory{
			CategoryName: "my-cat-update",
			InputType:    "POST_CALL",
		})
		require.NoError(t, err)

		updated, err := b.UpdateCallAnalyticsCategory(&transcribe.CallAnalyticsCategory{
			CategoryName: "my-cat-update",
			InputType:    "REAL_TIME",
			Rules: []transcribe.CallAnalyticsRule{
				{
					TranscriptFilter: &transcribe.TranscriptFilter{
						TranscriptFilterType: "EXACT",
						Targets:              []string{"cancellation"},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "REAL_TIME", updated.InputType)
		assert.Len(t, updated.Rules, 1)
	})
}

// ── HTTP handler accuracy tests ───────────────────────────────────────────────

func newAccuracyHandler(t *testing.T) (*transcribe.Handler, *transcribe.InMemoryBackend) {
	t.Helper()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	return h, b
}

func TestAccuracy_HTTP_StartTranscriptionJob_FullInput(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	rec := doTranscribeRequest(t, h, "StartTranscriptionJob", map[string]any{
		"TranscriptionJobName": "http-full-job",
		"LanguageCode":         "en-US",
		"MediaFormat":          "wav",
		"MediaSampleRateHertz": 16000,
		"Media":                map[string]any{"MediaFileUri": "s3://bucket/audio.wav"},
		"OutputBucketName":     "my-output",
		"Settings": map[string]any{
			"ShowSpeakerLabels": true,
			"MaxSpeakerLabels":  3,
		},
		"ContentRedaction": map[string]any{
			"RedactionType": "PII",
		},
		"JobExecutionSettings": map[string]any{
			"AllowDeferredExecution": true,
			"DataAccessRoleArn":      "arn:aws:iam::123456789012:role/transcribe",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "http-full-job")
	assert.Contains(t, body, "COMPLETED")
	assert.Contains(t, body, "JobExecutionSettings")
}

func TestAccuracy_HTTP_StartMedicalTranscriptionJob(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	rec := doTranscribeRequest(t, h, "StartMedicalTranscriptionJob", map[string]any{
		"MedicalTranscriptionJobName": "http-med-job",
		"LanguageCode":                "en-US",
		"Media":                       map[string]any{"MediaFileUri": "s3://b/f.mp3"},
		"Specialty":                   "PRIMARYCARE",
		"Type":                        "DICTATION",
		"OutputBucketName":            "med-output",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-med-job")
}

func TestAccuracy_HTTP_StartCallAnalyticsJob(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	rec := doTranscribeRequest(t, h, "StartCallAnalyticsJob", map[string]any{
		"CallAnalyticsJobName": "http-ca-job",
		"LanguageCode":         "en-US",
		"Media":                map[string]any{"MediaFileUri": "s3://b/f.mp3"},
		"ChannelDefinitions": []map[string]any{
			{"ChannelId": 0, "ParticipantRole": "AGENT"},
			{"ChannelId": 1, "ParticipantRole": "CUSTOMER"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-ca-job")
}

func TestAccuracy_HTTP_CreateVocabulary_WithPhrases(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	rec := doTranscribeRequest(t, h, "CreateVocabulary", map[string]any{
		"VocabularyName": "phrases-vocab",
		"LanguageCode":   "en-US",
		"Phrases":        []string{"AWS Lambda", "DynamoDB"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "phrases-vocab")
}

func TestAccuracy_HTTP_CreateLanguageModel_WithInputDataConfig(t *testing.T) {
	t.Parallel()

	h, _ := newAccuracyHandler(t)
	rec := doTranscribeRequest(t, h, "CreateLanguageModel", map[string]any{
		"ModelName":     "my-clm",
		"BaseModelName": "WideBand",
		"LanguageCode":  "en-US",
		"InputDataConfig": map[string]any{
			"S3Uri":             "s3://my-bucket/training/",
			"DataAccessRoleArn": "arn:aws:iam::123456789012:role/TranscribeRole",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-clm")
}
