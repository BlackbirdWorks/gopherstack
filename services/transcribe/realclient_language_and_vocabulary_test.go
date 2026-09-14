package transcribe_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// TestRealClient_LanguageAndVocabulary drives every gopherstack-n3zi
// uncovered transcribe op through the real aws-sdk-go-v2
// client (newTranscribeSDKClient, shared with wire_field_fixes_g8k9_test.go).
func TestRealClient_LanguageAndVocabulary(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testLanguageModelLifecycleRealClient, "language_model_lifecycle"},
		{testVocabularyLifecycleRealClient, "vocabulary_lifecycle"},
		{testVocabularyFilterLifecycleRealClient, "vocabulary_filter_lifecycle"},
		{testMedicalVocabularyLifecycleRealClient, "medical_vocabulary_lifecycle"},
		{testMedicalScribeJobsRealClient, "medical_scribe_jobs"},
		{testMedicalTranscriptionJobsRealClient, "medical_transcription_jobs"},
		{testCallAnalyticsJobsRealClient, "call_analytics_jobs"},
		{testCallAnalyticsCategoryRealClient, "call_analytics_category"},
		{testTagsRealClient, "tags"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testLanguageModelLifecycleRealClient covers CreateLanguageModel,
// DescribeLanguageModel, ListLanguageModels, DeleteLanguageModel.
func testLanguageModelLifecycleRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateLanguageModel(ctx, &transcribesdk.CreateLanguageModelInput{
		ModelName:     aws.String("my-lm"),
		BaseModelName: sdktypes.BaseModelNameWideBand,
		LanguageCode:  sdktypes.CLMLanguageCodeEnUs,
		InputDataConfig: &sdktypes.InputDataConfig{
			S3Uri:             aws.String("s3://bucket/training"),
			DataAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/transcribe"),
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeLanguageModel(ctx, &transcribesdk.DescribeLanguageModelInput{
		ModelName: aws.String("my-lm"),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.LanguageModel)
	assert.Equal(t, "my-lm", aws.ToString(descOut.LanguageModel.ModelName))
	assert.Equal(t, sdktypes.BaseModelNameWideBand, descOut.LanguageModel.BaseModelName)
	assert.Equal(t, sdktypes.ModelStatusCompleted, descOut.LanguageModel.ModelStatus)
	require.NotNil(t, descOut.LanguageModel.CreateTime)

	listOut, err := client.ListLanguageModels(ctx, &transcribesdk.ListLanguageModelsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Models, 1)
	assert.Equal(t, "my-lm", aws.ToString(listOut.Models[0].ModelName))

	_, err = client.DeleteLanguageModel(ctx, &transcribesdk.DeleteLanguageModelInput{
		ModelName: aws.String("my-lm"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListLanguageModels(ctx, &transcribesdk.ListLanguageModelsInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Models)
}

// testVocabularyLifecycleRealClient covers GetVocabulary, UpdateVocabulary,
// DeleteVocabulary (CreateVocabulary is already typed-covered elsewhere, used
// here only to seed state).
func testVocabularyLifecycleRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateVocabulary(ctx, &transcribesdk.CreateVocabularyInput{
		VocabularyName: aws.String("my-vocab"),
		LanguageCode:   sdktypes.LanguageCodeEnUs,
		Phrases:        []string{"hello", "world"},
	})
	require.NoError(t, err)

	getOut, err := client.GetVocabulary(ctx, &transcribesdk.GetVocabularyInput{
		VocabularyName: aws.String("my-vocab"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-vocab", aws.ToString(getOut.VocabularyName))
	assert.Equal(t, sdktypes.LanguageCodeEnUs, getOut.LanguageCode)
	assert.Equal(t, sdktypes.VocabularyStateReady, getOut.VocabularyState)

	updOut, err := client.UpdateVocabulary(ctx, &transcribesdk.UpdateVocabularyInput{
		VocabularyName: aws.String("my-vocab"),
		LanguageCode:   sdktypes.LanguageCodeEnUs,
		Phrases:        []string{"goodbye"},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-vocab", aws.ToString(updOut.VocabularyName))
	require.NotNil(t, updOut.LastModifiedTime)

	_, err = client.DeleteVocabulary(ctx, &transcribesdk.DeleteVocabularyInput{
		VocabularyName: aws.String("my-vocab"),
	})
	require.NoError(t, err)

	_, err = client.GetVocabulary(ctx, &transcribesdk.GetVocabularyInput{
		VocabularyName: aws.String("my-vocab"),
	})
	require.Error(t, err)
}

// testVocabularyFilterLifecycleRealClient covers CreateVocabularyFilter,
// GetVocabularyFilter, UpdateVocabularyFilter, ListVocabularyFilters,
// DeleteVocabularyFilter.
func testVocabularyFilterLifecycleRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateVocabularyFilter(ctx, &transcribesdk.CreateVocabularyFilterInput{
		VocabularyFilterName: aws.String("my-filter"),
		LanguageCode:         sdktypes.LanguageCodeEnUs,
		Words:                []string{"badword"},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-filter", aws.ToString(createOut.VocabularyFilterName))

	getOut, err := client.GetVocabularyFilter(ctx, &transcribesdk.GetVocabularyFilterInput{
		VocabularyFilterName: aws.String("my-filter"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-filter", aws.ToString(getOut.VocabularyFilterName))
	assert.Equal(t, sdktypes.LanguageCodeEnUs, getOut.LanguageCode)

	_, err = client.UpdateVocabularyFilter(ctx, &transcribesdk.UpdateVocabularyFilterInput{
		VocabularyFilterName: aws.String("my-filter"),
		Words:                []string{"otherword"},
	})
	require.NoError(t, err)

	listOut, err := client.ListVocabularyFilters(ctx, &transcribesdk.ListVocabularyFiltersInput{})
	require.NoError(t, err)
	require.Len(t, listOut.VocabularyFilters, 1)
	assert.Equal(t, "my-filter", aws.ToString(listOut.VocabularyFilters[0].VocabularyFilterName))

	_, err = client.DeleteVocabularyFilter(ctx, &transcribesdk.DeleteVocabularyFilterInput{
		VocabularyFilterName: aws.String("my-filter"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListVocabularyFilters(ctx, &transcribesdk.ListVocabularyFiltersInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.VocabularyFilters)
}

// testMedicalVocabularyLifecycleRealClient covers GetMedicalVocabulary,
// UpdateMedicalVocabulary, DeleteMedicalVocabulary.
func testMedicalVocabularyLifecycleRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateMedicalVocabulary(ctx, &transcribesdk.CreateMedicalVocabularyInput{
		VocabularyName:    aws.String("med-vocab"),
		LanguageCode:      sdktypes.LanguageCodeEnUs,
		VocabularyFileUri: aws.String("s3://bucket/med-vocab.txt"),
	})
	require.NoError(t, err)

	getOut, err := client.GetMedicalVocabulary(ctx, &transcribesdk.GetMedicalVocabularyInput{
		VocabularyName: aws.String("med-vocab"),
	})
	require.NoError(t, err)
	assert.Equal(t, "med-vocab", aws.ToString(getOut.VocabularyName))
	assert.Equal(t, sdktypes.VocabularyStateReady, getOut.VocabularyState)
	assert.Equal(t, "s3://bucket/med-vocab.txt", aws.ToString(getOut.DownloadUri))

	updOut, err := client.UpdateMedicalVocabulary(ctx, &transcribesdk.UpdateMedicalVocabularyInput{
		VocabularyName:    aws.String("med-vocab"),
		LanguageCode:      sdktypes.LanguageCodeEnUs,
		VocabularyFileUri: aws.String("s3://bucket/med-vocab-v2.txt"),
	})
	require.NoError(t, err)
	assert.Equal(t, "med-vocab", aws.ToString(updOut.VocabularyName))

	_, err = client.DeleteMedicalVocabulary(ctx, &transcribesdk.DeleteMedicalVocabularyInput{
		VocabularyName: aws.String("med-vocab"),
	})
	require.NoError(t, err)

	_, err = client.GetMedicalVocabulary(ctx, &transcribesdk.GetMedicalVocabularyInput{
		VocabularyName: aws.String("med-vocab"),
	})
	require.Error(t, err)
}

// testMedicalScribeJobsRealClient covers ListMedicalScribeJobs and
// DeleteMedicalScribeJob (StartMedicalScribeJob/GetMedicalScribeJob are
// already typed-covered elsewhere, used here only to seed state).
func testMedicalScribeJobsRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	_, err := client.StartMedicalScribeJob(ctx, &transcribesdk.StartMedicalScribeJobInput{
		MedicalScribeJobName: aws.String("scribe-job"),
		Media:                &sdktypes.Media{MediaFileUri: aws.String("s3://bucket/audio.wav")},
		DataAccessRoleArn:    aws.String("arn:aws:iam::123456789012:role/transcribe"),
		OutputBucketName:     aws.String("output-bucket"),
		Settings: &sdktypes.MedicalScribeSettings{
			ShowSpeakerLabels: aws.Bool(true),
			MaxSpeakerLabels:  aws.Int32(2),
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListMedicalScribeJobs(ctx, &transcribesdk.ListMedicalScribeJobsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.MedicalScribeJobSummaries, 1)
	assert.Equal(t, "scribe-job", aws.ToString(listOut.MedicalScribeJobSummaries[0].MedicalScribeJobName))
	assert.Equal(
		t, sdktypes.MedicalScribeJobStatusCompleted, listOut.MedicalScribeJobSummaries[0].MedicalScribeJobStatus,
	)

	_, err = client.DeleteMedicalScribeJob(ctx, &transcribesdk.DeleteMedicalScribeJobInput{
		MedicalScribeJobName: aws.String("scribe-job"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListMedicalScribeJobs(ctx, &transcribesdk.ListMedicalScribeJobsInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.MedicalScribeJobSummaries)
}

// testMedicalTranscriptionJobsRealClient covers ListMedicalTranscriptionJobs
// and DeleteMedicalTranscriptionJob.
func testMedicalTranscriptionJobsRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	_, err := client.StartMedicalTranscriptionJob(ctx, &transcribesdk.StartMedicalTranscriptionJobInput{
		MedicalTranscriptionJobName: aws.String("med-job"),
		LanguageCode:                sdktypes.LanguageCodeEnUs,
		Media:                       &sdktypes.Media{MediaFileUri: aws.String("s3://bucket/audio.wav")},
		Specialty:                   sdktypes.SpecialtyPrimarycare,
		Type:                        sdktypes.TypeConversation,
		OutputBucketName:            aws.String("output-bucket"),
	})
	require.NoError(t, err)

	listOut, err := client.ListMedicalTranscriptionJobs(ctx, &transcribesdk.ListMedicalTranscriptionJobsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.MedicalTranscriptionJobSummaries, 1)
	assert.Equal(t, "med-job", aws.ToString(listOut.MedicalTranscriptionJobSummaries[0].MedicalTranscriptionJobName))
	assert.Equal(
		t,
		sdktypes.TranscriptionJobStatusCompleted,
		listOut.MedicalTranscriptionJobSummaries[0].TranscriptionJobStatus,
	)

	_, err = client.DeleteMedicalTranscriptionJob(ctx, &transcribesdk.DeleteMedicalTranscriptionJobInput{
		MedicalTranscriptionJobName: aws.String("med-job"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListMedicalTranscriptionJobs(ctx, &transcribesdk.ListMedicalTranscriptionJobsInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.MedicalTranscriptionJobSummaries)
}

// testCallAnalyticsJobsRealClient covers ListCallAnalyticsJobs and
// DeleteCallAnalyticsJob.
func testCallAnalyticsJobsRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	_, err := client.StartCallAnalyticsJob(ctx, &transcribesdk.StartCallAnalyticsJobInput{
		CallAnalyticsJobName: aws.String("ca-job"),
		Media:                &sdktypes.Media{MediaFileUri: aws.String("s3://bucket/call.wav")},
	})
	require.NoError(t, err)

	listOut, err := client.ListCallAnalyticsJobs(ctx, &transcribesdk.ListCallAnalyticsJobsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.CallAnalyticsJobSummaries, 1)
	assert.Equal(t, "ca-job", aws.ToString(listOut.CallAnalyticsJobSummaries[0].CallAnalyticsJobName))
	assert.Equal(
		t,
		sdktypes.CallAnalyticsJobStatusCompleted,
		listOut.CallAnalyticsJobSummaries[0].CallAnalyticsJobStatus,
	)

	_, err = client.DeleteCallAnalyticsJob(ctx, &transcribesdk.DeleteCallAnalyticsJobInput{
		CallAnalyticsJobName: aws.String("ca-job"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListCallAnalyticsJobs(ctx, &transcribesdk.ListCallAnalyticsJobsInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.CallAnalyticsJobSummaries)
}

// testCallAnalyticsCategoryRealClient covers UpdateCallAnalyticsCategory and
// DeleteCallAnalyticsCategory.
func testCallAnalyticsCategoryRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCallAnalyticsCategory(ctx, &transcribesdk.CreateCallAnalyticsCategoryInput{
		CategoryName: aws.String("my-category"),
		InputType:    sdktypes.InputTypePostCall,
		Rules: []sdktypes.Rule{
			&sdktypes.RuleMemberNonTalkTimeFilter{Value: sdktypes.NonTalkTimeFilter{Threshold: aws.Int64(30000)}},
		},
	})
	require.NoError(t, err)

	updOut, err := client.UpdateCallAnalyticsCategory(ctx, &transcribesdk.UpdateCallAnalyticsCategoryInput{
		CategoryName: aws.String("my-category"),
		InputType:    sdktypes.InputTypeRealTime,
		Rules: []sdktypes.Rule{
			&sdktypes.RuleMemberTranscriptFilter{Value: sdktypes.TranscriptFilter{
				TranscriptFilterType: sdktypes.TranscriptFilterTypeExact,
				Targets:              []string{"cancellation"},
			}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updOut.CategoryProperties)
	assert.Equal(t, sdktypes.InputTypeRealTime, updOut.CategoryProperties.InputType)
	require.Len(t, updOut.CategoryProperties.Rules, 1)
	ruleMember, ok := updOut.CategoryProperties.Rules[0].(*sdktypes.RuleMemberTranscriptFilter)
	require.True(t, ok)
	assert.Equal(t, []string{"cancellation"}, ruleMember.Value.Targets)

	_, err = client.DeleteCallAnalyticsCategory(ctx, &transcribesdk.DeleteCallAnalyticsCategoryInput{
		CategoryName: aws.String("my-category"),
	})
	require.NoError(t, err)

	_, err = client.GetCallAnalyticsCategory(ctx, &transcribesdk.GetCallAnalyticsCategoryInput{
		CategoryName: aws.String("my-category"),
	})
	require.Error(t, err)
}

// testTagsRealClient covers ListTagsForResource and UntagResource
// (TagResource is already typed-covered elsewhere).
func testTagsRealClient(t *testing.T) {
	t.Helper()

	h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
	client := newTranscribeSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateVocabulary(ctx, &transcribesdk.CreateVocabularyInput{
		VocabularyName: aws.String("tag-vocab"),
		LanguageCode:   sdktypes.LanguageCodeEnUs,
		Phrases:        []string{"hi"},
	})
	require.NoError(t, err)
	_ = createOut

	resourceARN := "arn:aws:transcribe:us-east-1:123456789012:vocabulary/tag-vocab"

	_, err = client.TagResource(ctx, &transcribesdk.TagResourceInput{
		ResourceArn: aws.String(resourceARN),
		Tags: []sdktypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("ml")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &transcribesdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)
	assert.Equal(t, resourceARN, aws.ToString(listOut.ResourceArn))
	assert.ElementsMatch(t, []string{"env", "team"}, tagKeys(listOut.Tags))

	_, err = client.UntagResource(ctx, &transcribesdk.UntagResourceInput{
		ResourceArn: aws.String(resourceARN),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &transcribesdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listOut2.Tags[0].Key))
}

func tagKeys(tags []sdktypes.Tag) []string {
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		out = append(out, aws.ToString(tag.Key))
	}

	return out
}
