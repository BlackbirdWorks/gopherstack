package translate_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	translatesdk "github.com/aws/aws-sdk-go-v2/service/translate"
	translatetypes "github.com/aws/aws-sdk-go-v2/service/translate/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

// TestRealClient_LanguagesAndJobs drives the 10 ops that a real
// aws-sdk-go-v2 translate client had never exercised before this pass
// (gopherstack-n3zi): DeleteParallelData, DescribeTextTranslationJob,
// ListLanguages, ListParallelData, ListTagsForResource,
// StopTextTranslationJob, TagResource, TranslateDocument, UntagResource,
// UpdateParallelData.
func TestRealClient_LanguagesAndJobs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testListLanguagesRealClient, "list_languages"},
		{testTranslateDocumentRealClient, "translate_document"},
		{testParallelDataLifecycleRealClient, "parallel_data_lifecycle"},
		{testTextTranslationJobLifecycleRealClient, "text_translation_job_lifecycle"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func newRealClient(t *testing.T) *translatesdk.Client {
	t.Helper()

	h := translate.NewHandler(translate.NewInMemoryBackend("000000000000", wireTestRegion))

	return newTestTranslateSDKClient(t, h)
}

func testListLanguagesRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	out, err := client.ListLanguages(ctx, &translatesdk.ListLanguagesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.Languages)
	require.NotEmpty(t, aws.ToString(out.Languages[0].LanguageCode))
	require.NotEmpty(t, aws.ToString(out.Languages[0].LanguageName))
}

func testTranslateDocumentRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	out, err := client.TranslateDocument(ctx, &translatesdk.TranslateDocumentInput{
		SourceLanguageCode: aws.String("en"),
		TargetLanguageCode: aws.String("fr"),
		Document: &translatetypes.Document{
			Content:     []byte("hello world"),
			ContentType: aws.String("text/plain"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.TranslatedDocument)
	require.NotEmpty(t, out.TranslatedDocument.Content)
	require.Equal(t, "fr", aws.ToString(out.TargetLanguageCode))
}

func testParallelDataLifecycleRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	created, err := client.CreateParallelData(ctx, &translatesdk.CreateParallelDataInput{
		Name: aws.String("s19-pd"),
		ParallelDataConfig: &translatetypes.ParallelDataConfig{
			S3Uri:  aws.String("s3://bucket/f.tmx"),
			Format: translatetypes.ParallelDataFormatTmx,
		},
	})
	require.NoError(t, err)
	arn := aws.ToString(created.Name)
	require.NotEmpty(t, arn)

	// TagResource / ListTagsForResource / UntagResource against the parallel
	// data's real ARN (the describe response's Arn field, not its bare Name).
	desc, err := client.GetParallelData(ctx, &translatesdk.GetParallelDataInput{Name: aws.String("s19-pd")})
	require.NoError(t, err)
	resourceArn := aws.ToString(desc.ParallelDataProperties.Arn)
	require.NotEmpty(t, resourceArn)

	_, err = client.TagResource(ctx, &translatesdk.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        []translatetypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	listedTags, err := client.ListTagsForResource(ctx, &translatesdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Len(t, listedTags.Tags, 1)
	require.Equal(t, "env", aws.ToString(listedTags.Tags[0].Key))

	_, err = client.UntagResource(ctx, &translatesdk.UntagResourceInput{
		ResourceArn: aws.String(resourceArn),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	afterUntag, err := client.ListTagsForResource(ctx, &translatesdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Empty(t, afterUntag.Tags)

	// UpdateParallelData.
	updated, err := client.UpdateParallelData(ctx, &translatesdk.UpdateParallelDataInput{
		Name: aws.String("s19-pd"),
		ParallelDataConfig: &translatetypes.ParallelDataConfig{
			S3Uri:  aws.String("s3://bucket/f2.tmx"),
			Format: translatetypes.ParallelDataFormatTmx,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "s19-pd", aws.ToString(updated.Name))

	// ListParallelData.
	listed, err := client.ListParallelData(ctx, &translatesdk.ListParallelDataInput{})
	require.NoError(t, err)
	found := false
	for _, pd := range listed.ParallelDataPropertiesList {
		if aws.ToString(pd.Name) == "s19-pd" {
			found = true
		}
	}
	require.True(t, found, "ListParallelData must include the created parallel data")

	// DeleteParallelData.
	_, err = client.DeleteParallelData(ctx, &translatesdk.DeleteParallelDataInput{Name: aws.String("s19-pd")})
	require.NoError(t, err)
}

func testTextTranslationJobLifecycleRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	started, err := client.StartTextTranslationJob(ctx, &translatesdk.StartTextTranslationJobInput{
		JobName:             aws.String("s19-job"),
		SourceLanguageCode:  aws.String("en"),
		TargetLanguageCodes: []string{"fr"},
		DataAccessRoleArn:   aws.String("arn:aws:iam::000000000000:role/TranslateRole"),
		InputDataConfig: &translatetypes.InputDataConfig{
			S3Uri:       aws.String("s3://b/i/"),
			ContentType: aws.String("text/plain"),
		},
		OutputDataConfig: &translatetypes.OutputDataConfig{
			S3Uri: aws.String("s3://b/o/"),
		},
	})
	require.NoError(t, err)
	jobID := aws.ToString(started.JobId)

	// DescribeTextTranslationJob.
	desc, err := client.DescribeTextTranslationJob(
		ctx,
		&translatesdk.DescribeTextTranslationJobInput{JobId: aws.String(jobID)},
	)
	require.NoError(t, err)
	require.NotNil(t, desc.TextTranslationJobProperties)
	require.Equal(t, jobID, aws.ToString(desc.TextTranslationJobProperties.JobId))

	// StopTextTranslationJob.
	stopped, err := client.StopTextTranslationJob(
		ctx,
		&translatesdk.StopTextTranslationJobInput{JobId: aws.String(jobID)},
	)
	require.NoError(t, err)
	require.Equal(t, jobID, aws.ToString(stopped.JobId))
	require.NotEmpty(t, stopped.JobStatus)
}
