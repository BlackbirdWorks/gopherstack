package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	comprehendsdk "github.com/aws/aws-sdk-go-v2/service/comprehend"
	comprehendtypes "github.com/aws/aws-sdk-go-v2/service/comprehend/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createComprehendClient returns a Comprehend client pointed at the shared test container.
func createComprehendClient(t *testing.T) *comprehendsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return comprehendsdk.NewFromConfig(cfg, func(o *comprehendsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Comprehend_DetectSentiment exercises the real-time inference op and
// asserts the documented keyword-driven sentiment classification.
func TestIntegration_Comprehend_DetectSentiment(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		text     string
		expected comprehendtypes.SentimentType
	}{
		{name: "positive", text: "This product is great, I love it", expected: comprehendtypes.SentimentTypePositive},
		{name: "negative", text: "This is terrible and I hate it", expected: comprehendtypes.SentimentTypeNegative},
		{name: "neutral", text: "The package arrived on Tuesday", expected: comprehendtypes.SentimentTypeNeutral},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createComprehendClient(t)

			out, err := client.DetectSentiment(t.Context(), &comprehendsdk.DetectSentimentInput{
				Text:         aws.String(tt.text),
				LanguageCode: comprehendtypes.LanguageCodeEn,
			})
			require.NoError(t, err, "DetectSentiment should succeed")
			assert.Equal(t, tt.expected, out.Sentiment)
			require.NotNil(t, out.SentimentScore, "SentimentScore must be populated")
		})
	}
}

// TestIntegration_Comprehend_DetectDominantLanguage asserts the canned dominant-language
// response shape decodes against the AWS SDK deserialiser.
func TestIntegration_Comprehend_DetectDominantLanguage(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
		text string
	}{
		{name: "english_text", text: "The quick brown fox jumps over the lazy dog"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createComprehendClient(t)

			out, err := client.DetectDominantLanguage(t.Context(), &comprehendsdk.DetectDominantLanguageInput{
				Text: aws.String(tt.text),
			})
			require.NoError(t, err, "DetectDominantLanguage should succeed")
			require.NotEmpty(t, out.Languages, "at least one language must be returned")
			assert.Equal(t, "en", aws.ToString(out.Languages[0].LanguageCode))
		})
	}
}

// TestIntegration_Comprehend_EntityRecognizerLifecycle drives the create→describe→list→delete
// lifecycle of an entity recognizer resource.
func TestIntegration_Comprehend_EntityRecognizerLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "full_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createComprehendClient(t)

			createOut, err := client.CreateEntityRecognizer(ctx, &comprehendsdk.CreateEntityRecognizerInput{
				RecognizerName:    aws.String("integ-recognizer"),
				DataAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/comprehend"),
				LanguageCode:      comprehendtypes.LanguageCodeEn,
				InputDataConfig: &comprehendtypes.EntityRecognizerInputDataConfig{
					EntityTypes: []comprehendtypes.EntityTypesListItem{
						{Type: aws.String("PERSON")},
					},
					Documents: &comprehendtypes.EntityRecognizerDocuments{
						S3Uri: aws.String("s3://integ-bucket/docs/"),
					},
					Annotations: &comprehendtypes.EntityRecognizerAnnotations{
						S3Uri: aws.String("s3://integ-bucket/annotations/"),
					},
				},
			})
			require.NoError(t, err, "CreateEntityRecognizer should succeed")
			arn := aws.ToString(createOut.EntityRecognizerArn)
			require.NotEmpty(t, arn, "recognizer ARN must be returned")

			descOut, err := client.DescribeEntityRecognizer(ctx, &comprehendsdk.DescribeEntityRecognizerInput{
				EntityRecognizerArn: aws.String(arn),
			})
			require.NoError(t, err, "DescribeEntityRecognizer should succeed")
			require.NotNil(t, descOut.EntityRecognizerProperties)
			assert.Equal(t, arn, aws.ToString(descOut.EntityRecognizerProperties.EntityRecognizerArn))

			listOut, err := client.ListEntityRecognizers(ctx, &comprehendsdk.ListEntityRecognizersInput{})
			require.NoError(t, err, "ListEntityRecognizers should succeed")

			found := false
			for _, p := range listOut.EntityRecognizerPropertiesList {
				if aws.ToString(p.EntityRecognizerArn) == arn {
					found = true

					break
				}
			}

			assert.True(t, found, "created recognizer should appear in list")

			_, err = client.DeleteEntityRecognizer(ctx, &comprehendsdk.DeleteEntityRecognizerInput{
				EntityRecognizerArn: aws.String(arn),
			})
			require.NoError(t, err, "DeleteEntityRecognizer should succeed")
		})
	}
}
