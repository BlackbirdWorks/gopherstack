package comprehend_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/comprehend"
	"github.com/aws/aws-sdk-go-v2/service/comprehend/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_Slice29_RealClient drives every gopherstack-n3zi typed-slice-29
// uncovered comprehend op through the real aws-sdk-go-v2 client.
func Test_Slice29_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("single_document_detect", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestComprehendSDKClient(t, h)
		ctx := t.Context()

		entOut, err := client.DetectEntities(ctx, &comprehend.DetectEntitiesInput{
			Text:         aws.String("John visited Paris last summer."),
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, entOut.Entities)

		kpOut, err := client.DetectKeyPhrases(ctx, &comprehend.DetectKeyPhrasesInput{
			Text:         aws.String("The quick brown fox jumps over the lazy dog."),
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, kpOut.KeyPhrases)

		piiOut, err := client.DetectPiiEntities(ctx, &comprehend.DetectPiiEntitiesInput{
			Text:         aws.String("Contact me at jane@example.com"),
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		require.Len(t, piiOut.Entities, 1)
		assert.Equal(t, types.PiiEntityTypeEmail, piiOut.Entities[0].Type)

		synOut, err := client.DetectSyntax(ctx, &comprehend.DetectSyntaxInput{
			Text:         aws.String("The dog runs fast."),
			LanguageCode: types.SyntaxLanguageCodeEn,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, synOut.SyntaxTokens)

		toxicOut, err := client.DetectToxicContent(ctx, &comprehend.DetectToxicContentInput{
			TextSegments: []types.TextSegment{{Text: aws.String("hello there")}},
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		require.Len(t, toxicOut.ResultList, 1)

		classifyOut, err := client.ClassifyDocument(ctx, &comprehend.ClassifyDocumentInput{
			Text:        aws.String("The stock market and finance sector rallied today."),
			EndpointArn: aws.String("arn:aws:comprehend:us-east-1:123456789012:document-classifier-endpoint/fake"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, classifyOut.Classes)
		assert.Equal(t, "FINANCE", aws.ToString(classifyOut.Classes[0].Name))

		piiContainsOut, err := client.ContainsPiiEntities(ctx, &comprehend.ContainsPiiEntitiesInput{
			Text:         aws.String("my ssn is 123-45-6789"),
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		require.Len(t, piiContainsOut.Labels, 1)
		assert.Equal(t, types.PiiEntityTypeSsn, piiContainsOut.Labels[0].Name)
	})

	t.Run("batch_detect", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestComprehendSDKClient(t, h)
		ctx := t.Context()

		texts := []string{"I love this product", "This is terrible"}

		langOut, err := client.BatchDetectDominantLanguage(ctx, &comprehend.BatchDetectDominantLanguageInput{
			TextList: texts,
		})
		require.NoError(t, err)
		require.Len(t, langOut.ResultList, 2)
		assert.Empty(t, langOut.ErrorList)

		entOut, err := client.BatchDetectEntities(ctx, &comprehend.BatchDetectEntitiesInput{
			TextList:     []string{"Paris is nice", "Berlin too"},
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		require.Len(t, entOut.ResultList, 2)

		kpOut, err := client.BatchDetectKeyPhrases(ctx, &comprehend.BatchDetectKeyPhrasesInput{
			TextList:     texts,
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		require.Len(t, kpOut.ResultList, 2)

		sentOut, err := client.BatchDetectSentiment(ctx, &comprehend.BatchDetectSentimentInput{
			TextList:     texts,
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		require.Len(t, sentOut.ResultList, 2)
		assert.Equal(t, types.SentimentTypePositive, sentOut.ResultList[0].Sentiment)
		assert.Equal(t, types.SentimentTypeNegative, sentOut.ResultList[1].Sentiment)

		synOut, err := client.BatchDetectSyntax(ctx, &comprehend.BatchDetectSyntaxInput{
			TextList:     texts,
			LanguageCode: types.SyntaxLanguageCodeEn,
		})
		require.NoError(t, err)
		require.Len(t, synOut.ResultList, 2)

		tsOut, err := client.BatchDetectTargetedSentiment(ctx, &comprehend.BatchDetectTargetedSentimentInput{
			TextList:     []string{"The food at Acme diner was great"},
			LanguageCode: types.LanguageCodeEn,
		})
		require.NoError(t, err)
		require.Len(t, tsOut.ResultList, 1)
	})

	t.Run("resource_policy", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestComprehendSDKClient(t, h)
		ctx := t.Context()

		resourceARN := "arn:aws:comprehend:us-east-1:123456789012:document-classifier/my-classifier"

		putOut, err := client.PutResourcePolicy(ctx, &comprehend.PutResourcePolicyInput{
			ResourceArn:    aws.String(resourceARN),
			ResourcePolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.NoError(t, err)
		require.NotNil(t, putOut.PolicyRevisionId)

		descOut, err := client.DescribeResourcePolicy(ctx, &comprehend.DescribeResourcePolicyInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, aws.ToString(descOut.ResourcePolicy))
		assert.Equal(t, aws.ToString(putOut.PolicyRevisionId), aws.ToString(descOut.PolicyRevisionId))
	})

	t.Run("model_import_and_summaries", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestComprehendSDKClient(t, h)
		ctx := t.Context()

		docClassifierSourceARN := "arn:aws:comprehend:us-east-1:111111111111:document-classifier/source-model/version/v1"

		importOut, err := client.ImportModel(ctx, &comprehend.ImportModelInput{
			SourceModelArn:    aws.String(docClassifierSourceARN),
			DataAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/comprehend"),
			ModelName:         aws.String("imported-classifier"),
		})
		require.NoError(t, err)
		require.NotNil(t, importOut.ModelArn)

		listDCOut, err := client.ListDocumentClassifierSummaries(
			ctx,
			&comprehend.ListDocumentClassifierSummariesInput{},
		)
		require.NoError(t, err)
		require.Len(t, listDCOut.DocumentClassifierSummariesList, 1)
		assert.Equal(
			t,
			"imported-classifier",
			aws.ToString(listDCOut.DocumentClassifierSummariesList[0].DocumentClassifierName),
		)

		entRecSourceARN := "arn:aws:comprehend:us-east-1:111111111111:entity-recognizer/source-recognizer/version/v1"

		_, err = client.ImportModel(ctx, &comprehend.ImportModelInput{
			SourceModelArn:    aws.String(entRecSourceARN),
			DataAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/comprehend"),
			ModelName:         aws.String("imported-recognizer"),
		})
		require.NoError(t, err)

		listEROut, err := client.ListEntityRecognizerSummaries(ctx, &comprehend.ListEntityRecognizerSummariesInput{})
		require.NoError(t, err)
		require.Len(t, listEROut.EntityRecognizerSummariesList, 1)
		assert.Equal(t, "imported-recognizer", aws.ToString(listEROut.EntityRecognizerSummariesList[0].RecognizerName))

		_, err = client.StopTrainingDocumentClassifier(ctx, &comprehend.StopTrainingDocumentClassifierInput{
			DocumentClassifierArn: importOut.ModelArn,
		})
		require.NoError(t, err)

		_, err = client.StopTrainingEntityRecognizer(ctx, &comprehend.StopTrainingEntityRecognizerInput{
			EntityRecognizerArn: aws.String(
				"arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/imported-recognizer",
			),
		})
		require.NoError(t, err)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestComprehendSDKClient(t, h)
		ctx := t.Context()

		_, err := client.ImportModel(ctx, &comprehend.ImportModelInput{
			SourceModelArn:    aws.String("arn:aws:comprehend:us-east-1:111111111111:document-classifier/source"),
			DataAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/comprehend"),
			ModelName:         aws.String("tag-target"),
		})
		require.NoError(t, err)

		resourceARN := "arn:aws:comprehend:us-east-1:123456789012:document-classifier/tag-target"

		_, err = client.TagResource(ctx, &comprehend.TagResourceInput{
			ResourceArn: aws.String(resourceARN),
			Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
		})
		require.NoError(t, err)

		listOut, err := client.ListTagsForResource(ctx, &comprehend.ListTagsForResourceInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		require.Len(t, listOut.Tags, 1)
		assert.Equal(t, "env", aws.ToString(listOut.Tags[0].Key))

		_, err = client.UntagResource(ctx, &comprehend.UntagResourceInput{
			ResourceArn: aws.String(resourceARN),
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		listOut2, err := client.ListTagsForResource(ctx, &comprehend.ListTagsForResourceInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		assert.Empty(t, listOut2.Tags)
	})
}
