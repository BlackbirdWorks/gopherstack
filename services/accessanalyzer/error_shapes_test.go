package accessanalyzer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestGenerateFindingRecommendation_RealSDKClient_Succeeds proves
// GenerateFindingRecommendation works for a finding that actually exists.
// AnalyzerArn travels as a query parameter, not a JSON body member --
// awsRestjson1_serializeOpHttpBindingsGenerateFindingRecommendationInput
// (aws-sdk-go-v2/service/accessanalyzer@v1.51.4 serializers.go) sends zero
// body bytes for this operation. The handler previously decoded AnalyzerArn
// from the body, so json.Unmarshal on the empty body failed on every real
// call and every call -- regardless of whether the finding existed --
// returned ValidationException instead of succeeding.
func TestGenerateFindingRecommendation_RealSDKClient_Succeeds(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)
	client := newTestAccessAnalyzerClient(t, h)

	analyzer, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("sdk-recommendation-analyzer"),
		Type:         aatypes.TypeAccount,
	})
	require.NoError(t, err)

	findingID := accessanalyzer.SeedFinding(b, "sdk-recommendation-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::bucket")
	require.NotEmpty(t, findingID)

	_, err = client.GenerateFindingRecommendation(t.Context(), &aasdk.GenerateFindingRecommendationInput{
		AnalyzerArn: analyzer.Arn,
		Id:          aws.String(findingID),
	})
	require.NoError(t, err)
}

// TestNotFoundOpsWithoutModeledResourceNotFound proves that ListArchiveRules,
// GenerateFindingRecommendation, GetGeneratedPolicy and CancelPolicyGeneration
// report an unrecognized identifier as ValidationException, not
// ResourceNotFoundException. Their own deserializeOpError switches
// (aws-sdk-go-v2/service/accessanalyzer@v1.51.4 deserializers.go) do not type
// ResourceNotFoundException, so a client calling
// errors.As(err, &types.ResourceNotFoundException{}) would never match a
// ResourceNotFoundException body -- it would fall through to
// smithy.GenericAPIError instead. Matches the ValidationException precedent
// documented on account's errRegionNotFound.
func TestNotFoundOpsWithoutModeledResourceNotFound(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)
	client := newTestAccessAnalyzerClient(t, h)

	t.Run("ListArchiveRules unknown analyzer", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListArchiveRules(t.Context(), &aasdk.ListArchiveRulesInput{
			AnalyzerName: aws.String("no-such-analyzer"),
		})

		var ve *aatypes.ValidationException
		require.ErrorAs(t, err, &ve)

		var nfe *aatypes.ResourceNotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("GenerateFindingRecommendation unknown finding", func(t *testing.T) {
		t.Parallel()

		_, err := client.GenerateFindingRecommendation(t.Context(), &aasdk.GenerateFindingRecommendationInput{
			AnalyzerArn: aws.String("arn:aws:access-analyzer:us-east-1:000000000000:analyzer/does-not-matter"),
			Id:          aws.String("no-such-finding"),
		})

		var ve *aatypes.ValidationException
		require.ErrorAs(t, err, &ve)

		var nfe *aatypes.ResourceNotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("GetGeneratedPolicy unknown job", func(t *testing.T) {
		t.Parallel()

		_, err := client.GetGeneratedPolicy(t.Context(), &aasdk.GetGeneratedPolicyInput{
			JobId: aws.String("no-such-job"),
		})

		var ve *aatypes.ValidationException
		require.ErrorAs(t, err, &ve)

		var nfe *aatypes.ResourceNotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("CancelPolicyGeneration unknown job", func(t *testing.T) {
		t.Parallel()

		_, err := client.CancelPolicyGeneration(t.Context(), &aasdk.CancelPolicyGenerationInput{
			JobId: aws.String("no-such-job"),
		})

		var ve *aatypes.ValidationException
		require.ErrorAs(t, err, &ve)

		var nfe *aatypes.ResourceNotFoundException
		require.NotErrorAs(t, err, &nfe)
	})
}
