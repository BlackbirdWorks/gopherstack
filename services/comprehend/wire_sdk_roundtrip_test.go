package comprehend_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	comprehendsdk "github.com/aws/aws-sdk-go-v2/service/comprehend"
	"github.com/aws/aws-sdk-go-v2/service/comprehend/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// newTestComprehendSDKClient stands up the real aws-sdk-go-v2 comprehend
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production -- so a
// shape is verified by the real client's own
// awsAwsjson11_deserializeOpDocument<Op>Output, not gopherstack's own map
// keys.
func newTestComprehendSDKClient(t *testing.T, h *comprehend.Handler) *comprehendsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return comprehendsdk.NewFromConfig(cfg, func(o *comprehendsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestDetectTargetedSentiment_EntityShapeSDKRoundTrip proves
// DetectTargetedSentimentOutput.Entities is types.TargetedSentimentEntity
// (aws-sdk-go-v2/service/comprehend@v1.43.4 types/types.go:2799 -- only
// DescriptiveMentionIndex+Mentions), NOT a flattened Text/Score/
// BeginOffset/EndOffset/Type/Mentions shape. Before this fix, handler_
// detection.go's detectTargetedSentiment built each entity by starting
// from matchResult() (Text/Score/BeginOffset/EndOffset/Type) and bolting a
// "Mentions" key onto it -- those five extra top-level keys don't exist on
// types.TargetedSentimentEntity at all (confirmed against deserializers.go:
// 20032's awsAwsjson11_deserializeDocumentTargetedSentimentEntity, whose
// switch only recognizes "DescriptiveMentionIndex"/"Mentions" and silently
// drops any other key via its default case) -- and the real per-mention
// fields (types.TargetedSentimentMention: Text/Type/Score/BeginOffset/
// EndOffset/MentionSentiment, types/types.go:2821) were duplicated by hand
// instead of derived from the same mention object.
func TestDetectTargetedSentiment_EntityShapeSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	out, err := client.DetectTargetedSentiment(t.Context(), &comprehendsdk.DetectTargetedSentimentInput{
		LanguageCode: "en",
		Text:         aws.String("Alice loves the new product."),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Entities)

	entity := out.Entities[0]
	require.NotEmpty(t, entity.Mentions, "TargetedSentimentEntity.Mentions must be populated")
	require.NotEmpty(t, entity.DescriptiveMentionIndex, "DescriptiveMentionIndex must point into Mentions")
	assert.Equal(t, int32(0), entity.DescriptiveMentionIndex[0])

	mention := entity.Mentions[0]
	assert.NotEmpty(t, aws.ToString(mention.Text), "TargetedSentimentMention.Text must be populated")
	assert.NotNil(t, mention.Score)
	assert.NotNil(t, mention.BeginOffset)
	assert.NotNil(t, mention.EndOffset)
	require.NotNil(t, mention.MentionSentiment)
	assert.NotEmpty(t, string(mention.MentionSentiment.Sentiment))
}

// TestPutResourcePolicy_RevisionMismatchSDKRoundTrip proves a stale
// PolicyRevisionId on PutResourcePolicy surfaces as
// types.InvalidRequestException, not types.ResourceInUseException.
// PutResourcePolicy's own awsAwsjson11_deserializeOpErrorPutResourcePolicy
// switch (aws-sdk-go-v2/service/comprehend@v1.43.4 deserializers.go) types
// only InternalServerException, InvalidRequestException and
// ResourceNotFoundException -- ResourceInUseException is absent, so a
// ResourceInUseException wire code decodes to an untyped
// smithy.GenericAPIError and errors.As into *types.ResourceInUseException
// fails.
func TestPutResourcePolicy_RevisionMismatchSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	arn := "arn:aws:comprehend:us-east-1:000000000000:document-classifier/test"

	_, err := client.PutResourcePolicy(t.Context(), &comprehendsdk.PutResourcePolicyInput{
		ResourceArn:    aws.String(arn),
		ResourcePolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.PutResourcePolicy(t.Context(), &comprehendsdk.PutResourcePolicyInput{
		ResourceArn:      aws.String(arn),
		ResourcePolicy:   aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		PolicyRevisionId: aws.String("stale-revision"),
	})
	require.Error(t, err)

	var invalidRequest *types.InvalidRequestException
	require.ErrorAs(t, err, &invalidRequest)

	var resourceInUse *types.ResourceInUseException
	assert.NotErrorAs(t, err, &resourceInUse)
}

// TestDeleteResourcePolicy_RevisionMismatchSDKRoundTrip is the DeleteResourcePolicy
// counterpart: its deserializeOpError switch models the identical three
// codes as PutResourcePolicy's, also excluding ResourceInUseException.
func TestDeleteResourcePolicy_RevisionMismatchSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := comprehend.NewInMemoryBackend("000000000000", "us-east-1")
	h := comprehend.NewHandler(backend)
	client := newTestComprehendSDKClient(t, h)

	arn := "arn:aws:comprehend:us-east-1:000000000000:document-classifier/test"

	_, err := client.PutResourcePolicy(t.Context(), &comprehendsdk.PutResourcePolicyInput{
		ResourceArn:    aws.String(arn),
		ResourcePolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteResourcePolicy(t.Context(), &comprehendsdk.DeleteResourcePolicyInput{
		ResourceArn:      aws.String(arn),
		PolicyRevisionId: aws.String("stale-revision"),
	})
	require.Error(t, err)

	var invalidRequest *types.InvalidRequestException
	require.ErrorAs(t, err, &invalidRequest)

	var resourceInUse *types.ResourceInUseException
	assert.NotErrorAs(t, err, &resourceInUse)
}
