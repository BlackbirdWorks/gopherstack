package bedrock_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// newTestBedrockClient stands up the real aws-sdk-go-v2 Bedrock client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestBedrockClient(t *testing.T, h *bedrock.Handler) *bedrocksdk.Client {
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

	return bedrocksdk.NewFromConfig(cfg, func(o *bedrocksdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives Bedrock Create ops whose real Input
// struct accepts Tags through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
// InferenceProfile, MarketplaceModelEndpoint, ModelCopyJob, ModelImportJob,
// PromptRouter, and ModelCustomizationJob all correctly decoded Tags and
// stored them on the resource's own struct, but the resource kind was
// missing entirely from the tag registry (findTagsByARN/TagResource/
// UntagResource in tags.go), so ListTagsForResource always returned
// ResourceNotFoundException regardless of what was stored -- the iot shape
// from this issue (tags written to the resource's own struct, never reaching
// the map ListTagsForResource actually reads), except here the "map" is a
// switch of resource-kind cases rather than a literal map. Also fixed:
// CreateAutomatedReasoningPolicyVersion (bedrock@v1.66.4
// api_op_CreateAutomatedReasoningPolicyVersion.go confirms a real Tags field)
// never decoded Tags at all, a pure decode-drop.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	requireTags := func(t *testing.T, client *bedrocksdk.Client, resourceARN *string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &bedrocksdk.ListTagsForResourceInput{
			ResourceARN: resourceARN,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	}

	newClient := func(t *testing.T) *bedrocksdk.Client {
		t.Helper()

		return newTestBedrockClient(
			t,
			bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
		)
	}

	t.Run("createinferenceprofile", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateInferenceProfile(
			t.Context(),
			&bedrocksdk.CreateInferenceProfileInput{
				InferenceProfileName: aws.String("tagged-profile"),
				ModelSource: &types.InferenceProfileModelSourceMemberCopyFrom{
					Value: "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
				},
				Tags: tags,
			},
		)
		require.NoError(t, err)
		requireTags(t, client, out.InferenceProfileArn)
	})

	t.Run("createmarketplacemodelendpoint", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateMarketplaceModelEndpoint(
			t.Context(),
			&bedrocksdk.CreateMarketplaceModelEndpointInput{
				EndpointName: aws.String("tagged-endpoint"),
				ModelSourceIdentifier: aws.String(
					"arn:aws:sagemaker:us-east-1:123456789012:hub-content/marketplace/model/x",
				),
				EndpointConfig: &types.EndpointConfigMemberSageMaker{
					Value: types.SageMakerEndpoint{
						ExecutionRole:        aws.String("arn:aws:iam::123456789012:role/exec"),
						InitialInstanceCount: aws.Int32(1),
						InstanceType:         aws.String("ml.m5.xlarge"),
					},
				},
				Tags: tags,
			},
		)
		require.NoError(t, err)
		requireTags(t, client, out.MarketplaceModelEndpoint.EndpointArn)
	})

	t.Run("createmodelcopyjob", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateModelCopyJob(t.Context(), &bedrocksdk.CreateModelCopyJobInput{
			SourceModelArn: aws.String(
				"arn:aws:bedrock:us-east-1:123456789012:custom-model/source-model",
			),
			TargetModelName: aws.String("tagged-copy"),
			TargetModelTags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.JobArn)
	})

	t.Run("createmodelimportjob", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateModelImportJob(t.Context(), &bedrocksdk.CreateModelImportJobInput{
			JobName:           aws.String("tagged-import-job"),
			ImportedModelName: aws.String("tagged-imported-model"),
			RoleArn:           aws.String("arn:aws:iam::123456789012:role/import"),
			ModelDataSource: &types.ModelDataSourceMemberS3DataSource{
				Value: types.S3DataSource{S3Uri: aws.String("s3://bucket/model")},
			},
			JobTags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.JobArn)
	})

	t.Run("createpromptrouter", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreatePromptRouter(t.Context(), &bedrocksdk.CreatePromptRouterInput{
			PromptRouterName: aws.String("tagged-router"),
			Models: []types.PromptRouterTargetModel{
				{
					ModelArn: aws.String(
						"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
					),
				},
			},
			FallbackModel: &types.PromptRouterTargetModel{
				ModelArn: aws.String(
					"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-instant-v1",
				),
			},
			RoutingCriteria: &types.RoutingCriteria{ResponseQualityDifference: aws.Float64(0.1)},
			Tags:            tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.PromptRouterArn)
	})

	t.Run("createmodelcustomizationjob", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateModelCustomizationJob(
			t.Context(),
			&bedrocksdk.CreateModelCustomizationJobInput{
				JobName:             aws.String("tagged-customization-job"),
				CustomModelName:     aws.String("tagged-custom-model"),
				BaseModelIdentifier: aws.String("anthropic.claude-v2"),
				RoleArn:             aws.String("arn:aws:iam::123456789012:role/customize"),
				OutputDataConfig: &types.OutputDataConfig{
					S3Uri: aws.String("s3://bucket/output"),
				},
				TrainingDataConfig: &types.TrainingDataConfig{
					S3Uri: aws.String("s3://bucket/training"),
				},
				JobTags: tags,
			},
		)
		require.NoError(t, err)
		requireTags(t, client, out.JobArn)
	})

	t.Run("createautomatedreasoningpolicyversion", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		policy, err := client.CreateAutomatedReasoningPolicy(
			t.Context(),
			&bedrocksdk.CreateAutomatedReasoningPolicyInput{
				Name: aws.String("tagged-arp"),
			},
		)
		require.NoError(t, err)

		out, err := client.CreateAutomatedReasoningPolicyVersion(
			t.Context(),
			&bedrocksdk.CreateAutomatedReasoningPolicyVersionInput{
				PolicyArn:                 policy.PolicyArn,
				LastUpdatedDefinitionHash: policy.DefinitionHash,
				Tags:                      tags,
			},
		)
		require.NoError(t, err)
		requireTags(t, client, out.PolicyArn)
	})
}
