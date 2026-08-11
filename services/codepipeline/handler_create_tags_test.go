package codepipeline_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// newTestCodePipelineClient stands up the real aws-sdk-go-v2 CodePipeline
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestCodePipelineClient(t *testing.T, h *codepipeline.Handler) *cpsdk.Client {
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

	return cpsdk.NewFromConfig(cfg, func(o *cpsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every CodePipeline create-shaped op
// that accepts Tags in the real SDK (codepipeline@v1.49.4: CreatePipeline,
// CreateCustomActionType, PutWebhook) through a real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	newClient := func(t *testing.T) *cpsdk.Client {
		t.Helper()

		h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))

		return newTestCodePipelineClient(t, h)
	}

	requireTags := func(t *testing.T, client *cpsdk.Client, resourceARN string) {
		t.Helper()

		out, err := client.ListTagsForResource(t.Context(), &cpsdk.ListTagsForResourceInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	}

	t.Run("createpipeline", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreatePipeline(t.Context(), &cpsdk.CreatePipelineInput{
			Pipeline: &types.PipelineDeclaration{
				Name:    aws.String("tagged-pipeline"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/pipeline-role"),
				ArtifactStore: &types.ArtifactStore{
					Type:     types.ArtifactStoreTypeS3,
					Location: aws.String("my-artifact-bucket"),
				},
				Stages: []types.StageDeclaration{
					{
						Name: aws.String("Source"),
						Actions: []types.ActionDeclaration{
							{
								Name: aws.String("SourceAction"),
								ActionTypeId: &types.ActionTypeId{
									Category: types.ActionCategorySource,
									Owner:    types.ActionOwnerAws,
									Provider: aws.String("S3"),
									Version:  aws.String("1"),
								},
							},
						},
					},
				},
			},
			Tags: tags,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)

		requireTags(t, client, "arn:aws:codepipeline:us-east-1:123456789012:tagged-pipeline")
	})

	t.Run("createcustomactiontype", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateCustomActionType(t.Context(), &cpsdk.CreateCustomActionTypeInput{
			Category: types.ActionCategoryBuild,
			Provider: aws.String("tagged-provider"),
			Version:  aws.String("1"),
			InputArtifactDetails: &types.ArtifactDetails{
				MinimumCount: 0,
				MaximumCount: 5,
			},
			OutputArtifactDetails: &types.ArtifactDetails{
				MinimumCount: 0,
				MaximumCount: 5,
			},
			Tags: tags,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)

		arn := "arn:aws:codepipeline:us-east-1:123456789012:actiontype:Custom/Build/tagged-provider/1"
		requireTags(t, client, arn)
	})

	t.Run("putwebhook", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreatePipeline(t.Context(), &cpsdk.CreatePipelineInput{
			Pipeline: &types.PipelineDeclaration{
				Name:    aws.String("webhook-pipeline"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/pipeline-role"),
				ArtifactStore: &types.ArtifactStore{
					Type:     types.ArtifactStoreTypeS3,
					Location: aws.String("my-artifact-bucket"),
				},
				Stages: []types.StageDeclaration{
					{
						Name: aws.String("Source"),
						Actions: []types.ActionDeclaration{
							{
								Name: aws.String("SourceAction"),
								ActionTypeId: &types.ActionTypeId{
									Category: types.ActionCategorySource,
									Owner:    types.ActionOwnerAws,
									Provider: aws.String("S3"),
									Version:  aws.String("1"),
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		out, err := client.PutWebhook(t.Context(), &cpsdk.PutWebhookInput{
			Webhook: &types.WebhookDefinition{
				Name:                        aws.String("tagged-webhook"),
				TargetPipeline:              aws.String("webhook-pipeline"),
				TargetAction:                aws.String("SourceAction"),
				Authentication:              types.WebhookAuthenticationTypeUnauthenticated,
				AuthenticationConfiguration: &types.WebhookAuthConfiguration{},
				Filters: []types.WebhookFilterRule{
					{JsonPath: aws.String("$.ref")},
				},
			},
			Tags: tags,
		})
		require.NoError(t, err)
		require.Len(t, out.Webhook.Tags, 1)

		requireTags(t, client, aws.ToString(out.Webhook.Arn))
	})
}
