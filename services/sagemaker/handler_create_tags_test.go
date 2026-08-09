package sagemaker_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

const smTagsRTRegion = "us-east-1"

// newTestSageMakerClient stands up the real aws-sdk-go-v2 sagemaker client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestSageMakerClient(t *testing.T, h *sagemaker.Handler) *sagemakersdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(smTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sagemakersdk.NewFromConfig(cfg, func(o *sagemakersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip spot-checks resource kinds that ARE in
// findTagMapLocked's 18-kind ARN-index registry (services/sagemaker/tags.go)
// through the real SDK client, asserting ListTags sees what was supplied at
// creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *sagemakersdk.Client) *string
		name  string
	}{
		{
			name: "model",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateModel(t.Context(), &sagemakersdk.CreateModelInput{
					ModelName:        aws.String("tagged-model"),
					ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/access"),
					Tags: []smtypes.Tag{
						{Key: aws.String("env"), Value: aws.String("test")},
					},
				})
				require.NoError(t, err)

				return out.ModelArn
			},
		},
		{
			name: "endpoint config",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateEndpointConfig(
					t.Context(),
					&sagemakersdk.CreateEndpointConfigInput{
						EndpointConfigName: aws.String("tagged-epc"),
						ProductionVariants: []smtypes.ProductionVariant{
							{
								VariantName:          aws.String("v1"),
								ModelName:            aws.String("some-model"),
								InitialInstanceCount: aws.Int32(1),
								InstanceType:         smtypes.ProductionVariantInstanceTypeMlM5Large,
							},
						},
						Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return out.EndpointConfigArn
			},
		},
		{
			name: "algorithm",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateAlgorithm(t.Context(), &sagemakersdk.CreateAlgorithmInput{
					AlgorithmName: aws.String("tagged-algo"),
					TrainingSpecification: &smtypes.TrainingSpecification{
						TrainingImage: aws.String(
							"123456789012.dkr.ecr.us-east-1.amazonaws.com/algo:latest",
						),
						SupportedTrainingInstanceTypes: []smtypes.TrainingInstanceType{
							smtypes.TrainingInstanceTypeMlM5Large,
						},
						TrainingChannels: []smtypes.ChannelSpecification{
							{Name: aws.String("train"), SupportedContentTypes: []string{"text/csv"},
								SupportedInputModes: []smtypes.TrainingInputMode{
									smtypes.TrainingInputModeFile,
								}},
						},
					},
					Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.AlgorithmArn
			},
		},
		{
			name: "action",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateAction(t.Context(), &sagemakersdk.CreateActionInput{
					ActionName: aws.String("tagged-action"),
					ActionType: aws.String("ModelDeployment"),
					Source:     &smtypes.ActionSource{SourceUri: aws.String("s3://bucket/key")},
					Tags:       []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return out.ActionArn
			},
		},
		{
			name: "experiment",
			setup: func(t *testing.T, client *sagemakersdk.Client) *string {
				t.Helper()

				out, err := client.CreateExperiment(
					t.Context(),
					&sagemakersdk.CreateExperimentInput{
						ExperimentName: aws.String("tagged-experiment"),
						Tags: []smtypes.Tag{
							{Key: aws.String("env"), Value: aws.String("test")},
						},
					},
				)
				require.NoError(t, err)

				return out.ExperimentArn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
			client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

			resourceARN := tt.setup(t, client)
			require.NotNil(t, resourceARN)

			got, err := client.ListTags(
				t.Context(),
				&sagemakersdk.ListTagsInput{ResourceArn: resourceARN},
			)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}

// TestCreateWorkteam_TagsRoundTrip_KnownGap documents, rather than fixes,
// gopherstack-2mwl's largest outstanding instance: CreateWorkteam accepts
// Tags (sagemaker@v1.263.2 api_op_CreateWorkteam.go:78), and work teams are
// explicitly listed in the real AddTags doc comment (api_op_AddTags.go:13)
// as a taggable kind, but "workteam" is not one of the 18 kinds in
// findTagMapLocked's ARN-index registry (services/sagemaker/tags.go). 45 of
// sagemaker's 63 tag-accepting Create ops share this shape; workteam stands
// in for all of them here. This asserts the CURRENT (broken) behavior so the
// test goes red -- forcing a deliberate update -- the day the registry gains
// workteam's case, rather than silently drifting.
func TestCreateWorkteam_TagsRoundTrip_KnownGap(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smTagsRTRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	out, err := client.CreateWorkteam(t.Context(), &sagemakersdk.CreateWorkteamInput{
		WorkteamName: aws.String("tagged-workteam"),
		Description:  aws.String("desc"),
		MemberDefinitions: []smtypes.MemberDefinition{
			{CognitoMemberDefinition: &smtypes.CognitoMemberDefinition{
				ClientId:  aws.String("client-1"),
				UserPool:  aws.String("pool-1"),
				UserGroup: aws.String("group-1"),
			}},
		},
		Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	_, err = client.ListTags(t.Context(), &sagemakersdk.ListTagsInput{ResourceArn: out.WorkteamArn})
	require.Error(t, err, "known gap: workteam is absent from findTagMapLocked's registry")
	assert.Contains(t, err.Error(), "not found")
}
