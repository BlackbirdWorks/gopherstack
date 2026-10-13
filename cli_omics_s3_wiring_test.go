package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	omicstypes "github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	omicsbackend "github.com/blackbirdworks/gopherstack/services/omics"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

func newOmicsSDKClient(t *testing.T, h *omicsbackend.Handler) *omicssdk.Client {
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
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return omicssdk.NewFromConfig(cfg, func(o *omicssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("NoHostPrefix",
				func(
					ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
				) (middleware.InitializeOutput, middleware.Metadata, error) {
					return next.HandleInitialize(smithyhttp.DisableEndpointHostPrefix(ctx, true), in)
				}), middleware.Before)
		})
	})
}

func TestInitializeServices_OmicsS3Wiring(t *testing.T) {
	t.Parallel()

	const (
		bucket   = "omics-wiring"
		roleARN  = "arn:aws:iam::000000000000:role/omics"
		settings = `[{"runSettingId":"a","name":"run-a"},{"runSettingId":"b","name":"run-b"}]`
		registry = `{"registryMappings":[{"upstreamRegistryUrl":"registry-1.docker.io","ecrRepositoryPrefix":"docker-hub"}]}`
	)

	tests := []struct {
		run  func(ctx context.Context, t *testing.T, c *omicssdk.Client, wfID string)
		name string
	}{
		{
			name: "batch-s3-uri-settings",
			run: func(ctx context.Context, t *testing.T, c *omicssdk.Client, wfID string) {
				t.Helper()

				started, err := c.StartRunBatch(ctx, &omicssdk.StartRunBatchInput{
					RequestId: aws.String("batch-s3"),
					DefaultRunSetting: &omicstypes.DefaultRunSetting{
						RoleArn:    aws.String(roleARN),
						WorkflowId: aws.String(wfID),
					},
					BatchRunSettings: &omicstypes.BatchRunSettingsMemberS3UriSettings{
						Value: "s3://" + bucket + "/settings.json",
					},
				})
				require.NoError(t, err)

				got, err := c.GetBatch(ctx, &omicssdk.GetBatchInput{BatchId: started.Id})
				require.NoError(t, err)
				assert.EqualValues(t, 2, aws.ToInt32(got.TotalRuns))
			},
		},
		{
			name: "batch-s3-missing-object",
			run: func(ctx context.Context, t *testing.T, c *omicssdk.Client, wfID string) {
				t.Helper()

				_, err := c.StartRunBatch(ctx, &omicssdk.StartRunBatchInput{
					RequestId: aws.String("batch-missing"),
					DefaultRunSetting: &omicstypes.DefaultRunSetting{
						RoleArn:    aws.String(roleARN),
						WorkflowId: aws.String(wfID),
					},
					BatchRunSettings: &omicstypes.BatchRunSettingsMemberS3UriSettings{
						Value: "s3://" + bucket + "/nope.json",
					},
				})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")
			},
		},
		{
			name: "workflow-readme-and-registry-uri",
			run: func(ctx context.Context, t *testing.T, c *omicssdk.Client, _ string) {
				t.Helper()

				wf, err := c.CreateWorkflow(ctx, &omicssdk.CreateWorkflowInput{
					Name:                    aws.String("wf-uri"),
					Engine:                  omicstypes.WorkflowEngineWdl,
					RequestId:               aws.String("wf-uri"),
					ReadmeUri:               aws.String("s3://" + bucket + "/README.md"),
					ContainerRegistryMapUri: aws.String("s3://" + bucket + "/registry.json"),
				})
				require.NoError(t, err)

				got, err := c.GetWorkflow(ctx, &omicssdk.GetWorkflowInput{Id: wf.Id})
				require.NoError(t, err)
				assert.Equal(t, "# From S3", aws.ToString(got.Readme))
				require.NotNil(t, got.ContainerRegistryMap)
				require.Len(t, got.ContainerRegistryMap.RegistryMappings, 1)
				assert.Equal(
					t,
					"docker-hub",
					aws.ToString(got.ContainerRegistryMap.RegistryMappings[0].EcrRepositoryPrefix),
				)
			},
		},
		{
			name: "workflow-version-readme-uri-missing",
			run: func(ctx context.Context, t *testing.T, c *omicssdk.Client, wfID string) {
				t.Helper()

				_, err := c.CreateWorkflowVersion(ctx, &omicssdk.CreateWorkflowVersionInput{
					WorkflowId:  aws.String(wfID),
					VersionName: aws.String("v1"),
					RequestId:   aws.String("v1"),
					ReadmeUri:   aws.String("s3://" + bucket + "/absent.md"),
				})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			services, err := initializeServices(newTestAppContext(t, 19600, 19700))
			require.NoError(t, err)

			byName := serviceByName(services)

			omicsH, ok := byName["Omics"].(*omicsbackend.Handler)
			require.True(t, ok)

			s3H, ok := byName["S3"].(*s3backend.S3Handler)
			require.True(t, ok)

			s3Bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
			require.True(t, ok)

			ctx := t.Context()

			_, err = s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			for key, body := range map[string]string{
				"settings.json": settings, "README.md": "# From S3", "registry.json": registry,
			} {
				_, err = s3Bk.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(body)),
				})
				require.NoError(t, err)
			}

			client := newOmicsSDKClient(t, omicsH)

			wf, err := client.CreateWorkflow(ctx, &omicssdk.CreateWorkflowInput{
				Name: aws.String("base"), Engine: omicstypes.WorkflowEngineWdl, RequestId: aws.String("base"),
			})
			require.NoError(t, err)

			tt.run(ctx, t, client, aws.ToString(wf.Id))
		})
	}
}
