//go:build integration
// +build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mwaaSDK "github.com/aws/aws-sdk-go-v2/service/mwaa"
	mwaaSDKtypes "github.com/aws/aws-sdk-go-v2/service/mwaa/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMWAAClient returns an MWAA client pointed at the shared test container.
func createMWAAClient(t *testing.T) *mwaaSDK.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mwaaSDK.NewFromConfig(cfg, func(o *mwaaSDK.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MWAA_EnvironmentLifecycle tests environment creation, retrieval, and deletion.
func TestIntegration_MWAA_EnvironmentLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
	}{
		{
			name:    "full_lifecycle",
			envName: "int-test-mwaa-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMWAAClient(t)

			uniqueName := tt.envName + "-" + t.Name()

			// CreateEnvironment.
			createOut, err := client.CreateEnvironment(ctx, &mwaaSDK.CreateEnvironmentInput{
				Name:             aws.String(uniqueName),
				DagS3Path:        aws.String("dags/"),
				ExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/mwaa-role"),
				SourceBucketArn:  aws.String("arn:aws:s3:::my-mwaa-bucket"),
				NetworkConfiguration: &mwaaSDKtypes.NetworkConfiguration{
					SecurityGroupIds: []string{"sg-12345678"},
					SubnetIds:        []string{"subnet-12345678", "subnet-87654321"},
				},
			})
			require.NoError(t, err, "CreateEnvironment should succeed")
			require.NotNil(t, createOut.Arn)
			assert.NotEmpty(t, aws.ToString(createOut.Arn))

			envARN := aws.ToString(createOut.Arn)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteEnvironment(cleanupCtx, &mwaaSDK.DeleteEnvironmentInput{
					Name: aws.String(uniqueName),
				})
			})

			// GetEnvironment.
			getOut, err := client.GetEnvironment(ctx, &mwaaSDK.GetEnvironmentInput{
				Name: aws.String(uniqueName),
			})
			require.NoError(t, err, "GetEnvironment should succeed")
			require.NotNil(t, getOut.Environment)
			assert.Equal(t, uniqueName, aws.ToString(getOut.Environment.Name))
			assert.Equal(t, "AVAILABLE", string(getOut.Environment.Status))

			// ListEnvironments.
			listOut, err := client.ListEnvironments(ctx, &mwaaSDK.ListEnvironmentsInput{})
			require.NoError(t, err, "ListEnvironments should succeed")

			found := false
			for _, name := range listOut.Environments {
				if name == uniqueName {
					found = true

					break
				}
			}
			assert.True(t, found, "environment should appear in ListEnvironments")

			// TagResource.
			_, err = client.TagResource(ctx, &mwaaSDK.TagResourceInput{
				ResourceArn: aws.String(envARN),
				Tags:        map[string]string{"owner": "integration-test"},
			})
			require.NoError(t, err, "TagResource should succeed")

			// ListTagsForResource.
			tagsOut, err := client.ListTagsForResource(ctx, &mwaaSDK.ListTagsForResourceInput{
				ResourceArn: aws.String(envARN),
			})
			require.NoError(t, err, "ListTagsForResource should succeed")
			assert.Equal(t, "integration-test", tagsOut.Tags["owner"])

			// UntagResource.
			_, err = client.UntagResource(ctx, &mwaaSDK.UntagResourceInput{
				ResourceArn: aws.String(envARN),
				TagKeys:     []string{"owner"},
			})
			require.NoError(t, err, "UntagResource should succeed")

			// DeleteEnvironment.
			_, err = client.DeleteEnvironment(ctx, &mwaaSDK.DeleteEnvironmentInput{
				Name: aws.String(uniqueName),
			})
			require.NoError(t, err, "DeleteEnvironment should succeed")
		})
	}
}

// TestIntegration_MWAA_InvokeRestApi tests the InvokeRestApi operation.
func TestIntegration_MWAA_InvokeRestApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
		method  mwaaSDKtypes.RestApiMethod
		apiPath string
		wantErr bool
	}{
		{
			name:    "invoke_get_request",
			envName: "rest-api-env-get",
			method:  mwaaSDKtypes.RestApiMethodGet,
			apiPath: "/dags",
		},
		{
			name:    "invoke_post_request",
			envName: "rest-api-env-post",
			method:  mwaaSDKtypes.RestApiMethodPost,
			apiPath: "/dags/123/dagRuns",
		},
		{
			name:    "not_found_returns_error",
			envName: "nonexistent-rest-api-env",
			method:  mwaaSDKtypes.RestApiMethodGet,
			apiPath: "/dags",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMWAAClient(t)

			if !tt.wantErr {
				uniqueName := tt.envName + "-" + t.Name()
				_, err := client.CreateEnvironment(ctx, &mwaaSDK.CreateEnvironmentInput{
					Name:             aws.String(uniqueName),
					DagS3Path:        aws.String("dags/"),
					ExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/mwaa-role"),
					SourceBucketArn:  aws.String("arn:aws:s3:::my-mwaa-bucket"),
				})
				require.NoError(t, err, "CreateEnvironment should succeed")

				t.Cleanup(func() {
					cleanupCtx, cancel := cleanupContext(t)
					defer cancel()

					_, _ = client.DeleteEnvironment(cleanupCtx, &mwaaSDK.DeleteEnvironmentInput{
						Name: aws.String(uniqueName),
					})
				})

				out, err := client.InvokeRestApi(ctx, &mwaaSDK.InvokeRestApiInput{
					Name:   aws.String(uniqueName),
					Method: tt.method,
					Path:   aws.String(tt.apiPath),
				})
				require.NoError(t, err, "InvokeRestApi should succeed")
				require.NotNil(t, out.RestApiStatusCode)
				assert.Equal(t, int32(200), aws.ToInt32(out.RestApiStatusCode))
			} else {
				_, err := client.InvokeRestApi(ctx, &mwaaSDK.InvokeRestApiInput{
					Name:   aws.String(tt.envName),
					Method: tt.method,
					Path:   aws.String(tt.apiPath),
				})
				require.Error(t, err, "InvokeRestApi on nonexistent env should fail")
			}
		})
	}
}

// TestIntegration_MWAA_PublishMetrics tests the PublishMetrics operation.
func TestIntegration_MWAA_PublishMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		metrics []mwaaSDKtypes.MetricDatum
		name    string
		envName string
		wantErr bool
	}{
		{
			name:    "publish_metrics",
			envName: "metrics-env",
			metrics: []mwaaSDKtypes.MetricDatum{
				{
					MetricName: aws.String("TaskInstance"),
					Timestamp:  aws.Time(time.Now()),
					Value:      aws.Float64(1.0),
					Unit:       mwaaSDKtypes.UnitCount,
				},
			},
		},
		{
			name:    "not_found_returns_error",
			envName: "nonexistent-metrics-env",
			metrics: []mwaaSDKtypes.MetricDatum{
				{
					MetricName: aws.String("TaskInstance"),
					Timestamp:  aws.Time(time.Now()),
					Value:      aws.Float64(1.0),
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMWAAClient(t)

			if !tt.wantErr {
				uniqueName := tt.envName + "-" + t.Name()
				_, err := client.CreateEnvironment(ctx, &mwaaSDK.CreateEnvironmentInput{
					Name:             aws.String(uniqueName),
					DagS3Path:        aws.String("dags/"),
					ExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/mwaa-role"),
					SourceBucketArn:  aws.String("arn:aws:s3:::my-mwaa-bucket"),
				})
				require.NoError(t, err, "CreateEnvironment should succeed")

				t.Cleanup(func() {
					cleanupCtx, cancel := cleanupContext(t)
					defer cancel()

					_, _ = client.DeleteEnvironment(cleanupCtx, &mwaaSDK.DeleteEnvironmentInput{
						Name: aws.String(uniqueName),
					})
				})

				_, err = client.PublishMetrics(ctx, &mwaaSDK.PublishMetricsInput{
					EnvironmentName: aws.String(uniqueName),
					MetricData:      tt.metrics,
				})
				require.NoError(t, err, "PublishMetrics should succeed")
			} else {
				_, err := client.PublishMetrics(ctx, &mwaaSDK.PublishMetricsInput{
					EnvironmentName: aws.String(tt.envName),
					MetricData:      tt.metrics,
				})
				require.Error(t, err, "PublishMetrics on nonexistent env should fail")
			}
		})
	}
}
