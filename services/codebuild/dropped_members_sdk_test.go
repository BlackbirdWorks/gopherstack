package codebuild_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func newBatchClient(t *testing.T) (*codebuildsdk.Client, string) {
	t.Helper()

	client := newTestCodeBuildClient(t, codebuild.NewHandler(codebuild.NewInMemoryBackend("000000000000", "us-east-1")))
	name, _ := createRealClientProject(t, client, "idem-proj", buildListTwoNodesSpec)

	return client, name
}

func TestSDK_IdempotencyTokenReplaysAndRejectsMismatch(t *testing.T) {
	t.Parallel()

	type callFn func(token string, variant bool) (string, error)

	tests := []struct {
		// prepare returns a call that yields the ID of the resource a token produced.
		prepare func(t *testing.T, c *codebuildsdk.Client, project string) callFn
		name    string
	}{
		{
			name: "start build",
			prepare: func(t *testing.T, c *codebuildsdk.Client, project string) callFn {
				t.Helper()

				return func(token string, variant bool) (string, error) {
					in := &codebuildsdk.StartBuildInput{
						ProjectName: aws.String(project), IdempotencyToken: aws.String(token),
					}
					if variant {
						in.ImageOverride = aws.String("aws/codebuild/standard:6.0")
					}

					out, err := c.StartBuild(t.Context(), in)
					if err != nil {
						return "", err
					}

					return aws.ToString(out.Build.Id), nil
				}
			},
		},
		{
			name: "start build batch",
			prepare: func(t *testing.T, c *codebuildsdk.Client, project string) callFn {
				t.Helper()

				return func(token string, variant bool) (string, error) {
					in := &codebuildsdk.StartBuildBatchInput{
						ProjectName: aws.String(project), IdempotencyToken: aws.String(token),
					}
					if variant {
						in.ImageOverride = aws.String("aws/codebuild/standard:6.0")
					}

					out, err := c.StartBuildBatch(t.Context(), in)
					if err != nil {
						return "", err
					}

					return aws.ToString(out.BuildBatch.Id), nil
				}
			},
		},
		{
			name: "start sandbox",
			prepare: func(t *testing.T, c *codebuildsdk.Client, project string) callFn {
				t.Helper()

				other, _ := createRealClientProject(t, c, "idem-other", "")

				return func(token string, variant bool) (string, error) {
					name := project
					if variant {
						name = other
					}

					out, err := c.StartSandbox(t.Context(), &codebuildsdk.StartSandboxInput{
						ProjectName: aws.String(name), IdempotencyToken: aws.String(token),
					})
					if err != nil {
						return "", err
					}

					return aws.ToString(out.Sandbox.Id), nil
				}
			},
		},
		{
			name: "retry build",
			prepare: func(t *testing.T, c *codebuildsdk.Client, project string) callFn {
				t.Helper()

				a, err := c.StartBuild(t.Context(), &codebuildsdk.StartBuildInput{ProjectName: aws.String(project)})
				require.NoError(t, err)

				b, err := c.StartBuild(t.Context(), &codebuildsdk.StartBuildInput{ProjectName: aws.String(project)})
				require.NoError(t, err)

				return func(token string, variant bool) (string, error) {
					id := a.Build.Id
					if variant {
						id = b.Build.Id
					}

					out, retryErr := c.RetryBuild(t.Context(), &codebuildsdk.RetryBuildInput{
						Id: id, IdempotencyToken: aws.String(token),
					})
					if retryErr != nil {
						return "", retryErr
					}

					return aws.ToString(out.Build.Id), nil
				}
			},
		},
		{
			name: "retry build batch",
			prepare: func(t *testing.T, c *codebuildsdk.Client, project string) callFn {
				t.Helper()

				start, err := c.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
					ProjectName: aws.String(project),
				})
				require.NoError(t, err)

				_, err = c.StopBuildBatch(t.Context(), &codebuildsdk.StopBuildBatchInput{Id: start.BuildBatch.Id})
				require.NoError(t, err)

				return func(token string, variant bool) (string, error) {
					in := &codebuildsdk.RetryBuildBatchInput{
						Id: start.BuildBatch.Id, IdempotencyToken: aws.String(token),
					}
					if variant {
						in.RetryType = cbtypes.RetryBuildBatchTypeRetryFailedBuilds
					}

					out, retryErr := c.RetryBuildBatch(t.Context(), in)
					if retryErr != nil {
						return "", retryErr
					}

					return aws.ToString(out.BuildBatch.Id), nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, project := newBatchClient(t)
			call := tt.prepare(t, c, project)

			first, err := call("tok", false)
			require.NoError(t, err)

			replay, err := call("tok", false)
			require.NoError(t, err)
			assert.Equal(t, first, replay)

			_, err = call("tok", true)
			require.Error(t, err)

			fresh, err := call("tok-2", false)
			require.NoError(t, err)
			assert.NotEqual(t, first, fresh)
		})
	}
}

func TestSDK_LogsConfigOverrideAndBuildLogs(t *testing.T) {
	t.Parallel()

	override := &cbtypes.LogsConfig{
		CloudWatchLogs: &cbtypes.CloudWatchLogsConfig{
			Status: cbtypes.LogsConfigStatusTypeEnabled, GroupName: aws.String("/custom/group"),
		},
		S3Logs: &cbtypes.S3LogsConfig{
			Status: cbtypes.LogsConfigStatusTypeEnabled, Location: aws.String("bucket/prefix"),
		},
	}

	tests := []struct {
		name      string
		override  *cbtypes.LogsConfig
		wantGroup string
		wantS3    bool
	}{
		{name: "no config and no override", override: nil, wantGroup: ""},
		{name: "override applied", override: override, wantGroup: "/custom/group", wantS3: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, project := newBatchClient(t)

			out, err := c.StartBuild(t.Context(), &codebuildsdk.StartBuildInput{
				ProjectName: aws.String(project), LogsConfigOverride: tt.override,
			})
			require.NoError(t, err)

			got, err := c.BatchGetBuilds(t.Context(), &codebuildsdk.BatchGetBuildsInput{
				Ids: []string{aws.ToString(out.Build.Id)},
			})
			require.NoError(t, err)
			require.Len(t, got.Builds, 1)

			logs := got.Builds[0].Logs

			if tt.override == nil {
				assert.True(t, logs == nil || logs.CloudWatchLogs == nil)

				return
			}

			require.NotNil(t, logs)
			assert.Equal(t, tt.wantGroup, aws.ToString(logs.GroupName))
			require.NotNil(t, logs.CloudWatchLogs)
			assert.Equal(t, cbtypes.LogsConfigStatusTypeEnabled, logs.CloudWatchLogs.Status)
			assert.Equal(t, tt.wantS3, logs.S3Logs != nil)

			batch, err := c.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
				ProjectName: aws.String(project), LogsConfigOverride: tt.override,
			})
			require.NoError(t, err)
			require.NotNil(t, batch.BuildBatch.LogConfig)
			assert.Equal(t, "/custom/group", aws.ToString(batch.BuildBatch.LogConfig.CloudWatchLogs.GroupName))
		})
	}
}

func TestSDK_UpdateProjectVisibilityKeepsResourceAccessRole(t *testing.T) {
	t.Parallel()

	const roleARN = "arn:aws:iam::000000000000:role/logs-reader"

	tests := []struct {
		name string
		role string
		want string
	}{
		{name: "role stored", role: roleARN, want: roleARN},
		{name: "no role leaves unset", role: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newBatchClient(t)
			name, arn := createRealClientProject(t, c, "vis-proj", "")

			in := &codebuildsdk.UpdateProjectVisibilityInput{
				ProjectArn: aws.String(arn), ProjectVisibility: cbtypes.ProjectVisibilityTypePublicRead,
			}
			if tt.role != "" {
				in.ResourceAccessRole = aws.String(tt.role)
			}

			_, err := c.UpdateProjectVisibility(t.Context(), in)
			require.NoError(t, err)

			got, err := c.BatchGetProjects(t.Context(), &codebuildsdk.BatchGetProjectsInput{Names: []string{name}})
			require.NoError(t, err)
			require.Len(t, got.Projects, 1)
			assert.Equal(t, tt.want, aws.ToString(got.Projects[0].ResourceAccessRole))
		})
	}
}
