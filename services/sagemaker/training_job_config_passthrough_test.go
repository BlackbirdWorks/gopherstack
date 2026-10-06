package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTrainingJob_ConfigMembersRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		apply func(in *sagemakersdk.CreateTrainingJobInput)
		check func(t *testing.T, out *sagemakersdk.DescribeTrainingJobOutput)
		name  string
	}{
		{
			name: "retry_strategy",
			apply: func(in *sagemakersdk.CreateTrainingJobInput) {
				in.RetryStrategy = &smtypes.RetryStrategy{MaximumRetryAttempts: aws.Int32(3)}
			},
			check: func(t *testing.T, out *sagemakersdk.DescribeTrainingJobOutput) {
				t.Helper()
				require.NotNil(t, out.RetryStrategy)
				assert.EqualValues(t, 3, aws.ToInt32(out.RetryStrategy.MaximumRetryAttempts))
			},
		},
		{
			name: "experiment_config",
			apply: func(in *sagemakersdk.CreateTrainingJobInput) {
				in.ExperimentConfig = &smtypes.ExperimentConfig{
					ExperimentName: aws.String("exp"), TrialName: aws.String("trial"), RunName: aws.String("run"),
				}
			},
			check: func(t *testing.T, out *sagemakersdk.DescribeTrainingJobOutput) {
				t.Helper()
				require.NotNil(t, out.ExperimentConfig)
				assert.Equal(t, "exp", aws.ToString(out.ExperimentConfig.ExperimentName))
				assert.Equal(t, "run", aws.ToString(out.ExperimentConfig.RunName))
			},
		},
		{
			name: "profiler_and_debug",
			apply: func(in *sagemakersdk.CreateTrainingJobInput) {
				in.ProfilerConfig = &smtypes.ProfilerConfig{
					S3OutputPath: aws.String("s3://b/prof"), ProfilingIntervalInMilliseconds: aws.Int64(500),
				}
				in.DebugHookConfig = &smtypes.DebugHookConfig{
					S3OutputPath: aws.String("s3://b/dbg"), HookParameters: map[string]string{"k": "v"},
				}
				in.RemoteDebugConfig = &smtypes.RemoteDebugConfig{EnableRemoteDebug: aws.Bool(true)}
				in.InfraCheckConfig = &smtypes.InfraCheckConfig{EnableInfraCheck: aws.Bool(true)}
			},
			check: func(t *testing.T, out *sagemakersdk.DescribeTrainingJobOutput) {
				t.Helper()
				require.NotNil(t, out.ProfilerConfig)
				assert.Equal(t, "s3://b/prof", aws.ToString(out.ProfilerConfig.S3OutputPath))
				assert.EqualValues(t, 500, aws.ToInt64(out.ProfilerConfig.ProfilingIntervalInMilliseconds))
				require.NotNil(t, out.DebugHookConfig)
				assert.Equal(t, map[string]string{"k": "v"}, out.DebugHookConfig.HookParameters)
				require.NotNil(t, out.RemoteDebugConfig)
				assert.True(t, aws.ToBool(out.RemoteDebugConfig.EnableRemoteDebug))
				require.NotNil(t, out.InfraCheckConfig)
				assert.True(t, aws.ToBool(out.InfraCheckConfig.EnableInfraCheck))
			},
		},
		{
			name:  "none_supplied",
			apply: func(*sagemakersdk.CreateTrainingJobInput) {},
			check: func(t *testing.T, out *sagemakersdk.DescribeTrainingJobOutput) {
				t.Helper()
				assert.Nil(t, out.RetryStrategy)
				assert.Nil(t, out.ExperimentConfig)
				assert.Nil(t, out.ProfilerConfig)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			in := &sagemakersdk.CreateTrainingJobInput{
				TrainingJobName: aws.String("tj"),
				RoleArn:         aws.String("arn:aws:iam::123456789012:role/r"),
				AlgorithmSpecification: &smtypes.AlgorithmSpecification{
					TrainingImage: aws.String("img"), TrainingInputMode: smtypes.TrainingInputModeFile,
				},
				OutputDataConfig: &smtypes.OutputDataConfig{S3OutputPath: aws.String("s3://b/out")},
				ResourceConfig: &smtypes.ResourceConfig{
					InstanceType:   smtypes.TrainingInstanceTypeMlM5Large,
					InstanceCount:  aws.Int32(1),
					VolumeSizeInGB: aws.Int32(1),
				},
				StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(60)},
			}
			tt.apply(in)

			_, err := client.CreateTrainingJob(t.Context(), in)
			require.NoError(t, err)

			out, err := client.DescribeTrainingJob(t.Context(), &sagemakersdk.DescribeTrainingJobInput{
				TrainingJobName: aws.String("tj"),
			})
			require.NoError(t, err)
			tt.check(t, out)
		})
	}
}
