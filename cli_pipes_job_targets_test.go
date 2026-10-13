package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/pipes"
	ptypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

func TestPipesJobTargets(t *testing.T) {
	t.Parallel()

	const account = "000000000000"

	tests := []struct {
		setup  func(t *testing.T, fx *sfnFixture) (string, *ptypes.PipeTargetParameters)
		landed func(t *testing.T, fx *sfnFixture) bool
		name   string
	}{
		{
			setup: func(t *testing.T, fx *sfnFixture) (string, *ptypes.PipeTargetParameters) {
				t.Helper()

				jd := setupBatchQueue(t, fx)

				return "arn:aws:batch:us-east-1:" + account + ":job-queue/eb-queue", &ptypes.PipeTargetParameters{
					BatchJobParameters: &ptypes.PipeTargetBatchJobParameters{
						JobDefinition: aws.String(jd), JobName: aws.String("pipe-job"),
					},
				}
			},
			landed: batchJobExists,
			name:   "batch",
		},
		{
			setup: func(t *testing.T, fx *sfnFixture) (string, *ptypes.PipeTargetParameters) {
				t.Helper()

				setupSageMakerPipeline(t, fx)

				return "arn:aws:sagemaker:us-east-1:" + account + ":pipeline/eb-pipe", &ptypes.PipeTargetParameters{
					SageMakerPipelineParameters: &ptypes.PipeTargetSageMakerPipelineParameters{},
				}
			},
			landed: sageMakerExecutionExists,
			name:   "sagemaker_pipeline",
		},
		{
			setup: func(t *testing.T, _ *sfnFixture) (string, *ptypes.PipeTargetParameters) {
				t.Helper()

				return "arn:aws:redshift:us-east-1:" + account + ":cluster:pipe-cluster", &ptypes.PipeTargetParameters{
					RedshiftDataParameters: &ptypes.PipeTargetRedshiftDataParameters{
						Database: aws.String("dev"), DbUser: aws.String("admin"), Sqls: []string{"select 1"},
					},
				}
			},
			landed: redshiftStatementExists,
			name:   "redshift_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "Pipes")

			target, params := tt.setup(t, fx)
			srcURL, srcARN := authzQueue(t, fx, "pipe-src")

			_, err := pipes.NewFromConfig(fx.cfg).CreatePipe(t.Context(), &pipes.CreatePipeInput{
				Name: aws.String("p"), RoleArn: aws.String("arn:aws:iam::" + account + ":role/pipe"),
				Source: aws.String(srcARN), Target: aws.String(target), TargetParameters: params,
			})
			require.NoError(t, err)

			_, err = sqs.NewFromConfig(fx.cfg).SendMessage(t.Context(), &sqs.SendMessageInput{
				QueueUrl: aws.String(srcURL), MessageBody: aws.String("{}"),
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool { return tt.landed(t, fx) }, authzDeadline, authzTick)
		})
	}
}
