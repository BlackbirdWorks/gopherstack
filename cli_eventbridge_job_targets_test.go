package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchtypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker"
	"github.com/stretchr/testify/require"
)

func TestEventBridgeJobTargets(t *testing.T) {
	t.Parallel()

	const account = "000000000000"

	tests := []struct {
		setup  func(t *testing.T, fx *sfnFixture) ebtypes.Target
		landed func(t *testing.T, fx *sfnFixture) bool
		name   string
	}{
		{
			name: "codebuild",
			setup: func(t *testing.T, fx *sfnFixture) ebtypes.Target {
				t.Helper()

				_, err := codebuild.NewFromConfig(fx.cfg).CreateProject(t.Context(), &codebuild.CreateProjectInput{
					Name:        aws.String("eb-proj"),
					ServiceRole: aws.String("arn:aws:iam::" + account + ":role/cb"),
					Source: &cbtypes.ProjectSource{
						Type:      cbtypes.SourceTypeNoSource,
						Buildspec: aws.String("version: 0.2"),
					},
					Artifacts: &cbtypes.ProjectArtifacts{Type: cbtypes.ArtifactsTypeNoArtifacts},
					Environment: &cbtypes.ProjectEnvironment{
						Type:        cbtypes.EnvironmentTypeLinuxContainer,
						Image:       aws.String("aws/codebuild/standard:7.0"),
						ComputeType: cbtypes.ComputeTypeBuildGeneral1Small,
					},
				})
				require.NoError(t, err)

				return ebtypes.Target{Arn: aws.String("arn:aws:codebuild:us-east-1:" + account + ":project/eb-proj")}
			},
			landed: func(t *testing.T, fx *sfnFixture) bool {
				t.Helper()

				out, err := codebuild.NewFromConfig(fx.cfg).ListBuildsForProject(
					t.Context(), &codebuild.ListBuildsForProjectInput{ProjectName: aws.String("eb-proj")})

				return err == nil && len(out.Ids) > 0
			},
		},
		{
			name: "batch",
			setup: func(t *testing.T, fx *sfnFixture) ebtypes.Target {
				t.Helper()

				return ebtypes.Target{
					Arn: aws.String("arn:aws:batch:us-east-1:" + account + ":job-queue/eb-queue"),
					BatchParameters: &ebtypes.BatchParameters{
						JobDefinition: aws.String(setupBatchQueue(t, fx)), JobName: aws.String("eb-job"),
					},
				}
			},
			landed: batchJobExists,
		},
		{
			name: "sagemaker_pipeline",
			setup: func(t *testing.T, fx *sfnFixture) ebtypes.Target {
				t.Helper()

				setupSageMakerPipeline(t, fx)

				return ebtypes.Target{
					Arn: aws.String("arn:aws:sagemaker:us-east-1:" + account + ":pipeline/eb-pipe"),
					SageMakerPipelineParameters: &ebtypes.SageMakerPipelineParameters{
						PipelineParameterList: []ebtypes.SageMakerPipelineParameter{
							{Name: aws.String("p"), Value: aws.String("v")},
						},
					},
				}
			},
			landed: sageMakerExecutionExists,
		},
		{
			name: "redshift_data",
			setup: func(t *testing.T, _ *sfnFixture) ebtypes.Target {
				t.Helper()

				return ebtypes.Target{
					Arn: aws.String("arn:aws:redshift:us-east-1:" + account + ":cluster:eb-cluster"),
					RedshiftDataParameters: &ebtypes.RedshiftDataParameters{
						Database: aws.String("dev"), DbUser: aws.String("admin"), Sql: aws.String("select 1"),
					},
				}
			},
			landed: redshiftStatementExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "EventBridge")

			target := tt.setup(t, fx)
			target.Id = aws.String("t")

			ebc := eventbridge.NewFromConfig(fx.cfg)
			_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["job.targets"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("r"), Targets: []ebtypes.Target{target},
			})
			require.NoError(t, err)

			_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
				Source: aws.String("job.targets"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
			}}})
			require.NoError(t, err)

			require.Eventually(t, func() bool { return tt.landed(t, fx) }, authzDeadline, authzTick)
		})
	}
}

// setupBatchQueue creates the eb-queue job queue and returns a job definition ARN.
func setupBatchQueue(t *testing.T, fx *sfnFixture) string {
	t.Helper()

	bc := batch.NewFromConfig(fx.cfg)
	ce, err := bc.CreateComputeEnvironment(t.Context(), &batch.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String("eb-ce"),
		Type:                   batchtypes.CETypeManaged,
		State:                  batchtypes.CEStateEnabled,
	})
	require.NoError(t, err)

	_, err = bc.CreateJobQueue(t.Context(), &batch.CreateJobQueueInput{
		JobQueueName: aws.String("eb-queue"),
		Priority:     aws.Int32(1),
		State:        batchtypes.JQStateEnabled,
		ComputeEnvironmentOrder: []batchtypes.ComputeEnvironmentOrder{
			{ComputeEnvironment: ce.ComputeEnvironmentArn, Order: aws.Int32(1)},
		},
	})
	require.NoError(t, err)

	jd, err := bc.RegisterJobDefinition(t.Context(), &batch.RegisterJobDefinitionInput{
		JobDefinitionName:   aws.String("eb-jd"),
		Type:                batchtypes.JobDefinitionTypeContainer,
		ContainerProperties: &batchtypes.ContainerProperties{Image: aws.String("busybox")},
	})
	require.NoError(t, err)

	return aws.ToString(jd.JobDefinitionArn)
}

func setupSageMakerPipeline(t *testing.T, fx *sfnFixture) {
	t.Helper()

	_, err := sagemaker.NewFromConfig(fx.cfg).CreatePipeline(t.Context(), &sagemaker.CreatePipelineInput{
		PipelineName:       aws.String("eb-pipe"),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/sm"),
		PipelineDefinition: aws.String(`{"Version":"2020-12-01","Steps":[]}`),
	})
	require.NoError(t, err)
}

func batchJobExists(t *testing.T, fx *sfnFixture) bool {
	t.Helper()

	for _, st := range batchtypes.JobStatus("").Values() {
		out, err := batch.NewFromConfig(fx.cfg).ListJobs(t.Context(), &batch.ListJobsInput{
			JobQueue: aws.String("eb-queue"), JobStatus: st,
		})
		if err == nil && len(out.JobSummaryList) > 0 {
			return true
		}
	}

	return false
}

func sageMakerExecutionExists(t *testing.T, fx *sfnFixture) bool {
	t.Helper()

	out, err := sagemaker.NewFromConfig(fx.cfg).ListPipelineExecutions(
		t.Context(), &sagemaker.ListPipelineExecutionsInput{PipelineName: aws.String("eb-pipe")})

	return err == nil && len(out.PipelineExecutionSummaries) > 0
}

func redshiftStatementExists(t *testing.T, fx *sfnFixture) bool {
	t.Helper()

	out, err := redshiftdata.NewFromConfig(fx.cfg).ListStatements(t.Context(), &redshiftdata.ListStatementsInput{})

	return err == nil && len(out.Statements) > 0
}
