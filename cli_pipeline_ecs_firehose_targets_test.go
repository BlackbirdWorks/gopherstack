package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline"
	cptypes "github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/pipes"
	ptypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedtypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/stretchr/testify/require"

	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
)

func createPipeline(t *testing.T, fx *sfnFixture, name string) string {
	t.Helper()

	_, err := codepipeline.NewFromConfig(fx.cfg).CreatePipeline(t.Context(), &codepipeline.CreatePipelineInput{
		Pipeline: &cptypes.PipelineDeclaration{
			Name:    aws.String(name),
			RoleArn: aws.String(authzAccount + "cp"),
			ArtifactStore: &cptypes.ArtifactStore{
				Type: cptypes.ArtifactStoreTypeS3, Location: aws.String("cp-artifacts"),
			},
			Stages: []cptypes.StageDeclaration{{
				Name: aws.String("Source"),
				Actions: []cptypes.ActionDeclaration{{
					Name: aws.String("Src"),
					ActionTypeId: &cptypes.ActionTypeId{
						Category: cptypes.ActionCategorySource, Owner: cptypes.ActionOwnerAws,
						Provider: aws.String("S3"), Version: aws.String("1"),
					},
				}},
			}},
		},
	})
	require.NoError(t, err)

	return "arn:aws:codepipeline:us-east-1:000000000000:" + name
}

func pipelineExecuted(t *testing.T, fx *sfnFixture, name string) bool {
	t.Helper()

	out, err := codepipeline.NewFromConfig(fx.cfg).ListPipelineExecutions(
		t.Context(), &codepipeline.ListPipelineExecutionsInput{PipelineName: aws.String(name)})

	return err == nil && len(out.PipelineExecutionSummaries) > 0
}

func TestEventBridgeCodePipelineTarget(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	authzStartWorkers(t, fx, "EventBridge")

	pipelineARN := createPipeline(t, fx, "eb-pipeline")

	ebc := eventbridge.NewFromConfig(fx.cfg)
	_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
		Name: aws.String("r"), EventPattern: aws.String(`{"source":["cp.target"]}`),
	})
	require.NoError(t, err)

	_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
		Rule: aws.String("r"), Targets: []ebtypes.Target{{Id: aws.String("t"), Arn: aws.String(pipelineARN)}},
	})
	require.NoError(t, err)

	_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
		Source: aws.String("cp.target"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
	}}})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return pipelineExecuted(t, fx, "eb-pipeline") }, authzDeadline, authzTick)
}

func TestSchedulerDeliveryTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, fx *sfnFixture) string
		landed func(t *testing.T, fx *sfnFixture) bool
		name   string
	}{
		{
			name: "codepipeline",
			setup: func(t *testing.T, fx *sfnFixture) string {
				t.Helper()

				return createPipeline(t, fx, "sched-pipeline")
			},
			landed: func(t *testing.T, fx *sfnFixture) bool {
				t.Helper()

				return pipelineExecuted(t, fx, "sched-pipeline")
			},
		},
		{
			name: "firehose",
			setup: func(t *testing.T, fx *sfnFixture) string {
				t.Helper()

				s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
				_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("sched-fh")})
				require.NoError(t, err)

				authzFirehoseStream(t, fx, "sched-stream", "sched-fh", authzAccount+"fh", false)

				return "arn:aws:firehose:us-east-1:000000000000:deliverystream/sched-stream"
			},
			landed: func(t *testing.T, fx *sfnFixture) bool {
				t.Helper()

				fhH, ok := serviceByName(fx.services)["Firehose"].(*firehosebackend.Handler)
				require.True(t, ok)

				fhBk, ok := fhH.Backend.(*firehosebackend.InMemoryBackend)
				require.True(t, ok)
				fhBk.FlushAll(t.Context())

				s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
				listed, err := s3c.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: aws.String("sched-fh")})

				return err == nil && len(listed.Contents) > 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "Scheduler")

			targetARN := tt.setup(t, fx)

			_, err := scheduler.NewFromConfig(fx.cfg).CreateSchedule(t.Context(), &scheduler.CreateScheduleInput{
				Name:               aws.String("s"),
				ScheduleExpression: aws.String("rate(1 minute)"),
				FlexibleTimeWindow: &schedtypes.FlexibleTimeWindow{Mode: schedtypes.FlexibleTimeWindowModeOff},
				Target: &schedtypes.Target{
					Arn:     aws.String(targetARN),
					RoleArn: aws.String(authzAccount + "sched"),
				},
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool { return tt.landed(t, fx) }, authzDeadline, authzTick)
		})
	}
}

func TestPipesECSTarget(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	authzStartWorkers(t, fx, "Pipes")

	ecsc := ecs.NewFromConfig(fx.cfg)
	_, err := ecsc.CreateCluster(t.Context(), &ecs.CreateClusterInput{ClusterName: aws.String("pipe-c")})
	require.NoError(t, err)

	td, err := ecsc.RegisterTaskDefinition(t.Context(), &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("pipe-td"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("c"), Image: aws.String("busybox"), Essential: aws.Bool(true)},
		},
	})
	require.NoError(t, err)

	pipeFromQueue(t, fx, &pipes.CreatePipeInput{
		Target: aws.String("arn:aws:ecs:us-east-1:000000000000:cluster/pipe-c"),
		TargetParameters: &ptypes.PipeTargetParameters{EcsTaskParameters: &ptypes.PipeTargetEcsTaskParameters{
			TaskDefinitionArn: td.TaskDefinition.TaskDefinitionArn,
		}},
	}, "{}")

	require.Eventually(t, func() bool {
		out, listErr := ecsc.ListTasks(t.Context(), &ecs.ListTasksInput{Cluster: aws.String("pipe-c")})

		return listErr == nil && len(out.TaskArns) > 0
	}, authzDeadline, authzTick)
}

func TestBatchTargetsHonourRegion(t *testing.T) {
	t.Parallel()

	const queueARN = "arn:aws:batch:us-west-2:000000000000:job-queue/eb-queue"

	tests := []struct {
		deliver func(t *testing.T, fx *sfnFixture, jobDef string)
		name    string
	}{
		{
			name: "eventbridge",
			deliver: func(t *testing.T, fx *sfnFixture, jobDef string) {
				t.Helper()

				authzStartWorkers(t, fx, "EventBridge")

				ebc := eventbridge.NewFromConfig(fx.cfg)
				_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
					Name: aws.String("r"), EventPattern: aws.String(`{"source":["batch.region"]}`),
				})
				require.NoError(t, err)

				_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
					Rule: aws.String("r"), Targets: []ebtypes.Target{{
						Id: aws.String("t"), Arn: aws.String(queueARN),
						BatchParameters: &ebtypes.BatchParameters{
							JobDefinition: aws.String(jobDef),
							JobName:       aws.String("j"),
						},
					}},
				})
				require.NoError(t, err)

				_, err = ebc.PutEvents(
					t.Context(),
					&eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
						Source: aws.String("batch.region"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
					}}},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "pipes",
			deliver: func(t *testing.T, fx *sfnFixture, jobDef string) {
				t.Helper()

				authzStartWorkers(t, fx, "Pipes")

				pipeFromQueue(t, fx, &pipes.CreatePipeInput{
					Target: aws.String(queueARN),
					TargetParameters: &ptypes.PipeTargetParameters{
						BatchJobParameters: &ptypes.PipeTargetBatchJobParameters{
							JobDefinition: aws.String(jobDef), JobName: aws.String("j"),
						},
					},
				}, "{}")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			east := newSFNFixture(t)
			west := *east
			west.cfg = east.cfg.Copy()
			west.cfg.Region = "us-west-2"

			jobDef := setupBatchQueue(t, &west)
			tt.deliver(t, east, jobDef)

			require.Eventually(t, func() bool { return batchJobExists(t, &west) }, authzDeadline, authzTick)
			require.False(t, batchJobExists(t, east))
		})
	}
}
