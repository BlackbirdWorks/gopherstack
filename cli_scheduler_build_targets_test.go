package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedtypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/stretchr/testify/require"
)

func TestSchedulerCodeBuildTarget(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	authzStartWorkers(t, fx, "Scheduler")

	cb := codebuild.NewFromConfig(fx.cfg)
	_, err := cb.CreateProject(t.Context(), &codebuild.CreateProjectInput{
		Name:        aws.String("sched-proj"),
		ServiceRole: aws.String("arn:aws:iam::000000000000:role/cb"),
		Source:      &cbtypes.ProjectSource{Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String("version: 0.2")},
		Artifacts:   &cbtypes.ProjectArtifacts{Type: cbtypes.ArtifactsTypeNoArtifacts},
		Environment: &cbtypes.ProjectEnvironment{
			Type:        cbtypes.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: cbtypes.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)

	_, err = scheduler.NewFromConfig(fx.cfg).CreateSchedule(t.Context(), &scheduler.CreateScheduleInput{
		Name:               aws.String("s"),
		ScheduleExpression: aws.String("rate(1 minute)"),
		FlexibleTimeWindow: &schedtypes.FlexibleTimeWindow{Mode: schedtypes.FlexibleTimeWindowModeOff},
		Target: &schedtypes.Target{
			Arn:     aws.String("arn:aws:codebuild:us-east-1:000000000000:project/sched-proj"),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, lerr := cb.ListBuildsForProject(t.Context(), &codebuild.ListBuildsForProjectInput{
			ProjectName: aws.String("sched-proj"),
		})

		return lerr == nil && len(out.Ids) > 0
	}, authzDeadline, authzTick)
}
