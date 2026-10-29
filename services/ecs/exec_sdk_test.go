package ecs_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/require"
)

func TestExecuteCommand_Errors_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     any
		name        string
		interactive bool
		enableExec  bool
	}{
		{name: "not_interactive", enableExec: true, wantErr: new(*ecstypes.InvalidParameterException)},
		{name: "exec_not_enabled", interactive: true, wantErr: new(*ecstypes.InvalidParameterException)},
		{
			name: "agent_not_connected", interactive: true, enableExec: true,
			wantErr: new(*ecstypes.TargetNotConnectedException),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestECSClient(t, h)
			ctx := t.Context()

			_, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
				Family: aws.String("x"),
				ContainerDefinitions: []ecstypes.ContainerDefinition{
					{Name: aws.String("app"), Image: aws.String("nginx")},
				},
			})
			require.NoError(t, err)

			run, err := client.RunTask(ctx, &ecssdk.RunTaskInput{
				TaskDefinition: aws.String("x"), EnableExecuteCommand: tt.enableExec,
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				d, derr := client.DescribeTasks(ctx, &ecssdk.DescribeTasksInput{Tasks: []string{*run.Tasks[0].TaskArn}})

				return derr == nil && len(d.Tasks) == 1 && aws.ToString(d.Tasks[0].LastStatus) == "RUNNING"
			}, 5*time.Second, 5*time.Millisecond)

			_, err = client.ExecuteCommand(ctx, &ecssdk.ExecuteCommandInput{
				Task: run.Tasks[0].TaskArn, Command: aws.String("/bin/sh"), Interactive: tt.interactive,
			})
			require.Error(t, err)
			require.ErrorAs(t, err, tt.wantErr)
		})
	}
}
