package ecs

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errSessionBackend = errors.New("session backend down")

// idRunner launches instantly and reports one docker ID per container, like the docker runner.
type idRunner struct{}

func (idRunner) RunTask(*Task, *TaskDefinition) error { return nil }
func (idRunner) StopTask(*Task) error                 { return nil }
func (idRunner) ContainerIDs(taskArn string) []string {
	return []string{"dockerid-" + taskIDFromARN(taskArn) + "-0", "dockerid-" + taskIDFromARN(taskArn) + "-1"}
}

type recordingStarter struct {
	err error
	got []ExecSessionRequest
}

func (r *recordingStarter) StartExecSession(_ context.Context, req ExecSessionRequest) (ExecSessionGrant, error) {
	r.got = append(r.got, req)
	if r.err != nil {
		return ExecSessionGrant{}, r.err
	}

	return ExecSessionGrant{
		SessionID: "ecs-execute-command-1", StreamURL: req.StreamBase + "/s", TokenValue: "tok",
	}, nil
}

func TestExecuteCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		starter       *recordingStarter
		runner        TaskRunner
		wantErr       error
		name          string
		container     string
		command       string
		wantContainer string
		wantRuntimeID string
		enableExec    bool
		noInteractive bool
		noStarter     bool
	}{
		{
			name: "session", starter: &recordingStarter{}, runner: idRunner{}, enableExec: true,
			container: "web", command: "/bin/sh", wantContainer: "web", wantRuntimeID: "1",
		},
		{
			name: "default_container_not_allowed_with_two", starter: &recordingStarter{}, runner: idRunner{},
			enableExec: true, command: "ls", wantErr: ErrInvalidParameter,
		},
		{
			name: "unknown_container", starter: &recordingStarter{}, runner: idRunner{}, enableExec: true,
			container: "nope", command: "ls", wantErr: ErrInvalidParameter,
		},
		{
			name: "not_interactive", starter: &recordingStarter{}, runner: idRunner{}, enableExec: true,
			container: "web", command: "ls", noInteractive: true, wantErr: ErrInvalidParameter,
		},
		{
			name: "exec_not_enabled", starter: &recordingStarter{}, runner: idRunner{},
			container: "web", command: "ls", wantErr: ErrInvalidParameter,
		},
		{
			name: "missing_command", starter: &recordingStarter{}, runner: idRunner{}, enableExec: true,
			container: "web", wantErr: ErrInvalidParameter,
		},
		{
			name: "no_session_backend", runner: idRunner{}, noStarter: true, enableExec: true,
			container: "web", command: "ls", wantErr: ErrTargetNotConnected,
		},
		{
			name: "runtime_cannot_exec", starter: &recordingStarter{}, runner: NewNoopRunner(), enableExec: true,
			container: "web", command: "ls", wantErr: ErrTargetNotConnected,
		},
		{
			name: "session_backend_fails", starter: &recordingStarter{err: errSessionBackend}, runner: idRunner{},
			enableExec: true, container: "web", command: "ls", wantErr: ErrTargetNotConnected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", tt.runner)
			if !tt.noStarter {
				b.SetExecSessionStarter(tt.starter)
			}

			_, err := b.CreateCluster(CreateClusterInput{ClusterName: "x"})
			require.NoError(t, err)

			td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family: "x",
				ContainerDefinitions: []ContainerDefinition{
					{Name: "sidecar", Image: "busybox"}, {Name: "web", Image: "nginx"},
				},
			})
			require.NoError(t, err)

			tasks, _, err := b.RunTask(RunTaskInput{
				Cluster: "x", TaskDefinition: td.TaskDefinitionArn, EnableExecuteCommand: tt.enableExec,
			})
			require.NoError(t, err)

			tasks = settleTasks(t, b, tasks)
			require.Equal(t, statusRunning, tasks[0].LastStatus)

			ctx := WithStreamBase(t.Context(), "ws://emu:4566")
			out, err := b.ExecuteCommandContext(ctx, "x", tasks[0].TaskArn, tt.container, tt.command, !tt.noInteractive)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, out)

				return
			}

			require.NoError(t, err)
			assert.True(t, out.Interactive)
			assert.Equal(t, tt.wantContainer, out.ContainerName)
			assert.Equal(t, tasks[0].Containers[1].ContainerArn, out.ContainerArn)
			assert.Equal(t, "ecs-execute-command-1", out.Session.SessionID)
			assert.Equal(t, "ws://emu:4566/s", out.Session.StreamURL)

			require.Len(t, tt.starter.got, 1)
			req := tt.starter.got[0]
			assert.Equal(t, "dockerid-"+taskIDFromARN(tasks[0].TaskArn)+"-"+tt.wantRuntimeID, req.ContainerID)
			assert.Equal(t, "/bin/sh", req.Command)
			assert.Equal(t, "us-east-1", req.Region)
			assert.Equal(t, "ecs:x_"+taskIDFromARN(tasks[0].TaskArn)+"_"+req.ContainerID, req.Target)
		})
	}
}
