package container_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errExecFailed = errors.New("exec failed")

type execAPI struct {
	err    error
	gotID  string
	stdout string
	stderr string
	gotCmd []string
	mockAPI
	exitCode int
}

func (e *execAPI) ContainerExec(_ context.Context, id string, cmd []string) ([]byte, []byte, int, error) {
	e.gotID, e.gotCmd = id, cmd

	return []byte(e.stdout), []byte(e.stderr), e.exitCode, e.err
}

func TestDockerRuntime_Exec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		api     container.APIClient
		wantErr error
		name    string
		want    container.ExecResult
	}{
		{
			name: "ok",
			api:  &execAPI{stdout: "out", stderr: "err", exitCode: 3},
			want: container.ExecResult{Stdout: "out", Stderr: "err", ExitCode: 3},
		},
		{name: "client_error", api: &execAPI{err: errExecFailed}, wantErr: errExecFailed},
		{name: "unsupported_client", api: &mockAPI{}, wantErr: container.ErrExecUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newRuntime(tt.api)

			got, err := rt.Exec(t.Context(), "ctr-1", []string{"echo", "hi"})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			api, ok := tt.api.(*execAPI)
			require.True(t, ok)
			assert.Equal(t, "ctr-1", api.gotID)
			assert.Equal(t, []string{"echo", "hi"}, api.gotCmd)
		})
	}
}

type execEnvAPI struct {
	gotEnv []string
	execAPI
}

func (e *execEnvAPI) ContainerExecEnv(
	_ context.Context, id string, cmd, env []string,
) ([]byte, []byte, int, error) {
	e.gotID, e.gotCmd, e.gotEnv = id, cmd, env

	return []byte("out"), nil, 0, nil
}

type plainExecer struct{}

func (plainExecer) Exec(context.Context, string, []string) (container.ExecResult, error) {
	return container.ExecResult{Stdout: "plain"}, nil
}

func TestDockerRuntime_ExecEnv(t *testing.T) {
	t.Parallel()

	api := &execEnvAPI{}
	rt := newRuntime(api)

	got, err := rt.ExecEnv(t.Context(), "ctr-1", []string{"mysql"}, []string{"MYSQL_PWD=pw"})
	require.NoError(t, err)
	assert.Equal(t, "out", got.Stdout)
	assert.Equal(t, []string{"mysql"}, api.gotCmd)
	assert.Equal(t, []string{"MYSQL_PWD=pw"}, api.gotEnv)

	_, err = newRuntime(&mockAPI{}).ExecEnv(t.Context(), "ctr-1", nil, []string{"A=b"})
	require.ErrorIs(t, err, container.ErrExecUnsupported)
}

func TestExecWithEnv(t *testing.T) {
	t.Parallel()

	got, err := container.ExecWithEnv(t.Context(), plainExecer{}, "c", []string{"x"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "plain", got.Stdout)

	_, err = container.ExecWithEnv(t.Context(), plainExecer{}, "c", []string{"x"}, []string{"A=b"})
	require.ErrorIs(t, err, container.ErrExecUnsupported)
}
