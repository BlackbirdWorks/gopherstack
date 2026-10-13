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
