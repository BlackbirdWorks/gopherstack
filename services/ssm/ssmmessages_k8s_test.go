package ssm_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

var errExecDenied = errors.New("forbidden: pods/exec")

type fakePodExecer struct {
	err  error
	pod  string
	name string
	cmd  []string
	cols uint
	rows uint
}

func (f *fakePodExecer) ExecInteractive(
	_ context.Context, pod, name string, cmd []string, cols, rows uint,
) (container.PodShell, error) {
	f.pod, f.name, f.cmd, f.cols, f.rows = pod, name, cmd, cols, rows

	return nil, f.err
}

func TestKubernetesShellOpener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		execErr error
		wantErr error
		wantCmd []string
	}{
		{name: "command", id: "ecs-1/web", wantCmd: []string{"ls", "/"}},
		{name: "bad ref", id: "web", wantErr: ssmbackend.ErrBadContainerRef},
		{
			name:    "exec forbidden",
			id:      "ecs-1/web",
			execErr: errExecDenied,
			wantErr: errExecDenied,
			wantCmd: []string{"ls", "/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			x := &fakePodExecer{err: tt.execErr}
			opener, ok := ssmbackend.NewKubernetesShellOpener(x).(ssmbackend.CommandShellOpener)
			require.True(t, ok)

			_, err := opener.OpenCommand(t.Context(), tt.id, []string{"ls", "/"}, 100, 40)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				// the fake returns a nil shell with a nil error
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCmd, x.cmd)

			if tt.wantCmd != nil {
				assert.Equal(t, "ecs-1", x.pod)
				assert.Equal(t, "web", x.name)
				assert.Equal(t, uint(100), x.cols)
				assert.Equal(t, uint(40), x.rows)
			}
		})
	}
}

func TestKubernetesShellOpener_OpenShell(t *testing.T) {
	t.Parallel()

	x := &fakePodExecer{}
	_, err := ssmbackend.NewKubernetesShellOpener(x).OpenShell(t.Context(), "ecs-1/web", 80, 24)
	require.NoError(t, err)
	assert.Equal(t, "sh", x.cmd[0])
	assert.Equal(t, "web", x.name)
}
