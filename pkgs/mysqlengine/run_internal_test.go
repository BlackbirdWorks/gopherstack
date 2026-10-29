package mysqlengine

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errDockerDown = errors.New("docker down")

type execRuntime struct {
	err    error
	gotCmd []string
	gotEnv []string
	res    container.ExecResult
}

func (*execRuntime) CreateAndStart(context.Context, container.Spec) (string, error) { return "", nil }
func (*execRuntime) StopAndRemove(context.Context, string) error                    { return nil }
func (*execRuntime) StopContainer(context.Context, string) error                    { return nil }
func (*execRuntime) StartContainer(context.Context, string) error                   { return nil }

func (r *execRuntime) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	r.gotCmd = cmd

	return r.res, r.err
}

func (r *execRuntime) ExecEnv(ctx context.Context, id string, cmd, env []string) (container.ExecResult, error) {
	r.gotEnv = env

	return r.Exec(ctx, id, cmd)
}

func TestPasswordGoesInEnvNotArgv(t *testing.T) {
	t.Parallel()

	const secret = "s3cr3t-root-pw"

	rt := &execRuntime{}
	s := &Server{e: &Engine{cfg: Config{Runtime: rt}}, spec: Spec{RootPassword: secret}, dir: t.TempDir()}

	require.NoError(t, s.Restore(t.Context(), []byte("select 1")))
	assert.Equal(t, []string{"MYSQL_PWD=" + secret}, rt.gotEnv)
	assert.NotEmpty(t, rt.gotCmd)

	for _, a := range rt.gotCmd {
		assert.NotContains(t, a, secret)
	}
}

func TestRestoreErrorOmitsPassword(t *testing.T) {
	t.Parallel()

	const secret = "s3cr3t-root-pw"

	tests := []struct {
		rt   *execRuntime
		name string
	}{
		{name: "exec error", rt: &execRuntime{err: errDockerDown}},
		{name: "nonzero exit", rt: &execRuntime{res: container.ExecResult{ExitCode: 1, Stderr: "access denied"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := &Engine{cfg: Config{Runtime: tt.rt}}
			s := &Server{e: e, spec: Spec{RootPassword: secret}, dir: t.TempDir()}

			err := s.Restore(t.Context(), []byte("select 1"))
			require.Error(t, err)
			assert.NotContains(t, err.Error(), secret)
			assert.Contains(t, err.Error(), mysqlBin)
		})
	}
}
