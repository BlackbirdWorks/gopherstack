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
	err error
	res container.ExecResult
}

func (*execRuntime) CreateAndStart(context.Context, container.Spec) (string, error) { return "", nil }
func (*execRuntime) StopAndRemove(context.Context, string) error                    { return nil }
func (*execRuntime) StopContainer(context.Context, string) error                    { return nil }
func (*execRuntime) StartContainer(context.Context, string) error                   { return nil }

func (r *execRuntime) Exec(context.Context, string, []string) (container.ExecResult, error) {
	return r.res, r.err
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
