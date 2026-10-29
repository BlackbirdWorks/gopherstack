package gcpsql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

var errBoom = errors.New("boom")

type echoRuntime struct {
	err    error
	gotCmd []string
	gotEnv []string
	res    container.ExecResult
}

func (*echoRuntime) CreateAndStart(context.Context, container.Spec) (string, error) { return "", nil }
func (*echoRuntime) StopContainer(context.Context, string) error                    { return nil }
func (*echoRuntime) StartContainer(context.Context, string) error                   { return nil }
func (*echoRuntime) StopAndRemove(context.Context, string) error                    { return nil }

func (r *echoRuntime) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	r.gotCmd = cmd

	return r.res, r.err
}

func (r *echoRuntime) ExecEnv(ctx context.Context, id string, cmd, env []string) (container.ExecResult, error) {
	r.gotEnv = env

	return r.Exec(ctx, id, cmd)
}

func TestMySQLPasswordGoesInEnvNotArgv(t *testing.T) {
	t.Parallel()

	const pw = "s3cret-root-pw"

	rt := &echoRuntime{}
	e := &dockerEngine{
		rt: rt, mu: lockmetrics.New("gcpsql-test"), password: pw, kind: kindMySQL, attached: true, ctrID: "c1", id: "inst",
	}

	_, err := e.mysqlCmd(context.Background(), "SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []string{rootUserKey + pw}, rt.gotEnv)
	assert.Equal(t, mysqlClient, rt.gotCmd[0])

	for _, a := range rt.gotCmd {
		assert.NotContains(t, a, pw)
	}
}

func TestRunOutNeverLeaksPassword(t *testing.T) {
	t.Parallel()

	const (
		rootPW = "s3cret-root-pw"
		replPW = "s3cret-repl-pw"
	)

	tests := []struct {
		rt   *echoRuntime
		name string
	}{
		{
			name: "stderr echoes root",
			rt:   &echoRuntime{res: container.ExecResult{ExitCode: 1, Stderr: "near '" + rootPW + "'"}},
		},
		{name: "stdout echoes repl", rt: &echoRuntime{res: container.ExecResult{ExitCode: 1, Stdout: replPW}}},
		{
			name: "exec error echoes root",
			rt:   &echoRuntime{err: fmt.Errorf("%w: create exec env MYSQL_PWD=%s", errBoom, rootPW)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			ctx := logger.Save(context.Background(), slog.New(slog.NewTextHandler(&buf, nil)))
			e := &dockerEngine{
				rt: tt.rt, mu: lockmetrics.New("gcpsql-test"), password: rootPW, replPass: replPW,
				kind: kindMySQL, attached: true, ctrID: "c1", id: "inst",
			}

			_, err := e.mysqlCmd(ctx, "SET GLOBAL super_read_only=ON")
			require.Error(t, err)
			assert.NotContains(t, err.Error(), rootPW)
			assert.NotContains(t, err.Error(), replPW)

			e.reguard(ctx)
			assert.NotContains(t, buf.String(), rootPW)
			assert.NotContains(t, buf.String(), replPW)
			assert.Contains(t, buf.String(), "read-only guard failed")
		})
	}
}
