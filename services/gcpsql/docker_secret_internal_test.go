package gcpsql

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type echoRuntime struct {
	removeRecorder
	res container.ExecResult
	err error
}

func (r *echoRuntime) Exec(context.Context, string, []string) (container.ExecResult, error) {
	return r.res, r.err
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
		{name: "stderr echoes root", rt: &echoRuntime{res: container.ExecResult{ExitCode: 1, Stderr: "near '" + rootPW + "'"}}},
		{name: "stdout echoes repl", rt: &echoRuntime{res: container.ExecResult{ExitCode: 1, Stdout: replPW}}},
		{name: "exec error echoes root", rt: &echoRuntime{err: errors.New("create exec env MYSQL_PWD=" + rootPW)}},
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
