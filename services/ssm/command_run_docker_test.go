package ssm_test

import (
	"context"
	"encoding/base64"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

type fakeContainerAPI struct {
	killed  chan struct{}
	written map[string]*strings.Builder
	runCmd  []string
	removed []string
	mu      sync.Mutex
	block   bool
}

func (f *fakeContainerAPI) ContainerExec(_ context.Context, _ string, cmd []string) ([]byte, []byte, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	body := cmd[2]

	switch {
	case strings.Contains(body, "base64 -d"):
		dec, err := base64.StdEncoding.DecodeString(cmd[4])
		if err != nil {
			return nil, []byte(err.Error()), 1, nil
		}

		path := cmd[5]
		if f.written[path] == nil || strings.Contains(body, "> \"$2\"") && !strings.Contains(body, ">> ") {
			f.written[path] = &strings.Builder{}
		}

		f.written[path].Write(dec)
	case strings.Contains(body, "kill -KILL"):
		close(f.killed)
	case cmd[0] == "rm":
		f.removed = cmd[2:]
	default:
		f.runCmd = cmd
		if f.block {
			f.mu.Unlock()
			<-f.killed
			f.mu.Lock()

			return nil, nil, 137, nil
		}

		return []byte("out"), []byte("err"), 4, nil
	}

	return nil, nil, 0, nil
}

func TestContainerScriptExecutorRun(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("echo 0123456789\n", 8000)
	tests := []struct {
		name   string
		script string
		wd     string
	}{
		{name: "small", script: "echo hi\n"},
		{name: "chunked with workdir", script: big, wd: "/srv"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &fakeContainerAPI{killed: make(chan struct{}), written: map[string]*strings.Builder{}}
			ex := ssm.NewContainerScriptExecutor(api)

			res, err := ex.RunScript(t.Context(), "c1", ssm.ScriptRequest{
				Script: tt.script, WorkDir: tt.wd, Interpreter: []string{"sh", ssm.ScriptFilePlaceholder},
			})
			require.NoError(t, err)
			assert.Equal(t, 4, res.ExitCode)
			assert.Equal(t, "out", string(res.Stdout))
			assert.Equal(t, "err", string(res.Stderr))

			require.Len(t, api.written, 1)

			var path string
			for p, sb := range api.written {
				path = p
				assert.Equal(t, tt.script, sb.String())
			}

			require.GreaterOrEqual(t, len(api.runCmd), 8)
			assert.Equal(t, tt.wd, api.runCmd[5])
			assert.Equal(t, []string{"sh", path}, api.runCmd[6:])
			assert.Contains(t, api.removed, path)
		})
	}
}

func TestContainerScriptExecutorCancelKills(t *testing.T) {
	t.Parallel()

	api := &fakeContainerAPI{killed: make(chan struct{}), written: map[string]*strings.Builder{}, block: true}
	ex := ssm.NewContainerScriptExecutor(api)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() {
		_, err := ex.RunScript(
			ctx,
			"c1",
			ssm.ScriptRequest{Script: "sleep 99\n", Interpreter: []string{"sh", ssm.ScriptFilePlaceholder}},
		)
		done <- err
	}()

	require.Eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()

		return api.runCmd != nil
	}, 5e9, 1e6)
	cancel()

	require.ErrorIs(t, <-done, context.Canceled)

	select {
	case <-api.killed:
	default:
		t.Fatal("process was not killed")
	}
}
