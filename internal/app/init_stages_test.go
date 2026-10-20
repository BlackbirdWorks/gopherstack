package app

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
)

func TestInitStages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stage     inithooks.Stage
		wantState string
	}{
		{name: "boot", stage: inithooks.StageBoot, wantState: "SUCCESSFUL"},
		{name: "ready", stage: inithooks.StageReady, wantState: "SUCCESSFUL"},
		{name: "shutdown", stage: inithooks.StageShutdown, wantState: "SUCCESSFUL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			dir := filepath.Join(root, strings.ToLower(string(tt.stage))+".d")
			require.NoError(t, os.MkdirAll(dir, 0o750))
			body := "test \"$AWS_ENDPOINT_URL\" = http://localhost:1234\n"
			require.NoError(t, os.WriteFile(filepath.Join(dir, "a.sh"), []byte(body), 0o600))

			cli := &CLI{
				Port: "1234", Region: "us-east-1", InitDirs: []string{root},
				localstackInit: newLocalstackInitState(),
			}
			cli.localstackInit.setManager(inithooks.NewManager(inithooks.ManagerConfig{
				Roots: []string{root}, Env: initScriptEnv(cli),
			}))

			assert.False(t, cli.localstackInit.completed(string(tt.stage)))

			runInitStage(t.Context(), cli, tt.stage)

			assert.True(t, cli.localstackInit.completed(string(tt.stage)))

			got := cli.localstackInit.scripts()
			require.Len(t, got, 1)
			assert.Equal(t, tt.wantState, got[0]["state"])
			assert.Equal(t, string(tt.stage), got[0]["stage"])
		})
	}
}

//nolint:paralleltest // uses t.Setenv via parseCLI
func TestShutdownStage_RunsBeforeServerStops(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "shutdown.d")
	require.NoError(t, os.MkdirAll(dir, 0o750))

	out := filepath.Join(t.TempDir(), "out")
	body := "curl -sf \"$AWS_ENDPOINT_URL/_localstack/init/shutdown\" > " + out + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.sh"), []byte(body), 0o600))

	port := freeTCPPort(t)
	cli := parseCLI(t, map[string]string{"PORT": strconv.Itoa(port)})
	cli.InitDirs = []string{root}
	cli.InitScriptTimeout = 5 * time.Second

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)

	go func() { errCh <- run(ctx, cli) }()

	waitForServerReady(t, port)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(shutdownWaitTimeout):
		require.FailNow(t, "server did not shut down within timeout")
	}

	data, err := os.ReadFile(out)
	require.NoError(t, err, "shutdown script must reach the server while it is still up")
	assert.Contains(t, string(data), `"name":"a.sh"`)
	assert.Contains(t, string(data), `"state":"RUNNING"`)
	assert.Contains(t, string(data), `"completed":false`)
}

func TestInitDirs_ConfigRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dirs []string
	}{
		{name: "two dirs", dirs: []string{"/a/init", "/b/init"}},
		{name: "none", dirs: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := &CLI{DataDir: t.TempDir(), InitDirs: tt.dirs}
			require.NoError(t, src.SaveConfig())

			dst := &CLI{DataDir: src.DataDir}
			require.NoError(t, dst.LoadConfig())
			assert.Equal(t, tt.dirs, dst.InitDirs)

			viaSettings := &CLI{}
			viaSettings.UpdateSettings(src.GetSettings())
			assert.Equal(t, tt.dirs, viaSettings.InitDirs)
		})
	}
}
