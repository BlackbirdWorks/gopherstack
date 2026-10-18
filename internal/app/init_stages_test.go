package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
