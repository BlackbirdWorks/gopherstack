package inithooks_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
)

func writeScript(t *testing.T, root, stage, name, body string) {
	t.Helper()

	dir := filepath.Join(root, stage+".d")
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
}

func TestManager_RunStage(t *testing.T) {
	t.Parallel()

	_, pyErr := exec.LookPath("python3")
	hasPython := pyErr == nil

	tests := []struct {
		name    string
		script  string
		body    string
		want    inithooks.State
		skipped bool
	}{
		{name: "sh_ok", script: "a.sh", body: "echo hi\n", want: inithooks.StateSuccessful},
		{name: "sh_fail", script: "a.sh", body: "echo boom >&2\nexit 3\n", want: inithooks.StateError},
		{name: "py_ok", script: "a.py", body: "print('hi')\n", want: inithooks.StateSuccessful, skipped: !hasPython},
		{name: "py_missing", script: "a.py", body: "print('hi')\n", want: inithooks.StateError, skipped: hasPython},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.skipped {
				t.Skip("python availability does not match case")
			}

			root := t.TempDir()
			writeScript(t, root, "boot", tt.script, tt.body)
			writeScript(t, root, "boot", "ignored.txt", "x")

			m := inithooks.NewManager(inithooks.ManagerConfig{Roots: []string{root, "/nonexistent"}})
			assert.False(t, m.Completed(inithooks.StageBoot))

			m.Run(t.Context(), inithooks.StageBoot)

			got := m.Status()
			require.Len(t, got, 1)
			assert.Equal(t, tt.want, got[0].State)
			assert.Equal(t, inithooks.StageBoot, got[0].Stage)
			assert.Equal(t, tt.script, got[0].Name)
			assert.True(t, m.Completed(inithooks.StageBoot))
			assert.False(t, m.Completed(inithooks.StageReady))
		})
	}
}

func TestManager_OrderAndEnv(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	out := filepath.Join(t.TempDir(), "out.txt")

	writeScript(t, root, "ready", "02-b.sh", "echo b $INIT_TEST_VAR >> "+out+"\n")
	writeScript(t, root, "ready", "01-a.sh", "echo a >> "+out+"\nexit 1\n")
	writeScript(t, root, "ready", "03-c.sh", "echo c >> "+out+"\n")

	m := inithooks.NewManager(inithooks.ManagerConfig{Roots: []string{root}, Env: []string{"INIT_TEST_VAR=v"}})
	m.Run(t.Context(), inithooks.StageReady)

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Equal(t, "a\nb v\nc\n", string(data))

	got := m.Status()
	require.Len(t, got, 3)
	assert.Equal(t, inithooks.StateError, got[0].State)
	assert.Equal(t, inithooks.StateSuccessful, got[1].State)
	assert.Equal(t, inithooks.StateSuccessful, got[2].State)
}

func TestManager_Timeout(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeScript(t, root, "shutdown", "a.sh", "sleep 30\n")

	m := inithooks.NewManager(inithooks.ManagerConfig{Roots: []string{root}, Timeout: 200_000_000})
	m.Run(t.Context(), inithooks.StageShutdown)

	got := m.Status()
	require.Len(t, got, 1)
	assert.Equal(t, inithooks.StateError, got[0].State)
	assert.NotEmpty(t, got[0].Error)
	assert.True(t, m.Completed(inithooks.StageShutdown))
}
