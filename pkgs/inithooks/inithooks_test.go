package inithooks_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
)

func TestRun_SuccessfulScript(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	out := filepath.Join(dir, "output.txt")

	r := inithooks.New([]string{"echo hello > " + out}, 0, nil)
	r.Run(t.Context())

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Contains(t, string(data), "hello")
}

func TestRun_FailingScript_ContinuesNext(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	out := filepath.Join(dir, "output.txt")

	r := inithooks.New([]string{"exit 1", "echo second > " + out}, 0, nil)
	r.Run(t.Context())

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Contains(t, string(data), "second")
}

func TestRun_WithVariousConfigs(t *testing.T) {
	t.Parallel()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	tests := []struct {
		name    string
		scripts []string
		timeout time.Duration
		logger  *slog.Logger
	}{
		{name: "with_logger", scripts: []string{"echo hi"}, timeout: 5 * time.Second, logger: log},
		{name: "with_logger_failing_script", scripts: []string{"exit 42"}, timeout: 5 * time.Second, logger: log},
		{name: "empty_scripts"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := inithooks.New(tt.scripts, tt.timeout, tt.logger)
			r.Run(t.Context())
		})
	}
}

func TestRun_Timeout(t *testing.T) {
	t.Parallel()

	r := inithooks.New([]string{"sleep 10"}, 50*time.Millisecond, nil)

	start := time.Now()
	r.Run(t.Context())
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 3*time.Second)
}

func TestRun_MultipleScripts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	out1 := filepath.Join(dir, "out1.txt")
	out2 := filepath.Join(dir, "out2.txt")

	r := inithooks.New([]string{
		"echo first > " + out1,
		"echo second > " + out2,
	}, 0, nil)
	r.Run(t.Context())

	data1, err := os.ReadFile(out1)
	require.NoError(t, err)
	assert.Contains(t, string(data1), "first")

	data2, err := os.ReadFile(out2)
	require.NoError(t, err)
	assert.Contains(t, string(data2), "second")
}
