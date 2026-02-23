package inithooks_test

import (
	"context"
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

	// Script writes a marker file.
	script := "echo hello > " + out

	r := inithooks.New([]string{script}, 0, nil)
	r.Run(context.Background())

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Contains(t, string(data), "hello")
}

func TestRun_FailingScript_ContinuesNext(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	out := filepath.Join(dir, "output.txt")

	scripts := []string{
		"exit 1",
		"echo second > " + out,
	}

	r := inithooks.New(scripts, 0, nil)
	r.Run(context.Background())

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Contains(t, string(data), "second")
}

func TestRun_WithLogger(_ *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := inithooks.New([]string{"echo hi"}, 5*time.Second, log)

	r.Run(context.Background())
}

func TestRun_WithLogger_FailingScript(_ *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := inithooks.New([]string{"exit 42"}, 5*time.Second, log)

	// Should not panic or block.
	r.Run(context.Background())
}

func TestRun_EmptyScripts(_ *testing.T) {
	r := inithooks.New(nil, 0, nil)

	// Should complete immediately without error.
	r.Run(context.Background())
}

func TestRun_Timeout(t *testing.T) {
	t.Parallel()

	// Script sleeps much longer than the timeout.
	r := inithooks.New([]string{"sleep 10"}, 50*time.Millisecond, nil)

	start := time.Now()
	r.Run(context.Background())
	elapsed := time.Since(start)

	// Should complete well within a second (timeout + some overhead).
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
	r.Run(context.Background())

	data1, err := os.ReadFile(out1)
	require.NoError(t, err)
	assert.Contains(t, string(data1), "first")

	data2, err := os.ReadFile(out2)
	require.NoError(t, err)
	assert.Contains(t, string(data2), "second")
}
