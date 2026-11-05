package lambda

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type execRuntime struct {
	container.Runtime

	listErr  error
	mu       sync.Mutex
	cmds     []string
	listOut  string
	listExit int
}

func (e *execRuntime) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	script := cmd[len(cmd)-1]
	e.cmds = append(e.cmds, script)

	if script == imageExtensionListScript {
		return container.ExecResult{Stdout: e.listOut, ExitCode: e.listExit}, e.listErr
	}

	return container.ExecResult{}, nil
}

func TestStartImageExtensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		listErr     error
		name        string
		listOut     string
		wantStarted bool
		wantKnown   bool
		listExit    int
	}{
		{name: "two extensions", listOut: "alpha\nbeta\n", wantStarted: true, wantKnown: true},
		{name: "no extensions", listOut: "", wantKnown: true},
		{name: "no shell", listExit: 127},
		{name: "exec unsupported", listErr: container.ErrExecUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &execRuntime{listOut: tt.listOut, listExit: tt.listExit, listErr: tt.listErr}
			b := &InMemoryBackend{docker: rt}
			srv := newRuntimeServer(0)

			t.Cleanup(func() { srv.stop(context.Background()) })
			srv.exts.holdInit()
			b.startImageExtensions(t.Context(), srv, "cid")

			rt.mu.Lock()
			started := len(rt.cmds) == 2 && rt.cmds[1] == imageExtensionStartScript
			rt.mu.Unlock()

			assert.Equal(t, tt.wantStarted, started)

			srv.exts.mu.RLock("test")
			known, holding := srv.exts.externalKnown, srv.exts.holding
			srv.exts.mu.RUnlock()

			assert.Equal(t, tt.wantKnown, known)
			assert.False(t, holding, "the init hold is always released")

			if tt.wantStarted {
				_, hasAlpha := srv.exts.externalNames["alpha"]
				assert.True(t, hasAlpha)
			}
		})
	}
}

func TestImageInitHoldBlocksRegistrationUntilDiscovery(t *testing.T) {
	t.Parallel()

	h := newExtHarness(t)
	h.srv.exts.holdInit()

	done := make(chan int, 1)

	go func() {
		status, _, _, _ := h.doRaw(t.Context(), http.MethodPost, "/2020-01-01/extension/register",
			map[string]string{headerExtensionName: "internal"}, `{"events":["SHUTDOWN"]}`)
		done <- status
	}()

	select {
	case <-done:
		t.Fatal("registration must wait for extension discovery")
	default:
	}

	h.srv.exts.setExternal([]string{"ext"})
	assert.Equal(t, http.StatusBadRequest, <-done)
}

func TestExtensionStartLoopTagsExtensionOutput(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ext := filepath.Join(dir, "ext")
	require.NoError(t, os.WriteFile(ext, []byte("#!/bin/sh\necho out-line\necho err-line >&2\nprintf partial\n"), 0o600))
	require.NoError(t, os.Chmod(ext, 0o755)) // #nosec G302

	script := strings.ReplaceAll(extensionStartLoop, "/opt/extensions", dir) + "wait\n"
	out, err := exec.CommandContext(t.Context(), "/bin/sh", "-c", script).CombinedOutput()

	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Skipf("no /bin/sh: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	want := []string{
		extensionLogMarker + "err-line", extensionLogMarker + "out-line", extensionLogMarker + "partial",
	}

	assert.ElementsMatch(t, want, lines)
}
