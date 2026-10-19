//go:build unix

package extensions

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func processGone(pid int) bool {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return true
	}

	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}

	i := bytes.LastIndexByte(b, ')')

	return i >= 0 && strings.HasPrefix(strings.TrimSpace(string(b[i+1:])), "Z")
}

func TestStopKillsGrandchild(t *testing.T) {
	t.Parallel()

	pidFile := filepath.Join(t.TempDir(), "pid")
	spec := Spec{Name: "wrap", Command: []string{"sh", "-c", "sleep 300 & echo $! > " + pidFile + "; wait"}}

	r := newProcessRunner(logger.NewTestLogger(), spec, "")
	require.NoError(t, r.Start(t.Context(), nil))

	exited := make(chan struct{})

	go func() {
		_ = r.Wait()

		close(exited)
	}()

	var pid int

	require.Eventually(t, func() bool {
		b, err := os.ReadFile(pidFile)
		if err != nil {
			return false
		}

		pid, err = strconv.Atoi(strings.TrimSpace(string(b)))

		return err == nil && pid > 0
	}, 20*time.Second, 10*time.Millisecond)
	require.False(t, processGone(pid))

	r.Stop()
	<-exited

	require.Eventually(t, func() bool { return processGone(pid) }, 20*time.Second, 10*time.Millisecond)
}
