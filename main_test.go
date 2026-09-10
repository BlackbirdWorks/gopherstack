package main

import (
	"context"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepositoryHygieneNoTempFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args struct {
			checkReject func(relPath string, isDir bool) bool
		}
		want struct {
			description string
		}
	}{
		{
			name: "jetbrains safe write temp files",
			args: struct {
				checkReject func(relPath string, isDir bool) bool
			}{
				checkReject: func(relPath string, isDir bool) bool {
					base := filepath.Base(relPath)

					return !isDir && (strings.HasPrefix(base, ".!") || strings.Contains(base, ".!"))
				},
			},
			want: struct {
				description string
			}{
				description: "files starting with or containing .! (JetBrains safe-write)",
			},
		},
		{
			name: "editor swap and backup files",
			args: struct {
				checkReject func(relPath string, isDir bool) bool
			}{
				checkReject: func(relPath string, isDir bool) bool {
					base := filepath.Base(relPath)

					return !isDir && (strings.HasSuffix(base, ".swp") || strings.HasSuffix(base, ".swo") ||
						strings.HasSuffix(base, ".swn") || strings.HasSuffix(base, "~") ||
						(strings.HasPrefix(base, "#") && strings.HasSuffix(base, "#")))
				},
			},
			want: struct {
				description string
			}{
				description: "editor swap or backup files (.swp, .swo, .swn, ~, #*#)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			var violations []string

			err := filepath.WalkDir(".", func(path string, d fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if d.IsDir() &&
					(d.Name() == ".git" || d.Name() == "node_modules" || d.Name() == ".svelte-kit" || d.Name() == ".dolt") {
					return filepath.SkipDir
				}
				if tt.args.checkReject(path, d.IsDir()) {
					violations = append(violations, path)
				}

				return nil
			})

			require.NoError(t, err)
			assert.Empty(t, violations, "found disallowed files (%s): %v", tt.want.description, violations)
		})
	}
}

func TestMultipleServersStartupAndShutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		demo bool
	}{
		{
			name: "server startup without DEMO",
			demo: false,
		},
		{
			name: "server startup with DEMO",
			demo: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port, stopChan, errChan := startServerRetryingPort(t, tt.demo)

			t.Logf("Server responding successfully on port :%d", port)

			close(stopChan)

			select {
			case runErr := <-errChan:
				require.NoError(t, runErr)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "server did not shut down within timeout")
			}
		})
	}
}

// startServerRetryingPort starts Gopherstack on a freshly picked ephemeral port and waits for its
// dashboard to answer. freeTCPPort's Listen-then-Close releases the port immediately, but the real
// bind happens much later, at the end of run's init chain -- a wide TOCTOU window in which something
// else can take the same port first. When that happens run exits with a bind error before the
// dashboard ever comes up; this retries on a fresh port rather than burning the rest of the deadline
// on a port already lost (gopherstack-tajh, following the pkgs/dns TOCTOU precedent fixed in
// gopherstack-nn94/7tbt). A plain timeout with no error from run is a genuine hang, not a port race,
// and is not retried -- retrying would not fix it and could make things worse under load.
func startServerRetryingPort(t *testing.T, demo bool) (int, chan struct{}, chan error) {
	t.Helper()

	const (
		maxAttempts   = 5
		dashboardWait = 10 * time.Second
		dashboardPoll = 100 * time.Millisecond
	)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		port := freeTCPPort(t)
		stopChan := make(chan struct{})
		errChan := make(chan error, 1)

		go func() {
			errChan <- startServerOnPort(t, port, demo, stopChan)
		}()

		up, runErr := waitForDashboard(port, errChan, dashboardWait, dashboardPoll)
		if up {
			return port, stopChan, errChan
		}

		if runErr == nil {
			require.FailNow(t, fmt.Sprintf(
				"failed to reach server on :%d: timed out after %s with no error from run (attempt %d/%d)",
				port, dashboardWait, attempt, maxAttempts))

			return 0, nil, nil
		}

		if attempt == maxAttempts {
			require.FailNow(t, fmt.Sprintf(
				"server on :%d failed to start after %d attempts, last error: %v", port, maxAttempts, runErr))

			return 0, nil, nil
		}

		t.Logf("attempt %d/%d: server on :%d exited before answering (%v); retrying on a fresh port",
			attempt, maxAttempts, port, runErr)
	}

	return 0, nil, nil
}

// waitForDashboard polls the dashboard on port until it answers, run exits (delivering to errChan),
// or the deadline elapses. It mirrors require.Eventually's polling shape but, unlike Eventually,
// never marks the test failed itself -- the caller decides whether a given outcome is retryable.
func waitForDashboard(port int, errChan <-chan error, timeout, interval time.Duration) (bool, error) {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case runErr := <-errChan:
			return false, runErr
		default:
		}

		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/dashboard", port))
		if err == nil {
			ready := resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusInternalServerError
			resp.Body.Close()

			if ready {
				return true, nil
			}
		}

		if time.Now().After(deadline) {
			return false, nil
		}

		<-ticker.C
	}
}

// startServerOnPort starts Gopherstack on the given port using the CLI run path.
// It returns when the stopChan is closed.
func startServerOnPort(t *testing.T, port int, demo bool, stopChan chan struct{}) error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cli := CLI{
		LogLevel:       "info",
		Port:           strconv.Itoa(port),
		Region:         "us-east-1",
		Demo:           demo,
		PortRangeStart: 10000,
		PortRangeEnd:   10100,
	}

	errChan := make(chan error, 1)

	go func() {
		errChan <- run(ctx, cli)
	}()

	select {
	case <-stopChan:
		cancel()

		return <-errChan
	case err := <-errChan:
		return err
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}
