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
		name        string
		checkReject func(relPath string, isDir bool) bool
		description string
	}{
		{
			name: "jetbrains safe write temp files",
			checkReject: func(relPath string, isDir bool) bool {
				base := filepath.Base(relPath)

				return !isDir && (strings.HasPrefix(base, ".!") || strings.Contains(base, ".!"))
			},
			description: "files starting with or containing .! (JetBrains safe-write)",
		},
		{
			name: "editor swap and backup files",
			checkReject: func(relPath string, isDir bool) bool {
				base := filepath.Base(relPath)

				return !isDir && (strings.HasSuffix(base, ".swp") || strings.HasSuffix(base, ".swo") ||
					strings.HasSuffix(base, "~") || (strings.HasPrefix(base, "#") && strings.HasSuffix(base, "#")))
			},
			description: "editor swap or backup files (.swp, .swo, ~, #*#)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var violations []string

			err := filepath.WalkDir(".", func(path string, d fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if d.IsDir() &&
					(d.Name() == ".git" || d.Name() == "node_modules" || d.Name() == ".svelte-kit" || d.Name() == ".dolt") {
					return filepath.SkipDir
				}
				if tt.checkReject(path, d.IsDir()) {
					violations = append(violations, path)
				}

				return nil
			})

			require.NoError(t, err)
			assert.Empty(t, violations, "found disallowed files (%s): %v", tt.description, violations)
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
			port := freeTCPPort(t)
			stopChan := make(chan struct{})
			errChan := make(chan error, 1)

			go func() {
				errChan <- startServerOnPort(t, port, tt.demo, stopChan)
			}()

			// Give the server time to start by polling the dashboard.
			require.Eventually(t, func() bool {
				client := &http.Client{Timeout: 1 * time.Second}
				resp, err := client.Get(fmt.Sprintf("http://localhost:%d/dashboard", port))
				if err != nil {
					return false
				}
				defer resp.Body.Close()

				return resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusInternalServerError
			}, 10*time.Second, 100*time.Millisecond, "failed to reach server on :%d", port)

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
