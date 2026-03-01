package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipleServersStartupAndShutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port string
		demo bool
	}{
		{
			name: "server startup without DEMO",
			port: ":8001",
			demo: false,
		},
		{
			name: "server startup with DEMO",
			port: ":8002",
			demo: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stopChan := make(chan struct{})

			go func() {
				_ = startServerOnPort(t, tt.port, tt.demo, stopChan)
			}()

			// Give the server time to start.
			time.Sleep(1 * time.Second)

			client := &http.Client{Timeout: 5 * time.Second}

			resp, err := client.Get("http://localhost" + tt.port + "/dashboard")
			require.NoError(t, err, "failed to reach server on %s", tt.port)
			defer resp.Body.Close()

			assert.True(t,
				resp.StatusCode >= 200 && resp.StatusCode < 500,
				"unexpected status code: %d", resp.StatusCode)

			t.Logf("Server responding with status %d on port %s", resp.StatusCode, tt.port)

			close(stopChan)
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// startServerOnPort starts Gopherstack on the given port using the CLI run path.
// It returns when the stopChan is closed.
func startServerOnPort(t *testing.T, port string, demo bool, stopChan chan struct{}) error {
	cli := CLI{
		LogLevel: "info",
		Port:     port,
		Region:   "us-east-1",
		Demo:     demo,
	}

	errChan := make(chan error, 1)

	go func() {
		errChan <- run(t.Context(), cli)
	}()

	select {
	case <-stopChan:
		return nil
	case err := <-errChan:
		return err
	}
}
