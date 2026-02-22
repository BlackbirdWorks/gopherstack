package main

import (
	"context"
	"net/http"
	"testing"
	"time"
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
				_ = startServerOnPort(tt.port, tt.demo, stopChan)
			}()

			// Give the server time to start.
			time.Sleep(1 * time.Second)

			client := &http.Client{Timeout: 5 * time.Second}

			resp, err := client.Get("http://localhost" + tt.port + "/dashboard")
			if err != nil {
				t.Fatalf("failed to reach server on %s: %v", tt.port, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode >= 500 {
				t.Fatalf("unexpected status code: %d", resp.StatusCode)
			}

			t.Logf("Server responding with status %d on port %s", resp.StatusCode, tt.port)

			close(stopChan)
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// startServerOnPort starts Gopherstack on the given port using the CLI run path.
// It returns when the stopChan is closed.
func startServerOnPort(port string, demo bool, stopChan chan struct{}) error {
	cli := CLI{
		LogLevel: "info",
		Port:     port,
		Region:   "us-east-1",
		Demo:     demo,
	}

	errChan := make(chan error, 1)

	go func() {
		errChan <- run(context.Background(), cli)
	}()

	select {
	case <-stopChan:
		return nil
	case err := <-errChan:
		return err
	}
}
