package main

import (
	"net/http"
	"testing"
	"time"
)

func TestServerStartupAndShutdown(t *testing.T) {
	tests := []struct {
		name string
		demo bool
		port string
	}{
		{
			name: "server startup without DEMO",
			demo: false,
			port: ":8001",
		},
		{
			name: "server startup with DEMO",
			demo: true,
			port: ":8002",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
		errChan <- run(cli)
	}()

	select {
	case <-stopChan:
		return nil
	case err := <-errChan:
		return err
	}
}

