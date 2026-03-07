package lambda_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestDefaultSettings_EnvVarOverrides verifies that DefaultSettings respects environment variable
// overrides for all configurable fields. These tests are NOT parallel because they set env vars.
func TestDefaultSettings_EnvVarOverrides(t *testing.T) {
	tests := []struct {
		env              map[string]string
		name             string
		wantDockerHost   string
		wantRuntime      string
		wantPoolSize     int
		checkIdleTimeout bool
	}{
		{
			name:           "LAMBDA_DOCKER_HOST overrides docker host",
			env:            map[string]string{"LAMBDA_DOCKER_HOST": "192.168.1.100"},
			wantDockerHost: "192.168.1.100",
			wantPoolSize:   3,
			wantRuntime:    "docker",
		},
		{
			name:           "LAMBDA_POOL_SIZE valid integer",
			env:            map[string]string{"LAMBDA_POOL_SIZE": "5"},
			wantPoolSize:   5,
			wantRuntime:    "docker",
			wantDockerHost: "", // OS-dependent; don't assert exact value
		},
		{
			name:           "LAMBDA_POOL_SIZE invalid is ignored",
			env:            map[string]string{"LAMBDA_POOL_SIZE": "not-a-number"},
			wantPoolSize:   3, // falls back to default
			wantRuntime:    "docker",
			wantDockerHost: "",
		},
		{
			name:             "LAMBDA_IDLE_TIMEOUT valid duration",
			env:              map[string]string{"LAMBDA_IDLE_TIMEOUT": "5m"},
			wantPoolSize:     3,
			wantRuntime:      "docker",
			checkIdleTimeout: true,
			wantDockerHost:   "",
		},
		{
			name:           "CONTAINER_RUNTIME overrides runtime",
			env:            map[string]string{"CONTAINER_RUNTIME": "podman"},
			wantPoolSize:   3,
			wantRuntime:    "podman",
			wantDockerHost: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Not parallel: tests set environment variables.
			// Clear all related env vars first for deterministic behavior regardless
			// of what the developer or CI environment has set.
			t.Setenv("LAMBDA_DOCKER_HOST", "")
			t.Setenv("LAMBDA_POOL_SIZE", "")
			t.Setenv("LAMBDA_IDLE_TIMEOUT", "")
			t.Setenv("CONTAINER_RUNTIME", "")

			// Apply per-test overrides.
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			s := lambda.DefaultSettings()

			if tt.wantDockerHost != "" {
				assert.Equal(t, tt.wantDockerHost, s.DockerHost)
			}

			assert.Equal(t, tt.wantPoolSize, s.PoolSize)
			assert.Equal(t, tt.wantRuntime, s.ContainerRuntime)

			if tt.checkIdleTimeout {
				assert.Equal(t, "5m0s", s.IdleTimeout.String())
			}
		})
	}
}
