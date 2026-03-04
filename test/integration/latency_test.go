package integration_test

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startLatencyContainer starts a Gopherstack container with LATENCY_MS set.
func startLatencyContainer(t *testing.T, latencyMs string) (testcontainers.Container, string) {
	t.Helper()

	ctx := t.Context()

	dockerfile := "Dockerfile"
	if _, err := os.Stat("../../bin/gopherstack"); err == nil {
		dockerfile = "Dockerfile.test"
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../../",
			Dockerfile: dockerfile,
		},
		Env: map[string]string{
			"LATENCY_MS": latencyMs,
		},
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForHTTP("/_gopherstack/health").
			WithStatusCodeMatcher(func(status int) bool { return status == 200 }).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "failed to start latency container")

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	mappedPort, err := container.MappedPort(ctx, "8000")
	require.NoError(t, err)

	ep := fmt.Sprintf("http://localhost:%s", mappedPort.Port())

	return container, ep
}

// TestIntegration_Latency_ResponsesAreSlow verifies that when LATENCY_MS is set,
// request latency is measurably greater than without it. It sends several requests
// and asserts the total elapsed time exceeds a conservative minimum threshold.
func TestIntegration_Latency_ResponsesAreSlow(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	const (
		latencyMs  = "50" // random sleep in [0, 50) ms per request
		requests   = 5    // number of requests to send
		minTotalMs = 50   // conservatively: at least one request should sleep >= 10 ms
	)

	_, ep := startLatencyContainer(t, latencyMs)

	client := &http.Client{Timeout: 10 * time.Second}
	url := ep + "/_gopherstack/health"

	start := time.Now()

	for range requests {
		resp, err := client.Get(url)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	elapsed := time.Since(start)

	// With LATENCY_MS=50, each request sleeps uniformly in [0,50)ms.
	// Expected total sleep over 5 requests is ~125ms (5 × 25ms mean), but we
	// assert a very conservative floor of 50ms to avoid flakiness on slow CI
	// machines where container startup and network overhead dominate.
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(minTotalMs),
		"expected total elapsed time >= %dms with LATENCY_MS=%s; got %v", minTotalMs, latencyMs, elapsed)
}

// TestIntegration_Latency_DisabledByDefault verifies that without LATENCY_MS,
// the health endpoint responds quickly (below 500 ms for 5 requests).
func TestIntegration_Latency_DisabledByDefault(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	const requests = 5

	client := &http.Client{Timeout: 10 * time.Second}
	url := endpoint + "/_gopherstack/health"

	start := time.Now()

	for range requests {
		resp, err := client.Get(url)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	elapsed := time.Since(start)

	// Without latency injection, 5 health requests against a local container
	// should complete well under 500ms.
	assert.Less(t, elapsed.Milliseconds(), int64(500),
		"expected fast responses without latency; got %v", elapsed)
}
