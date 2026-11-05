package lambdaweb_test

import (
	"encoding/json"
	"net/http"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const dockerTestImage = "node:20-slim"

const nodeApp = `const http = require('http');
const [host, port] = process.env.AWS_LAMBDA_HTTP_ENDPOINT.split(':');
http.createServer((req, res) => {
  res.setHeader('x-app-host', require('os').hostname());
  res.setHeader('x-app-saw-endpoint', req.headers['lambda-web-endpoint-name'] || '');
  res.end('hello ' + req.url + ' ' + (process.env.GREETING || ''));
}).listen(Number(port), host, () => console.log('listening'));
`

func dockerRuntime(t *testing.T) container.Runtime {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	if err = exec.CommandContext(t.Context(), "docker", "image", "inspect", dockerTestImage).Run(); err != nil {
		t.Skipf("image %s not present", dockerTestImage)
	}

	return rt
}

// TestDockerEngine_ServesRealApp runs a real node container behind the endpoint domain. It is skipped when no
// container runtime or the node image is available locally.
func TestDockerEngine_ServesRealApp(t *testing.T) {
	t.Parallel()

	rt := dockerRuntime(t)

	env := newTestEnv(t)
	codes := newFakeCodes()
	codes.objects["code/app.zip@v1"] = buildZip(t, map[string]string{"index.js": nodeApp})
	env.backend.SetCodeStore(codes)
	logs := &fakeLogs{}
	env.backend.SetLogSink(logs)
	env.backend.SetLauncher(lambdaweb.NewDockerLauncher(env.backend, lambdaweb.DockerConfig{
		Runtime: rt, Image: dockerTestImage, ListenTimeout: 60 * time.Second,
	}))

	rc := revisionConfig()
	rc.ServiceConfig.EnvironmentVariables = map[string]string{"GREETING": "world"}

	created, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("real"), RevisionConfig: rc, EndpointConfig: endpointConfig(),
	})
	require.NoError(t, err)
	env.settle()

	d := &dataEnv{
		testEnv: env, domain: aws.ToString(created.Endpoint.DomainName),
		http: &http.Client{Timeout: 2 * time.Minute},
	}

	resp := d.do(t, http.MethodGet, d.domain, "/ping", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "hello /ping world", body(t, resp))
	assert.Equal(t, "prod", resp.Header.Get("X-App-Saw-Endpoint"))
	assert.NotEmpty(t, resp.Header.Get("Lambda-Web-Logstream"))

	assert.Eventually(
		t,
		func() bool { return len(logs.snapshot("/aws/lambda/web/real")) > 0 },
		10*time.Second,
		50*time.Millisecond,
	)

	var sawApp bool

	for _, line := range logs.snapshot("/aws/lambda/web/real") {
		var ev struct {
			Message any    `json:"message"`
			Level   string `json:"level"`
			Type    string `json:"type"`
		}

		require.NoError(t, json.Unmarshal([]byte(line), &ev), "every shipped line is a JSON log event")

		sawApp = sawApp || (ev.Message == "listening" && ev.Level == "INFO")
	}

	assert.True(t, sawApp, "the app's stdout line is shipped as an INFO event")
}

// TestDockerEngine_ScalesOutAndToZero runs two concurrent requests against a one-request-per-environment revision
// and expects two containers, then scale-to-zero after the idle timeout.
func TestDockerEngine_ScalesOutAndToZero(t *testing.T) {
	t.Parallel()

	rt := dockerRuntime(t)

	env := newTestEnv(t)
	codes := newFakeCodes()
	codes.objects["code/app.zip@v1"] = buildZip(t, map[string]string{"index.js": nodeApp})
	env.backend.SetCodeStore(codes)
	env.backend.SetLauncher(lambdaweb.NewDockerLauncher(env.backend, lambdaweb.DockerConfig{
		Runtime: rt, Image: dockerTestImage, ListenTimeout: 60 * time.Second,
	}))

	rc := revisionConfig()
	rc.ServiceConfig.MaxConcurrencyPerEnvironment = aws.Int32(1)
	ec := endpointConfig()
	ec.ScalingConfig = &types.ScalingConfig{MaxEnvironments: aws.Int32(2)}

	created, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("scale"), RevisionConfig: rc, EndpointConfig: ec,
	})
	require.NoError(t, err)
	env.settle()

	d := &dataEnv{
		testEnv: env, domain: aws.ToString(created.Endpoint.DomainName),
		http: &http.Client{Timeout: 2 * time.Minute},
	}

	hosts := make(chan string, 2)

	var wg sync.WaitGroup

	for range 2 {
		wg.Go(func() {
			resp := d.do(t, http.MethodGet, d.domain, "/", nil)
			hosts <- resp.Header.Get("X-App-Host")
		})
	}

	wg.Wait()
	close(hosts)

	seen := map[string]bool{}
	for h := range hosts {
		seen[h] = true
	}

	assert.Len(t, seen, 2, "each request ran in its own container")

	env.clock.Advance(10 * time.Minute)
	env.backend.SweepEnvironments()

	assert.Eventually(t, func() bool {
		out, cerr := exec.CommandContext(t.Context(), "docker", "ps", "-q", "--filter", "name=gopherstack-lambdaweb-scale-").
			Output()

		return cerr == nil && len(out) == 0
	}, 60*time.Second, 500*time.Millisecond)
}
