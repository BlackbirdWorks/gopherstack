package lambdaweb_test

import (
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const dockerTestImage = "node:20-slim"

const nodeApp = `const http = require('http');
const [host, port] = process.env.AWS_LAMBDA_HTTP_ENDPOINT.split(':');
http.createServer((req, res) => {
  res.setHeader('x-app-saw-endpoint', req.headers['lambda-web-endpoint-name'] || '');
  res.end('hello ' + req.url + ' ' + (process.env.GREETING || ''));
}).listen(Number(port), host, () => console.log('listening'));
`

// TestDockerEngine_ServesRealApp runs a real node container behind the endpoint domain. It is skipped when no
// container runtime or the node image is available locally.
func TestDockerEngine_ServesRealApp(t *testing.T) {
	t.Parallel()

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
	assert.Equal(t, []string{"listening"}, logs.snapshot("/aws/lambda/web/real")[:1])
}
