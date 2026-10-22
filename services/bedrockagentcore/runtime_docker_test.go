package bedrockagentcore_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

const agentDockerfile = `FROM python:3.12-slim
COPY server.py /server.py
EXPOSE 8080
CMD ["python3", "/server.py"]
`

// agentServer implements the AgentCore runtime service contract: GET /ping and POST /invocations on port 8080.
const agentServer = `import json, os, signal, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

signal.signal(signal.SIGTERM, lambda *a: os._exit(0))
CONTAINER = uuid.uuid4().hex
COUNT = 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        body = json.dumps({"status": "Healthy"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        global COUNT
        COUNT += 1
        data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if data.get("fail"):
            self.send_response(400)
            self.send_header("Content-Length", "4")
            self.end_headers()
            self.wfile.write(b"nope")
            return
        if data.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            for i in range(3):
                self.wfile.write(("data: chunk %d\n\n" % i).encode())
                self.wfile.flush()
            return
        body = json.dumps({
            "container": CONTAINER, "count": COUNT, "echo": data,
            "greeting": os.environ.get("GREETING"),
            "session": self.headers.get("X-Amzn-Bedrock-AgentCore-Runtime-Session-Id"),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
`

func dockerRuntime(t *testing.T) container.Runtime {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err = rt.Ping(ctx); err != nil {
		_ = rt.Close()
		t.Skipf("container runtime not reachable: %v", err)
	}

	return rt
}

// buildAgentImage builds the test agent and removes it by ID when the test ends; it returns the ECR-style URI.
func buildAgentImage(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(agentDockerfile), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "server.py"), []byte(agentServer), 0o600))

	uri := testAccount + ".dkr.ecr.us-east-1.amazonaws.com/agentcore-it:" + awsid.NewN("", 8)

	out, err := exec.CommandContext(t.Context(), "docker", "build", "-q", "-t", uri, dir).Output()
	if err != nil {
		t.Skipf("docker build failed (base image unavailable?): %v", err)
	}

	id := strings.TrimSpace(string(out))

	t.Cleanup(func() {
		_ = exec.CommandContext(context.Background(), "docker", "rmi", "-f", id).Run()
	})

	return uri
}

func liveContainers(t *testing.T, image string) []string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--filter", "ancestor="+image).Output()
	require.NoError(t, err)

	return strings.Fields(string(out))
}

func dockerFixture(t *testing.T, ctl fakeControl) *fixture {
	t.Helper()

	rt := dockerRuntime(t)
	f := newFixture(t, ctl)
	f.backend.EnableEngine(bedrockagentcore.EngineConfig{
		Runtime: rt, PollInterval: 100 * time.Millisecond, StartTimeout: 2 * time.Minute,
	})

	return f
}

const runtimeARN = "arn:aws:bedrock-agentcore:us-east-1:" + testAccount + ":runtime/" + testRuntime

func invokeRuntime(
	t *testing.T,
	f *fixture,
	session string,
	payload any,
) (map[string]any, *sdk.InvokeAgentRuntimeOutput) {
	t.Helper()

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	out, err := f.client.InvokeAgentRuntime(t.Context(), &sdk.InvokeAgentRuntimeInput{
		AgentRuntimeArn: new(
			runtimeARN,
		),
		RuntimeSessionId: &session,
		Payload:          body,
		ContentType:      new("application/json"),
	})
	require.NoError(t, err)

	defer out.Response.Close()

	raw, err := io.ReadAll(out.Response)
	require.NoError(t, err)

	var got map[string]any
	if json.Unmarshal(raw, &got) != nil {
		got = map[string]any{"raw": string(raw)}
	}

	return got, out
}

func TestInvokeAgentRuntime_Docker(t *testing.T) {
	t.Parallel()

	image := buildAgentImage(t)
	ctl := defaultControl()
	ctl.runtimes[testRuntime+"|DEFAULT"] = bedrockagentcore.RuntimeInfo{
		ID: testRuntime, ImageURI: image, Protocol: "HTTP", Env: map[string]string{"GREETING": "hi"},
	}

	f := dockerFixture(t, ctl)
	sessA := "session-a-" + strings.Repeat("a", 30)
	sessB := "session-b-" + strings.Repeat("b", 30)

	first, out := invokeRuntime(t, f, sessA, map[string]any{"prompt": "hello"})
	assert.EqualValues(t, 200, *out.StatusCode)
	assert.Equal(t, sessA, *out.RuntimeSessionId)
	assert.Equal(t, "hi", first["greeting"], "runtime environment variables reach the container")
	assert.Equal(t, sessA, first["session"])
	assert.Equal(t, "hello", first["echo"].(map[string]any)["prompt"])
	assert.EqualValues(t, 1, first["count"])

	again, _ := invokeRuntime(t, f, sessA, map[string]any{"prompt": "again"})
	assert.Equal(t, first["container"], again["container"], "the session keeps its container warm")
	assert.EqualValues(t, 2, again["count"])

	other, _ := invokeRuntime(t, f, sessB, map[string]any{})
	assert.NotEqual(t, first["container"], other["container"], "another session gets its own container")

	stream, sout := invokeRuntime(t, f, sessA, map[string]any{"stream": true})
	assert.Equal(t, "text/event-stream", *sout.ContentType)
	assert.Equal(t, "data: chunk 0\n\ndata: chunk 1\n\ndata: chunk 2\n\n", stream["raw"])

	_, err := f.client.InvokeAgentRuntime(t.Context(), &sdk.InvokeAgentRuntimeInput{
		AgentRuntimeArn: new(runtimeARN), RuntimeSessionId: &sessA, Payload: []byte(`{"fail":true}`),
	})
	assert.Equal(t, "RuntimeClientError", apiCode(err), "an agent HTTP error surfaces as RuntimeClientError")

	f.clock.advance(16 * time.Minute)
	f.backend.ReapIdle()

	fresh, _ := invokeRuntime(t, f, sessA, map[string]any{})
	assert.NotEqual(t, first["container"], fresh["container"], "the idle session was reaped on the injected clock")
	assert.EqualValues(t, 1, fresh["count"])

	stop, err := f.client.StopRuntimeSession(t.Context(), &sdk.StopRuntimeSessionInput{
		AgentRuntimeArn: new(runtimeARN), RuntimeSessionId: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, sessA, *stop.RuntimeSessionId)

	_, err = f.client.StopRuntimeSession(t.Context(), &sdk.StopRuntimeSessionInput{
		AgentRuntimeArn: new(runtimeARN), RuntimeSessionId: &sessA,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	f.backend.Close()
	assert.Empty(t, liveContainers(t, image), "Close removes every runtime container")
}

func TestCodeInterpreter_Docker(t *testing.T) {
	t.Parallel()

	f := dockerFixture(t, defaultControl())
	session := startCI(t, f)

	_, err := invoke(t, f, session, types.ToolNameWriteFiles, &types.ToolArguments{
		Content: []types.InputContentBlock{{Path: new("in.txt"), Text: new("forty-two")}},
	})
	require.NoError(t, err)

	tests := []struct {
		name   string
		tool   types.ToolName
		args   *types.ToolArguments
		stdout string
		failed bool
	}{
		{
			name: "python", tool: types.ToolNameExecuteCode, stdout: "4\n",
			args: &types.ToolArguments{Code: new("print(2 + 2)"), Language: types.ProgrammingLanguagePython},
		},
		{
			name:   "reads written file",
			tool:   types.ToolNameExecuteCode,
			stdout: "forty-two\n",
			args: &types.ToolArguments{
				Code:     new("print(open('in.txt').read())"),
				Language: types.ProgrammingLanguagePython,
			},
		},
		{
			name: "shell", tool: types.ToolNameExecuteCommand, stdout: "hi\n",
			args: &types.ToolArguments{Command: new("echo hi")},
		},
		{
			name: "python error", tool: types.ToolNameExecuteCode, failed: true,
			args: &types.ToolArguments{Code: new("raise SystemExit(3)"), Language: types.ProgrammingLanguagePython},
		},
	}

	for _, tt := range tests {
		res, ierr := invoke(t, f, session, tt.tool, tt.args)
		require.NoError(t, ierr, tt.name)
		assert.Equal(t, tt.failed, *res.IsError, tt.name)

		if tt.failed {
			assert.EqualValues(t, 3, *res.StructuredContent.ExitCode, tt.name)

			continue
		}

		assert.Equal(t, tt.stdout, *res.StructuredContent.Stdout, tt.name)
	}

	res, err := invoke(
		t,
		f,
		session,
		types.ToolNameExecuteCommand,
		&types.ToolArguments{Command: new("echo out > made.txt")},
	)
	require.NoError(t, err)
	assert.False(t, *res.IsError)

	read, err := invoke(t, f, session, types.ToolNameReadFiles, &types.ToolArguments{Paths: []string{"made.txt"}})
	require.NoError(t, err)
	assert.Equal(t, "out\n", *read.Content[0].Resource.Text, "files written by code are visible to readFiles")

	_, err = f.client.StopCodeInterpreterSession(t.Context(), &sdk.StopCodeInterpreterSessionInput{
		CodeInterpreterIdentifier: new(ciID), SessionId: &session,
	})
	require.NoError(t, err)
}
