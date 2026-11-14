package app

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

const telemetryExtensionSource = `package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	cl = new(http.Client)
	mu sync.Mutex
)

func record(s string) {
	mu.Lock()
	defer mu.Unlock()
	f, err := os.OpenFile("/ext-out/telemetry.ndjson", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, s)
}

func main() {
	api := os.Getenv("AWS_LAMBDA_RUNTIME_API")
	req, _ := http.NewRequest(http.MethodPost, "http://"+api+"/2020-01-01/extension/register",
		strings.NewReader(` + "`" + `{"events":["INVOKE","SHUTDOWN"]}` + "`" + `))
	req.Header.Set("Lambda-Extension-Name", filepath.Base(os.Args[0]))
	resp, err := cl.Do(req)
	if err != nil {
		os.Exit(1)
	}
	id := resp.Header.Get("Lambda-Extension-Identifier")
	resp.Body.Close()

	ln, err := net.Listen("tcp", "0.0.0.0:4243")
	if err != nil {
		os.Exit(1)
	}
	go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		record(string(b))
	}))

	sub, _ := http.NewRequest(http.MethodPut, "http://"+api+"/2022-07-01/telemetry", strings.NewReader(
		` + "`" + `{"schemaVersion":"2022-12-13","types":["platform","function","extension"],
		"buffering":{"maxItems":1000,"maxBytes":262144,"timeoutMs":25},
		"destination":{"protocol":"HTTP","URI":"http://sandbox.localdomain:4243/"}}` + "`" + `))
	sub.Header.Set("Lambda-Extension-Identifier", id)
	sresp, err := cl.Do(sub)
	if err != nil || sresp.StatusCode != 200 {
		record("SUBSCRIBE-FAILED")
		os.Exit(1)
	}
	sresp.Body.Close()
	fmt.Println("extension says hello")

	for {
		nreq, _ := http.NewRequest(http.MethodGet, "http://"+api+"/2020-01-01/extension/event/next", nil)
		nreq.Header.Set("Lambda-Extension-Identifier", id)
		nresp, err := cl.Do(nreq)
		if err != nil {
			os.Exit(1)
		}
		raw, _ := io.ReadAll(nresp.Body)
		nresp.Body.Close()
		if strings.Contains(string(raw), "SHUTDOWN") {
			return
		}
	}
}
`

const telemetryHandler = `def handler(event, context):
    print("hello from function")
    return {"ok": True}
`

const telemetryDockerfile = `FROM ` + efsDockerRuntimeImage + `
COPY handler.py ${LAMBDA_TASK_ROOT}/
COPY ext-bin /opt/extensions/ext-bin
CMD ["handler.handler"]
`

func buildTelemetryExtension(t *testing.T) []byte {
	t.Helper()

	dir := t.TempDir()
	src := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(src, []byte(telemetryExtensionSource), 0o600))

	out := filepath.Join(dir, "ext-bin")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", out, src)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOFLAGS=", "GOWORK=off", "GO111MODULE=off")

	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot build extension binary: %v: %s", err, combined)
	}

	bin, err := os.ReadFile(out)
	require.NoError(t, err)

	return bin
}

func buildExtensionImage(t *testing.T, bin []byte) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.py"), []byte(telemetryHandler), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ext-bin"), bin, 0o600))
	require.NoError(t, os.Chmod(filepath.Join(dir, "ext-bin"), 0o755)) // #nosec G302 -- executable extension
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(telemetryDockerfile), 0o600))

	tag := "gopherstack-telemetry-ext-test:" + strings.ToLower(filepath.Base(dir))
	build := exec.CommandContext(t.Context(), "docker", "build", "-q", "-t", tag, dir)
	if out, err := build.CombinedOutput(); err != nil {
		t.Skipf("cannot build image: %v: %s", err, out)
	}

	t.Cleanup(func() {
		_ = exec.CommandContext(context.Background(), "docker", "rmi", "-f", tag).Run()
	})

	return tag
}

func TestLambdaTelemetryAPIDocker(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("docker test")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", efsDockerRuntimeImage).Run(); err != nil {
		t.Skipf("lambda runtime image %s not available locally: %v", efsDockerRuntimeImage, err)
	}

	bin := buildTelemetryExtension(t)
	imageTag := buildExtensionImage(t, bin)

	tests := []struct {
		setup func(t *testing.T, bk *lambdabackend.InMemoryBackend, fn *lambdabackend.FunctionConfiguration)
		name  string
	}{
		{
			name: "zip with layer",
			setup: func(t *testing.T, bk *lambdabackend.InMemoryBackend, fn *lambdabackend.FunctionConfiguration) {
				t.Helper()

				layer, err := bk.PublishLayerVersion(&lambdabackend.PublishLayerVersionInput{
					LayerName: "telemetry-layer",
					Content:   &lambdabackend.LayerVersionContentInput{ZipFile: extensionLayerZip(t, bin)},
				})
				require.NoError(t, err)

				fn.PackageType = lambdabackend.PackageTypeZip
				fn.Runtime = "python3.12"
				fn.Handler = "tel_handler.handler"
				fn.ZipData = zipOf(t, "tel_handler.py", telemetryHandler)
				fn.Layers = []*lambdabackend.FunctionLayer{{Arn: layer.LayerVersionArn}}
			},
		},
		{
			name: "container image",
			setup: func(t *testing.T, _ *lambdabackend.InMemoryBackend, fn *lambdabackend.FunctionConfiguration) {
				t.Helper()

				fn.PackageType = lambdabackend.PackageTypeImage
				fn.ImageURI = imageTag
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, err := container.NewRuntime(
				container.Config{Logger: slog.Default(), PoolSize: 2, IdleTimeout: time.Minute},
			)
			if err != nil {
				t.Skipf("container runtime unavailable: %v", err)
			}

			t.Cleanup(func() { _ = rt.Close() })

			outDir := t.TempDir()
			require.NoError(t, os.Chmod(outDir, 0o777)) // #nosec G302 -- container root writes here

			settings := lambdabackend.DefaultSettings()
			settings.DockerFlags = "-v " + outDir + ":/ext-out"

			bk := lambdabackend.NewInMemoryBackend(rt, portalloctest.New(t, 20), settings, "000000000000", "us-east-1")
			t.Cleanup(func() { bk.Close(context.Background()) })

			fn := &lambdabackend.FunctionConfiguration{FunctionName: "tel-fn", Timeout: 60, MemorySize: 256}
			tt.setup(t, bk, fn)
			require.NoError(t, bk.CreateFunction(fn))

			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()

			out, status, invokeErr := bk.InvokeFunction(
				ctx,
				"tel-fn",
				lambdabackend.InvocationTypeRequestResponse,
				[]byte(`{}`),
			)
			require.NoError(t, invokeErr)
			require.Equal(t, 200, status)
			assert.Contains(t, string(out), "true")

			read := func() string {
				b, _ := os.ReadFile(filepath.Join(outDir, "telemetry.ndjson"))

				return string(b)
			}

			for _, want := range []string{
				`"type":"platform.initStart"`, `"type":"platform.telemetrySubscription"`, `"type":"platform.extension"`,
				`"type":"platform.initRuntimeDone"`, `"type":"platform.initReport"`, `"type":"platform.start"`,
				`"type":"platform.runtimeDone"`, `"type":"platform.report"`,
				`"type":"function","record":"hello from function"`,
				`"type":"extension","record":"extension says hello"`,
				`"memorySizeMB":256`, `"functionName":"tel-fn"`,
			} {
				require.Eventually(t, func() bool { return strings.Contains(read(), want) },
					30*time.Second, 50*time.Millisecond, "missing %s in %s", want, read())
			}

			assert.NotContains(t, read(), "SUBSCRIBE-FAILED")
			assert.Contains(t, read(), `"maxMemoryUsedMB"`)

			require.NoError(t, bk.DeleteFunction("tel-fn"))
		})
	}
}
