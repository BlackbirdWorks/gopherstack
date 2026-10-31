package app

import (
	"archive/zip"
	"bytes"
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

const extensionSource = `package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func logLine(s string) {
	f, err := os.OpenFile("/ext-out/external.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		os.Exit(1)
	}
	id := resp.Header.Get("Lambda-Extension-Identifier")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	logLine("REGISTERED " + string(body))
	for {
		nreq, _ := http.NewRequest(http.MethodGet, "http://"+api+"/2020-01-01/extension/event/next", nil)
		nreq.Header.Set("Lambda-Extension-Identifier", id)
		nresp, err := http.DefaultClient.Do(nreq)
		if err != nil {
			os.Exit(1)
		}
		raw, _ := io.ReadAll(nresp.Body)
		nresp.Body.Close()
		var ev map[string]any
		_ = json.Unmarshal(raw, &ev)
		logLine(string(raw))
		if ev["eventType"] == "SHUTDOWN" {
			return
		}
	}
}
`

const extensionHandler = `import json, os, threading, urllib.request

def _internal():
    api = os.environ["AWS_LAMBDA_RUNTIME_API"]
    req = urllib.request.Request("http://%s/2020-01-01/extension/register" % api,
        data=json.dumps({"events": ["INVOKE"]}).encode(), headers={"Lambda-Extension-Name": "internal-ext"})
    ident = urllib.request.urlopen(req).headers["Lambda-Extension-Identifier"]
    def loop():
        while True:
            r = urllib.request.Request("http://%s/2020-01-01/extension/event/next" % api,
                headers={"Lambda-Extension-Identifier": ident})
            body = urllib.request.urlopen(r).read().decode()
            with open("/ext-out/internal.log", "a") as f:
                f.write(body + "\n")
    threading.Thread(target=loop, daemon=True).start()

_internal()

def handler(event, context):
    return {"flag": os.environ.get("EXT_FLAG"), "arn": context.invoked_function_arn}
`

func buildExtensionBinary(t *testing.T) []byte {
	t.Helper()

	dir := t.TempDir()
	src := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(src, []byte(extensionSource), 0o600))

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

func extensionLayerZip(t *testing.T, bin []byte) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	hdr := &zip.FileHeader{Name: "extensions/ext-bin", Method: zip.Deflate}
	hdr.SetMode(0o755)

	w, err := zw.CreateHeader(hdr)
	require.NoError(t, err)
	_, err = w.Write(bin)
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestLambdaExtensionsAndDockerFlagsDocker(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("docker test")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", efsDockerRuntimeImage).Run(); err != nil {
		t.Skipf("lambda runtime image %s not available locally: %v", efsDockerRuntimeImage, err)
	}

	rt, err := container.NewRuntime(container.Config{
		Logger: slog.Default(), PoolSize: 2, IdleTimeout: time.Minute,
	})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	outDir := t.TempDir()
	require.NoError(t, os.Chmod(outDir, 0o777)) // #nosec G302 -- container root writes here

	settings := lambdabackend.DefaultSettings()
	settings.DockerFlags = "-e EXT_FLAG=visible -v " + outDir + ":/ext-out --privileged"

	pa := portalloctest.New(t, 20)
	bk := lambdabackend.NewInMemoryBackend(rt, pa, settings, "000000000000", "us-east-1")
	t.Cleanup(func() { bk.Close(context.Background()) })

	layer, err := bk.PublishLayerVersion(&lambdabackend.PublishLayerVersionInput{
		LayerName: "ext-layer",
		Content:   &lambdabackend.LayerVersionContentInput{ZipFile: extensionLayerZip(t, buildExtensionBinary(t))},
	})
	require.NoError(t, err)

	require.NoError(t, bk.CreateFunction(&lambdabackend.FunctionConfiguration{
		FunctionName: "ext-fn",
		PackageType:  lambdabackend.PackageTypeZip,
		Runtime:      "python3.12",
		Handler:      "index.handler",
		ZipData:      zipOf(t, "index.py", extensionHandler),
		Timeout:      60,
		Layers:       []*lambdabackend.FunctionLayer{{Arn: layer.LayerVersionArn}},
	}))

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	var firstReq string

	for range 2 {
		out, status, invokeErr := bk.InvokeFunction(ctx, "ext-fn", lambdabackend.InvocationTypeRequestResponse, []byte(`{}`))
		require.NoError(t, invokeErr)
		require.Equal(t, 200, status)
		assert.Contains(t, string(out), `"flag": "visible"`, "LAMBDA_DOCKER_FLAGS -e reached the container")
		assert.Contains(t, string(out), "arn:aws:lambda:us-east-1:000000000000:function:ext-fn")

		if firstReq == "" {
			firstReq = string(out)
		}
	}

	readLog := func(name string) string {
		b, _ := os.ReadFile(filepath.Join(outDir, name))

		return string(b)
	}

	require.Eventually(t, func() bool {
		return strings.Count(readLog("external.log"), `"eventType":"INVOKE"`) == 2 &&
			strings.Count(readLog("internal.log"), `"eventType":"INVOKE"`) == 2
	}, 30*time.Second, 50*time.Millisecond, "external=%q internal=%q", readLog("external.log"), readLog("internal.log"))

	assert.Contains(t, readLog("external.log"), `"functionName":"ext-fn"`)
	assert.NotContains(t, readLog("external.log"), "SHUTDOWN")

	require.NoError(t, bk.DeleteFunction("ext-fn"))

	require.Eventually(t, func() bool {
		return strings.Contains(readLog("external.log"), `"eventType":"SHUTDOWN"`)
	}, 30*time.Second, 50*time.Millisecond, "external=%q", readLog("external.log"))

	assert.Contains(t, readLog("external.log"), `"shutdownReason":"spindown"`)
	assert.NotContains(t, readLog("internal.log"), "SHUTDOWN", "internal extensions cannot register for SHUTDOWN")
}
