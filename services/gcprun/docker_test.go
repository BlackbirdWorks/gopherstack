package gcprun_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/services/gcprun"
)

// dockerfile installs a tiny nc-based HTTP server: it answers /slow after four seconds and otherwise serves the
// file under /www named by the request path.
const dockerfile = `FROM alpine:3.21
RUN mkdir -p /www && printf '%s\n' '#!/bin/sh' \
  'read -r method path rest' \
  'while read -r l; do [ "$l" = "$(printf "\r")" ] && break; done' \
  'case "$path" in /slow) sleep 4; body=slow-done;; /) body=$(cat /www/index.html);; *) body=$(cat "/www$path" 2>/dev/null);; esac' \
  'printf "HTTP/1.1 200 OK\r\nContent-Length: %s\r\nConnection: close\r\n\r\n%s" "${#body}" "$body"' \
  > /handler.sh && chmod +x /handler.sh
`

// dockerEnv builds a tiny image from a local base and returns an env backed by the real container runtime.
func dockerEnv(t *testing.T) (*env, string) {
	t.Helper()

	if os.Getenv("GOPHERSTACK_TEST_DOCKER") != "1" {
		t.Skip("set GOPHERSTACK_TEST_DOCKER=1 to run docker-backed Cloud Run tests")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	raw := make([]byte, 4)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	image := "gopherstack-gcprun-test:" + hex.EncodeToString(raw)

	build := exec.CommandContext(t.Context(), "docker", "build", "-q", "-t", image, "-")
	build.Stdin = strings.NewReader(dockerfile)

	if out, berr := build.CombinedOutput(); berr != nil {
		t.Skipf("cannot build the test image (is alpine:3.21 available?): %v: %s", berr, out)
	}

	t.Cleanup(func() {
		_ = exec.CommandContext(context.WithoutCancel(t.Context()), "docker", "rmi", "-f", image).Run()
	})

	lr := gcpserve.NewLazyRuntime(slog.Default(), "gcprun.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	_, err = lr.Get()
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	return newEnvWith(t, newFakeRuntime(), func() (gcprun.Runner, error) { return lr.Get() }), image
}

func containerCount(t *testing.T, prefix string) int {
	t.Helper()

	var out bytes.Buffer

	cmd := exec.CommandContext(t.Context(), "docker", "ps", "-q", "--filter", "name="+prefix)
	cmd.Stdout = &out
	require.NoError(t, cmd.Run())

	return len(strings.Fields(out.String()))
}

func TestDocker_SidecarSharesLocalhostAndVolumes(t *testing.T) {
	t.Parallel()

	e, image := dockerEnv(t)

	sidecar := &runpb.Container{
		Name: "sidecar", Image: image,
		Command: []string{"sh", "-c"},
		Args: []string{
			"echo sidecar-ok > /www/index.html && echo shared-ok > /shared/f.txt && " +
				"exec nc -lk -p 9000 -e /handler.sh",
		},
		StartupProbe: tcpProbe(9000, 1, 30),
		VolumeMounts: []*runpb.VolumeMount{{Name: "scratch", MountPath: "/shared"}},
	}
	app := &runpb.Container{
		Name: "app", Image: image, DependsOn: []string{"sidecar"},
		Ports:   []*runpb.ContainerPort{{ContainerPort: 8080}},
		Command: []string{"sh", "-c"},
		Args: []string{
			"echo app-ok > /www/index.html && wget -qO- http://localhost:9000/ > /www/side.txt && " +
				"cat /shared/f.txt > /www/shared.txt && exec nc -lk -p $PORT -e /handler.sh",
		},
		VolumeMounts: []*runpb.VolumeMount{{Name: "scratch", MountPath: "/shared"}},
	}

	spec := multiService(app, sidecar)
	spec.Template.Volumes = []*runpb.Volume{{
		Name: "scratch", VolumeType: &runpb.Volume_EmptyDir{EmptyDir: &runpb.EmptyDirVolumeSource{
			Medium: runpb.EmptyDirVolumeSource_MEMORY,
		}},
	}}

	svc, err := e.deploy(t, "web", spec)
	require.NoError(t, err)

	host := strings.TrimPrefix(svc.GetUri(), "http://")

	tests := []struct {
		path string
		want string
	}{
		{"/", "app-ok"},
		{"/side.txt", "sidecar-ok"},
		{"/shared.txt", "shared-ok"},
	}

	for _, tt := range tests {
		code, body := e.get(t, host, tt.path)
		assert.Equal(t, 200, code, tt.path)
		assert.Equal(t, tt.want, strings.TrimSpace(body), tt.path)
	}
}

func TestDocker_ConcurrencyScalesOut(t *testing.T) {
	t.Parallel()

	e, image := dockerEnv(t)

	spec := svcSpec(image)
	spec.Template.MaxInstanceRequestConcurrency = 1
	spec.Template.Scaling = &runpb.RevisionScaling{MaxInstanceCount: 3}
	spec.Template.Containers[0].Ports = []*runpb.ContainerPort{{ContainerPort: 8080}}
	spec.Template.Containers[0].Command = []string{"sh", "-c"}
	spec.Template.Containers[0].Args = []string{"echo ok > /www/index.html && exec nc -lk -p $PORT -e /handler.sh"}

	svc, err := e.deploy(t, "scaled", spec)
	require.NoError(t, err)

	host := strings.TrimPrefix(svc.GetUri(), "http://")
	first := e.fetch(t, host, "/slow")
	second := e.fetch(t, host, "/slow")

	rev := svc.GetLatestReadyRevision()
	prefix := "gopherstack-run-" + rev[strings.LastIndex(rev, "/")+1:]

	require.Eventually(t, func() bool { return containerCount(t, prefix) == 2 }, time.Minute, 100*time.Millisecond,
		"a second instance starts while the first is busy")

	for _, ch := range []<-chan result{first, second} {
		r := <-ch
		require.NoError(t, r.err)
		assert.Equal(t, 200, r.code)
		assert.Contains(t, r.body, "slow-done")
	}
}
