package gcpfunctions_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

const dockerOptIn = "GOPHERSTACK_GCF_DOCKER_TEST"

// TestDocker_NodeFunction runs a real Node.js function through the Functions Framework. It needs a container
// runtime, network access for npx and the node image, so it is opt-in via GOPHERSTACK_GCF_DOCKER_TEST=1.
func TestDocker_NodeFunction(t *testing.T) {
	t.Parallel()

	if os.Getenv(dockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", dockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "gcpfunctions.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	e := newEnv(t, false)
	e.handler.Backend.SetRuntime(func() (gcpfunctions.Runner, error) { return lr.Get() })
	e.handler.Backend.SetStartupTimeout(5 * time.Minute)

	fn := e.createFunction(t, "real", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+e.handler.IngressAddr()+"/", nil)
	require.NoError(t, err)

	req.Host = hostOf(fn)

	resp, err := e.http.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// handlerImage is a tiny image whose "npx" stands in for the Functions Framework: it serves the PORT with nc and
// logs the CloudEvent headers of every request to the container's stdout.
const handlerImage = `FROM alpine:3.21
RUN printf '%s\n' '#!/bin/sh' \
  'read -r method path rest' \
  'while read -r l; do l=$(printf "%s" "$l" | tr -d "\r"); [ -z "$l" ] && break; \
case "$l" in [Cc]e-*|[Cc]ontent-[Tt]ype:*) echo "recv $l" > /proc/1/fd/1;; esac; done' \
  'printf "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"' > /handler.sh \
  && chmod +x /handler.sh \
  && printf '%s\n' '#!/bin/sh' 'exec nc -lk -p "$PORT" -e /handler.sh' > /usr/local/bin/npx \
  && chmod +x /usr/local/bin/npx
`

// TestDocker_StorageTriggeredFunction deploys a function triggered by the GCS emulator onto a real container and
// checks the CloudEvent headers that reach it. It builds a tiny local image, so only Docker and alpine are needed.
func TestDocker_StorageTriggeredFunction(t *testing.T) {
	t.Parallel()

	if os.Getenv(dockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", dockerOptIn)
	}

	raw := make([]byte, 4)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	image := "gopherstack-gcf-test:" + hex.EncodeToString(raw)

	build := exec.CommandContext(t.Context(), "docker", "build", "-q", "-t", image, "-")
	build.Stdin = strings.NewReader(handlerImage)

	if out, berr := build.CombinedOutput(); berr != nil {
		t.Skipf("cannot build the test image (is alpine:3.21 available?): %v: %s", berr, out)
	}

	t.Cleanup(func() {
		_ = exec.CommandContext(context.WithoutCancel(t.Context()), "docker", "rmi", "-f", image).Run()
	})

	lr := gcpserve.NewLazyRuntime(slog.Default(), "gcpfunctions.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err = lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	gcs := gcpstorage.NewInMemoryBackend()
	_, err = gcs.CreateBucket("test-project", gcpstorage.Patch{"name": json.RawMessage(`"inbox"`)})
	require.NoError(t, err)

	e := newEnv(t, false)
	ch := make(chan *loggingpb.LogEntry, 64)
	e.handler.Backend.SetLogSink(chanSink{ch: ch})
	e.handler.Backend.SetRuntime(func() (gcpfunctions.Runner, error) { return lr.Get() })
	e.handler.Backend.SetImageResolver(func(_, _ string) string { return image })
	e.handler.Backend.SetBucketLookup(realBuckets{gcs})
	gcs.ObserveObjects(func(ev gcpstorage.ObjectEvent) {
		e.handler.Backend.HandleStorageEvent(gcpfunctions.StorageEvent{
			Type: string(ev.Type), Bucket: ev.Object.Bucket, Name: ev.Object.Name, Data: ev.Data(), Time: ev.Time,
		})
	})

	triggerFn(t, e, typeFinalized, 0, filter("bucket", "inbox"))

	_, err = gcs.PutObject("inbox", "docs/a.txt", []byte("hello"), gcpstorage.Patch{}, gcpstorage.Preconds{})
	require.NoError(t, err)

	want := []string{
		"recv ce-type: " + typeFinalized,
		"recv ce-subject: objects/docs/a.txt",
		"recv ce-source: //storage.googleapis.com/projects/_/buckets/inbox",
		"recv content-type: application/json",
	}
	seen := map[string]bool{}
	deadline := time.After(2 * time.Minute)

	for len(seen) < len(want) {
		select {
		case entry := <-ch:
			line := strings.ToLower(entry.GetTextPayload())
			for _, w := range want {
				if line == strings.ToLower(w) {
					seen[w] = true
				}
			}
		case <-deadline:
			require.FailNow(t, "container never logged the CloudEvent headers", "seen: %v", seen)
		}
	}

	assert.Len(t, seen, len(want))
}

type realBuckets struct{ b *gcpstorage.InMemoryBackend }

func (r realBuckets) BucketExists(name string) bool {
	_, err := r.b.GetBucket(name, gcpstorage.Preconds{})

	return err == nil
}
