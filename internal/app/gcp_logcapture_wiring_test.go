package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcprunbackend "github.com/blackbirdworks/gopherstack/services/gcprun"
)

// logRuntime serves HTTP on each container's published port and emits fixed output lines.
type logRuntime struct {
	boxes map[string]*logBox
	mu    sync.Mutex
	seq   int
}

type logBox struct {
	srv  *http.Server
	gone chan struct{}
}

func (r *logRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	_, host, _, err := container.ParsePortSpec(spec.Ports[0])
	if err != nil {
		return "", err
	}

	l, err := net.Listen("tcp", net.JoinHostPort(container.LoopbackHost, host))
	if err != nil {
		return "", err
	}

	b := &logBox{gone: make(chan struct{}), srv: &http.Server{Handler: http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") },
	)}}

	go func() { _ = b.srv.Serve(l) }()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.seq++
	id := fmt.Sprintf("log-%d", r.seq)

	if r.boxes == nil {
		r.boxes = map[string]*logBox{}
	}

	r.boxes[id] = b

	return id, nil
}

func (r *logRuntime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	b := r.boxes[id]
	delete(r.boxes, id)
	r.mu.Unlock()

	if b != nil {
		_ = b.srv.Close()

		close(b.gone)
	}

	return nil
}

func (r *logRuntime) StreamLogs(ctx context.Context, id string, stdout, stderr io.Writer) error {
	r.mu.Lock()
	b := r.boxes[id]
	r.mu.Unlock()

	if b == nil {
		return nil
	}

	_, _ = io.WriteString(stdout, "booted\n")
	_, _ = io.WriteString(stderr, "warn: slow disk\n")

	select {
	case <-b.gone:
	case <-ctx.Done():
	}

	return nil
}

func TestWireGCPLogCaptureCloudRun(t *testing.T) {
	t.Parallel()

	logH := gcploggingbackend.NewHandler(gcploggingbackend.NewInMemoryBackend())
	runH := gcprunbackend.NewHandler(gcprunbackend.NewInMemoryBackend())
	rt := &logRuntime{}

	runH.Backend.SetRuntime(func() (gcprunbackend.Runner, error) { return rt, nil })
	wireGCPLogCapture(map[string]service.Registerable{"GCPLogging": logH, "GCPRun": runH})
	t.Cleanup(runH.Backend.Reset)

	op, err := runH.Backend.CreateService(&runpb.CreateServiceRequest{
		Parent: "projects/test-project/locations/us-central1", ServiceId: "web",
		Service: &runpb.Service{Template: &runpb.RevisionTemplate{
			Containers: []*runpb.Container{{Image: "example/web:1"}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, op)

	runH.Backend.Drain()

	svc, err := runH.Backend.GetService("projects/test-project/locations/us-central1/services/web")
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, svc.GetUri()+"/x", nil)
	rec := httptest.NewRecorder()
	runH.Backend.IngressHandler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var got []*loggingpb.LogEntry

	require.Eventually(t, func() bool {
		var lerr error

		got, _, lerr = logH.Backend.ListLogEntries(&loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{"projects/test-project"},
			Filter:        `resource.type="cloud_run_revision"`,
		})

		return lerr == nil && len(got) == 3
	}, 10*time.Second, 10*time.Millisecond)

	byLog := map[string]*loggingpb.LogEntry{}
	for _, e := range got {
		byLog[e.GetLogName()] = e
		assert.Equal(t, "web", e.GetResource().GetLabels()["service_name"])
	}

	assert.Equal(t, "booted", byLog["projects/test-project/logs/run.googleapis.com%2Fstdout"].GetTextPayload())
	assert.Equal(t, "warn: slow disk", byLog["projects/test-project/logs/run.googleapis.com%2Fstderr"].GetTextPayload())
	reqLog := byLog["projects/test-project/logs/run.googleapis.com%2Frequests"]
	assert.Equal(t, int32(http.StatusOK), reqLog.GetHttpRequest().GetStatus())
}

func TestWireGCPLogCaptureMissingServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		byName map[string]service.Registerable
		name   string
	}{
		{name: "empty", byName: map[string]service.Registerable{}},
		{name: "no logging", byName: map[string]service.Registerable{
			"GCPRun": gcprunbackend.NewHandler(gcprunbackend.NewInMemoryBackend()),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.NotPanics(t, func() { wireGCPLogCapture(tt.byName) })
		})
	}
}
