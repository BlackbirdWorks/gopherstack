package lambda //nolint:testpackage // needs access to runtimeServer.queue and lookupOrRegisterRuntime.

// Benchmarks the Invoke request path's own overhead -- qualifier resolution,
// recursion-loop tracking, FIS fault check, concurrency accounting, and
// runtime lookup -- excluding actual container execution. Event-type
// invocations return as soon as the request is queued for the (mocked)
// container, so the timed loop never blocks on a real function running; a
// background goroutine drains the runtime queue so it never backs up and
// forces the slow (5-minute-timeout) enqueue path.

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	gophercontainer "github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// benchMockDockerAPI is a minimal container.APIClient that never talks to a
// real daemon, mirroring handler_runtime_test.go's mockDockerAPI (that copy
// lives in package lambda_test and so isn't visible here).
type benchMockDockerAPI struct{}

func (benchMockDockerAPI) ImagePull(context.Context, string, image.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (benchMockDockerAPI) ImageList(context.Context, image.ListOptions) ([]image.Summary, error) {
	return nil, nil
}

func (benchMockDockerAPI) ContainerCreate(
	context.Context, *container.Config, *container.HostConfig, any, any, string,
) (container.CreateResponse, error) {
	return container.CreateResponse{ID: "bench-container"}, nil
}

func (benchMockDockerAPI) ContainerStart(context.Context, string, container.StartOptions) error {
	return nil
}

func (benchMockDockerAPI) ContainerStop(context.Context, string, container.StopOptions) error {
	return nil
}

func (benchMockDockerAPI) ContainerRemove(context.Context, string, container.RemoveOptions) error {
	return nil
}

func (benchMockDockerAPI) Ping(context.Context) (any, error) { return struct{}{}, nil }
func (benchMockDockerAPI) Close() error                      { return nil }

// BenchmarkInvokeFunction_Overhead measures InvokeFunction's synchronous
// overhead for an Event (fire-and-forget) invocation against an
// already-running mocked runtime, isolating request-path cost from
// container execution time.
func BenchmarkInvokeFunction_Overhead(b *testing.B) {
	pa, err := portalloc.New(19900, 19950)
	if err != nil {
		b.Fatalf("portalloc.New: %v", err)
	}

	dc := gophercontainer.NewDockerRuntimeWithAPI(benchMockDockerAPI{}, gophercontainer.Config{
		PoolSize: 3,
	})

	bk := NewInMemoryBackend(dc, pa, DefaultSettings(), "000000000000", "us-east-1")
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		bk.Close(ctx)
	})

	fn := &FunctionConfiguration{
		FunctionName: "bench-overhead-fn",
		ImageURI:     "myimage:latest",
		Timeout:      3,
	}
	if createErr := bk.CreateFunction(fn); createErr != nil {
		b.Fatalf("CreateFunction: %v", createErr)
	}

	// Force runtime (and mock container) creation once, outside the timed loop.
	if _, _, warmupErr := bk.InvokeFunction(
		b.Context(), fn.FunctionName, InvocationTypeEvent, []byte(`{}`),
	); warmupErr != nil {
		b.Fatalf("warmup InvokeFunction: %v", warmupErr)
	}

	rt, err := bk.lookupOrRegisterRuntime(fn)
	if err != nil {
		b.Fatalf("lookupOrRegisterRuntime: %v", err)
	}
	if rt.srv == nil {
		b.Fatal("runtime server not started after warmup invoke")
	}

	// Drain the runtime queue continuously so the benchmark never hits the
	// slow (blocking, 5-minute-timeout) enqueue path once the small buffered
	// queue (runtimeQueueSize) fills up. Stops when the backend shuts down
	// (b.Cleanup above), since srv.queue is never closed independently.
	stopDrain := make(chan struct{})
	b.Cleanup(func() { close(stopDrain) })

	const drainers = 8
	for range drainers {
		go func() {
			for {
				select {
				case <-rt.srv.queue:
				case <-stopDrain:
					return
				}
			}
		}()
	}

	payload := []byte(`{"key":"value"}`)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if _, _, invokeErr := bk.InvokeFunction(
			b.Context(), fn.FunctionName, InvocationTypeEvent, payload,
		); invokeErr != nil {
			b.Fatalf("InvokeFunction: %v", invokeErr)
		}
	}
}
