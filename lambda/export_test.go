// This file exports internal types for use in external (_test) packages.
// It is compiled only during testing.
package lambda

import (
	"context"
	"log/slog"
	"time"
)

// ExportedRuntimeServer wraps the internal runtimeServer for white-box testing.
type ExportedRuntimeServer struct {
	inner *runtimeServer
}

// NewExportedRuntimeServer creates an ExportedRuntimeServer on the given port.
// The server is not started until Start is called.
func NewExportedRuntimeServer(port int) *ExportedRuntimeServer {
	return &ExportedRuntimeServer{
		inner: newRuntimeServer(port, slog.Default()),
	}
}

// Start begins listening on the configured port.
func (e *ExportedRuntimeServer) Start(ctx context.Context) error {
	return e.inner.start(ctx)
}

// Stop shuts down the server.
func (e *ExportedRuntimeServer) Stop(ctx context.Context) {
	e.inner.stop(ctx)
}

// Invoke enqueues a payload and waits for the container response.
func (e *ExportedRuntimeServer) Invoke(ctx context.Context, payload []byte, timeout time.Duration) ([]byte, bool, error) {
	return e.inner.invoke(ctx, payload, timeout)
}
