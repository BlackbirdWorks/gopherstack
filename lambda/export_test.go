// This file exports internal types for use in external (_test) packages.
// It is compiled only during testing.

package lambda

import (
	"context"
	"net/http"
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
		inner: newRuntimeServer(port),
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
func (e *ExportedRuntimeServer) Invoke(
	ctx context.Context,
	payload []byte,
	timeout time.Duration,
) ([]byte, bool, error) {
	return e.inner.invoke(ctx, payload, timeout)
}

// BaseImageForRuntime exports the internal runtimeBaseImages lookup for testing.
func BaseImageForRuntime(runtime string) string {
	return baseImageForRuntime(runtime)
}

// ExtractZip exports the internal extractZip function for testing.
func ExtractZip(zipData []byte) (string, error) { return extractZip(zipData) }

// StreamNameFromARN exports the internal streamNameFromARN function for testing.
func StreamNameFromARN(arn string) string { return streamNameFromARN(arn) }

// FunctionNameFromARN exports the internal functionNameFromARN function for testing.
func FunctionNameFromARN(arn string) string { return functionNameFromARN(arn) }

// PollOnce triggers a single poll cycle on the given EventSourcePoller.
func PollOnce(ctx context.Context, p *EventSourcePoller) { p.poll(ctx) }

// BuildURLEventPayload exports buildURLEventPayload for testing.
func BuildURLEventPayload(b *InMemoryBackend, r *http.Request) ([]byte, error) {
	return b.buildURLEventPayload(r)
}

// WriteFunctionURLResponse exports writeFunctionURLResponse for testing.
func WriteFunctionURLResponse(w http.ResponseWriter, result []byte) {
	writeFunctionURLResponse(w, result)
}

// SetDNSRegistrarExported exports SetDNSRegistrar for testing.
func SetDNSRegistrarExported(b *InMemoryBackend, dns DNSRegistrar) {
	b.SetDNSRegistrar(dns)
}
