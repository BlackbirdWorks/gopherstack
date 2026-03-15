// This file exports internal types for use in external (_test) packages.
// It is compiled only during testing.

package lambda

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// ParseLayerARN exports parseLayerARN for testing.
func ParseLayerARN(layerVersionARN string) (string, int64) {
	return parseLayerARN(layerVersionARN)
}

// PrepareLayerMount exports prepareLayerMount for testing.
func PrepareLayerMount(b *InMemoryBackend, fn *FunctionConfiguration) (string, []string, error) {
	return b.prepareLayerMount(fn)
}

// IsSQSARN exports the internal isSQSARN function for testing.
func IsSQSARN(resourceARN string) bool { return isSQSARN(resourceARN) }

// IsDynamoDBStreamARN exports the internal isDynamoDBStreamARN function for testing.
func IsDynamoDBStreamARN(resourceARN string) bool { return isDynamoDBStreamARN(resourceARN) }

// SetSQSReaderOnPoller exports SetSQSReader on EventSourcePoller for testing.
func SetSQSReaderOnPoller(p *EventSourcePoller, r SQSReader) { p.SetSQSReader(r) }

// SetDynamoDBStreamsReaderOnPoller exports SetDynamoDBStreamsReader on EventSourcePoller for testing.
func SetDynamoDBStreamsReaderOnPoller(p *EventSourcePoller, r DynamoDBStreamsReader) {
	p.SetDynamoDBStreamsReader(r)
}

// SetSQSInvoker sets a test-only override for the Lambda invocation step in the
// SQS ESM poller. When fn is non-nil it is called instead of InvokeFunction,
// allowing unit tests to make Lambda invocation succeed without a Docker daemon.
func SetSQSInvoker(p *EventSourcePoller, fn func(ctx context.Context, fnName string) error) {
	p.sqsInvoker = fn
}

// SetDDBInvoker sets a test-only override for the Lambda invocation step in the
// DynamoDB Streams ESM poller. When fn is non-nil it is called instead of InvokeFunction.
func SetDDBInvoker(p *EventSourcePoller, fn func(ctx context.Context, fnName string, payload []byte) error) {
	p.ddbInvoker = fn
}

// InjectRuntimeEntry inserts a synthetic functionRuntime into the backend's runtimes map
// so that Close() tests can verify runtime cleanup without a real container.
// zipDir and layerDirs will be cleaned up by Close().
func InjectRuntimeEntry(b *InMemoryBackend, functionName, zipDir string, layerDirs []string, port int) {
	b.mu.Lock("InjectRuntimeEntry")
	defer b.mu.Unlock()

	b.runtimes[functionName] = &functionRuntime{
		mu:        lockmetrics.New("lambda.runtime.test"),
		zipDir:    zipDir,
		layerDirs: layerDirs,
		port:      port,
		started:   true,
	}
}

// FunctionNamesFromARNs exports functionNamesFromARNs for testing.
func FunctionNamesFromARNs(arns []string) []string { return functionNamesFromARNs(arns) }

// ParseInvocationPercentage exports parseInvocationPercentage for testing.
func ParseInvocationPercentage(s string) float64 { return parseInvocationPercentage(s) }

// ParseInvocationDelayMs exports parseInvocationDelayMs for testing.
func ParseInvocationDelayMs(s string) int { return parseInvocationDelayMs(s) }

// ParseIntSafe exports parseIntSafe for testing.
func ParseIntSafe(s string, out *int) error { return parseIntSafe(s, out) }

// ExpiryFromDuration exports expiryFromDuration for testing.
func ExpiryFromDuration(d time.Duration) time.Time { return expiryFromDuration(d) }

// SetFISFault exports setFISFault for testing.
func SetFISFault(b *InMemoryBackend, name string, fault *FISInvocationFault) {
	b.setFISFault(name, fault)
}

// ClearFISFault exports clearFISFault for testing.
func ClearFISFault(b *InMemoryBackend, name string) { b.clearFISFault(name) }

// CheckFISFault exports checkFISFault for testing.
func CheckFISFault(b *InMemoryBackend, name string) *FISInvocationFault {
	return b.checkFISFault(name)
}

// ReleaseConcurrencySlot exports releaseConcurrencySlot for testing.
func ReleaseConcurrencySlot(b *InMemoryBackend, functionName string) {
	b.releaseConcurrencySlot(functionName)
}

// AcquireConcurrencySlot exports acquireConcurrencySlot for testing.
func AcquireConcurrencySlot(b *InMemoryBackend, functionName string) (bool, error) {
	return b.acquireConcurrencySlot(functionName)
}

// MinEventAgeInSeconds exports minEventAgeInSeconds for use in tests.
const MinEventAgeInSeconds = minEventAgeInSeconds

func ShardIteratorsLen(p *EventSourcePoller) int {
	p.mu.RLock("ShardIteratorsLen")
	defer p.mu.RUnlock()

	return len(p.shardIterators)
}

// RuntimeQueueSize exposes the internal runtimeQueueSize constant for test use.
const RuntimeQueueSize = runtimeQueueSize

// PendingLen returns the number of entries in the runtime server's pending invocations map.
// Intended for use in unit tests to verify stale-pending cleanup.
func PendingLen(s *ExportedRuntimeServer) int {
	count := 0
	s.inner.pending.Range(func(_, _ any) bool {
		count++

		return true
	})

	return count
}

// QueueLen returns the current number of items in the runtime server's invocation queue.
func QueueLen(s *ExportedRuntimeServer) int {
	return len(s.inner.queue)
}

// FillQueue fills the runtime server's queue with dummy placeholder entries up to n items.
// Returns the actual number added.
func FillQueue(s *ExportedRuntimeServer, n int) int {
	added := 0

	for range n {
		select {
		case s.inner.queue <- &pendingInvocation{}:
			added++
		default:
			return added
		}
	}

	return added
}

// DrainQueue removes all items currently in the queue and returns the count drained.
func DrainQueue(s *ExportedRuntimeServer) int {
	n := 0

	for {
		select {
		case <-s.inner.queue:
			n++
		default:
			return n
		}
	}
}

// DrainN removes at most n items from the runtime server's invocation queue,
// returning the actual number removed. Unlike DrainQueue, it stops after n
// dequeues so that items inserted concurrently by background goroutines (e.g.
// the slow-path in enqueueAsyncInvocation) are not inadvertently consumed.
func DrainN(s *ExportedRuntimeServer, n int) int {
	removed := 0

	for removed < n {
		select {
		case <-s.inner.queue:
			removed++
		default:
			return removed
		}
	}

	return removed
}

// EnqueueAsync calls enqueueAsyncInvocation on b with a synthetic pendingInvocation.
// It returns the requestID of the created invocation so tests can simulate container
// responses or verify pending-map cleanup. createdAt sets the event creation time; pass
// a zero [time.Time] to default to [time.Now].
func EnqueueAsync(
	ctx context.Context,
	b *InMemoryBackend,
	srv *ExportedRuntimeServer,
	functionName string,
	payload []byte,
	timeout time.Duration,
	trackConcurrency bool,
	createdAt ...time.Time,
) string {
	eventTime := time.Now()
	if len(createdAt) > 0 && !createdAt[0].IsZero() {
		eventTime = createdAt[0]
	}

	inv := &pendingInvocation{
		requestID: uuid.New().String(),
		payload:   payload,
		deadline:  eventTime.Add(timeout),
		createdAt: eventTime,
		result:    make(chan invocationResult, 1),
	}

	b.enqueueAsyncInvocation(ctx, srv.inner, functionName, inv, timeout, trackConcurrency)

	return inv.requestID
}
