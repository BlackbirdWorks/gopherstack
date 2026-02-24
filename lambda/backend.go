package lambda

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/docker"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

var (
	// ErrFunctionNotFound is returned when the specified Lambda function does not exist.
	ErrFunctionNotFound = errors.New("ResourceNotFoundException")
	// ErrFunctionAlreadyExists is returned when creating a function that already exists.
	ErrFunctionAlreadyExists = errors.New("ResourceConflictException")
	// ErrLambdaUnavailable is returned when Lambda cannot invoke (no Docker or no port range).
	ErrLambdaUnavailable = errors.New("ServiceException")
)

// StorageBackend defines the interface for Lambda backend operations.
type StorageBackend interface {
	CreateFunction(fn *FunctionConfiguration) error
	GetFunction(name string) (*FunctionConfiguration, error)
	ListFunctions() []*FunctionConfiguration
	DeleteFunction(name string) error
	UpdateFunction(fn *FunctionConfiguration) error
	InvokeFunction(ctx context.Context, name string, invocationType InvocationType, payload []byte) ([]byte, int, error)
}

// functionRuntime holds the runtime server and startup state for a single Lambda function.
type functionRuntime struct {
	srv      *runtimeServer
	startErr error
	mu       sync.Mutex
	port     int
	started  bool
}

// InMemoryBackend is a concurrency-safe in-memory Lambda backend.
type InMemoryBackend struct {
	functions map[string]*FunctionConfiguration
	runtimes  map[string]*functionRuntime
	docker    *docker.Client
	portAlloc *portalloc.Allocator
	logger    *slog.Logger
	settings  Settings
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new Lambda in-memory backend.
func NewInMemoryBackend(
	dockerClient *docker.Client,
	portAlloc *portalloc.Allocator,
	settings Settings,
	accountID, region string,
	log *slog.Logger,
) *InMemoryBackend {
	return &InMemoryBackend{
		functions: make(map[string]*FunctionConfiguration),
		runtimes:  make(map[string]*functionRuntime),
		docker:    dockerClient,
		portAlloc: portAlloc,
		settings:  settings,
		accountID: accountID,
		region:    region,
		logger:    log,
	}
}

// CreateFunction stores a new Lambda function configuration.
func (b *InMemoryBackend) CreateFunction(fn *FunctionConfiguration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.functions[fn.FunctionName]; exists {
		return ErrFunctionAlreadyExists
	}

	b.functions[fn.FunctionName] = fn

	return nil
}

// GetFunction retrieves a Lambda function configuration by name.
func (b *InMemoryBackend) GetFunction(name string) (*FunctionConfiguration, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	return fn, nil
}

// ListFunctions returns all Lambda function configurations sorted by name.
func (b *InMemoryBackend) ListFunctions() []*FunctionConfiguration {
	b.mu.RLock()
	defer b.mu.RUnlock()

	fns := make([]*FunctionConfiguration, 0, len(b.functions))
	for _, fn := range b.functions {
		fns = append(fns, fn)
	}

	sort.Slice(fns, func(i, j int) bool {
		return fns[i].FunctionName < fns[j].FunctionName
	})

	return fns
}

// DeleteFunction removes a Lambda function and cleans up its runtime server.
func (b *InMemoryBackend) DeleteFunction(name string) error {
	b.mu.Lock()

	if _, ok := b.functions[name]; !ok {
		b.mu.Unlock()

		return ErrFunctionNotFound
	}

	delete(b.functions, name)

	rt := b.runtimes[name]
	delete(b.runtimes, name)
	b.mu.Unlock()

	// Clean up runtime resources outside the lock to avoid blocking while stopping the server.
	if rt != nil {
		if rt.srv != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rt.srv.stop(shutdownCtx)
		}

		if rt.port > 0 && b.portAlloc != nil {
			_ = b.portAlloc.Release(rt.port)
		}
	}

	return nil
}

// UpdateFunction replaces a Lambda function's configuration.
func (b *InMemoryBackend) UpdateFunction(fn *FunctionConfiguration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.functions[fn.FunctionName]; !ok {
		return ErrFunctionNotFound
	}

	b.functions[fn.FunctionName] = fn

	return nil
}

// InvokeFunction invokes a Lambda function and returns the response payload and HTTP status code.
func (b *InMemoryBackend) InvokeFunction(
	ctx context.Context,
	name string,
	invocationType InvocationType,
	payload []byte,
) ([]byte, int, error) {
	fn, err := b.GetFunction(name)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	if invocationType == InvocationTypeDryRun {
		return nil, http.StatusNoContent, nil
	}

	srv, srvErr := b.getOrCreateRuntime(ctx, fn)
	if srvErr != nil {
		return nil, http.StatusInternalServerError, srvErr
	}

	timeout := time.Duration(fn.Timeout) * time.Second
	if timeout <= 0 {
		timeout = defaultFunctionTimeout
	}

	if invocationType == InvocationTypeEvent {
		inv := &pendingInvocation{
			requestID: uuid.New().String(),
			payload:   payload,
			deadline:  time.Now().Add(timeout),
			result:    make(chan invocationResult, 1),
		}

		select {
		case srv.queue <- inv:
		default:
			// Queue full — drop for async (Event) invocations.
		}

		return nil, http.StatusAccepted, nil
	}

	result, isError, invokeErr := srv.invoke(ctx, payload, timeout)
	if invokeErr != nil {
		return nil, http.StatusInternalServerError, invokeErr
	}

	// Per Lambda convention, function-level errors (isError=true) still return HTTP 200.
	// The caller can inspect the response body for error details.
	// X-Amz-Function-Error header enhancement can be added if needed.
	_ = isError

	return result, http.StatusOK, nil
}

// defaultFunctionTimeout is used when the function has no timeout configured.
const defaultFunctionTimeout = 3 * time.Second

// getOrCreateRuntime returns the runtime server for a function, creating it on first use.
// Must not be called with b.mu held.
func (b *InMemoryBackend) getOrCreateRuntime(ctx context.Context, fn *FunctionConfiguration) (*runtimeServer, error) {
	if b.portAlloc == nil {
		return nil, fmt.Errorf("%w: no port range configured", ErrLambdaUnavailable)
	}

	if b.docker == nil {
		return nil, fmt.Errorf("%w: Docker unavailable", ErrLambdaUnavailable)
	}

	b.mu.Lock()
	rt, ok := b.runtimes[fn.FunctionName]

	if !ok {
		rt = &functionRuntime{}
		b.runtimes[fn.FunctionName] = rt
	}

	b.mu.Unlock()

	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.started {
		return rt.srv, rt.startErr
	}

	port, portErr := b.portAlloc.Acquire(fmt.Sprintf("lambda:%s", fn.FunctionName))
	if portErr != nil {
		rt.startErr = fmt.Errorf("%w: port allocation failed: %w", ErrLambdaUnavailable, portErr)
		rt.started = true

		return nil, rt.startErr
	}

	srv := newRuntimeServer(port, b.logger)

	if startErr := srv.start(ctx); startErr != nil {
		_ = b.portAlloc.Release(port)
		rt.startErr = fmt.Errorf("%w: runtime server start failed: %w", ErrLambdaUnavailable, startErr)
		rt.started = true

		return nil, rt.startErr
	}

	rt.srv = srv
	rt.port = port
	rt.started = true

	if containerErr := b.startContainer(ctx, fn, port); containerErr != nil {
		b.logger.WarnContext(ctx, "lambda: failed to start container", "function", fn.FunctionName, "error", containerErr)
	}

	return srv, nil
}

// startContainer creates and starts a Lambda container for the given function.
func (b *InMemoryBackend) startContainer(ctx context.Context, fn *FunctionConfiguration, runtimePort int) error {
	env := []string{
		fmt.Sprintf("AWS_LAMBDA_RUNTIME_API=%s:%d", b.settings.DockerHost, runtimePort),
		"AWS_DEFAULT_REGION=" + b.region,
		"AWS_REGION=" + b.region,
		"AWS_LAMBDA_FUNCTION_NAME=" + fn.FunctionName,
		fmt.Sprintf("AWS_LAMBDA_FUNCTION_MEMORY_SIZE=%d", fn.MemorySize),
		fmt.Sprintf("AWS_LAMBDA_FUNCTION_TIMEOUT=%d", fn.Timeout),
	}

	if fn.Environment != nil {
		for k, v := range fn.Environment.Variables {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	spec := docker.ContainerSpec{
		Image: fn.ImageURI,
		Name:  fmt.Sprintf("gopherstack-lambda-%s-%s", fn.FunctionName, uuid.New().String()[:8]),
		Env:   env,
	}

	_, err := b.docker.CreateAndStart(ctx, spec)

	return err
}
