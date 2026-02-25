package lambda

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	// ErrESMNotFound is returned when an event source mapping UUID is not found.
	ErrESMNotFound = errors.New("ResourceNotFoundException")
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

// S3CodeFetcher can retrieve zip bytes from an S3-compatible store.
// It is used by InMemoryBackend to pull Zip Lambda code from S3.
type S3CodeFetcher interface {
	GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error)
}

// functionRuntime holds the runtime server and startup state for a single Lambda function.
type functionRuntime struct {
	srv      *runtimeServer
	startErr error
	zipDir   string // temp directory for extracted Zip Lambda code; cleaned up on delete
	mu       sync.Mutex
	port     int
	started  bool
}

// InMemoryBackend is a concurrency-safe in-memory Lambda backend.
type InMemoryBackend struct {
	functions           map[string]*FunctionConfiguration
	runtimes            map[string]*functionRuntime
	eventSourceMappings map[string]*EventSourceMapping
	kinesisPoller       *EventSourcePoller
	docker              *docker.Client
	portAlloc           *portalloc.Allocator
	s3Fetcher           S3CodeFetcher
	logger              *slog.Logger
	accountID           string
	region              string
	settings            Settings
	mu                  sync.RWMutex
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
		functions:           make(map[string]*FunctionConfiguration),
		runtimes:            make(map[string]*functionRuntime),
		eventSourceMappings: make(map[string]*EventSourceMapping),
		docker:              dockerClient,
		portAlloc:           portAlloc,
		settings:            settings,
		accountID:           accountID,
		region:              region,
		logger:              log,
	}
}

// SetS3CodeFetcher sets the S3CodeFetcher for fetching Zip Lambda code from S3.
func (b *InMemoryBackend) SetS3CodeFetcher(f S3CodeFetcher) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.s3Fetcher = f
}

// SetKinesisPoller sets the event source poller for Kinesis stream polling.
func (b *InMemoryBackend) SetKinesisPoller(p *EventSourcePoller) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.kinesisPoller = p
}

// StartKinesisPoller starts the Kinesis event source poller if one has been set.
func (b *InMemoryBackend) StartKinesisPoller(ctx context.Context) {
	b.mu.RLock()
	p := b.kinesisPoller
	b.mu.RUnlock()

	if p != nil {
		p.Start(ctx)
	}
}

// CreateEventSourceMapping creates a new event source mapping.
func (b *InMemoryBackend) CreateEventSourceMapping(input *CreateEventSourceMappingInput) (*EventSourceMapping, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.New().String()
	state := ESMStateEnabled
	if !input.Enabled {
		state = ESMStateDisabled
	}

	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	startingPosition := input.StartingPosition
	if startingPosition == "" {
		startingPosition = "TRIM_HORIZON"
	}

	fnARN := fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", b.region, b.accountID, input.FunctionName)

	m := &EventSourceMapping{
		UUID:             id,
		EventSourceARN:   input.EventSourceARN,
		FunctionARN:      fnARN,
		State:            state,
		BatchSize:        batchSize,
		StartingPosition: startingPosition,
		LastModified:     time.Now(),
	}

	b.eventSourceMappings[id] = m

	return m, nil
}

// GetEventSourceMapping retrieves an event source mapping by UUID.
func (b *InMemoryBackend) GetEventSourceMapping(uuid string) (*EventSourceMapping, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	m, ok := b.eventSourceMappings[uuid]
	if !ok {
		return nil, ErrESMNotFound
	}

	return m, nil
}

// ListEventSourceMappings returns all event source mappings, optionally filtered by function name.
func (b *InMemoryBackend) ListEventSourceMappings(functionName string) []*EventSourceMapping {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*EventSourceMapping, 0, len(b.eventSourceMappings))
	for _, m := range b.eventSourceMappings {
		if functionName != "" && !strings.HasSuffix(m.FunctionARN, ":function:"+functionName) {
			continue
		}

		result = append(result, m)
	}

	return result
}

// DeleteEventSourceMapping removes an event source mapping by UUID.
func (b *InMemoryBackend) DeleteEventSourceMapping(id string) (*EventSourceMapping, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	m, ok := b.eventSourceMappings[id]
	if !ok {
		return nil, ErrESMNotFound
	}

	delete(b.eventSourceMappings, id)

	return m, nil
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
			shutdownCtx, cancel := context.WithTimeout(context.Background(), containerShutdownTimeout)
			defer cancel()
			rt.srv.stop(shutdownCtx)
		}

		if rt.port > 0 && b.portAlloc != nil {
			_ = b.portAlloc.Release(rt.port)
		}

		if rt.zipDir != "" {
			_ = os.RemoveAll(rt.zipDir)
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

// containerShutdownTimeout is the maximum time to wait for a container to stop.
const containerShutdownTimeout = 5 * time.Second

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

	zipDir, containerErr := b.startContainer(ctx, fn, port)
	if containerErr != nil {
		b.logger.WarnContext(
			ctx, "lambda: failed to start container",
			"function", fn.FunctionName, "error", containerErr,
		)
	}

	rt.zipDir = zipDir

	return srv, nil
}

// runtimeImageForRuntime maps a Lambda runtime identifier to the corresponding
// AWS public ECR base image reference.
//
//nolint:gochecknoglobals // intentional package-level lookup table
var runtimeBaseImages = map[string]string{
	"python3.13":      "public.ecr.aws/lambda/python:3.13",
	"python3.12":      "public.ecr.aws/lambda/python:3.12",
	"python3.11":      "public.ecr.aws/lambda/python:3.11",
	"python3.10":      "public.ecr.aws/lambda/python:3.10",
	"python3.9":       "public.ecr.aws/lambda/python:3.9",
	"nodejs22.x":      "public.ecr.aws/lambda/nodejs:22",
	"nodejs20.x":      "public.ecr.aws/lambda/nodejs:20",
	"nodejs18.x":      "public.ecr.aws/lambda/nodejs:18",
	"java21":          "public.ecr.aws/lambda/java:21",
	"java17":          "public.ecr.aws/lambda/java:17",
	"java11":          "public.ecr.aws/lambda/java:11",
	"dotnet9":         "public.ecr.aws/lambda/dotnet:9",
	"dotnet8":         "public.ecr.aws/lambda/dotnet:8",
	"ruby3.3":         "public.ecr.aws/lambda/ruby:3.3",
	"ruby3.2":         "public.ecr.aws/lambda/ruby:3.2",
	"provided.al2023": "public.ecr.aws/lambda/provided:al2023",
	"provided.al2":    "public.ecr.aws/lambda/provided:al2",
	"provided":        "public.ecr.aws/lambda/provided:alami",
}

// baseImageForRuntime returns the ECR base image for the given runtime string.
// Returns "" if the runtime is unknown.
func baseImageForRuntime(runtime string) string {
	return runtimeBaseImages[runtime]
}

// extractZip extracts zip bytes into a new temporary directory and returns the directory path.
// The caller is responsible for calling [os.RemoveAll] on the returned path when done.
func extractZip(zipData []byte) (string, error) {
	dir, err := os.MkdirTemp("", "gopherstack-lambda-zip-*")
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}

	r, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("open zip: %w", err)
	}

	for _, f := range r.File {
		if extractErr := extractZipFile(dir, f); extractErr != nil {
			_ = os.RemoveAll(dir)

			return "", extractErr
		}
	}

	return dir, nil
}

// extractZipFile extracts a single [zip.File] entry into destDir.
func extractZipFile(destDir string, f *zip.File) error {
	// Sanitize path to prevent zip-slip attacks.
	destPath := filepath.Join(destDir, filepath.Clean("/"+f.Name))
	destPath = filepath.Join(destDir, strings.TrimPrefix(destPath, destDir))

	if f.FileInfo().IsDir() {
		return os.MkdirAll(destPath, f.Mode())
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o750); err != nil {
		return fmt.Errorf("mkdir %q: %w", filepath.Dir(destPath), err)
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("open zip entry %q: %w", f.Name, err)
	}
	defer rc.Close()

	outFile, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
	if err != nil {
		return fmt.Errorf("create file %q: %w", destPath, err)
	}
	defer outFile.Close()

	if _, copyErr := io.Copy(outFile, rc); copyErr != nil { //nolint:gosec // zip entries are trusted input from user
		return fmt.Errorf("extract file %q: %w", f.Name, copyErr)
	}

	return nil
}

// startContainer creates and starts a Lambda container for the given function.
// For Zip functions it extracts the code to a temp directory and bind-mounts it.
// Returns the temp directory path (non-empty only for Zip functions) and any error.
func (b *InMemoryBackend) startContainer(
	ctx context.Context,
	fn *FunctionConfiguration,
	runtimePort int,
) (string, error) {
	env := []string{
		fmt.Sprintf("AWS_LAMBDA_RUNTIME_API=%s:%d", b.settings.DockerHost, runtimePort),
		"AWS_DEFAULT_REGION=" + b.region,
		"AWS_REGION=" + b.region,
		"AWS_LAMBDA_FUNCTION_NAME=" + fn.FunctionName,
		fmt.Sprintf("AWS_LAMBDA_FUNCTION_MEMORY_SIZE=%d", fn.MemorySize),
		fmt.Sprintf("AWS_LAMBDA_FUNCTION_TIMEOUT=%d", fn.Timeout),
	}

	if fn.Handler != "" {
		env = append(env, "AWS_LAMBDA_FUNCTION_HANDLER="+fn.Handler)
	}

	if fn.Environment != nil {
		for k, v := range fn.Environment.Variables {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	if fn.PackageType == PackageTypeZip {
		return b.startZipContainer(ctx, fn, env)
	}

	spec := docker.ContainerSpec{
		Image: fn.ImageURI,
		Name:  fmt.Sprintf("gopherstack-lambda-%s-%s", fn.FunctionName, uuid.New().String()[:8]),
		Env:   env,
	}

	_, err := b.docker.CreateAndStart(ctx, spec)

	return "", err
}

// startZipContainer handles container startup for Zip-packaged Lambda functions.
// It fetches the zip (from inline ZipData or S3), extracts it to a temp directory,
// and bind-mounts the directory into the appropriate AWS base image container.
func (b *InMemoryBackend) startZipContainer(
	ctx context.Context,
	fn *FunctionConfiguration,
	env []string,
) (string, error) {
	baseImage := baseImageForRuntime(fn.Runtime)
	if baseImage == "" {
		return "", fmt.Errorf("%w: unsupported runtime %q", ErrLambdaUnavailable, fn.Runtime)
	}

	// Resolve zip bytes from inline data or S3.
	zipData := fn.ZipData
	if len(zipData) == 0 && fn.S3BucketCode != "" && fn.S3KeyCode != "" {
		if b.s3Fetcher == nil {
			return "", fmt.Errorf("%w: S3 code delivery requires S3 integration", ErrLambdaUnavailable)
		}

		var fetchErr error

		zipData, fetchErr = b.s3Fetcher.GetObjectBytes(ctx, fn.S3BucketCode, fn.S3KeyCode)
		if fetchErr != nil {
			return "", fmt.Errorf("%w: failed to fetch zip from S3: %w", ErrLambdaUnavailable, fetchErr)
		}
	}

	if len(zipData) == 0 {
		return "", fmt.Errorf("%w: no zip data available for function %q", ErrLambdaUnavailable, fn.FunctionName)
	}

	zipDir, extractErr := extractZip(zipData)
	if extractErr != nil {
		return "", fmt.Errorf("%w: zip extraction failed: %w", ErrLambdaUnavailable, extractErr)
	}

	spec := docker.ContainerSpec{
		Image:  baseImage,
		Name:   fmt.Sprintf("gopherstack-lambda-%s-%s", fn.FunctionName, uuid.New().String()[:8]),
		Env:    env,
		Mounts: []string{zipDir + ":/var/task:ro"},
	}

	if fn.Handler != "" {
		spec.Cmd = []string{fn.Handler}
	}

	if _, err := b.docker.CreateAndStart(ctx, spec); err != nil {
		_ = os.RemoveAll(zipDir)

		return "", err
	}

	return zipDir, nil
}
