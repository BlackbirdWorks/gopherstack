package lambda

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

var (
	// ErrFunctionNotFound is returned when the specified Lambda function does not exist.
	ErrFunctionNotFound = errors.New("ResourceNotFoundException")
	// ErrFunctionAlreadyExists is returned when creating a function that already exists.
	ErrFunctionAlreadyExists = errors.New("ResourceConflictException")
	// ErrLambdaUnavailable is returned when Lambda cannot invoke (no Docker or no port range).
	ErrLambdaUnavailable = errors.New("ServiceException")
	// ErrInvalidParameterValue is returned when a request parameter has an invalid value.
	ErrInvalidParameterValue = errors.New("InvalidParameterValueException")
	// ErrESMNotFound is returned when an event source mapping UUID is not found.
	ErrESMNotFound = errors.New("ResourceNotFoundException")
	// ErrFunctionURLNotFound is returned when no function URL config exists for the function.
	ErrFunctionURLNotFound = errors.New("ResourceNotFoundException")
	// ErrVersionNotFound is returned when the specified function version does not exist.
	ErrVersionNotFound = errors.New("ResourceNotFoundException")
	// ErrAliasNotFound is returned when the specified alias does not exist.
	ErrAliasNotFound = errors.New("ResourceNotFoundException")
	// ErrAliasAlreadyExists is returned when creating an alias that already exists.
	ErrAliasAlreadyExists = errors.New("ResourceConflictException")
	// ErrLayerNotFound is returned when the specified layer does not exist.
	ErrLayerNotFound = errors.New("ResourceNotFoundException")
	// ErrLayerVersionNotFound is returned when the specified layer version does not exist.
	ErrLayerVersionNotFound = errors.New("ResourceNotFoundException")
	// ErrZipSlip is returned when a ZIP archive entry has a path that would escape the target directory.
	ErrZipSlip = errors.New("zip entry escapes target directory")
	// ErrEventInvokeConfigNotFound is returned when no event invoke config exists for the function.
	ErrEventInvokeConfigNotFound = errors.New("ResourceNotFoundException")
	// ErrTooManyRequests is returned when a function's reserved concurrency limit is exhausted.
	ErrTooManyRequests = errors.New("TooManyRequestsException")
	// ErrFunctionConcurrencyNotFound is returned when a function has no reserved concurrency configured.
	ErrFunctionConcurrencyNotFound = errors.New("ResourceNotFoundException")
	// ErrProvisionedConcurrencyConfigNotFound is returned when no provisioned concurrency config exists for the qualifier.
	ErrProvisionedConcurrencyConfigNotFound = errors.New("ResourceNotFoundException")
)

// versionLatest is the sentinel qualifier for the live function configuration.
const versionLatest = "$LATEST"

// lambdaDefaultMaxItems is the default page size for ListFunctions.
const lambdaDefaultMaxItems = 50

// StorageBackend defines the interface for Lambda backend operations.
type StorageBackend interface {
	CreateFunction(fn *FunctionConfiguration) error
	GetFunction(name string) (*FunctionConfiguration, error)
	ListFunctions(marker string, maxItems int) page.Page[*FunctionConfiguration]
	DeleteFunction(name string) error
	UpdateFunction(fn *FunctionConfiguration) error
	InvokeFunction(ctx context.Context, name string, invocationType InvocationType, payload []byte) ([]byte, int, error)
}

// QualifierInvoker is an optional extension of StorageBackend that supports qualified invocations.
// Backends implement this to support ?Qualifier= on Invoke (alias or version qualifier).
type QualifierInvoker interface {
	InvokeFunctionWithQualifier(
		ctx context.Context, name, qualifier string, invocationType InvocationType, payload []byte,
	) ([]byte, int, error)
}

// S3CodeFetcher can retrieve zip bytes from an S3-compatible store.
// It is used by InMemoryBackend to pull Zip Lambda code from S3.
type S3CodeFetcher interface {
	GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error)
}

// CWLogsBackend is the minimum CloudWatch Logs interface needed by Lambda for log delivery.
type CWLogsBackend interface {
	EnsureLogGroupAndStream(groupName, streamName string) error
	PutLogLines(groupName, streamName string, messages []string) error
}

// DNSRegistrar is an optional interface for registering synthetic DNS hostnames.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// functionRuntime holds the runtime server and startup state for a single Lambda function.
type functionRuntime struct {
	startErr  error
	srv       *runtimeServer
	mu        *lockmetrics.RWMutex
	zipDir    string
	layerDirs []string
	port      int
	started   bool
}

// functionURLServer holds a running HTTP listener for a Lambda function URL.
type functionURLServer struct {
	listener net.Listener
	server   *http.Server
	port     int
}

// InMemoryBackend is a concurrency-safe in-memory Lambda backend.
type InMemoryBackend struct {
	cwLogs              CWLogsBackend
	s3Fetcher           S3CodeFetcher
	docker              container.Runtime
	dnsRegistrar        DNSRegistrar
	kinesisPoller       *EventSourcePoller
	eventSourceMappings map[string]*EventSourceMapping
	aliases             map[string]map[string]*FunctionAlias
	versionCounters     map[string]int
	functions           map[string]*FunctionConfiguration
	functionURLServers  map[string]*functionURLServer
	functionURLConfigs  map[string]*FunctionURLConfig
	versions            map[string][]*FunctionVersion
	// eventInvokeConfigs stores async invocation config keyed by function name.
	eventInvokeConfigs map[string]*FunctionEventInvokeConfig
	// functionConcurrencies stores reserved concurrent executions per function name.
	// If a function name is not present in the map, no limit is set (use account default).
	functionConcurrencies map[string]int
	// activeConcurrencies tracks the number of active synchronous invocations per function name.
	activeConcurrencies map[string]int
	// provisionedConcurrencies stores provisioned concurrency configs keyed by
	// function name → qualifier → config.
	provisionedConcurrencies map[string]map[string]*ProvisionedConcurrencyConfig
	// layers stores layer versions keyed by layerName → []LayerVersion (ordered by version).
	layers map[string][]*LayerVersion
	// layerVersionCounters tracks the next version number per layer.
	layerVersionCounters map[string]int64
	// layerPolicies stores per-version resource policy statements keyed by
	// layerName → versionNumber → statementID → LayerVersionStatement.
	layerPolicies map[string]map[int64]map[string]*LayerVersionStatement
	// fisFaults maps function names to FIS invocation fault configuration.
	// When a function is in this map, invocations return an error with the given probability.
	fisFaults map[string]*FISInvocationFault
	portAlloc *portalloc.Allocator
	runtimes  map[string]*functionRuntime
	mu        *lockmetrics.RWMutex
	region    string
	accountID string
	settings  Settings
}

// NewInMemoryBackend creates a new Lambda in-memory backend.
func NewInMemoryBackend(
	dockerClient container.Runtime,
	portAlloc *portalloc.Allocator,
	settings Settings,
	accountID, region string,
) *InMemoryBackend {
	return &InMemoryBackend{
		functions:                make(map[string]*FunctionConfiguration),
		runtimes:                 make(map[string]*functionRuntime),
		eventSourceMappings:      make(map[string]*EventSourceMapping),
		functionURLConfigs:       make(map[string]*FunctionURLConfig),
		functionURLServers:       make(map[string]*functionURLServer),
		versions:                 make(map[string][]*FunctionVersion),
		aliases:                  make(map[string]map[string]*FunctionAlias),
		versionCounters:          make(map[string]int),
		layers:                   make(map[string][]*LayerVersion),
		layerVersionCounters:     make(map[string]int64),
		layerPolicies:            make(map[string]map[int64]map[string]*LayerVersionStatement),
		eventInvokeConfigs:       make(map[string]*FunctionEventInvokeConfig),
		functionConcurrencies:    make(map[string]int),
		activeConcurrencies:      make(map[string]int),
		provisionedConcurrencies: make(map[string]map[string]*ProvisionedConcurrencyConfig),
		fisFaults:                make(map[string]*FISInvocationFault),
		docker:                   dockerClient,
		portAlloc:                portAlloc,
		settings:                 settings,
		accountID:                accountID,
		region:                   region,
		mu:                       lockmetrics.New("lambda"),
	}
}

// Close shuts down all active function URL servers and runtime API servers.
// It is safe to call concurrently and should be called when the backend is no longer needed.
func (b *InMemoryBackend) Close(ctx context.Context) {
	b.mu.Lock("Close")

	urlServers := make([]*functionURLServer, 0, len(b.functionURLServers))
	for _, srv := range b.functionURLServers {
		urlServers = append(urlServers, srv)
	}

	rts := make([]*functionRuntime, 0, len(b.runtimes))
	for _, rt := range b.runtimes {
		rts = append(rts, rt)
	}

	b.mu.Unlock()

	var wg sync.WaitGroup

	for _, srv := range urlServers {
		wg.Go(func() {
			_ = srv.server.Shutdown(ctx)

			if b.portAlloc != nil {
				_ = b.portAlloc.Release(srv.port)
			}
		})
	}

	for _, rt := range rts {
		wg.Go(func() {
			if rt.srv != nil {
				rt.srv.stop(ctx)
			}

			if rt.port > 0 && b.portAlloc != nil {
				_ = b.portAlloc.Release(rt.port)
			}

			if rt.zipDir != "" {
				_ = os.RemoveAll(rt.zipDir)
			}

			for _, d := range rt.layerDirs {
				_ = os.RemoveAll(d)
			}

			rt.mu.Close()
		})
	}

	wg.Wait()
}

// SetDNSRegistrar sets the optional DNS registrar used to register function URL hostnames.
func (b *InMemoryBackend) SetDNSRegistrar(r DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = r
}

// SetS3CodeFetcher sets the S3CodeFetcher for fetching Zip Lambda code from S3.
func (b *InMemoryBackend) SetS3CodeFetcher(f S3CodeFetcher) {
	b.mu.Lock("SetS3CodeFetcher")
	defer b.mu.Unlock()
	b.s3Fetcher = f
}

// SetCWLogsBackend sets the CloudWatch Logs backend for Lambda log delivery.
func (b *InMemoryBackend) SetCWLogsBackend(cwl CWLogsBackend) {
	b.mu.Lock("SetCWLogsBackend")
	defer b.mu.Unlock()
	b.cwLogs = cwl
}

// SetKinesisPoller sets the event source poller for Kinesis stream polling.
func (b *InMemoryBackend) SetKinesisPoller(p *EventSourcePoller) {
	b.mu.Lock("SetKinesisPoller")
	defer b.mu.Unlock()
	b.kinesisPoller = p
}

// StartKinesisPoller starts the Kinesis event source poller if one has been set.
func (b *InMemoryBackend) StartKinesisPoller(ctx context.Context) {
	b.mu.RLock("StartKinesisPoller")
	p := b.kinesisPoller
	b.mu.RUnlock()

	if p != nil {
		p.Start(ctx)
	}
}

// SetSQSReader sets the SQS reader on the event source poller so that SQS
// queues can trigger Lambda functions via event source mappings.
func (b *InMemoryBackend) SetSQSReader(r SQSReader) {
	b.mu.RLock("SetSQSReader")
	p := b.kinesisPoller
	b.mu.RUnlock()

	if p != nil {
		p.SetSQSReader(r)
	}
}

// SetDynamoDBStreamsReader sets the DynamoDB Streams reader on the event source poller so
// that DynamoDB stream records can trigger Lambda functions via event source mappings.
func (b *InMemoryBackend) SetDynamoDBStreamsReader(r DynamoDBStreamsReader) {
	b.mu.RLock("SetDynamoDBStreamsReader")
	p := b.kinesisPoller
	b.mu.RUnlock()

	if p != nil {
		p.SetDynamoDBStreamsReader(r)
	}
}

// CreateEventSourceMapping creates a new event source mapping.
func (b *InMemoryBackend) CreateEventSourceMapping(input *CreateEventSourceMappingInput) (*EventSourceMapping, error) {
	b.mu.Lock("CreateEventSourceMapping")
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

	fnARN := arn.Build("lambda", b.region, b.accountID, "function:"+input.FunctionName)

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
	b.mu.RLock("GetEventSourceMapping")
	defer b.mu.RUnlock()

	m, ok := b.eventSourceMappings[uuid]
	if !ok {
		return nil, ErrESMNotFound
	}

	return m, nil
}

// ListEventSourceMappings returns a page of event source mappings, optionally filtered by function name.
func (b *InMemoryBackend) ListEventSourceMappings(
	functionName, marker string,
	maxItems int,
) page.Page[*EventSourceMapping] {
	b.mu.RLock("ListEventSourceMappings")
	defer b.mu.RUnlock()

	result := make([]*EventSourceMapping, 0, len(b.eventSourceMappings))
	for _, m := range b.eventSourceMappings {
		if functionName != "" && !strings.HasSuffix(m.FunctionARN, ":function:"+functionName) {
			continue
		}

		result = append(result, m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].UUID < result[j].UUID
	})

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems)
}

// DeleteEventSourceMapping removes an event source mapping by UUID.
func (b *InMemoryBackend) DeleteEventSourceMapping(id string) (*EventSourceMapping, error) {
	b.mu.Lock("DeleteEventSourceMapping")
	defer b.mu.Unlock()

	m, ok := b.eventSourceMappings[id]
	if !ok {
		return nil, ErrESMNotFound
	}

	delete(b.eventSourceMappings, id)

	return m, nil
}

// functionURLHostname returns the synthetic DNS hostname for a function URL.
func (b *InMemoryBackend) functionURLHostname(functionName string) string {
	return fmt.Sprintf("%s.lambda-url.%s.on.aws", functionName, b.region)
}

// CreateFunctionURLConfig creates a function URL endpoint for the given function.
// It allocates a port, starts an HTTP listener, registers DNS, and returns the config.
func (b *InMemoryBackend) CreateFunctionURLConfig(functionName, authType string) (*FunctionURLConfig, error) {
	b.mu.Lock("CreateFunctionURLConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions[functionName]; !ok {
		return nil, ErrFunctionNotFound
	}

	// If a URL config already exists, return ResourceConflictException
	if _, exists := b.functionURLConfigs[functionName]; exists {
		return nil, ErrFunctionAlreadyExists
	}

	urlStr, startErr := b.allocateAndStartURLServer(functionName)
	if startErr != nil {
		return nil, startErr
	}

	now := time.Now().UTC().Format(time.RFC3339)
	cfg := &FunctionURLConfig{
		FunctionArn:      buildURLARN(b.region, b.accountID, functionName),
		FunctionURL:      urlStr,
		AuthType:         authType,
		CreationTime:     now,
		LastModifiedTime: now,
	}

	b.functionURLConfigs[functionName] = cfg

	return cfg, nil
}

// allocateAndStartURLServer allocates a port, starts the HTTP listener, optionally registers DNS,
// and returns the function URL string. Must be called with b.mu already held (write).
func (b *InMemoryBackend) allocateAndStartURLServer(functionName string) (string, error) {
	if b.portAlloc == nil {
		return fmt.Sprintf("http://localhost/%s/", functionName), nil
	}

	port, allocErr := b.portAlloc.Acquire(fmt.Sprintf("lambda-url:%s", functionName))
	if allocErr != nil {
		return "", fmt.Errorf("%w: port allocation failed: %w", ErrLambdaUnavailable, allocErr)
	}

	srv, listenErr := b.startFunctionURLServer(functionName, port)
	if listenErr != nil {
		_ = b.portAlloc.Release(port)

		return "", fmt.Errorf("%w: failed to start URL listener: %w", ErrLambdaUnavailable, listenErr)
	}

	b.functionURLServers[functionName] = srv
	hostname := b.functionURLHostname(functionName)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(hostname)

		return "http://" + net.JoinHostPort(hostname, strconv.Itoa(port)) + "/", nil
	}

	// No DNS registered; use loopback so the URL is immediately reachable.
	return "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) + "/", nil
}

// GetFunctionURLConfig returns the function URL config for a function.
func (b *InMemoryBackend) GetFunctionURLConfig(functionName string) (*FunctionURLConfig, error) {
	b.mu.RLock("GetFunctionURLConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.functionURLConfigs[functionName]
	if !ok {
		return nil, ErrFunctionURLNotFound
	}

	return cfg, nil
}

// DeleteFunctionURLConfig removes the function URL config, stops the listener, and deregisters DNS.
func (b *InMemoryBackend) DeleteFunctionURLConfig(functionName string) error {
	b.mu.Lock("DeleteFunctionURLConfig")

	if _, ok := b.functionURLConfigs[functionName]; !ok {
		b.mu.Unlock()

		return ErrFunctionURLNotFound
	}

	delete(b.functionURLConfigs, functionName)

	srv := b.functionURLServers[functionName]
	delete(b.functionURLServers, functionName)
	dns := b.dnsRegistrar
	hostname := b.functionURLHostname(functionName)
	b.mu.Unlock()

	if srv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), containerShutdownTimeout)
		defer cancel()
		_ = srv.server.Shutdown(shutdownCtx)

		if b.portAlloc != nil {
			_ = b.portAlloc.Release(srv.port)
		}
	}

	if dns != nil {
		dns.Deregister(hostname)
	}

	return nil
}

// functionURLReadHeaderTimeout is the timeout for reading HTTP request headers on the function URL listener.
const functionURLReadHeaderTimeout = 30 * time.Second

// startFunctionURLServer starts an HTTP server on the given port that converts HTTP requests
// to Lambda invocation events and returns the function's response.
func (b *InMemoryBackend) startFunctionURLServer(functionName string, port int) (*functionURLServer, error) {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", b.buildFunctionURLHandler(functionName))

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: functionURLReadHeaderTimeout,
	}

	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			slog.Default().Warn("lambda: function URL server stopped", "function", functionName, "error", serveErr)
		}
	}()

	return &functionURLServer{listener: ln, server: srv, port: port}, nil
}

// lambdaURLEvent is a simplified Lambda Function URL (HTTP API v2) event.
type lambdaURLEvent struct {
	RawPath         string            `json:"rawPath"`
	RawQueryString  string            `json:"rawQueryString"`
	Headers         map[string]string `json:"headers"`
	Body            string            `json:"body,omitempty"`
	Version         string            `json:"version"`
	RouteKey        string            `json:"routeKey"`
	IsBase64Encoded bool              `json:"isBase64Encoded"`
}

// lambdaURLResponse is a simplified Lambda Function URL response.
type lambdaURLResponse struct {
	Headers         map[string]string `json:"headers,omitempty"`
	Body            string            `json:"body,omitempty"`
	StatusCode      int               `json:"statusCode"`
	IsBase64Encoded bool              `json:"isBase64Encoded,omitempty"`
}

// buildFunctionURLHandler builds an [http.HandlerFunc] that invokes the Lambda function.
func (b *InMemoryBackend) buildFunctionURLHandler(functionName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		payload, buildErr := b.buildURLEventPayload(r)
		if buildErr != nil {
			http.Error(w, buildErr.Error(), http.StatusInternalServerError)

			return
		}

		result, _, invokeErr := b.InvokeFunction(r.Context(), functionName, InvocationTypeRequestResponse, payload)
		if invokeErr != nil {
			http.Error(w, invokeErr.Error(), http.StatusInternalServerError)

			return
		}

		writeFunctionURLResponse(w, result)
	}
}

// buildURLEventPayload converts an HTTP request to a Lambda Function URL event payload.
func (b *InMemoryBackend) buildURLEventPayload(r *http.Request) ([]byte, error) {
	var bodyBytes []byte

	if r.Body != nil {
		var readErr error

		bodyBytes, readErr = io.ReadAll(r.Body)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read request body: %w", readErr)
		}
	}

	headers := make(map[string]string, len(r.Header))
	for k, vs := range r.Header {
		headers[strings.ToLower(k)] = strings.Join(vs, ",")
	}

	event := lambdaURLEvent{
		Version:        "2.0",
		RouteKey:       "$default",
		RawPath:        r.URL.Path,
		RawQueryString: r.URL.RawQuery,
		Headers:        headers,
	}

	if len(bodyBytes) > 0 {
		event.Body = base64.StdEncoding.EncodeToString(bodyBytes)
		event.IsBase64Encoded = true
	}

	return json.Marshal(event)
}

// writeFunctionURLResponse writes the Lambda function URL response to the HTTP response writer.
func writeFunctionURLResponse(w http.ResponseWriter, result []byte) {
	// Try to parse as Lambda function URL response format.
	var resp lambdaURLResponse
	if jsonErr := json.Unmarshal(result, &resp); jsonErr == nil && resp.StatusCode != 0 {
		for k, v := range resp.Headers {
			w.Header().Set(k, v)
		}

		w.WriteHeader(resp.StatusCode)
		writeFunctionURLBody(w, resp)

		return
	}

	// Fall back to returning raw result.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(result) //nolint:gosec // G705: writing Lambda invoke result to HTTP response is intentional
}

// writeFunctionURLBody writes the body portion of a Lambda URL response.
func writeFunctionURLBody(w http.ResponseWriter, resp lambdaURLResponse) {
	if resp.IsBase64Encoded {
		decoded, decErr := base64.StdEncoding.DecodeString(resp.Body)
		if decErr == nil {
			_, _ = w.Write(decoded)
		}

		return
	}

	_, _ = w.Write([]byte(resp.Body))
}

// buildURLARN constructs an ARN for a Lambda function URL.
func buildURLARN(region, accountID, functionName string) string {
	return arn.Build("lambda", region, accountID, "function:"+functionName)
}

// CreateFunction stores a new Lambda function configuration.
func (b *InMemoryBackend) CreateFunction(fn *FunctionConfiguration) error {
	b.mu.Lock("CreateFunction")
	defer b.mu.Unlock()

	if _, exists := b.functions[fn.FunctionName]; exists {
		return ErrFunctionAlreadyExists
	}

	b.functions[fn.FunctionName] = fn

	return nil
}

// GetFunction retrieves a Lambda function configuration by name.
func (b *InMemoryBackend) GetFunction(name string) (*FunctionConfiguration, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	return fn, nil
}

// ListFunctions returns a page of Lambda function configurations sorted by name.
func (b *InMemoryBackend) ListFunctions(marker string, maxItems int) page.Page[*FunctionConfiguration] {
	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	fns := make([]*FunctionConfiguration, 0, len(b.functions))
	for _, fn := range b.functions {
		fns = append(fns, fn)
	}

	sort.Slice(fns, func(i, j int) bool {
		return fns[i].FunctionName < fns[j].FunctionName
	})

	return page.New(fns, marker, maxItems, lambdaDefaultMaxItems)
}

// DeleteFunction removes a Lambda function and cleans up its runtime server.
func (b *InMemoryBackend) DeleteFunction(name string) error {
	b.mu.Lock("DeleteFunction")

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

		for _, d := range rt.layerDirs {
			_ = os.RemoveAll(d)
		}

		rt.mu.Close()
	}

	return nil
}

// UpdateFunction replaces a Lambda function's configuration.
func (b *InMemoryBackend) UpdateFunction(fn *FunctionConfiguration) error {
	b.mu.Lock("UpdateFunction")
	defer b.mu.Unlock()

	if _, ok := b.functions[fn.FunctionName]; !ok {
		return ErrFunctionNotFound
	}

	b.functions[fn.FunctionName] = fn

	return nil
}

// PublishVersion creates an immutable version snapshot of the current $LATEST function config.
func (b *InMemoryBackend) PublishVersion(name, description string) (*FunctionVersion, error) {
	b.mu.Lock("PublishVersion")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	b.versionCounters[name]++
	versionNum := strconv.Itoa(b.versionCounters[name])

	ver := &FunctionVersion{
		FunctionName: fn.FunctionName,
		FunctionArn:  buildVersionARN(b.region, b.accountID, fn.FunctionName, versionNum),
		Description:  description,
		Version:      versionNum,
		Runtime:      fn.Runtime,
		Handler:      fn.Handler,
		Role:         fn.Role,
		MemorySize:   fn.MemorySize,
		Timeout:      fn.Timeout,
		PackageType:  fn.PackageType,
		ImageURI:     fn.ImageURI,
		Environment:  deepCopyEnvironment(fn.Environment),
		Layers:       deepCopyFunctionLayers(fn.Layers),
		CodeSize:     fn.CodeSize,
		RevisionID:   uuid.New().String(),
		CreatedAt:    fn.LastModified,
		State:        fn.State,
	}

	b.versions[name] = append(b.versions[name], ver)

	return ver, nil
}

// GetVersion returns a specific version snapshot of a function.
// Pass "$LATEST" to get the live function config as a FunctionVersion.
func (b *InMemoryBackend) GetVersion(name, version string) (*FunctionVersion, error) {
	b.mu.RLock("GetVersion")
	defer b.mu.RUnlock()

	if version == versionLatest {
		fn, ok := b.functions[name]
		if !ok {
			return nil, ErrFunctionNotFound
		}

		return fnToVersion(fn), nil
	}

	for _, v := range b.versions[name] {
		if v.Version == version {
			return v, nil
		}
	}

	return nil, ErrVersionNotFound
}

// ListVersionsByFunction returns a page of published versions for a function (including $LATEST).
func (b *InMemoryBackend) ListVersionsByFunction(
	name, marker string,
	maxItems int,
) (page.Page[*FunctionVersion], error) {
	b.mu.RLock("ListVersionsByFunction")
	defer b.mu.RUnlock()

	fn, ok := b.functions[name]
	if !ok {
		return page.Page[*FunctionVersion]{}, ErrFunctionNotFound
	}

	result := make([]*FunctionVersion, 0, len(b.versions[name])+1)

	// $LATEST is always first.
	result = append(result, fnToVersion(fn))
	result = append(result, b.versions[name]...)

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems), nil
}

// versionInList reports whether target matches any version in the list.
func versionInList(versions []*FunctionVersion, target string) bool {
	for _, v := range versions {
		if v.Version == target {
			return true
		}
	}

	return false
}

// CreateAlias creates a new alias for a Lambda function pointing to a version.
func (b *InMemoryBackend) CreateAlias(name string, input *CreateAliasInput) (*FunctionAlias, error) {
	b.mu.Lock("CreateAlias")
	defer b.mu.Unlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	// Validate the target version: must be "$LATEST" or an existing published version.
	if input.FunctionVersion != versionLatest {
		if !versionInList(b.versions[name], input.FunctionVersion) {
			return nil, ErrVersionNotFound
		}
	}

	if _, ok := b.aliases[name]; !ok {
		b.aliases[name] = make(map[string]*FunctionAlias)
	}

	if _, exists := b.aliases[name][input.Name]; exists {
		return nil, ErrAliasAlreadyExists
	}

	alias := &FunctionAlias{
		Name:            input.Name,
		AliasArn:        buildAliasARN(b.region, b.accountID, name, input.Name),
		FunctionVersion: input.FunctionVersion,
		Description:     input.Description,
		RevisionID:      uuid.New().String(),
	}

	b.aliases[name][input.Name] = alias

	return alias, nil
}

// GetAlias returns a named alias for a function.
func (b *InMemoryBackend) GetAlias(name, aliasName string) (*FunctionAlias, error) {
	b.mu.RLock("GetAlias")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	aliasMap, ok := b.aliases[name]
	if !ok {
		return nil, ErrAliasNotFound
	}

	alias, ok := aliasMap[aliasName]
	if !ok {
		return nil, ErrAliasNotFound
	}

	return alias, nil
}

// ListAliases returns a page of aliases for a function sorted by name.
func (b *InMemoryBackend) ListAliases(name, marker string, maxItems int) (page.Page[*FunctionAlias], error) {
	b.mu.RLock("ListAliases")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return page.Page[*FunctionAlias]{}, ErrFunctionNotFound
	}

	aliasMap := b.aliases[name]
	result := make([]*FunctionAlias, 0, len(aliasMap))

	for _, a := range aliasMap {
		result = append(result, a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems), nil
}

// UpdateAlias updates an existing alias.
func (b *InMemoryBackend) UpdateAlias(name, aliasName string, input *UpdateAliasInput) (*FunctionAlias, error) {
	b.mu.Lock("UpdateAlias")
	defer b.mu.Unlock()

	aliasMap, ok := b.aliases[name]
	if !ok {
		return nil, ErrAliasNotFound
	}

	alias, ok := aliasMap[aliasName]
	if !ok {
		return nil, ErrAliasNotFound
	}

	if input.FunctionVersion != "" {
		alias.FunctionVersion = input.FunctionVersion
	}

	if input.Description != "" {
		alias.Description = input.Description
	}

	alias.RevisionID = uuid.New().String()

	return alias, nil
}

// DeleteAlias removes a named alias from a function.
func (b *InMemoryBackend) DeleteAlias(name, aliasName string) error {
	b.mu.Lock("DeleteAlias")
	defer b.mu.Unlock()

	aliasMap, hasMap := b.aliases[name]
	if !hasMap {
		return ErrAliasNotFound
	}

	if _, hasAlias := aliasMap[aliasName]; !hasAlias {
		return ErrAliasNotFound
	}

	delete(aliasMap, aliasName)

	return nil
}

// resolveQualifier resolves a function name with an optional qualifier to a FunctionConfiguration.
// Qualifier may be a version number, alias name, or "$LATEST" (default when empty).
// Returns the resolved function config.
func (b *InMemoryBackend) resolveQualifier(name, qualifier string) (*FunctionConfiguration, error) {
	if qualifier == "" || qualifier == versionLatest {
		return b.GetFunction(name)
	}

	// Check if qualifier is an alias; if so, resolve to the target version string.
	// Hold a single RLock for both the alias lookup and the version search to avoid
	// TOCTOU races with concurrent alias/version updates.
	b.mu.RLock("resolveQualifier")

	if aliasMap := b.aliases[name]; aliasMap != nil {
		if alias, ok := aliasMap[qualifier]; ok {
			qualifier = alias.FunctionVersion
		}
	}

	// Now qualifier is a version number. Find the version snapshot.
	for _, v := range b.versions[name] {
		if v.Version == qualifier {
			fn := versionToFn(v)
			b.mu.RUnlock()

			return fn, nil
		}
	}

	b.mu.RUnlock()

	// If it's "$LATEST" after alias resolution, fall through to live config.
	if qualifier == versionLatest {
		return b.GetFunction(name)
	}

	return nil, ErrVersionNotFound
}

// deepCopyEnvironment returns a deep copy of an EnvironmentConfig, or nil if src is nil.
func deepCopyEnvironment(src *EnvironmentConfig) *EnvironmentConfig {
	if src == nil {
		return nil
	}

	vars := make(map[string]string, len(src.Variables))
	maps.Copy(vars, src.Variables)

	return &EnvironmentConfig{Variables: vars}
}

// deepCopyFunctionLayers returns a shallow copy of a FunctionLayer slice.
func deepCopyFunctionLayers(src []*FunctionLayer) []*FunctionLayer {
	if len(src) == 0 {
		return nil
	}

	dst := make([]*FunctionLayer, len(src))
	for i, l := range src {
		if l == nil {
			continue
		}

		cp := *l
		dst[i] = &cp
	}

	return dst
}

// fnToVersion converts a live FunctionConfiguration to a $LATEST FunctionVersion.
func fnToVersion(fn *FunctionConfiguration) *FunctionVersion {
	return &FunctionVersion{
		FunctionName: fn.FunctionName,
		FunctionArn:  fn.FunctionArn,
		Description:  fn.Description,
		Version:      versionLatest,
		Runtime:      fn.Runtime,
		Handler:      fn.Handler,
		Role:         fn.Role,
		MemorySize:   fn.MemorySize,
		Timeout:      fn.Timeout,
		PackageType:  fn.PackageType,
		ImageURI:     fn.ImageURI,
		Environment:  fn.Environment,
		Layers:       fn.Layers,
		CodeSize:     fn.CodeSize,
		RevisionID:   fn.RevisionID,
		CreatedAt:    fn.LastModified,
		State:        fn.State,
	}
}

// versionToFn synthesises a FunctionConfiguration from an immutable version snapshot.
// This is used for qualified invocations.
func versionToFn(v *FunctionVersion) *FunctionConfiguration {
	return &FunctionConfiguration{
		FunctionName: v.FunctionName,
		FunctionArn:  v.FunctionArn,
		Description:  v.Description,
		Runtime:      v.Runtime,
		Handler:      v.Handler,
		Role:         v.Role,
		MemorySize:   v.MemorySize,
		Timeout:      v.Timeout,
		PackageType:  v.PackageType,
		ImageURI:     v.ImageURI,
		Environment:  v.Environment,
		CodeSize:     v.CodeSize,
		RevisionID:   v.RevisionID,
		LastModified: v.CreatedAt,
		State:        v.State,
	}
}

// buildVersionARN constructs a Lambda function version ARN.
func buildVersionARN(region, accountID, functionName, version string) string {
	return arn.Build("lambda", region, accountID, "function:"+functionName+":"+version)
}

// buildAliasARN constructs a Lambda function alias ARN.
func buildAliasARN(region, accountID, functionName, aliasName string) string {
	return arn.Build("lambda", region, accountID, "function:"+functionName+":"+aliasName)
}

// InvokeFunction invokes a Lambda function without a qualifier (equivalent to "$LATEST").
// For qualified invocations (alias or version number), use InvokeFunctionWithQualifier.
func (b *InMemoryBackend) InvokeFunction(
	ctx context.Context,
	name string,
	invocationType InvocationType,
	payload []byte,
) ([]byte, int, error) {
	return b.InvokeFunctionWithQualifier(ctx, name, "", invocationType, payload)
}

// asyncInvocationEnqueueTimeout is the maximum time a background goroutine will wait
// to place an async (Event) invocation into the runtime queue. If the queue remains
// full after this duration the invocation is dropped with a warning log.
const asyncInvocationEnqueueTimeout = 5 * time.Minute

// InvokeFunctionWithQualifier invokes a Lambda function using an optional qualifier.
func (b *InMemoryBackend) InvokeFunctionWithQualifier(
	ctx context.Context,
	name, qualifier string,
	invocationType InvocationType,
	payload []byte,
) ([]byte, int, error) {
	fn, err := b.resolveQualifier(name, qualifier)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	if invocationType == InvocationTypeDryRun {
		return nil, http.StatusNoContent, nil
	}

	// Check FIS fault injection state for this function.
	fisPayload, fisStatus, fisErr := b.applyFISFaultToInvocation(ctx, fn.FunctionName)
	if fisPayload != nil || fisErr != nil {
		return fisPayload, fisStatus, fisErr
	}

	// Enforce reserved concurrency limits for all invocation types.
	// Reserved concurrency of 0 blocks all invocations; non-zero limits are enforced
	// for both synchronous (RequestResponse) and asynchronous (Event) invocations.
	trackConcurrency, concErr := b.acquireConcurrencySlot(fn.FunctionName)
	if concErr != nil {
		return nil, http.StatusTooManyRequests, concErr
	}

	// For synchronous invocations, release the concurrency slot when this function returns.
	// For async (Event) invocations, enqueueAsyncInvocation releases the slot after the
	// invocation completes or times out.
	if trackConcurrency && invocationType != InvocationTypeEvent {
		defer b.releaseConcurrencySlot(fn.FunctionName)
	}

	srv, srvErr := b.getOrCreateRuntime(ctx, fn)
	if srvErr != nil {
		// Release the slot on error regardless of invocation type.
		if trackConcurrency {
			b.releaseConcurrencySlot(fn.FunctionName)
		}

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

		b.enqueueAsyncInvocation(ctx, srv, fn.FunctionName, inv, timeout, trackConcurrency)

		return nil, http.StatusAccepted, nil
	}

	result, isError, invokeErr := srv.invoke(ctx, payload, timeout)
	if invokeErr != nil {
		return nil, http.StatusInternalServerError, invokeErr
	}

	// Per Lambda convention, function-level errors (isError=true) still return HTTP 200.
	_ = isError

	b.pushInvocationLog(fn.FunctionName, payload, result)

	return result, http.StatusOK, nil
}

// enqueueAsyncInvocation launches a background goroutine that places inv into the
// runtime queue, blocking until space is available or asyncInvocationEnqueueTimeout
// elapses. When trackConcurrency is true the goroutine releases the concurrency slot
// once the container finishes processing the invocation (or after a deadline timeout).
// [context.WithoutCancel] is used so the goroutine is not affected by the caller's
// HTTP-request context being cancelled after the 202 response is sent.
func (b *InMemoryBackend) enqueueAsyncInvocation(
	ctx context.Context,
	srv *runtimeServer,
	functionName string,
	inv *pendingInvocation,
	timeout time.Duration,
	trackConcurrency bool,
) {
	go func() {
		enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), asyncInvocationEnqueueTimeout)
		defer cancel()

		select {
		case srv.queue <- inv:
			if !trackConcurrency {
				return
			}

			// Wait for the container to finish processing this invocation before
			// releasing the concurrency slot.
			waitTimer := time.NewTimer(timeout + containerResponseGracePeriod)

			select {
			case <-inv.result:
			case <-waitTimer.C:
			}

			// Stop the timer and drain the channel if it already fired to
			// prevent a stale value accumulating in the channel buffer.
			if !waitTimer.Stop() {
				select {
				case <-waitTimer.C:
				default:
				}
			}

			b.releaseConcurrencySlot(functionName)

		case <-enqueueCtx.Done():
			slog.Default().Warn(
				"lambda: async invocation dropped: queue full",
				"function", functionName, "requestID", inv.requestID,
			)

			if trackConcurrency {
				b.releaseConcurrencySlot(functionName)
			}
		}
	}()
}

// acquireConcurrencySlot checks and optionally increments the active concurrency counter
// for a function. It returns (true, nil) when a slot was acquired (caller must release),
// (false, nil) when the function has no reserved concurrency limit, or (false, err) when
// the limit is already exhausted. Must not be called with b.mu held.
func (b *InMemoryBackend) acquireConcurrencySlot(functionName string) (bool, error) {
	b.mu.Lock("acquireConcurrencySlot")
	defer b.mu.Unlock()

	reserved, hasLimit := b.functionConcurrencies[functionName]
	if !hasLimit {
		return false, nil
	}

	// Reserved concurrency of 0 disables all invocations regardless of type.
	if reserved == 0 {
		return false, fmt.Errorf("%w: reserved concurrency is 0 for function %s", ErrTooManyRequests, functionName)
	}

	active := b.activeConcurrencies[functionName]
	if active >= reserved {
		return false, fmt.Errorf(
			"%w: concurrent execution limit reached for function %s",
			ErrTooManyRequests,
			functionName,
		)
	}

	b.activeConcurrencies[functionName]++

	return true, nil
}

// releaseConcurrencySlot decrements the active concurrency counter for a function.
// Must not be called with b.mu held.
func (b *InMemoryBackend) releaseConcurrencySlot(functionName string) {
	b.mu.Lock("releaseConcurrencySlot")
	defer b.mu.Unlock()

	if b.activeConcurrencies[functionName] > 0 {
		b.activeConcurrencies[functionName]--
	}
}

// pushInvocationLog writes a minimal invocation log entry to CloudWatch Logs when a backend is set.
func (b *InMemoryBackend) pushInvocationLog(functionName string, _ []byte, result []byte) {
	b.mu.RLock("pushInvocationLog")
	cwl := b.cwLogs
	b.mu.RUnlock()

	if cwl == nil {
		return
	}

	groupName := "/aws/lambda/" + functionName
	streamName := time.Now().UTC().Format("2006/01/02") + "/[$LATEST]" + uuid.New().String()[:8]

	if err := cwl.EnsureLogGroupAndStream(groupName, streamName); err != nil {
		slog.Default().Warn("pushInvocationLog: failed to ensure log group/stream",
			"function", functionName, "error", err)

		return
	}

	messages := []string{
		"END RequestId: " + uuid.New().String(),
		"REPORT response length: " + strconv.Itoa(len(result)),
	}

	if err := cwl.PutLogLines(groupName, streamName, messages); err != nil {
		slog.Default().Warn("pushInvocationLog: failed to put log lines",
			"function", functionName, "error", err)
	}
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
		return nil, fmt.Errorf("%w: container runtime unavailable", ErrLambdaUnavailable)
	}

	b.mu.Lock("getOrCreateRuntime")
	rt, ok := b.runtimes[fn.FunctionName]

	if !ok {
		rt = &functionRuntime{mu: lockmetrics.New("lambda.runtime")}
		b.runtimes[fn.FunctionName] = rt
	}

	b.mu.Unlock()

	rt.mu.Lock("getOrCreateRuntime")
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

	srv := newRuntimeServer(port)

	if startErr := srv.start(ctx); startErr != nil {
		_ = b.portAlloc.Release(port)
		rt.startErr = fmt.Errorf("%w: runtime server start failed: %w", ErrLambdaUnavailable, startErr)
		rt.started = true

		return nil, rt.startErr
	}

	rt.srv = srv
	rt.port = port
	rt.started = true

	zipDir, layerDirs, containerErr := b.startContainer(ctx, fn, port)
	if containerErr != nil {
		// Container startup failure is fatal: stop the runtime server, release the
		// port, and surface the error so the caller gets an immediate failure instead
		// of silently timing out on every subsequent invoke.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), containerShutdownTimeout)
		defer cancel()
		srv.stop(shutdownCtx)
		_ = b.portAlloc.Release(port)

		for _, d := range layerDirs {
			_ = os.RemoveAll(d) // #nosec G703
		}

		if zipDir != "" {
			_ = os.RemoveAll(zipDir) // #nosec G703
		}

		rt.startErr = fmt.Errorf("%w: container startup failed: %w", ErrLambdaUnavailable, containerErr)

		return nil, rt.startErr
	}

	rt.zipDir = zipDir
	rt.layerDirs = layerDirs

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
	// Normalize and validate the entry name to prevent zip-slip.
	cleanName := filepath.Clean(f.Name)
	if cleanName == "" || cleanName == "." {
		return nil // skip empty / current-dir entries
	}

	if filepath.IsAbs(cleanName) {
		return fmt.Errorf("%w: %q has absolute path", ErrZipSlip, f.Name)
	}

	destPath := filepath.Join(destDir, cleanName)

	rel, relErr := filepath.Rel(destDir, destPath)
	if relErr != nil {
		return fmt.Errorf("zip entry %q path resolution failed: %w", f.Name, relErr)
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("%w: %q", ErrZipSlip, f.Name)
	}

	if f.FileInfo().IsDir() {
		return os.MkdirAll(destPath, f.Mode()) // #nosec G703
	}

	parentDir := filepath.Dir(destPath)
	if err := os.MkdirAll(parentDir, 0o750); err != nil { // #nosec G703
		return fmt.Errorf("mkdir %q: %w", parentDir, err)
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("open zip entry %q: %w", f.Name, err)
	}
	defer rc.Close()

	outFile, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode()) // #nosec G304 G703
	if err != nil {
		return fmt.Errorf("create file %q: %w", destPath, err)
	}
	defer outFile.Close()

	if _, copyErr := io.Copy(outFile, rc); copyErr != nil { // #nosec G110
		return fmt.Errorf("extract file %q: %w", f.Name, copyErr)
	}

	return nil
}

// startContainer creates and starts a Lambda container for the given function.
// For Zip functions it extracts the code to a temp directory and bind-mounts it.
// Returns the temp directory path (non-empty only for Zip functions), a list of
// layer temp directories, and any error.
func (b *InMemoryBackend) startContainer(
	ctx context.Context,
	fn *FunctionConfiguration,
	runtimePort int,
) (string, []string, error) {
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

	// Extract layer zips and prepare the /opt mount for both Zip and Image functions.
	layerMount, layerDirs, layerErr := b.prepareLayerMount(fn)
	if layerErr != nil {
		slog.Default().WarnContext(ctx, "lambda: layer extraction failed",
			"function", fn.FunctionName, "error", layerErr)
	}

	if fn.PackageType == PackageTypeZip {
		zipDir, err := b.startZipContainer(ctx, fn, env, layerMount)

		return zipDir, layerDirs, err
	}

	spec := container.Spec{
		Image: fn.ImageURI,
		Name:  fmt.Sprintf("gopherstack-lambda-%s-%s", fn.FunctionName, uuid.New().String()[:8]),
		Env:   env,
	}

	if layerMount != "" {
		spec.Mounts = append(spec.Mounts, layerMount)
	}

	_, err := b.docker.CreateAndStart(ctx, spec)

	return "", layerDirs, err
}

// startZipContainer handles container startup for Zip-packaged Lambda functions.
// It fetches the zip (from inline ZipData or S3), extracts it to a temp directory,
// and bind-mounts the directory into the appropriate AWS base image container.
// An optional layerMount bind-mount string (host:/opt:ro) can also be provided.
func (b *InMemoryBackend) startZipContainer(
	ctx context.Context,
	fn *FunctionConfiguration,
	env []string,
	layerMount string,
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

	mounts := []string{zipDir + ":/var/task:ro"}
	if layerMount != "" {
		mounts = append(mounts, layerMount)
	}

	spec := container.Spec{
		Image:  baseImage,
		Name:   fmt.Sprintf("gopherstack-lambda-%s-%s", fn.FunctionName, uuid.New().String()[:8]),
		Env:    env,
		Mounts: mounts,
	}

	if fn.Handler != "" {
		spec.Cmd = []string{fn.Handler}
	}

	if _, err := b.docker.CreateAndStart(ctx, spec); err != nil {
		_ = os.RemoveAll(zipDir) // #nosec G703

		return "", err
	}

	return zipDir, nil
}

// parseLayerARN extracts the layer name and version number from a layer version ARN.
// Expected format: arn:aws:lambda:{region}:{account}:layer:{name}:{version}
// Returns empty name and 0 version if the ARN is not in the expected format.
func parseLayerARN(layerVersionARN string) (string, int64) {
	// Split on ":" — the resource part is "layer:{name}:{version}".
	parts := strings.Split(layerVersionARN, ":")
	// A valid ARN has exactly 8 colon-separated parts.
	const layerARNParts = 8
	if len(parts) != layerARNParts {
		return "", 0
	}

	if parts[5] != "layer" {
		return "", 0
	}

	layerName := parts[6]

	var v int64

	if _, err := fmt.Sscanf(parts[7], "%d", &v); err != nil {
		return "", 0
	}

	return layerName, v
}

// prepareLayerMount extracts all layers attached to fn into a single merged temp directory
// and returns the bind-mount string ("{dir}:/opt:ro"), a list of temp dirs (for cleanup),
// and any error. If the function has no layers with zip data, returns ("", nil, nil).
// ZIP extraction is performed outside the backend lock to avoid blocking concurrent operations.
func (b *InMemoryBackend) prepareLayerMount(fn *FunctionConfiguration) (string, []string, error) {
	if len(fn.Layers) == 0 {
		return "", nil, nil
	}

	// Collect the zip bytes under the read lock, then release before doing any I/O.
	type layerEntry struct {
		name    string
		zipData []byte
		version int64
	}

	var entries []layerEntry

	b.mu.RLock("prepareLayerMount")

	for _, fl := range fn.Layers {
		if fl == nil || fl.Arn == "" {
			continue
		}

		layerName, layerVersion := parseLayerARN(fl.Arn)
		if layerName == "" {
			continue
		}

		versions := b.layers[layerName]

		for _, lv := range versions {
			if lv.Version == layerVersion && len(lv.ZipData) > 0 {
				data := make([]byte, len(lv.ZipData))
				copy(data, lv.ZipData)
				entries = append(entries, layerEntry{name: layerName, version: layerVersion, zipData: data})

				break
			}
		}
	}

	b.mu.RUnlock()

	if len(entries) == 0 {
		return "", nil, nil
	}

	// Create a single temp directory to merge all layer contents into (matches AWS behaviour).
	optDir, mkErr := os.MkdirTemp("", "gopherstack-lambda-layers-*")
	if mkErr != nil {
		return "", nil, fmt.Errorf("create layer temp dir: %w", mkErr)
	}

	for _, entry := range entries {
		if extractErr := extractZipIntoDir(optDir, entry.zipData); extractErr != nil {
			_ = os.RemoveAll(optDir)

			return "", nil, fmt.Errorf("extract layer %q v%d: %w", entry.name, entry.version, extractErr)
		}
	}

	return optDir + ":/opt:ro", []string{optDir}, nil
}

// extractZipIntoDir extracts zip bytes into an existing directory (used for layer extraction).
func extractZipIntoDir(dir string, zipData []byte) error {
	r, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}

	for _, f := range r.File {
		if extractErr := extractZipFile(dir, f); extractErr != nil {
			return extractErr
		}
	}

	return nil
}

// buildLayerARN constructs a Lambda layer ARN.
func (b *InMemoryBackend) buildLayerARN(layerName string) string {
	return arn.Build("lambda", b.region, b.accountID, "layer:"+layerName)
}

// buildLayerVersionARN constructs a Lambda layer version ARN.
func (b *InMemoryBackend) buildLayerVersionARN(layerName string, version int64) string {
	return fmt.Sprintf("%s:%d", b.buildLayerARN(layerName), version)
}

// PublishLayerVersion creates a new immutable version of the named layer.
func (b *InMemoryBackend) PublishLayerVersion(input *PublishLayerVersionInput) (*PublishLayerVersionOutput, error) {
	if input == nil || input.Content == nil {
		return nil, fmt.Errorf("%w: Content is required", ErrLambdaUnavailable)
	}

	if input.LayerName == "" {
		return nil, fmt.Errorf("%w: LayerName is required", ErrInvalidParameterValue)
	}

	b.mu.Lock("PublishLayerVersion")
	defer b.mu.Unlock()

	b.layerVersionCounters[input.LayerName]++
	version := b.layerVersionCounters[input.LayerName]

	zipData := input.Content.ZipFile
	codeSize := int64(len(zipData))

	lv := &LayerVersion{
		LayerVersionArn:    b.buildLayerVersionARN(input.LayerName, version),
		Description:        input.Description,
		CreatedDate:        time.Now().UTC().Format(time.RFC3339),
		Version:            version,
		CompatibleRuntimes: input.CompatibleRuntimes,
		LicenseInfo:        input.LicenseInfo,
		ZipData:            zipData,
		Content: &LayerVersionContent{
			CodeSize: codeSize,
		},
	}

	b.layers[input.LayerName] = append(b.layers[input.LayerName], lv)

	return &PublishLayerVersionOutput{
		LayerVersionArn:    lv.LayerVersionArn,
		LayerArn:           b.buildLayerARN(input.LayerName),
		Description:        lv.Description,
		CreatedDate:        lv.CreatedDate,
		Content:            lv.Content,
		CompatibleRuntimes: lv.CompatibleRuntimes,
		LicenseInfo:        lv.LicenseInfo,
		Version:            lv.Version,
	}, nil
}

// GetLayerVersion retrieves metadata for a specific layer version.
func (b *InMemoryBackend) GetLayerVersion(layerName string, version int64) (*GetLayerVersionOutput, error) {
	b.mu.RLock("GetLayerVersion")
	defer b.mu.RUnlock()

	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return nil, ErrLayerNotFound
	}

	for _, lv := range versions {
		if lv.Version == version {
			return &GetLayerVersionOutput{
				LayerVersionArn:    lv.LayerVersionArn,
				LayerArn:           b.buildLayerARN(layerName),
				Description:        lv.Description,
				CreatedDate:        lv.CreatedDate,
				Content:            lv.Content,
				CompatibleRuntimes: lv.CompatibleRuntimes,
				LicenseInfo:        lv.LicenseInfo,
				Version:            lv.Version,
			}, nil
		}
	}

	return nil, ErrLayerVersionNotFound
}

// ListLayers returns a summary of all layers with their latest version.
func (b *InMemoryBackend) ListLayers() []*Layer {
	b.mu.RLock("ListLayers")
	defer b.mu.RUnlock()

	result := make([]*Layer, 0, len(b.layers))

	names := make([]string, 0, len(b.layers))
	for name := range b.layers {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		versions := b.layers[name]
		if len(versions) == 0 {
			continue
		}

		latest := versions[len(versions)-1]

		result = append(result, &Layer{
			LayerArn:  b.buildLayerARN(name),
			LayerName: name,
			LatestMatchingVersion: &LayerVersion{
				LayerVersionArn:    latest.LayerVersionArn,
				Description:        latest.Description,
				CreatedDate:        latest.CreatedDate,
				Content:            latest.Content,
				CompatibleRuntimes: latest.CompatibleRuntimes,
				LicenseInfo:        latest.LicenseInfo,
				Version:            latest.Version,
			},
		})
	}

	return result
}

// ListLayerVersions returns all versions of a specific layer in descending order.
func (b *InMemoryBackend) ListLayerVersions(layerName string) ([]*LayerVersion, error) {
	b.mu.RLock("ListLayerVersions")
	defer b.mu.RUnlock()

	versions, ok := b.layers[layerName]
	if !ok {
		return nil, ErrLayerNotFound
	}

	// Return a copy in reverse order (newest first).
	result := make([]*LayerVersion, len(versions))
	for i, lv := range versions {
		result[len(versions)-1-i] = &LayerVersion{
			LayerVersionArn:    lv.LayerVersionArn,
			Description:        lv.Description,
			CreatedDate:        lv.CreatedDate,
			Content:            lv.Content,
			CompatibleRuntimes: lv.CompatibleRuntimes,
			LicenseInfo:        lv.LicenseInfo,
			Version:            lv.Version,
		}
	}

	return result, nil
}

// DeleteLayerVersion removes an immutable layer version.
func (b *InMemoryBackend) DeleteLayerVersion(layerName string, version int64) error {
	b.mu.Lock("DeleteLayerVersion")
	defer b.mu.Unlock()

	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return ErrLayerNotFound
	}

	for i, lv := range versions {
		if lv.Version == version {
			b.layers[layerName] = append(versions[:i], versions[i+1:]...)

			// Clean up policy entries for deleted version.
			if b.layerPolicies[layerName] != nil {
				delete(b.layerPolicies[layerName], version)
			}

			return nil
		}
	}

	return ErrLayerVersionNotFound
}

// GetLayerVersionPolicy returns the resource policy for a layer version.
func (b *InMemoryBackend) GetLayerVersionPolicy(layerName string, version int64) (*LayerVersionPolicy, error) {
	b.mu.RLock("GetLayerVersionPolicy")
	defer b.mu.RUnlock()

	// Verify the version exists.
	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return nil, ErrLayerNotFound
	}

	found := false

	for _, lv := range versions {
		if lv.Version == version {
			found = true

			break
		}
	}

	if !found {
		return nil, ErrLayerVersionNotFound
	}

	stmts := b.layerPolicies[layerName][version]

	policy, marshalErr := buildLayerPolicy(stmts)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return &LayerVersionPolicy{
		Policy:     policy,
		RevisionID: "1",
	}, nil
}

// AddLayerVersionPermission adds a permission statement to a layer version's resource policy.
func (b *InMemoryBackend) AddLayerVersionPermission(
	layerName string, version int64, input *AddLayerVersionPermissionInput,
) (*AddLayerVersionPermissionOutput, error) {
	b.mu.Lock("AddLayerVersionPermission")
	defer b.mu.Unlock()

	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return nil, ErrLayerNotFound
	}

	found := false

	for _, lv := range versions {
		if lv.Version == version {
			found = true

			break
		}
	}

	if !found {
		return nil, ErrLayerVersionNotFound
	}

	if b.layerPolicies[layerName] == nil {
		b.layerPolicies[layerName] = make(map[int64]map[string]*LayerVersionStatement)
	}

	if b.layerPolicies[layerName][version] == nil {
		b.layerPolicies[layerName][version] = make(map[string]*LayerVersionStatement)
	}

	stmt := &LayerVersionStatement{
		StatementID: input.StatementID,
		Action:      input.Action,
		Principal:   input.Principal,
	}

	b.layerPolicies[layerName][version][input.StatementID] = stmt

	stmtJSON, marshalErr := json.Marshal(stmt)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return &AddLayerVersionPermissionOutput{
		Statement:  string(stmtJSON),
		RevisionID: "1",
	}, nil
}

// RemoveLayerVersionPermission removes a permission statement from a layer version's resource policy.
func (b *InMemoryBackend) RemoveLayerVersionPermission(layerName string, version int64, statementID string) error {
	b.mu.Lock("RemoveLayerVersionPermission")
	defer b.mu.Unlock()

	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return ErrLayerNotFound
	}

	found := false

	for _, lv := range versions {
		if lv.Version == version {
			found = true

			break
		}
	}

	if !found {
		return ErrLayerVersionNotFound
	}

	if b.layerPolicies[layerName] == nil || b.layerPolicies[layerName][version] == nil {
		return nil
	}

	delete(b.layerPolicies[layerName][version], statementID)

	return nil
}

// buildLayerPolicy serialises a map of statements to a JSON IAM policy document string.
func buildLayerPolicy(stmts map[string]*LayerVersionStatement) (string, error) {
	type policyDocument struct {
		Version   string              `json:"Version"`
		Statement []map[string]string `json:"Statement"`
	}

	statements := make([]map[string]string, 0, len(stmts))

	stmtIDs := make([]string, 0, len(stmts))
	for sid := range stmts {
		stmtIDs = append(stmtIDs, sid)
	}

	sort.Strings(stmtIDs)

	for _, sid := range stmtIDs {
		s := stmts[sid]
		statements = append(statements, map[string]string{
			"Sid":       s.StatementID,
			"Effect":    "Allow",
			"Principal": s.Principal,
			"Action":    s.Action,
		})
	}

	doc := policyDocument{
		Version:   "2012-10-17",
		Statement: statements,
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// maxRetryAttempts is the maximum allowed value for MaximumRetryAttempts.
const maxRetryAttempts = 2

// minEventAgeInSeconds is the minimum allowed value for MaximumEventAgeInSeconds.
const minEventAgeInSeconds = 60

// maxEventAgeInSeconds is the maximum allowed value for MaximumEventAgeInSeconds.
const maxEventAgeInSeconds = 21600

// PutFunctionEventInvokeConfig creates or replaces the event invoke configuration for a function.
func (b *InMemoryBackend) PutFunctionEventInvokeConfig(
	name string,
	input *PutFunctionEventInvokeConfigInput,
) (*FunctionEventInvokeConfig, error) {
	b.mu.Lock("PutFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if err := validateEventInvokeConfigInput(input); err != nil {
		return nil, err
	}

	cfg := &FunctionEventInvokeConfig{
		FunctionArn:              fn.FunctionArn,
		LastModified:             time.Now().UTC(),
		MaximumRetryAttempts:     input.MaximumRetryAttempts,
		MaximumEventAgeInSeconds: input.MaximumEventAgeInSeconds,
		DestinationConfig:        input.DestinationConfig,
	}

	b.eventInvokeConfigs[name] = cfg

	return cfg, nil
}

// GetFunctionEventInvokeConfig returns the event invoke configuration for a function.
func (b *InMemoryBackend) GetFunctionEventInvokeConfig(name string) (*FunctionEventInvokeConfig, error) {
	b.mu.RLock("GetFunctionEventInvokeConfig")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.eventInvokeConfigs[name]
	if !ok {
		return nil, ErrEventInvokeConfigNotFound
	}

	return cfg, nil
}

// UpdateFunctionEventInvokeConfig updates the event invoke configuration for a function.
// It returns ErrEventInvokeConfigNotFound if no config exists yet.
func (b *InMemoryBackend) UpdateFunctionEventInvokeConfig(
	name string,
	input *PutFunctionEventInvokeConfigInput,
) (*FunctionEventInvokeConfig, error) {
	b.mu.Lock("UpdateFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.eventInvokeConfigs[name]
	if !ok {
		return nil, ErrEventInvokeConfigNotFound
	}

	if err := validateEventInvokeConfigInput(input); err != nil {
		return nil, err
	}

	if input.MaximumRetryAttempts != nil {
		cfg.MaximumRetryAttempts = input.MaximumRetryAttempts
	}

	if input.MaximumEventAgeInSeconds != nil {
		cfg.MaximumEventAgeInSeconds = input.MaximumEventAgeInSeconds
	}

	if input.DestinationConfig != nil {
		cfg.DestinationConfig = input.DestinationConfig
	}

	cfg.FunctionArn = fn.FunctionArn
	cfg.LastModified = time.Now().UTC()

	return cfg, nil
}

// DeleteFunctionEventInvokeConfig removes the event invoke configuration for a function.
func (b *InMemoryBackend) DeleteFunctionEventInvokeConfig(name string) error {
	b.mu.Lock("DeleteFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions[name]; !ok {
		return ErrFunctionNotFound
	}

	if _, ok := b.eventInvokeConfigs[name]; !ok {
		return ErrEventInvokeConfigNotFound
	}

	delete(b.eventInvokeConfigs, name)

	return nil
}

// ListFunctionEventInvokeConfigs returns a page of event invoke configurations for a function.
func (b *InMemoryBackend) ListFunctionEventInvokeConfigs(
	name, marker string,
	maxItems int,
) ([]*FunctionEventInvokeConfig, string, error) {
	b.mu.RLock("ListFunctionEventInvokeConfigs")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, "", ErrFunctionNotFound
	}

	var result []*FunctionEventInvokeConfig

	if cfg, ok := b.eventInvokeConfigs[name]; ok {
		result = []*FunctionEventInvokeConfig{cfg}
	}

	p := page.New(result, marker, maxItems, lambdaDefaultMaxItems)

	return p.Data, p.Next, nil
}

// validateEventInvokeConfigInput validates MaximumRetryAttempts and MaximumEventAgeInSeconds.
func validateEventInvokeConfigInput(input *PutFunctionEventInvokeConfigInput) error {
	if input.MaximumRetryAttempts != nil {
		v := *input.MaximumRetryAttempts
		if v < 0 || v > maxRetryAttempts {
			return fmt.Errorf(
				"%w: MaximumRetryAttempts must be between 0 and %d",
				ErrInvalidParameterValue,
				maxRetryAttempts,
			)
		}
	}

	if input.MaximumEventAgeInSeconds != nil {
		v := *input.MaximumEventAgeInSeconds
		if v < minEventAgeInSeconds || v > maxEventAgeInSeconds {
			return fmt.Errorf(
				"%w: MaximumEventAgeInSeconds must be between %d and %d",
				ErrInvalidParameterValue, minEventAgeInSeconds, maxEventAgeInSeconds,
			)
		}
	}

	return nil
}

// PutFunctionConcurrency sets the reserved concurrent executions for a function.
// Setting ReservedConcurrentExecutions to 0 disables all invocations of the function.
func (b *InMemoryBackend) PutFunctionConcurrency(name string, reserved int) (*FunctionConcurrency, error) {
	b.mu.Lock("PutFunctionConcurrency")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if reserved < 0 {
		return nil, fmt.Errorf("%w: ReservedConcurrentExecutions must be >= 0", ErrInvalidParameterValue)
	}

	b.functionConcurrencies[name] = reserved
	fn.ReservedConcurrentExecutions = &reserved

	return &FunctionConcurrency{ReservedConcurrentExecutions: reserved}, nil
}

// GetFunctionConcurrency returns the reserved concurrent executions for a function.
func (b *InMemoryBackend) GetFunctionConcurrency(name string) (*FunctionConcurrency, error) {
	b.mu.RLock("GetFunctionConcurrency")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	reserved, ok := b.functionConcurrencies[name]
	if !ok {
		return nil, ErrFunctionConcurrencyNotFound
	}

	return &FunctionConcurrency{ReservedConcurrentExecutions: reserved}, nil
}

// DeleteFunctionConcurrency removes the reserved concurrency setting for a function,
// restoring it to the account-level default.
func (b *InMemoryBackend) DeleteFunctionConcurrency(name string) error {
	b.mu.Lock("DeleteFunctionConcurrency")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return ErrFunctionNotFound
	}

	delete(b.functionConcurrencies, name)
	fn.ReservedConcurrentExecutions = nil

	return nil
}

// PutProvisionedConcurrencyConfig sets the provisioned concurrency configuration for a function qualifier.
// The qualifier must be a version number or alias name; $LATEST is not supported.
// Status is returned as READY immediately (stub implementation — no actual pre-warming).
func (b *InMemoryBackend) PutProvisionedConcurrencyConfig(
	name, qualifier string,
	requested int,
) (*ProvisionedConcurrencyConfig, error) {
	b.mu.Lock("PutProvisionedConcurrencyConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if requested <= 0 {
		return nil, fmt.Errorf("%w: ProvisionedConcurrentExecutions must be > 0", ErrInvalidParameterValue)
	}

	if qualifier == versionLatest {
		return nil, fmt.Errorf("%w: provisioned concurrency is not supported for $LATEST", ErrInvalidParameterValue)
	}

	if _, exists := b.provisionedConcurrencies[name]; !exists {
		b.provisionedConcurrencies[name] = make(map[string]*ProvisionedConcurrencyConfig)
	}

	cfg := &ProvisionedConcurrencyConfig{
		AllocatedProvisionedConcurrentExecutions: requested,
		AvailableProvisionedConcurrentExecutions: requested,
		FunctionArn:                              buildAliasARN(b.region, b.accountID, fn.FunctionName, qualifier),
		LastModified:                             time.Now().UTC().Format(time.RFC3339),
		RequestedProvisionedConcurrentExecutions: requested,
		Status:                                   "READY",
	}

	b.provisionedConcurrencies[name][qualifier] = cfg

	return cfg, nil
}

// GetProvisionedConcurrencyConfig returns the provisioned concurrency configuration for a function qualifier.
func (b *InMemoryBackend) GetProvisionedConcurrencyConfig(
	name, qualifier string,
) (*ProvisionedConcurrencyConfig, error) {
	b.mu.RLock("GetProvisionedConcurrencyConfig")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	qualifiers, ok := b.provisionedConcurrencies[name]
	if !ok {
		return nil, ErrProvisionedConcurrencyConfigNotFound
	}

	cfg, ok := qualifiers[qualifier]
	if !ok {
		return nil, ErrProvisionedConcurrencyConfigNotFound
	}

	return cfg, nil
}

// DeleteProvisionedConcurrencyConfig removes the provisioned concurrency configuration for a function qualifier.
func (b *InMemoryBackend) DeleteProvisionedConcurrencyConfig(name, qualifier string) error {
	b.mu.Lock("DeleteProvisionedConcurrencyConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions[name]; !ok {
		return ErrFunctionNotFound
	}

	qualifiers, ok := b.provisionedConcurrencies[name]
	if !ok {
		return ErrProvisionedConcurrencyConfigNotFound
	}

	if _, exists := qualifiers[qualifier]; !exists {
		return ErrProvisionedConcurrencyConfigNotFound
	}

	delete(qualifiers, qualifier)

	if len(qualifiers) == 0 {
		delete(b.provisionedConcurrencies, name)
	}

	return nil
}

// ListProvisionedConcurrencyConfigs returns all provisioned concurrency configurations for a function.
func (b *InMemoryBackend) ListProvisionedConcurrencyConfigs(name string) ([]*ProvisionedConcurrencyConfig, error) {
	b.mu.RLock("ListProvisionedConcurrencyConfigs")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	qualifiers := b.provisionedConcurrencies[name]
	configs := make([]*ProvisionedConcurrencyConfig, 0, len(qualifiers))

	for _, cfg := range qualifiers {
		configs = append(configs, cfg)
	}

	return configs, nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
// All active function URL server listeners are shut down before state is cleared
// so ports are released and stale handlers are removed.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")

	// Snapshot URL servers and runtimes for shutdown outside the lock.
	urlServers := make([]*functionURLServer, 0, len(b.functionURLServers))
	for _, srv := range b.functionURLServers {
		urlServers = append(urlServers, srv)
	}

	rts := make([]*functionRuntime, 0, len(b.runtimes))
	for _, rt := range b.runtimes {
		rts = append(rts, rt)
	}

	b.functions = make(map[string]*FunctionConfiguration)
	b.aliases = make(map[string]map[string]*FunctionAlias)
	b.versionCounters = make(map[string]int)
	b.versions = make(map[string][]*FunctionVersion)
	b.layers = make(map[string][]*LayerVersion)
	b.layerVersionCounters = make(map[string]int64)
	b.layerPolicies = make(map[string]map[int64]map[string]*LayerVersionStatement)
	b.eventSourceMappings = make(map[string]*EventSourceMapping)
	b.eventInvokeConfigs = make(map[string]*FunctionEventInvokeConfig)
	b.functionConcurrencies = make(map[string]int)
	b.activeConcurrencies = make(map[string]int)
	b.provisionedConcurrencies = make(map[string]map[string]*ProvisionedConcurrencyConfig)
	b.fisFaults = make(map[string]*FISInvocationFault)
	b.runtimes = make(map[string]*functionRuntime)
	b.functionURLServers = make(map[string]*functionURLServer)
	b.functionURLConfigs = make(map[string]*FunctionURLConfig)

	b.mu.Unlock()

	// Shut down URL servers and release ports outside the lock.
	ctx, cancel := context.WithTimeout(context.Background(), containerShutdownTimeout)
	defer cancel()

	var wg sync.WaitGroup

	for _, srv := range urlServers {
		wg.Go(func() {
			_ = srv.server.Shutdown(ctx)

			if b.portAlloc != nil {
				_ = b.portAlloc.Release(srv.port)
			}
		})
	}

	for _, rt := range rts {
		wg.Go(func() { rt.mu.Close() })
	}

	wg.Wait()
}
