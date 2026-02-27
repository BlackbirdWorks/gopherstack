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

	"github.com/google/uuid"

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
)

// versionLatest is the sentinel qualifier for the live function configuration.
const versionLatest = "$LATEST"

// StorageBackend defines the interface for Lambda backend operations.
type StorageBackend interface {
	CreateFunction(fn *FunctionConfiguration) error
	GetFunction(name string) (*FunctionConfiguration, error)
	ListFunctions() []*FunctionConfiguration
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
	srv      *runtimeServer
	startErr error
	zipDir   string // temp directory for extracted Zip Lambda code; cleaned up on delete
	mu       sync.Mutex
	port     int
	started  bool
}

// functionURLServer holds a running HTTP listener for a Lambda function URL.
type functionURLServer struct {
	listener net.Listener
	server   *http.Server
	port     int
}

// InMemoryBackend is a concurrency-safe in-memory Lambda backend.
type InMemoryBackend struct {
	functions           map[string]*FunctionConfiguration
	runtimes            map[string]*functionRuntime
	eventSourceMappings map[string]*EventSourceMapping
	functionURLConfigs  map[string]*FunctionURLConfig
	functionURLServers  map[string]*functionURLServer
	// versions stores immutable version snapshots: functionName → []FunctionVersion (sorted by number)
	versions map[string][]*FunctionVersion
	// aliases stores alias configs: functionName → aliasName → FunctionAlias
	aliases map[string]map[string]*FunctionAlias
	// versionCounters tracks the next version number per function
	versionCounters map[string]int
	kinesisPoller   *EventSourcePoller
	cwLogs          CWLogsBackend
	dnsRegistrar    DNSRegistrar
	docker          container.Runtime
	portAlloc       *portalloc.Allocator
	s3Fetcher       S3CodeFetcher
	logger          *slog.Logger
	accountID       string
	region          string
	settings        Settings
	mu              sync.RWMutex
}

// NewInMemoryBackend creates a new Lambda in-memory backend.
func NewInMemoryBackend(
	dockerClient container.Runtime,
	portAlloc *portalloc.Allocator,
	settings Settings,
	accountID, region string,
	log *slog.Logger,
) *InMemoryBackend {
	return &InMemoryBackend{
		functions:           make(map[string]*FunctionConfiguration),
		runtimes:            make(map[string]*functionRuntime),
		eventSourceMappings: make(map[string]*EventSourceMapping),
		functionURLConfigs:  make(map[string]*FunctionURLConfig),
		functionURLServers:  make(map[string]*functionURLServer),
		versions:            make(map[string][]*FunctionVersion),
		aliases:             make(map[string]map[string]*FunctionAlias),
		versionCounters:     make(map[string]int),
		docker:              dockerClient,
		portAlloc:           portAlloc,
		settings:            settings,
		accountID:           accountID,
		region:              region,
		logger:              log,
	}
}

// SetDNSRegistrar sets the optional DNS registrar used to register function URL hostnames.
func (b *InMemoryBackend) SetDNSRegistrar(r DNSRegistrar) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dnsRegistrar = r
}

// SetS3CodeFetcher sets the S3CodeFetcher for fetching Zip Lambda code from S3.
func (b *InMemoryBackend) SetS3CodeFetcher(f S3CodeFetcher) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.s3Fetcher = f
}

// SetCWLogsBackend sets the CloudWatch Logs backend for Lambda log delivery.
func (b *InMemoryBackend) SetCWLogsBackend(cwl CWLogsBackend) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cwLogs = cwl
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

// functionURLHostname returns the synthetic DNS hostname for a function URL.
func (b *InMemoryBackend) functionURLHostname(functionName string) string {
	return fmt.Sprintf("%s.lambda-url.%s.on.aws", functionName, b.region)
}

// CreateFunctionURLConfig creates a function URL endpoint for the given function.
// It allocates a port, starts an HTTP listener, registers DNS, and returns the config.
func (b *InMemoryBackend) CreateFunctionURLConfig(functionName, authType string) (*FunctionURLConfig, error) {
	b.mu.Lock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	cfg, ok := b.functionURLConfigs[functionName]
	if !ok {
		return nil, ErrFunctionURLNotFound
	}

	return cfg, nil
}

// DeleteFunctionURLConfig removes the function URL config, stops the listener, and deregisters DNS.
func (b *InMemoryBackend) DeleteFunctionURLConfig(functionName string) error {
	b.mu.Lock()

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
			if b.logger != nil {
				b.logger.Warn("lambda: function URL server stopped", "function", functionName, "error", serveErr)
			}
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
	return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", region, accountID, functionName)
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

// PublishVersion creates an immutable version snapshot of the current $LATEST function config.
func (b *InMemoryBackend) PublishVersion(name, description string) (*FunctionVersion, error) {
	b.mu.Lock()
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
	b.mu.RLock()
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

// ListVersionsByFunction returns all published versions for a function (including $LATEST).
func (b *InMemoryBackend) ListVersionsByFunction(name string) ([]*FunctionVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	result := make([]*FunctionVersion, 0, len(b.versions[name])+1)

	// $LATEST is always first.
	result = append(result, fnToVersion(fn))
	result = append(result, b.versions[name]...)

	return result, nil
}

// CreateAlias creates a new alias for a Lambda function pointing to a version.
func (b *InMemoryBackend) CreateAlias(name string, input *CreateAliasInput) (*FunctionAlias, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	// Validate the target version: must be "$LATEST" or an existing published version.
	if input.FunctionVersion != versionLatest {
		found := false

		for _, v := range b.versions[name] {
			if v.Version == input.FunctionVersion {
				found = true

				break
			}
		}

		if !found {
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
	b.mu.RLock()
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

// ListAliases returns all aliases for a function sorted by name.
func (b *InMemoryBackend) ListAliases(name string) ([]*FunctionAlias, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	aliasMap := b.aliases[name]
	result := make([]*FunctionAlias, 0, len(aliasMap))

	for _, a := range aliasMap {
		result = append(result, a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// UpdateAlias updates an existing alias.
func (b *InMemoryBackend) UpdateAlias(name, aliasName string, input *UpdateAliasInput) (*FunctionAlias, error) {
	b.mu.Lock()
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
	b.mu.Lock()
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
	b.mu.RLock()

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
	return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s:%s", region, accountID, functionName, version)
}

// buildAliasARN constructs a Lambda function alias ARN.
func buildAliasARN(region, accountID, functionName, aliasName string) string {
	return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s:%s", region, accountID, functionName, aliasName)
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
	_ = isError

	b.pushInvocationLog(fn.FunctionName, payload, result)

	return result, http.StatusOK, nil
}

// pushInvocationLog writes a minimal invocation log entry to CloudWatch Logs when a backend is set.
func (b *InMemoryBackend) pushInvocationLog(functionName string, _ []byte, result []byte) {
	b.mu.RLock()
	cwl := b.cwLogs
	b.mu.RUnlock()

	if cwl == nil {
		return
	}

	groupName := "/aws/lambda/" + functionName
	streamName := time.Now().UTC().Format("2006/01/02") + "/[$LATEST]" + uuid.New().String()[:8]

	if err := cwl.EnsureLogGroupAndStream(groupName, streamName); err != nil {
		b.logger.Warn("pushInvocationLog: failed to ensure log group/stream",
			"function", functionName, "error", err)

		return
	}

	messages := []string{
		"END RequestId: " + uuid.New().String(),
		"REPORT response length: " + strconv.Itoa(len(result)),
	}

	if err := cwl.PutLogLines(groupName, streamName, messages); err != nil {
		b.logger.Warn("pushInvocationLog: failed to put log lines",
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
		return os.MkdirAll(destPath, f.Mode()) // #nosec G703
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o750); err != nil { // #nosec G703
		return fmt.Errorf("mkdir %q: %w", filepath.Dir(destPath), err)
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

	spec := container.Spec{
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

	spec := container.Spec{
		Image:  baseImage,
		Name:   fmt.Sprintf("gopherstack-lambda-%s-%s", fn.FunctionName, uuid.New().String()[:8]),
		Env:    env,
		Mounts: []string{zipDir + ":/var/task:ro"},
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
