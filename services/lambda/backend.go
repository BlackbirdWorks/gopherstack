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
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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
	// ErrCodeSigningConfigNotFound is returned when a function has no code signing config associated.
	ErrCodeSigningConfigNotFound = errors.New("CodeSigningConfigNotFoundException")
	// ErrNoPolicyFound is returned when a function has no resource-based policy (no permissions).
	ErrNoPolicyFound = errors.New("ResourceNotFoundException")
)

// versionLatest is the sentinel qualifier for the live function configuration.
const versionLatest = "$LATEST"

// lambdaDefaultMaxItems is the default page size for ListFunctions.
const lambdaDefaultMaxItems = 50

// globalRand is used for non-security random choices (e.g. weighted alias routing).
//
//nolint:gochecknoglobals // intentional package-level RNG for weighted routing
var globalRand = mrand.New(mrand.NewPCG(0, 1)) //nolint:gosec // non-security use

// defaultEphemeralStorageSize is the default /tmp storage size in MB for Lambda functions.
const defaultEphemeralStorageSize int32 = 512

// minEphemeralStorageSize is the minimum /tmp size in MB accepted by AWS Lambda.
const minEphemeralStorageSize int32 = 512

// maxEphemeralStorageSize is the maximum /tmp size in MB accepted by AWS Lambda.
const maxEphemeralStorageSize int32 = 10240

// maxCleanupConcurrency is the maximum number of concurrent runtime cleanup goroutines.
const maxCleanupConcurrency = 64

// maxConcurrentInvocationLogs bounds the number of in-flight async log delivery
// goroutines spawned by dispatchInvocationLog. When saturated, additional log
// emissions are dropped (with a warn log) rather than queued, preventing
// unbounded goroutine growth under high invocation throughput when the
// CloudWatch Logs backend is slow or unavailable.
const maxConcurrentInvocationLogs = 256

const extractParentDirPerm = 0o750

// invocationChainKeyType is the context key type used to track the current Lambda invocation chain.
// Its value is a []string of function names currently in the call stack.
type invocationChainKeyType struct{}

// withInvocationChain returns a context carrying the updated invocation chain.
// Uses a []string instead of a map to avoid per-call heap allocation on the hot invocation path.
// make+copy ensures the new slice never shares backing array with existing.
func withInvocationChain(ctx context.Context, functionName string) context.Context {
	existing, _ := ctx.Value(invocationChainKeyType{}).([]string)
	next := make([]string, len(existing)+1)
	copy(next, existing)
	next[len(existing)] = functionName

	return context.WithValue(ctx, invocationChainKeyType{}, next)
}

// invocationChainContains reports whether functionName is already in the call chain.
func invocationChainContains(ctx context.Context, functionName string) bool {
	chain, _ := ctx.Value(invocationChainKeyType{}).([]string)

	return slices.Contains(chain, functionName)
}

// StorageBackend defines the interface for Lambda backend operations.
type StorageBackend interface {
	CreateFunction(fn *FunctionConfiguration) error
	GetFunction(name string) (*FunctionConfiguration, error)
	ListFunctions(marker string, maxItems int) page.Page[*FunctionConfiguration]
	DeleteFunction(name string) error
	UpdateFunction(fn *FunctionConfiguration) error
	InvokeFunction(
		ctx context.Context,
		name string,
		invocationType InvocationType,
		payload []byte,
	) ([]byte, int, error)
	Purge(ctx context.Context, cutoff time.Time)
}

// QualifierInvoker is an optional extension of StorageBackend that supports qualified invocations.
// Backends implement this to support ?Qualifier= on Invoke (alias or version qualifier).
type QualifierInvoker interface {
	InvokeFunctionWithQualifier(
		ctx context.Context, name, qualifier, clientContext, logType string, invocationType InvocationType, payload []byte,
	) ([]byte, string, int, error)
}

// QualifierResolver is an optional extension of StorageBackend that resolves a
// qualifier (version number or alias name) to a function configuration for
// GetFunction/GetFunctionConfiguration. Backends implement this to support
// ?Qualifier= on the read paths.
type QualifierResolver interface {
	GetFunctionByQualifier(name, qualifier string) (*FunctionConfiguration, error)
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
//
// Locking discipline:
//   - lastUsed is read and written while b.mu is held (write lock); it does not require rt.mu.
//   - started, startErr, srv, port, zipDir, layerDirs, and containerID are protected by rt.mu.
//     They are set once during startup and read afterwards without b.mu held.
type functionRuntime struct {
	lastUsed    time.Time
	startErr    error
	srv         *runtimeServer
	mu          *lockmetrics.RWMutex
	zipDir      string
	containerID string
	layerDirs   []string
	port        int
	started     bool
}

// functionURLServer holds a running HTTP listener for a Lambda function URL.
type functionURLServer struct {
	listener net.Listener
	server   *http.Server
	port     int
}

// InMemoryBackend is a concurrency-safe in-memory Lambda backend.
type InMemoryBackend struct {
	cwLogs                   CWLogsBackend
	s3Fetcher                S3CodeFetcher
	docker                   container.Runtime
	dnsRegistrar             DNSRegistrar
	ctx                      context.Context
	logSem                   chan struct{}
	fisFaults                map[string]*FISInvocationFault
	versionCounters          map[string]int
	functions                map[string]*FunctionConfiguration
	functionURLServers       map[string]*functionURLServer
	functionURLConfigs       map[string]*FunctionURLConfig
	versions                 map[string][]*FunctionVersion
	eventInvokeConfigs       map[string]*FunctionEventInvokeConfig
	functionConcurrencies    map[string]int
	kinesisPoller            *EventSourcePoller
	pollerCancel             context.CancelFunc
	provisionedConcurrencies map[string]map[string]*ProvisionedConcurrencyConfig
	layers                   map[string][]*LayerVersion
	eventSourceMappings      map[string]*EventSourceMapping
	esmByFunctionARN         map[string]map[string]struct{}
	versionIndex             map[string]map[string]*FunctionVersion
	cleanupSem               chan struct{}
	layerVersionCounters     map[string]int64
	layerPolicies            map[string]map[int64]map[string]*LayerVersionStatement
	aliases                  map[string]map[string]*FunctionAlias
	permissions              map[string]map[string]*FunctionPermission
	codeSigningConfigs       map[string]*CodeSigningConfig
	fnCodeSigningConfigs     map[string]string
	capacityProviders        map[string]*CapacityProvider
	runtimeManagementConfigs map[string]*RuntimeManagementConfig
	functionRecursionConfigs map[string]*FunctionRecursionConfig
	functionScalingConfigs   map[string]*FunctionScalingConfig
	durableExecs             *durableExecutionStore
	asyncEnqueueWaiters      chan struct{}
	shutdown                 chan struct{}
	mu                       *lockmetrics.RWMutex
	portAlloc                *portalloc.Allocator
	runtimes                 map[string]*functionRuntime
	activeConcurrencies      map[string]int
	accountID                string
	region                   string
	settings                 Settings
	asyncWG                  sync.WaitGroup
	cscIDCounter             int
	shutdownOnce             sync.Once
}

// NewInMemoryBackend creates a new Lambda in-memory backend with a background service context.
func NewInMemoryBackend(
	dockerClient container.Runtime,
	portAlloc *portalloc.Allocator,
	settings Settings,
	accountID, region string,
) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), dockerClient, portAlloc, settings, accountID, region)
}

// NewInMemoryBackendWithContext creates a new Lambda in-memory backend whose background
// goroutines are bounded by svcCtx. If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	dockerClient container.Runtime,
	portAlloc *portalloc.Allocator,
	settings Settings,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	return &InMemoryBackend{
		functions:                make(map[string]*FunctionConfiguration),
		runtimes:                 make(map[string]*functionRuntime),
		eventSourceMappings:      make(map[string]*EventSourceMapping),
		esmByFunctionARN:         make(map[string]map[string]struct{}),
		versionIndex:             make(map[string]map[string]*FunctionVersion),
		cleanupSem:               make(chan struct{}, maxCleanupConcurrency),
		logSem:                   make(chan struct{}, maxConcurrentInvocationLogs),
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
		permissions:              make(map[string]map[string]*FunctionPermission),
		codeSigningConfigs:       make(map[string]*CodeSigningConfig),
		fnCodeSigningConfigs:     make(map[string]string),
		capacityProviders:        make(map[string]*CapacityProvider),
		runtimeManagementConfigs: make(map[string]*RuntimeManagementConfig),
		functionRecursionConfigs: make(map[string]*FunctionRecursionConfig),
		functionScalingConfigs:   make(map[string]*FunctionScalingConfig),
		durableExecs:             newDurableExecutionStore(),
		asyncEnqueueWaiters:      make(chan struct{}, maxAsyncEnqueueWaiters),
		shutdown:                 make(chan struct{}),
		docker:                   dockerClient,
		portAlloc:                portAlloc,
		settings:                 settings,
		accountID:                accountID,
		region:                   region,
		ctx:                      svcCtx,
		mu:                       lockmetrics.New("lambda"),
	}
}

// Close shuts down all active function URL servers and runtime API servers.
// It is safe to call concurrently and should be called when the backend is no longer needed.
func (b *InMemoryBackend) Close(ctx context.Context) {
	// Signal in-flight async (Event) invocation goroutines to stop waiting on a
	// container response so they exit instead of lingering until their per-event
	// timeout. shutdownOnce keeps Close idempotent and safe to call concurrently.
	b.shutdownOnce.Do(func() {
		close(b.shutdown)
	})

	b.mu.Lock("Close")

	urlServers := make([]*functionURLServer, 0, len(b.functionURLServers))
	for _, srv := range b.functionURLServers {
		urlServers = append(urlServers, srv)
	}

	rts := make([]*functionRuntime, 0, len(b.runtimes))
	for _, rt := range b.runtimes {
		rts = append(rts, rt)
	}

	cancel := b.pollerCancel
	b.pollerCancel = nil

	b.mu.Unlock()

	// Stop the event-source poller goroutine if it was started.
	if cancel != nil {
		cancel()
	}

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
			b.cleanupRuntime(ctx, rt)
		})
	}

	wg.Wait()

	// Wait for async invocation goroutines (unblocked by closing b.shutdown above)
	// to finish so no background work outlives the backend.
	b.asyncWG.Wait()
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
// It stores a cancel function so Close() can stop the poller gracefully.
func (b *InMemoryBackend) StartKinesisPoller(ctx context.Context) {
	b.mu.Lock("StartKinesisPoller")
	p := b.kinesisPoller
	b.mu.Unlock()

	if p == nil {
		return
	}

	pollerCtx, cancel := context.WithCancel(ctx)

	b.mu.Lock("StartKinesisPoller.storeCancel")
	b.pollerCancel = cancel
	b.mu.Unlock()

	p.Start(pollerCtx)
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

// esmFunctionName normalizes a function reference (bare name or full function ARN)
// to the bare function name used for event-source-mapping indexing.
func esmFunctionName(functionName string) string {
	if strings.HasPrefix(functionName, "arn:aws:lambda:") {
		parts := strings.Split(functionName, ":")

		return parts[len(parts)-1]
	}

	return functionName
}

// CreateEventSourceMapping creates a new event source mapping.
func (b *InMemoryBackend) CreateEventSourceMapping(
	input *CreateEventSourceMappingInput,
) (*EventSourceMapping, error) {
	b.mu.Lock("CreateEventSourceMapping")
	defer b.mu.Unlock()

	if input.EventSourceARN == "" {
		return nil, fmt.Errorf("%w: EventSourceARN must not be empty", ErrInvalidParameterValue)
	}

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

	// The function may be supplied as a bare name or a full function ARN. Normalize
	// to the bare name so the stored index key matches lookups by name.
	fnARN := arn.Build(
		"lambda",
		b.region,
		b.accountID,
		"function:"+esmFunctionName(input.FunctionName),
	)

	m := &EventSourceMapping{
		UUID:                                id,
		EventSourceARN:                      input.EventSourceARN,
		FunctionARN:                         fnARN,
		State:                               state,
		BatchSize:                           batchSize,
		StartingPosition:                    startingPosition,
		LastProcessingResult:                "No records processed",
		LastModified:                        time.Now(),
		FilterCriteria:                      input.FilterCriteria,
		DestinationConfig:                   input.DestinationConfig,
		AmazonManagedKafkaEventSourceConfig: input.AmazonManagedKafkaEventSourceConfig,
		SelfManagedKafkaEventSourceConfig:   input.SelfManagedKafkaEventSourceConfig,
		SelfManagedEventSource:              input.SelfManagedEventSource,
		DocumentDBEventSourceConfig:         input.DocumentDBEventSourceConfig,
		SourceAccessConfigurations:          input.SourceAccessConfigurations,
		Topics:                              input.Topics,
		Queues:                              input.Queues,
		MaximumBatchingWindowInSeconds:      input.MaximumBatchingWindowInSeconds,
		TumblingWindowInSeconds:             input.TumblingWindowInSeconds,
		MaximumRecordAgeInSeconds:           input.MaximumRecordAgeInSeconds,
		MaximumRetryAttempts:                input.MaximumRetryAttempts,
		ParallelizationFactor:               input.ParallelizationFactor,
		BisectBatchOnFunctionError:          input.BisectBatchOnFunctionError,
		FunctionResponseTypes:               input.FunctionResponseTypes,
	}

	b.eventSourceMappings[id] = m

	if b.esmByFunctionARN[fnARN] == nil {
		b.esmByFunctionARN[fnARN] = make(map[string]struct{})
	}
	b.esmByFunctionARN[fnARN][id] = struct{}{}

	if input.Enabled && b.kinesisPoller != nil {
		b.kinesisPoller.Notify()
	}

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
	functionName, eventSourceARN, marker string,
	maxItems int,
) page.Page[*EventSourceMapping] {
	b.mu.RLock("ListEventSourceMappings")
	defer b.mu.RUnlock()

	var result []*EventSourceMapping

	if functionName != "" {
		fnARN := arn.Build(
			"lambda",
			b.region,
			b.accountID,
			"function:"+esmFunctionName(functionName),
		)
		ids := b.esmByFunctionARN[fnARN]
		result = make([]*EventSourceMapping, 0, len(ids))
		for id := range ids {
			if m, ok := b.eventSourceMappings[id]; ok {
				result = append(result, m)
			}
		}
	} else {
		result = make([]*EventSourceMapping, 0, len(b.eventSourceMappings))
		for _, m := range b.eventSourceMappings {
			result = append(result, m)
		}
	}

	// Apply optional EventSourceArn filter.
	if eventSourceARN != "" {
		filtered := result[:0]
		for _, m := range result {
			if m.EventSourceARN == eventSourceARN {
				filtered = append(filtered, m)
			}
		}
		result = filtered
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
	if ids := b.esmByFunctionARN[m.FunctionARN]; ids != nil {
		delete(ids, id)
		if len(ids) == 0 {
			delete(b.esmByFunctionARN, m.FunctionARN)
		}
	}
	if b.kinesisPoller != nil {
		b.kinesisPoller.RemoveMapping(id)
	}

	return m, nil
}

// functionURLHostname returns the synthetic DNS hostname for a function URL.
func (b *InMemoryBackend) functionURLHostname(functionName string) string {
	return fmt.Sprintf("%s.lambda-url.%s.on.aws", functionName, b.region)
}

// CreateFunctionURLConfig creates a function URL endpoint for the given function.
// It allocates a port, starts an HTTP listener, registers DNS, and returns the config.
// The mutex is released before port allocation and listener startup (IO) to avoid
// holding the lock during potentially slow system calls.
func (b *InMemoryBackend) CreateFunctionURLConfig(
	ctx context.Context,
	functionName, authType string,
	cors *FunctionURLCors,
	invokeMode string,
) (*FunctionURLConfig, error) {
	b.mu.Lock("CreateFunctionURLConfig.check")

	if _, ok := b.functions[functionName]; !ok {
		b.mu.Unlock()

		return nil, ErrFunctionNotFound
	}

	if _, exists := b.functionURLConfigs[functionName]; exists {
		b.mu.Unlock()

		return nil, ErrFunctionAlreadyExists
	}

	b.mu.Unlock()

	// Allocate port and start listener outside the lock (IO).
	urlStr, startErr := b.allocateAndStartURLServerUnlocked(ctx, functionName)
	if startErr != nil {
		return nil, startErr
	}

	if invokeMode == "" {
		invokeMode = "BUFFERED"
	}

	now := time.Now().UTC().Format(time.RFC3339)
	cfg := &FunctionURLConfig{
		FunctionArn:      buildURLARN(b.region, b.accountID, functionName),
		FunctionURL:      urlStr,
		AuthType:         authType,
		InvokeMode:       invokeMode,
		CreationTime:     now,
		LastModifiedTime: now,
		Cors:             cors,
	}

	// Re-acquire the lock to commit the config. Check for a concurrent winner.
	b.mu.Lock("CreateFunctionURLConfig.commit")
	defer b.mu.Unlock()

	if _, exists := b.functionURLConfigs[functionName]; exists {
		// Another goroutine won the race. Our server was already committed to
		// b.functionURLServers by allocateAndStartURLServerUnlocked; remove it
		// under the lock and schedule shutdown outside.
		ourSrv := b.functionURLServers[functionName]
		if ourSrv != nil && ourSrv.port != 0 {
			delete(b.functionURLServers, functionName)

			go func(s *functionURLServer) {
				shutdownCtx, cancel := context.WithTimeout(
					context.WithoutCancel(ctx),
					containerShutdownTimeout,
				)
				defer cancel()
				_ = s.server.Shutdown(shutdownCtx)

				if b.portAlloc != nil {
					_ = b.portAlloc.Release(s.port)
				}
			}(ourSrv)
		}

		return nil, ErrFunctionAlreadyExists
	}

	b.functionURLConfigs[functionName] = cfg

	return cfg, nil
}

// allocateAndStartURLServerUnlocked allocates a port and starts the HTTP listener
// without holding b.mu. The caller must commit srv to b.functionURLServers under the lock.
func (b *InMemoryBackend) allocateAndStartURLServerUnlocked(
	ctx context.Context,
	functionName string,
) (string, error) {
	urlStr, srv, err := b.doAllocateAndStart(ctx, functionName)
	if err != nil {
		return "", err
	}

	if srv != nil {
		b.mu.Lock("allocateAndStartURLServerUnlocked.commit")
		b.functionURLServers[functionName] = srv
		b.mu.Unlock()
	}

	return urlStr, nil
}

// doAllocateAndStart is the core port-alloc + listener startup logic used by
// allocateAndStartURLServerUnlocked.
func (b *InMemoryBackend) doAllocateAndStart(
	ctx context.Context,
	functionName string,
) (string, *functionURLServer, error) {
	if b.portAlloc == nil {
		return fmt.Sprintf("http://localhost/%s/", functionName), nil, nil
	}

	port, allocErr := b.portAlloc.Acquire("lambda-url:" + functionName)
	if allocErr != nil {
		return "", nil, fmt.Errorf("%w: port allocation failed: %w", ErrLambdaUnavailable, allocErr)
	}

	srv, listenErr := b.startFunctionURLServer(ctx, functionName, port)
	if listenErr != nil {
		_ = b.portAlloc.Release(port)

		return "", nil, fmt.Errorf(
			"%w: failed to start URL listener: %w",
			ErrLambdaUnavailable,
			listenErr,
		)
	}

	hostname := b.functionURLHostname(functionName)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(hostname)

		return "http://" + net.JoinHostPort(hostname, strconv.Itoa(port)) + "/", srv, nil
	}

	// No DNS registered; use loopback so the URL is immediately reachable.
	return "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) + "/", srv, nil
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
		shutdownCtx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
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
func (b *InMemoryBackend) startFunctionURLServer(
	ctx context.Context,
	functionName string,
	port int,
) (*functionURLServer, error) {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", b.buildFunctionURLHandler(functionName))

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: functionURLReadHeaderTimeout,
	}

	log := logger.Load(ctx)

	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil &&
			!errors.Is(serveErr, http.ErrServerClosed) {
			log.WarnContext(
				ctx,
				"lambda: function URL server stopped",
				"function",
				functionName,
				"error",
				serveErr,
			)
		}
	}()

	return &functionURLServer{listener: ln, server: srv, port: port}, nil
}

// lambdaURLHTTPContext contains the HTTP-specific fields of the Lambda URL request context.
type lambdaURLHTTPContext struct {
	Method   string `json:"method"`
	Path     string `json:"path"`
	Protocol string `json:"protocol"`
	SourceIP string `json:"sourceIp"`
}

// lambdaURLRequestContext contains request context metadata for Lambda Function URL events.
type lambdaURLRequestContext struct {
	HTTP       lambdaURLHTTPContext `json:"http"`
	RequestID  string               `json:"requestId"`
	Stage      string               `json:"stage"`
	DomainName string               `json:"domainName"`
	Time       string               `json:"time"`
	TimeEpoch  int64                `json:"timeEpoch"`
}

// lambdaURLEvent is a simplified Lambda Function URL (HTTP API v2) event.
type lambdaURLEvent struct {
	Headers         map[string]string       `json:"headers"`
	RawPath         string                  `json:"rawPath"`
	RawQueryString  string                  `json:"rawQueryString"`
	Body            string                  `json:"body,omitempty"`
	Version         string                  `json:"version"`
	RouteKey        string                  `json:"routeKey"`
	RequestContext  lambdaURLRequestContext `json:"requestContext"`
	IsBase64Encoded bool                    `json:"isBase64Encoded"`
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

		result, _, invokeErr := b.InvokeFunction(
			r.Context(),
			functionName,
			InvocationTypeRequestResponse,
			payload,
		)
		if invokeErr != nil {
			http.Error(w, invokeErr.Error(), http.StatusInternalServerError)

			return
		}

		writeFunctionURLResponse(w, result)
	}
}

// maxFunctionURLBodyBytes caps the request body for Lambda Function URL invokes.
// AWS limits the synchronous Lambda invoke payload to 6 MiB; bodies larger than
// that cannot be forwarded anyway, so cap reads to prevent unbounded memory use.
const maxFunctionURLBodyBytes = 6 * 1024 * 1024

// buildURLEventPayload converts an HTTP request to a Lambda Function URL event payload.
func (b *InMemoryBackend) buildURLEventPayload(r *http.Request) ([]byte, error) {
	var bodyBytes []byte

	if r.Body != nil {
		var readErr error

		bodyBytes, readErr = io.ReadAll(http.MaxBytesReader(nil, r.Body, maxFunctionURLBodyBytes))
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
		RequestContext: lambdaURLRequestContext{
			HTTP: lambdaURLHTTPContext{
				Method:   r.Method,
				Path:     r.URL.Path,
				Protocol: r.Proto,
				SourceIP: func() string {
					ip, _, _ := net.SplitHostPort(r.RemoteAddr)
					if ip == "" {
						return r.RemoteAddr
					}

					return ip
				}(),
			},
			RequestID:  uuid.New().String(),
			Stage:      "$default",
			DomainName: r.Host,
			TimeEpoch:  time.Now().UTC().UnixMilli(),
			Time:       time.Now().UTC().Format(time.RFC3339Nano),
		},
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
	_, _ = w.Write(result) //nolint:gosec // function output is raw Lambda payload passthrough
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
// validateEphemeralStorage normalises fn.EphemeralStorage, setting the default when nil and
// returning an error when the supplied size is outside the allowed range.
func validateEphemeralStorage(fn *FunctionConfiguration) error {
	if fn.EphemeralStorage == nil {
		fn.EphemeralStorage = &EphemeralStorageConfig{Size: defaultEphemeralStorageSize}

		return nil
	}

	if fn.EphemeralStorage.Size < minEphemeralStorageSize ||
		fn.EphemeralStorage.Size > maxEphemeralStorageSize {
		return fmt.Errorf(
			"%w: EphemeralStorage.Size must be between %d and %d MB",
			ErrInvalidParameterValue, minEphemeralStorageSize, maxEphemeralStorageSize,
		)
	}

	return nil
}

func (b *InMemoryBackend) CreateFunction(fn *FunctionConfiguration) error {
	// AWS rejects function names longer than 64 chars (function name only,
	// not including any qualifier or ARN).
	const maxFunctionNameLength = 64
	if l := len(fn.FunctionName); l == 0 || l > maxFunctionNameLength {
		return fmt.Errorf("%w: FunctionName must be 1-%d characters",
			ErrInvalidParameterValue, maxFunctionNameLength)
	}

	b.mu.Lock("CreateFunction")
	defer b.mu.Unlock()

	if _, exists := b.functions[fn.FunctionName]; exists {
		return ErrFunctionAlreadyExists
	}

	if fn.MemorySize != 0 &&
		(fn.MemorySize < 128 || fn.MemorySize > 10240 || fn.MemorySize%64 != 0) {
		return fmt.Errorf(
			"%w: MemorySize must be between 128 and 10240 and divisible by 64",
			ErrInvalidParameterValue,
		)
	}

	if fn.Tags == nil {
		fn.Tags = make(map[string]string)
	}

	if len(fn.Architectures) == 0 {
		fn.Architectures = []string{"x86_64"}
	}

	if err := validateEphemeralStorage(fn); err != nil {
		return err
	}

	if fn.TracingConfig == nil {
		fn.TracingConfig = &TracingConfig{Mode: "PassThrough"}
	}

	if fn.LoggingConfig == nil {
		fn.LoggingConfig = &LoggingConfig{
			LogFormat: "Text",
			LogGroup:  "/aws/lambda/" + fn.FunctionName,
		}
	}

	if fn.PackageType == "" {
		if fn.ImageURI != "" {
			fn.PackageType = "Image"
		} else {
			fn.PackageType = "Zip"
		}
	}

	// AWS Lambda always sets Version to "$LATEST" for the live (mutable) code.
	// Published versions have numbered versions (1, 2, …) in separate records.
	fn.Version = versionLatest

	b.functions[fn.FunctionName] = fn

	return nil
}

// GetFunction retrieves a Lambda function configuration by name.
func (b *InMemoryBackend) GetFunction(name string) (*FunctionConfiguration, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	name = extractFunctionName(name)
	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	return fn, nil
}

// GetFunctionByQualifier returns the configuration for a specific qualifier
// (version number, alias name, "$LATEST", or empty for $LATEST).
//
// Matching real AWS GetFunction/GetFunctionConfiguration behaviour:
//   - "" or "$LATEST" returns the live function configuration unchanged.
//   - A numeric version returns the immutable published snapshot, with
//     FunctionArn suffixed ":<version>" and Version set to that number.
//   - An alias name resolves to the alias's primary target version, but the
//     returned FunctionArn is suffixed with the alias name (":<alias>") — AWS
//     echoes the qualifier you asked for in the ARN while reporting the
//     resolved Version. Weighted routing config does NOT affect GetFunction.
//
// Returns ErrFunctionNotFound when the function does not exist and
// ErrVersionNotFound when the qualifier resolves to no known version/alias.
func (b *InMemoryBackend) GetFunctionByQualifier(
	name, qualifier string,
) (*FunctionConfiguration, error) {
	if qualifier == "" || qualifier == versionLatest {
		return b.GetFunction(name)
	}

	b.mu.RLock("GetFunctionByQualifier")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	// Resolve an alias qualifier to its primary target version, but remember the
	// alias name so the returned ARN carries the alias suffix (AWS behaviour).
	resolved := qualifier
	aliasSuffix := ""

	if aliasMap := b.aliases[name]; aliasMap != nil {
		if alias, ok := aliasMap[qualifier]; ok {
			resolved = alias.FunctionVersion
			aliasSuffix = qualifier
		}
	}

	if resolved == versionLatest {
		// Alias points at $LATEST: return the live config but with the alias ARN.
		fn := b.functions[name]
		cfg := versionToConfig(fnToVersion(fn))
		cfg.FunctionArn = buildVersionARN(b.region, b.accountID, name, aliasSuffix)

		return cfg, nil
	}

	vMap := b.versionIndex[name]
	if vMap == nil {
		return nil, ErrVersionNotFound
	}

	v, ok := vMap[resolved]
	if !ok {
		return nil, ErrVersionNotFound
	}

	cfg := versionToConfig(v)

	// For an alias qualifier, AWS returns the ARN with the alias suffix while
	// the Version field reports the resolved numeric version.
	if aliasSuffix != "" {
		cfg.FunctionArn = buildVersionARN(b.region, b.accountID, name, aliasSuffix)
	}

	return cfg, nil
}

// ListFunctions returns a page of Lambda function configurations sorted by name.
func (b *InMemoryBackend) ListFunctions(
	marker string,
	maxItems int,
) page.Page[*FunctionConfiguration] {
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

// ListFunctionsAll returns a page of all published versions across all functions,
// sorted by FunctionName then numerically by version. This is the response for
// ListFunctions?FunctionVersion=ALL.
func (b *InMemoryBackend) ListFunctionsAll(
	marker string,
	maxItems int,
) page.Page[*FunctionConfiguration] {
	b.mu.RLock("ListFunctionsAll")
	defer b.mu.RUnlock()

	var fns []*FunctionConfiguration

	// Include $LATEST for each function.
	for _, fn := range b.functions {
		fns = append(fns, fn)
	}

	// Include all published versions.
	for name, vMap := range b.versionIndex {
		for _, v := range vMap {
			cfg := versionToConfig(v)
			cfg.FunctionName = name
			fns = append(fns, cfg)
		}
	}

	// Sort by FunctionName, then by Version (numerically: $LATEST sorts last).
	sort.Slice(fns, func(i, j int) bool {
		if fns[i].FunctionName != fns[j].FunctionName {
			return fns[i].FunctionName < fns[j].FunctionName
		}
		// $LATEST > any number
		if fns[i].Version == versionLatest {
			return false
		}
		if fns[j].Version == versionLatest {
			return true
		}
		// Both are version numbers — compare numerically.
		ni, _ := strconv.Atoi(fns[i].Version)
		nj, _ := strconv.Atoi(fns[j].Version)

		return ni < nj
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

	// Cascade-delete event source mappings for this function.
	fnARN := arn.Build("lambda", b.region, b.accountID, "function:"+name)
	var esmIDsToRemove []string
	if ids, ok := b.esmByFunctionARN[fnARN]; ok {
		for id := range ids {
			delete(b.eventSourceMappings, id)
			esmIDsToRemove = append(esmIDsToRemove, id)
		}
		delete(b.esmByFunctionARN, fnARN)
	}

	b.mu.Unlock()

	for _, id := range esmIDsToRemove {
		if b.kinesisPoller != nil {
			b.kinesisPoller.RemoveMapping(id)
		}
	}

	// Clean up runtime resources; must not hold b.mu while stopping the server.
	if rt != nil {
		shutdownCtx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
		defer cancel()
		b.cleanupRuntime(shutdownCtx, rt)
	}

	return nil
}

// UpdateFunction replaces a Lambda function's configuration.
// Any running container is evicted so the next invocation picks up the new code/config.
func (b *InMemoryBackend) UpdateFunction(fn *FunctionConfiguration) error {
	b.mu.Lock("UpdateFunction")

	if _, ok := b.functions[fn.FunctionName]; !ok {
		b.mu.Unlock()

		return ErrFunctionNotFound
	}

	b.functions[fn.FunctionName] = fn

	// Evict the running runtime so the next invocation gets a fresh container with the
	// updated code or configuration (mirrors AWS/LocalStack behaviour).
	rt := b.runtimes[fn.FunctionName]
	if rt != nil {
		delete(b.runtimes, fn.FunctionName)
	}

	b.mu.Unlock()

	// Clean up the old container asynchronously — we must not hold b.mu while stopping.
	// rt is passed as a parameter to make the capture explicit and safe against future refactoring.
	if rt != nil {
		// Capture sem under RLock so that a concurrent Reset() cannot replace b.cleanupSem
		// between the send and the goroutine's deferred release.
		b.mu.RLock("cleanupSem.updateFn")
		sem := b.cleanupSem
		b.mu.RUnlock()

		select {
		case sem <- struct{}{}:
			go func(evicted *functionRuntime) { // #nosec G118 -- intentional detached context for background cleanup
				defer func() { <-sem }()
				shutdownCtx, cancel := context.WithTimeout(
					b.ctx,
					containerShutdownTimeout,
				)
				defer cancel()
				b.cleanupRuntime(shutdownCtx, evicted)
			}(
				rt,
			)
		default:
			// Already at max concurrent cleanups; run inline (rare, only under extreme load).
			shutdownCtx, cancel := context.WithTimeout(
				b.ctx,
				containerShutdownTimeout,
			)
			defer cancel()
			b.cleanupRuntime(shutdownCtx, rt)
		}
	}

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
		FunctionName:      fn.FunctionName,
		FunctionArn:       buildVersionARN(b.region, b.accountID, fn.FunctionName, versionNum),
		Description:       description,
		Version:           versionNum,
		Runtime:           fn.Runtime,
		Handler:           fn.Handler,
		Role:              fn.Role,
		MemorySize:        fn.MemorySize,
		Timeout:           fn.Timeout,
		PackageType:       fn.PackageType,
		ImageURI:          fn.ImageURI,
		ImageConfig:       fn.ImageConfig,
		VpcConfig:         fn.VpcConfig,
		TracingConfig:     fn.TracingConfig,
		FileSystemConfigs: fn.FileSystemConfigs,
		DeadLetterConfig:  fn.DeadLetterConfig,
		Environment:       deepCopyEnvironment(fn.Environment),
		Layers:            deepCopyFunctionLayers(fn.Layers),
		CodeSize:          fn.CodeSize,
		RevisionID:        uuid.New().String(),
		CreatedAt:         fn.LastModified,
		State:             fn.State,
		SnapStart:         copySnapStart(fn.SnapStart),
	}

	b.versions[name] = append(b.versions[name], ver)

	if b.versionIndex[name] == nil {
		b.versionIndex[name] = make(map[string]*FunctionVersion)
	}
	b.versionIndex[name][versionNum] = ver

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

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	if vMap := b.versionIndex[name]; vMap != nil {
		if v, ok := vMap[version]; ok {
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
func (b *InMemoryBackend) CreateAlias(
	name string,
	input *CreateAliasInput,
) (*FunctionAlias, error) {
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
		RoutingConfig:   input.RoutingConfig,
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
// If functionVersion is non-empty, only aliases pointing to that version are returned.
func (b *InMemoryBackend) ListAliases(
	name, functionVersion, marker string,
	maxItems int,
) (page.Page[*FunctionAlias], error) {
	b.mu.RLock("ListAliases")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return page.Page[*FunctionAlias]{}, ErrFunctionNotFound
	}

	aliasMap := b.aliases[name]
	result := make([]*FunctionAlias, 0, len(aliasMap))

	for _, a := range aliasMap {
		if functionVersion != "" && a.FunctionVersion != functionVersion {
			continue
		}

		result = append(result, a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems), nil
}

// UpdateAlias updates an existing alias.
func (b *InMemoryBackend) UpdateAlias(
	name, aliasName string,
	input *UpdateAliasInput,
) (*FunctionAlias, error) {
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

	if input.RoutingConfig != nil {
		alias.RoutingConfig = input.RoutingConfig
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

// extractFunctionName parses an ARN and returns the function name.
func extractFunctionName(name string) string {
	if strings.Contains(name, ":function:") {
		parts := strings.Split(name, ":")
		for i, p := range parts {
			if p == "function" && i+1 < len(parts) {
				return parts[i+1]
			}
		}
	}

	return name
}

// resolveQualifier resolves a function name with an optional qualifier to a FunctionConfiguration.
// Qualifier may be a version number, alias name, or "$LATEST" (default when empty).
// Returns the resolved function config.
func (b *InMemoryBackend) resolveQualifier(name, qualifier string) (*FunctionConfiguration, error) {
	name = extractFunctionName(name)
	if qualifier == "" || qualifier == versionLatest {
		return b.GetFunction(name)
	}

	// Check if qualifier is an alias; if so, resolve to the target version string.
	// Hold a single RLock for both the alias lookup and the version search to avoid
	// TOCTOU races with concurrent alias/version updates.
	b.mu.RLock("resolveQualifier")

	if aliasMap := b.aliases[name]; aliasMap != nil {
		if alias, ok := aliasMap[qualifier]; ok {
			qualifier = selectAliasVersion(alias)
		}
	}

	// Now qualifier is a version number. Find the version snapshot.
	if vMap := b.versionIndex[name]; vMap != nil {
		if v, ok := vMap[qualifier]; ok {
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

// selectAliasVersion picks the target version for an alias invocation, respecting weighted
// routing when RoutingConfig.AdditionalVersionWeights is set.
//
// AWS routing: AdditionalVersionWeights maps a secondary version to a weight (0–1).
// A random float in [0,1) < secondaryWeight routes to the secondary version; otherwise
// the primary alias.FunctionVersion is used.
func selectAliasVersion(alias *FunctionAlias) string {
	if alias.RoutingConfig == nil || len(alias.RoutingConfig.AdditionalVersionWeights) == 0 {
		return alias.FunctionVersion
	}

	// Accumulate weights; the first bucket whose cumulative weight exceeds a random
	// value [0,1) wins. If no bucket wins (total weight < 1), the primary version is used.
	r := globalRand.Float64()
	var cumulative float64

	for version, weight := range alias.RoutingConfig.AdditionalVersionWeights {
		cumulative += weight

		if r < cumulative {
			return version
		}
	}

	return alias.FunctionVersion
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
		FunctionName:      fn.FunctionName,
		FunctionArn:       fn.FunctionArn,
		Description:       fn.Description,
		Version:           versionLatest,
		Runtime:           fn.Runtime,
		Handler:           fn.Handler,
		Role:              fn.Role,
		MemorySize:        fn.MemorySize,
		Timeout:           fn.Timeout,
		PackageType:       fn.PackageType,
		ImageURI:          fn.ImageURI,
		ImageConfig:       fn.ImageConfig,
		Environment:       fn.Environment,
		VpcConfig:         fn.VpcConfig,
		TracingConfig:     fn.TracingConfig,
		FileSystemConfigs: fn.FileSystemConfigs,
		DeadLetterConfig:  fn.DeadLetterConfig,
		Layers:            fn.Layers,
		CodeSize:          fn.CodeSize,
		RevisionID:        fn.RevisionID,
		CreatedAt:         fn.LastModified,
		State:             fn.State,
		CodeSha256:        fn.CodeSha256,
		SnapStart:         copySnapStart(fn.SnapStart),
	}
}

// copySnapStart returns a copy of the SnapStart response so version snapshots do
// not alias the live function's configuration. Returns nil for an unset config
// (field omitted from responses).
func copySnapStart(cfg *SnapStartResponse) *SnapStartResponse {
	if cfg == nil {
		return nil
	}

	dup := *cfg

	return &dup
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
		SnapStart:    v.SnapStart,
		Version:      v.Version,
	}
}

// versionToConfig builds a complete FunctionConfiguration response from an
// immutable version snapshot. Unlike versionToFn (used for the invocation hot
// path, which only needs runtime-critical fields), this preserves every
// control-plane field AWS returns from GetFunction on a published version,
// including Version, Layers, VpcConfig, TracingConfig, and the version ARN.
func versionToConfig(v *FunctionVersion) *FunctionConfiguration {
	return &FunctionConfiguration{
		FunctionName:      v.FunctionName,
		FunctionArn:       v.FunctionArn,
		Description:       v.Description,
		Runtime:           v.Runtime,
		Handler:           v.Handler,
		Role:              v.Role,
		MemorySize:        v.MemorySize,
		Timeout:           v.Timeout,
		PackageType:       v.PackageType,
		ImageURI:          v.ImageURI,
		ImageConfig:       v.ImageConfig,
		Environment:       deepCopyEnvironment(v.Environment),
		VpcConfig:         v.VpcConfig,
		TracingConfig:     v.TracingConfig,
		FileSystemConfigs: v.FileSystemConfigs,
		DeadLetterConfig:  v.DeadLetterConfig,
		Layers:            deepCopyFunctionLayers(v.Layers),
		CodeSize:          v.CodeSize,
		CodeSha256:        v.CodeSha256,
		RevisionID:        v.RevisionID,
		LastModified:      v.CreatedAt,
		State:             v.State,
		Version:           v.Version,
		SnapStart:         copySnapStart(v.SnapStart),
		// Published versions are immutable: their last-update status is always
		// Successful (AWS never reports Pending/InProgress for a numbered version).
		LastUpdateStatus: LastUpdateStatusSuccessful,
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
	result, _, statusCode, err := b.InvokeFunctionWithQualifier(ctx, name, "", "", "", invocationType, payload)

	return result, statusCode, err
}

// asyncInvocationEnqueueTimeout is the maximum time a background goroutine will wait
// to place an async (Event) invocation into the runtime queue. If the queue remains
// full after this duration the invocation is dropped with a warning log.
const asyncInvocationEnqueueTimeout = 5 * time.Minute

// maxAsyncEnqueueWaiters bounds the number of goroutines allowed to block while
// waiting for space in a runtime async invocation queue.
const maxAsyncEnqueueWaiters = 128

// checkRecursiveLoop returns an error when fn is already in the invocation chain and
// its RecursiveLoop config is set to "Deny".
func (b *InMemoryBackend) checkRecursiveLoop(ctx context.Context, functionName string) error {
	if !invocationChainContains(ctx, functionName) {
		return nil
	}

	b.mu.RLock("checkRecursiveLoop")
	rc := b.functionRecursionConfigs[functionName]
	b.mu.RUnlock()

	mode := "Terminate"
	if rc != nil {
		mode = rc.RecursiveLoop
	}

	if mode == "Deny" {
		return fmt.Errorf(
			"%w: recursive invocation detected for function %s with RecursiveLoop=Deny",
			ErrInvalidParameterValue, functionName,
		)
	}

	return nil
}

// InvokeFunctionWithQualifier invokes a Lambda function using an optional qualifier.
func (b *InMemoryBackend) InvokeFunctionWithQualifier(
	ctx context.Context,
	name, qualifier, clientContext, logType string,
	invocationType InvocationType,
	payload []byte,
) ([]byte, string, int, error) {
	fn, err := b.resolveQualifier(name, qualifier)
	if err != nil {
		return nil, "", http.StatusNotFound, err
	}

	if invocationType == InvocationTypeDryRun {
		return nil, "", http.StatusNoContent, nil
	}

	// Enforce RecursiveLoop=Deny: reject self-invocations when the function name
	// is already in the current invocation chain.
	if loopErr := b.checkRecursiveLoop(ctx, fn.FunctionName); loopErr != nil {
		return nil, "", http.StatusBadRequest, loopErr
	}

	// Propagate the invocation chain to nested Lambda calls.
	ctx = withInvocationChain(ctx, fn.FunctionName)

	// Check FIS fault injection state for this function.
	fisPayload, fisStatus, fisErr := b.applyFISFaultToInvocation(ctx, fn.FunctionName)
	if fisPayload != nil || fisErr != nil {
		return fisPayload, "", fisStatus, fisErr
	}

	// Enforce reserved concurrency limits for all invocation types.
	// Reserved concurrency of 0 blocks all invocations; non-zero limits are enforced
	// for both synchronous (RequestResponse) and asynchronous (Event) invocations.
	trackConcurrency, concErr := b.acquireConcurrencySlot(fn.FunctionName)
	if concErr != nil {
		return nil, "", http.StatusTooManyRequests, concErr
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

		return nil, "", http.StatusInternalServerError, srvErr
	}

	timeout := time.Duration(fn.Timeout) * time.Second
	if timeout <= 0 {
		timeout = defaultFunctionTimeout
	}

	if invocationType == InvocationTypeEvent {
		b.invokeEvent(ctx, fn, srv, payload, clientContext, timeout, trackConcurrency)

		return nil, "", http.StatusAccepted, nil
	}

	return b.invokeSync(ctx, fn, srv, payload, clientContext, logType, timeout)
}

func (b *InMemoryBackend) invokeSync(
	ctx context.Context,
	fn *FunctionConfiguration,
	srv *runtimeServer,
	payload []byte,
	clientContext, logType string,
	timeout time.Duration,
) ([]byte, string, int, error) {
	result, isError, reqID, invokeErr := srv.invoke(ctx, payload, clientContext, timeout)
	if invokeErr != nil {
		if errors.Is(invokeErr, ErrInvocationTimeout) {
			b.cleanupTimedOutRuntime(fn.FunctionName)
		}

		return nil, "", http.StatusInternalServerError, invokeErr
	}

	_ = isError

	b.dispatchInvocationLog(context.WithoutCancel(ctx), fn.FunctionName, payload, result)

	var logResult string
	if logType == LogTypeTail {
		logData := fmt.Sprintf(
			"START RequestId: %s Version: $LATEST\nEND RequestId: %s\n"+
				"REPORT RequestId: %s\tDuration: 1.00 ms\tBilled Duration: 1 ms\tMemory Size: 128 MB\tMax Memory Used: 64 MB\n",
			reqID, reqID, reqID,
		)
		logResult = base64.StdEncoding.EncodeToString([]byte(logData))
	}

	return result, logResult, http.StatusOK, nil
}

func (b *InMemoryBackend) invokeEvent(
	ctx context.Context,
	fn *FunctionConfiguration,
	srv *runtimeServer,
	payload []byte,
	clientContext string,
	timeout time.Duration,
	trackConcurrency bool,
) {
	inv := &pendingInvocation{
		requestID:     uuid.New().String(),
		payload:       payload,
		clientContext: clientContext,
		deadline:      time.Now().Add(timeout),
		createdAt:     time.Now(),
		result:        make(chan invocationResult, 1),
	}

	b.enqueueAsyncInvocation(ctx, srv, fn.FunctionName, inv, timeout, trackConcurrency)
}

// enqueueAsyncInvocation places inv into the runtime queue and then waits for the
// container to respond. The wait serves two purposes:
//  1. Hold the concurrency slot for the full execution duration when trackConcurrency is true.
//  2. Remove any stale srv.pending entry when a container picks up the invocation via
//     /next but never calls /response or /error (e.g., crash), preventing a memory leak.
//
// The enqueue attempts a non-blocking fast path first. If the queue is full a background
// goroutine blocks for up to asyncInvocationEnqueueTimeout before giving up.
// [context.WithoutCancel] detaches the goroutine from the caller's HTTP-request context
// so cancellation of the 202 response does not abort the background work.
func (b *InMemoryBackend) enqueueAsyncInvocation(
	ctx context.Context,
	srv *runtimeServer,
	functionName string,
	inv *pendingInvocation,
	timeout time.Duration,
	trackConcurrency bool,
) {
	log := logger.Load(ctx)

	// Fast path: try a non-blocking enqueue without spawning a goroutine.
	// Even on the fast path we still need a goroutine to clean up srv.pending on
	// container timeout, so only skip the goroutine when there's nothing to track
	// and the queue has immediate space.
	if !trackConcurrency {
		select {
		case srv.queue <- inv:
			// Invocation queued; spawn a minimal goroutine only to clean up srv.pending
			// if the container picks up the invocation but never responds.
			b.asyncWG.Go(func() {
				b.waitAndCleanPending(log, srv, inv, timeout, false, functionName)
			})

			return
		default:
		}
	}

	// Slow path: queue was full (or a slot is held); block until space is available.
	select {
	case b.asyncEnqueueWaiters <- struct{}{}:
	default:
		log.WarnContext(ctx, "lambda: async invocation dropped: enqueue waiters saturated",
			"function", functionName, "requestID", inv.requestID)

		if trackConcurrency {
			b.releaseConcurrencySlot(functionName)
		}

		return
	}

	b.asyncWG.Go(func() {
		defer func() {
			<-b.asyncEnqueueWaiters
		}()

		enqueueCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			asyncInvocationEnqueueTimeout,
		)
		defer cancel()

		select {
		case srv.queue <- inv:
			b.waitAndCleanPending(log, srv, inv, timeout, trackConcurrency, functionName)

		case <-enqueueCtx.Done():
			log.WarnContext(ctx, "lambda: async invocation dropped: queue full",
				"function", functionName, "requestID", inv.requestID)

			if trackConcurrency {
				b.releaseConcurrencySlot(functionName)
			}

		case <-b.shutdown:
			// Backend is shutting down; drop the still-queueing invocation.
			if trackConcurrency {
				b.releaseConcurrencySlot(functionName)
			}
		}
	})
}

// defaultAsyncMaxRetryAttempts is the number of automatic retries AWS Lambda performs
// for async (Event) invocations that fail with a function error. This matches the AWS default.
const defaultAsyncMaxRetryAttempts = 2

// waitAndCleanPending is the exit point for every async invocation goroutine. It runs
// the retry loop and, once all attempts are exhausted or completed, releases the
// concurrency slot if one was acquired.
func (b *InMemoryBackend) waitAndCleanPending(
	log *slog.Logger,
	srv *runtimeServer,
	inv *pendingInvocation,
	timeout time.Duration,
	trackConcurrency bool,
	functionName string,
) {
	b.runAsyncInvocationRetryLoop(log, srv, inv, timeout, functionName)

	if trackConcurrency {
		b.releaseConcurrencySlot(functionName)
	}
}

// runAsyncInvocationRetryLoop executes the async invocation and retries on function errors
// according to the function's event invoke configuration (MaximumRetryAttempts,
// MaximumEventAgeInSeconds). Default retry count mirrors AWS Lambda: 2 retries.
func (b *InMemoryBackend) runAsyncInvocationRetryLoop(
	log *slog.Logger,
	srv *runtimeServer,
	inv *pendingInvocation,
	timeout time.Duration,
	functionName string,
) {
	maxRetries, maxEventAgeDL := b.readAsyncRetryConfig(functionName, inv.createdAt)
	currentInv := inv

	for attempt := range maxRetries + 1 {
		result, ok, containerTimedOut := b.waitForAsyncResult(srv, currentInv, timeout)
		if !ok {
			// A container timeout means the process is hung; evict it so the next
			// invocation gets a fresh container, matching the synchronous timeout path.
			if containerTimedOut {
				b.cleanupTimedOutRuntime(functionName)
			}

			return
		}

		if !result.isError || attempt == maxRetries {
			if !result.isError {
				b.dispatchInvocationLog(
					b.ctx,
					functionName,
					inv.payload,
					result.payload,
				)
			} else {
				log.Warn("lambda: async invocation failed after retries",
					"function", functionName, "attempts", attempt+1)
			}

			return
		}

		newInv := scheduleAsyncRetry(b.ctx, log, srv, inv, timeout, maxEventAgeDL, attempt+1, functionName)
		if newInv == nil {
			return // retry dropped (queue full or event too old)
		}

		currentInv = newInv
	}
}

// readAsyncRetryConfig returns the effective maximum retry attempts and the event-age deadline
// for an async invocation. If no event invoke configuration exists, the AWS defaults are used
// (2 retries, no age limit).
func (b *InMemoryBackend) readAsyncRetryConfig(
	functionName string,
	createdAt time.Time,
) (int, time.Time) {
	b.mu.RLock("readAsyncRetryConfig")
	defer b.mu.RUnlock()

	maxRetries := defaultAsyncMaxRetryAttempts

	cfg, ok := b.eventInvokeConfigs[functionName]
	if !ok {
		return maxRetries, time.Time{}
	}

	if cfg.MaximumRetryAttempts != nil {
		maxRetries = *cfg.MaximumRetryAttempts
	}

	var maxEventAgeDL time.Time

	if cfg.MaximumEventAgeInSeconds != nil {
		maxEventAgeDL = createdAt.Add(time.Duration(*cfg.MaximumEventAgeInSeconds) * time.Second)
	}

	return maxRetries, maxEventAgeDL
}

// waitForAsyncResult waits for a pending invocation to receive a container response or for
// the function timeout to elapse. On timeout it removes the stale srv.pending entry that
// handleNext stored (preventing a memory leak).
// Returns:
//   - (result, true, false)  — container responded in time
//   - (zero, false, true)    — container timed out; the caller should clean up the runtime
func (b *InMemoryBackend) waitForAsyncResult(
	srv *runtimeServer,
	inv *pendingInvocation,
	timeout time.Duration,
) (invocationResult, bool, bool) {
	waitTimer := time.NewTimer(timeout + containerResponseGracePeriod)
	defer func() {
		if !waitTimer.Stop() {
			select {
			case <-waitTimer.C:
			default:
			}
		}
	}()

	select {
	case result := <-inv.result:
		return result, true, false
	case <-waitTimer.C:
		// Container timed out; remove the stale pending entry to prevent a memory leak.
		srv.pending.LoadAndDelete(inv.requestID)

		return invocationResult{}, false, true
	case <-b.shutdown:
		// Backend is shutting down; abandon the wait and remove the stale pending
		// entry. Treated like a timeout (not a container timeout) so the caller
		// stops retrying without evicting a runtime.
		srv.pending.LoadAndDelete(inv.requestID)

		return invocationResult{}, false, false
	}
}

// scheduleAsyncRetry creates a new pendingInvocation for a retry attempt and enqueues it.
// It returns the new invocation on success or nil if the event is too old or the queue
// remains full after asyncInvocationEnqueueTimeout.
func scheduleAsyncRetry(
	ctx context.Context,
	log *slog.Logger,
	srv *runtimeServer,
	original *pendingInvocation,
	timeout time.Duration,
	maxEventAgeDL time.Time,
	attempt int,
	functionName string,
) *pendingInvocation {
	if !maxEventAgeDL.IsZero() && time.Now().After(maxEventAgeDL) {
		log.WarnContext(ctx, "lambda: async retry dropped: event age exceeded",
			"function", functionName, "attempt", attempt)

		return nil
	}

	newInv := &pendingInvocation{
		requestID: uuid.New().String(),
		payload:   original.payload,
		deadline:  time.Now().Add(timeout),
		result:    make(chan invocationResult, 1),
		createdAt: original.createdAt,
	}

	ctx, cancel := context.WithTimeout(ctx, asyncInvocationEnqueueTimeout)
	defer cancel()

	select {
	case srv.queue <- newInv:
		return newInv
	case <-ctx.Done():
		log.WarnContext(ctx, "lambda: async retry dropped: queue full",
			"function", functionName, "requestID", newInv.requestID, "attempt", attempt)

		return nil
	}
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
		// No reserved concurrency limit — check scaling config MaximumConcurrency instead.
		if sc, ok := b.functionScalingConfigs[functionName]; ok && sc.MaximumConcurrency != nil {
			active := b.activeConcurrencies[functionName]
			if active >= *sc.MaximumConcurrency {
				return false, fmt.Errorf(
					"%w: scaling concurrency limit reached for function %s",
					ErrTooManyRequests,
					functionName,
				)
			}

			b.activeConcurrencies[functionName]++

			return true, nil
		}

		return false, nil
	}

	// Reserved concurrency of 0 disables all invocations regardless of type.
	if reserved == 0 {
		return false, fmt.Errorf(
			"%w: reserved concurrency is 0 for function %s",
			ErrTooManyRequests,
			functionName,
		)
	}

	active := b.activeConcurrencies[functionName]
	if active >= reserved {
		return false, fmt.Errorf(
			"%w: concurrent execution limit reached for function %s",
			ErrTooManyRequests,
			functionName,
		)
	}

	// Also enforce MaximumConcurrency from scaling config when set.
	if sc, ok := b.functionScalingConfigs[functionName]; ok && sc.MaximumConcurrency != nil {
		if active >= *sc.MaximumConcurrency {
			return false, fmt.Errorf(
				"%w: scaling concurrency limit reached for function %s",
				ErrTooManyRequests,
				functionName,
			)
		}
	}

	b.activeConcurrencies[functionName]++

	return true, nil
}

// releaseConcurrencySlot decrements the active concurrency counter for a function.
// Entries are deleted when the count reaches zero to prevent unbounded map growth.
// Must not be called with b.mu held.
func (b *InMemoryBackend) releaseConcurrencySlot(functionName string) {
	b.mu.Lock("releaseConcurrencySlot")
	defer b.mu.Unlock()

	if b.activeConcurrencies[functionName] > 0 {
		b.activeConcurrencies[functionName]--
		if b.activeConcurrencies[functionName] == 0 {
			delete(b.activeConcurrencies, functionName)
		}
	}
}

// dispatchInvocationLog asynchronously emits an invocation log entry. The
// goroutine count is bounded by b.logSem; when saturated, the log is dropped
// (best-effort observability) so a slow CloudWatch Logs backend cannot leak
// goroutines under high invocation throughput.
func (b *InMemoryBackend) dispatchInvocationLog(
	ctx context.Context,
	functionName string,
	payload, result []byte,
) {
	// Capture the semaphore channel under the read lock so that a concurrent Reset()
	// cannot replace b.logSem between the send and the goroutine's deferred release.
	b.mu.RLock("dispatchInvocationLog.sem")
	sem := b.logSem
	b.mu.RUnlock()

	select {
	case sem <- struct{}{}:
	default:
		logger.Load(ctx).WarnContext(ctx, "lambda: invocation log dropped: logSem saturated",
			"function", functionName)

		return
	}

	go func() {
		defer func() { <-sem }()
		b.pushInvocationLog(ctx, functionName, payload, result)
	}()
}

// pushInvocationLog writes a minimal invocation log entry to CloudWatch Logs when a backend is set.
func (b *InMemoryBackend) pushInvocationLog(
	ctx context.Context,
	functionName string,
	_ []byte,
	result []byte,
) {
	b.mu.RLock("pushInvocationLog")
	cwl := b.cwLogs
	b.mu.RUnlock()

	if cwl == nil {
		return
	}

	groupName := "/aws/lambda/" + functionName
	streamName := time.Now().UTC().Format("2006/01/02") + "/[$LATEST]" + uuid.New().String()[:8]

	if err := cwl.EnsureLogGroupAndStream(groupName, streamName); err != nil {
		logger.Load(ctx).WarnContext(ctx, "pushInvocationLog: failed to ensure log group/stream",
			"function", functionName, "error", err)

		return
	}

	requestID := uuid.New().String()
	report := "REPORT RequestId: " + requestID +
		"\tDuration: 0.00 ms\tBilled Duration: 1 ms\tMemory Size: 128 MB\tMax Memory Used: 128 MB"
	messages := []string{
		"START RequestId: " + requestID + " Version: $LATEST",
	}
	if len(result) > 0 {
		messages = append(messages, string(result))
	}
	messages = append(
		messages,
		"END RequestId: "+requestID,
		report,
	)

	if err := cwl.PutLogLines(groupName, streamName, messages); err != nil {
		logger.Load(ctx).WarnContext(ctx, "pushInvocationLog: failed to put log lines",
			"function", functionName, "error", err)
	}
}

// defaultFunctionTimeout is used when the function has no timeout configured.
const defaultFunctionTimeout = 3 * time.Second

// containerShutdownTimeout is the maximum time to wait for a container to stop.
const containerShutdownTimeout = 5 * time.Second

// evictLRURuntimeLocked removes the least-recently-used runtime entry (other than
// skipName) when the runtimes map exceeds the configured MaxRuntimes limit.
// It returns the evicted entry so the caller can release its resources outside the lock.
// The early-return on len(b.runtimes) <= maxRuntimes keeps the common (under-limit) path
// cost to a single comparison, so callers do not need to guard the call site.
// Must be called with b.mu held (write lock).
func (b *InMemoryBackend) evictLRURuntimeLocked(skipName string) *functionRuntime {
	maxRuntimes := b.settings.MaxRuntimes
	if maxRuntimes <= 0 {
		maxRuntimes = defaultMaxRuntimes
	}

	if len(b.runtimes) <= maxRuntimes {
		return nil
	}

	var (
		lruName string
		lruRT   *functionRuntime
	)

	for name, rt := range b.runtimes {
		if name == skipName {
			continue
		}

		if lruRT == nil || rt.lastUsed.Before(lruRT.lastUsed) {
			lruName = name
			lruRT = rt
		}
	}

	if lruRT != nil {
		delete(b.runtimes, lruName)
	}

	return lruRT
}

// cleanupRuntime stops the container, shuts down the runtime server, releases the port,
// and removes any temp directories associated with rt. It is safe to call with a nil rt.
// Fields are snapshotted under rt.mu to avoid data races with concurrent startup.
func (b *InMemoryBackend) cleanupRuntime(ctx context.Context, rt *functionRuntime) {
	if rt == nil {
		return
	}

	// Snapshot all resource handles under rt.mu so we don't race with getOrCreateRuntime
	// which writes containerID, srv, port, zipDir, and layerDirs under rt.mu.
	rt.mu.Lock("cleanupRuntime")
	containerID := rt.containerID
	srv := rt.srv
	port := rt.port
	zipDir := rt.zipDir
	layerDirs := rt.layerDirs
	rt.mu.Unlock()

	shutdownCtx, cancel := context.WithTimeout(ctx, containerShutdownTimeout)
	defer cancel()

	defer func() {
		// Stop and remove the container
		if containerID != "" && b.docker != nil {
			if !b.settings.KeepContainers {
				_ = b.docker.StopAndRemove(ctx, containerID)
			}
		}
	}()

	if srv != nil {
		srv.stop(shutdownCtx)
	}

	if port > 0 && b.portAlloc != nil {
		_ = b.portAlloc.Release(port)
	}

	if zipDir != "" {
		_ = os.RemoveAll(zipDir) // #nosec G703
	}

	for _, d := range layerDirs {
		_ = os.RemoveAll(d) // #nosec G703
	}

	rt.mu.Close()
}

// cleanupTimedOutRuntime removes the named runtime from the runtimes map and
// asynchronously stops its container and releases its resources. It is called when
// an invocation times out so that the next invocation creates a fresh container.
func (b *InMemoryBackend) cleanupTimedOutRuntime(functionName string) {
	b.mu.Lock("cleanupTimedOutRuntime")
	rt := b.runtimes[functionName]
	delete(b.runtimes, functionName)
	b.mu.Unlock()

	if rt == nil {
		return
	}

	b.mu.RLock("cleanupSem.timedOut")
	sem := b.cleanupSem
	b.mu.RUnlock()

	select {
	case sem <- struct{}{}:
	default:
		// Already at max concurrent cleanups; skip
		return
	}
	go func() {
		defer func() { <-sem }()
		ctx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
		defer cancel()
		b.cleanupRuntime(ctx, rt)
	}()
}

// getOrCreateRuntime returns the runtime server for a function, creating it on first use.
// Must not be called with b.mu held.
func (b *InMemoryBackend) getOrCreateRuntime(
	ctx context.Context,
	fn *FunctionConfiguration,
) (*runtimeServer, error) {
	b.mu.Lock("getOrCreateRuntime")
	rt, ok := b.runtimes[fn.FunctionName]

	if ok {
		// Touch lastUsed under the lock so concurrent callers see a consistent value.
		rt.lastUsed = time.Now()
		b.mu.Unlock()
	} else {
		// Only check for required infrastructure when actually creating a new runtime.
		if b.portAlloc == nil {
			b.mu.Unlock()

			return nil, fmt.Errorf("%w: no port range configured", ErrLambdaUnavailable)
		}

		if b.docker == nil {
			b.mu.Unlock()

			return nil, fmt.Errorf("%w: container runtime unavailable", ErrLambdaUnavailable)
		}

		rt = &functionRuntime{mu: lockmetrics.New("lambda.runtime"), lastUsed: time.Now()}
		b.runtimes[fn.FunctionName] = rt
		evicted := b.evictLRURuntimeLocked(fn.FunctionName)
		b.mu.Unlock()

		// Clean up the evicted runtime asynchronously outside b.mu.
		if evicted != nil {
			// Capture sem under RLock so that a concurrent Reset() cannot replace
			// b.cleanupSem between the send and the goroutine's deferred release.
			b.mu.RLock("cleanupSem.evict")
			sem := b.cleanupSem
			b.mu.RUnlock()

			select {
			case sem <- struct{}{}:
				go func(rt *functionRuntime) { // #nosec G118 -- intentional detached cleanup goroutine
					defer func() { <-sem }()
					cleanupCtx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
					defer cancel()
					b.cleanupRuntime(cleanupCtx, rt)
				}(evicted)
			default:
				// cleanupSem is full; run inline to avoid leaking the evicted runtime's resources.
				cleanupCtx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
				defer cancel()
				b.cleanupRuntime(cleanupCtx, evicted)
			}
		}
	}

	rt.mu.Lock("getOrCreateRuntime")

	if rt.started {
		srv, startErr := rt.srv, rt.startErr
		rt.mu.Unlock()

		return srv, startErr
	}

	port, portErr := b.portAlloc.Acquire("lambda:" + fn.FunctionName)
	if portErr != nil {
		rt.startErr = fmt.Errorf("%w: port allocation failed: %w", ErrLambdaUnavailable, portErr)
		rt.started = true
		rt.mu.Unlock()

		return nil, rt.startErr
	}

	srv := newRuntimeServer(port)

	if startErr := srv.start(ctx); startErr != nil {
		_ = b.portAlloc.Release(port)
		rt.startErr = fmt.Errorf(
			"%w: runtime server start failed: %w",
			ErrLambdaUnavailable,
			startErr,
		)
		rt.started = true
		rt.mu.Unlock()

		return nil, rt.startErr
	}

	rt.srv = srv
	rt.port = port
	rt.started = true

	zipDir, layerDirs, containerID, containerErr := b.startContainer(ctx, fn, port)
	if containerErr != nil {
		startErr := b.handleContainerStartFailure(
			ctx, fn.FunctionName, rt, srv, port, zipDir, layerDirs, containerID, containerErr,
		)

		return nil, startErr
	}

	rt.zipDir = zipDir
	rt.layerDirs = layerDirs
	rt.containerID = containerID
	rt.mu.Unlock()

	return srv, nil
}

// handleContainerStartFailure cleans up all resources after startContainer returns an error.
// It stops the runtime server, releases the port, removes any partial container and temp dirs,
// sets rt.startErr, unlocks rt.mu, and then removes the stale map entry under b.mu so that
// the next invocation can retry. This helper exists to keep getOrCreateRuntime within the
// statement-count limit.
func (b *InMemoryBackend) handleContainerStartFailure(
	ctx context.Context,
	functionName string,
	rt *functionRuntime,
	srv *runtimeServer,
	port int,
	zipDir string,
	layerDirs []string,
	containerID string,
	containerErr error,
) error {
	// Container startup failure is fatal: stop the runtime server, release the
	// port, and surface the error so the caller gets an immediate failure instead
	// of silently timing out on every subsequent invoke.
	shutdownCtx, cancel := context.WithTimeout(ctx, containerShutdownTimeout)
	defer cancel()
	srv.stop(shutdownCtx)
	_ = b.portAlloc.Release(port)

	// Stop any container that was created before the error occurred.
	if containerID != "" && b.docker != nil {
		if !b.settings.KeepContainers {
			_ = b.docker.StopAndRemove(ctx, containerID)
		}
	}

	for _, d := range layerDirs {
		_ = os.RemoveAll(d) // #nosec G703
	}

	if zipDir != "" {
		_ = os.RemoveAll(zipDir) // #nosec G703
	}

	startErr := fmt.Errorf("%w: container startup failed: %w", ErrLambdaUnavailable, containerErr)
	rt.startErr = startErr
	rt.mu.Unlock()

	// Remove the stale entry so the next invocation gets a fresh attempt rather
	// than perpetually returning this error. Lock ordering is safe: rt.mu is
	// released above before acquiring b.mu.
	b.mu.Lock("getOrCreateRuntime-evict-failed")
	if b.runtimes[functionName] == rt {
		delete(b.runtimes, functionName)
	}
	b.mu.Unlock()

	return startErr
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

// deprecatedRuntimes are AWS Lambda runtime identifiers that are valid (AWS
// recognises them and CreateFunction is accepted) but are past their
// deprecation date and can no longer be executed. We accept them at the
// control plane for parity — real AWS returns InvalidParameterValueException
// only for runtimes it has never heard of, not for deprecated ones — but they
// are deliberately absent from runtimeBaseImages so the Docker run path treats
// them as unknown.
//
//nolint:gochecknoglobals // intentional package-level lookup set
var deprecatedRuntimes = map[string]struct{}{
	"nodejs":         {},
	"nodejs4.3":      {},
	"nodejs4.3-edge": {},
	"nodejs6.10":     {},
	"nodejs8.10":     {},
	"nodejs10.x":     {},
	"nodejs12.x":     {},
	"nodejs14.x":     {},
	"nodejs16.x":     {},
	"python2.7":      {},
	"python3.6":      {},
	"python3.7":      {},
	"python3.8":      {},
	"java8":          {},
	"java8.al2":      {},
	"dotnetcore1.0":  {},
	"dotnetcore2.0":  {},
	"dotnetcore2.1":  {},
	"dotnetcore3.1":  {},
	"dotnet5.0":      {},
	"dotnet6":        {},
	"dotnet7":        {},
	"go1.x":          {},
	"ruby2.5":        {},
	"ruby2.7":        {},
	"provided.al2":   {}, // still runnable, but listed here as a safety net
}

// isValidRuntime reports whether a runtime identifier is one AWS Lambda
// recognises — either a currently runnable runtime (runtimeBaseImages) or a
// known-but-deprecated one. Unknown identifiers are rejected by CreateFunction
// with InvalidParameterValueException, matching real AWS, which enforces an
// enum constraint on the 'runtime' member.
func isValidRuntime(runtime string) bool {
	if _, ok := runtimeBaseImages[runtime]; ok {
		return true
	}

	_, ok := deprecatedRuntimes[runtime]

	return ok
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
	if err := os.MkdirAll(parentDir, extractParentDirPerm); err != nil { // #nosec G703
		return fmt.Errorf("mkdir %q: %w", parentDir, err)
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("open zip entry %q: %w", f.Name, err)
	}
	defer rc.Close()

	outFile, err := os.OpenFile(
		destPath,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		f.Mode(),
	) // #nosec G304 G703
	if err != nil {
		return fmt.Errorf("create file %q: %w", destPath, err)
	}
	defer outFile.Close()

	//nolint:gosec // zip input is generated by the local build pipeline.
	if _, copyErr := io.Copy(
		outFile,
		rc,
	); copyErr != nil {
		return fmt.Errorf("extract file %q: %w", f.Name, copyErr)
	}

	return nil
}

// startContainer creates and starts a Lambda container for the given function.
// For Zip functions it extracts the code to a temp directory and bind-mounts it.
// Returns the temp directory path (non-empty only for Zip functions), a list of
// layer temp directories, the container ID (empty if creation failed), and any error.
func (b *InMemoryBackend) startContainer(
	ctx context.Context,
	fn *FunctionConfiguration,
	runtimePort int,
) (string, []string, string, error) {
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
		logger.Load(ctx).WarnContext(ctx, "lambda: layer extraction failed",
			"function", fn.FunctionName, "error", layerErr)
	}

	if fn.PackageType == PackageTypeZip {
		zipDir, containerID, err := b.startZipContainer(ctx, fn, env, layerMount)

		return zipDir, layerDirs, containerID, err
	}

	spec := container.Spec{
		Image: fn.ImageURI,
		Name:  fmt.Sprintf("gopherstack-lambda-%s-%s", fn.FunctionName, uuid.New().String()[:8]),
		Env:   env,
	}

	if layerMount != "" {
		spec.Mounts = append(spec.Mounts, layerMount)
	}

	containerID, err := b.docker.CreateAndStart(ctx, spec)

	return "", layerDirs, containerID, err
}

// startZipContainer handles container startup for Zip-packaged Lambda functions.
// It fetches the zip (from inline ZipData or S3), extracts it to a temp directory,
// and bind-mounts the directory into the appropriate AWS base image container.
// An optional layerMount bind-mount string (host:/opt:ro) can also be provided.
// Returns the temp directory path, the container ID, and any error.
func (b *InMemoryBackend) startZipContainer(
	ctx context.Context,
	fn *FunctionConfiguration,
	env []string,
	layerMount string,
) (string, string, error) {
	baseImage := baseImageForRuntime(fn.Runtime)
	if baseImage == "" {
		return "", "", fmt.Errorf("%w: unsupported runtime %q", ErrLambdaUnavailable, fn.Runtime)
	}

	// Resolve zip bytes from inline data or S3.
	zipData := fn.ZipData
	if len(zipData) == 0 && fn.S3BucketCode != "" && fn.S3KeyCode != "" {
		if b.s3Fetcher == nil {
			return "", "", fmt.Errorf(
				"%w: S3 code delivery requires S3 integration",
				ErrLambdaUnavailable,
			)
		}

		var fetchErr error

		zipData, fetchErr = b.s3Fetcher.GetObjectBytes(ctx, fn.S3BucketCode, fn.S3KeyCode)
		if fetchErr != nil {
			return "", "", fmt.Errorf(
				"%w: failed to fetch zip from S3: %w",
				ErrLambdaUnavailable,
				fetchErr,
			)
		}
	}

	if len(zipData) == 0 {
		return "", "", fmt.Errorf(
			"%w: no zip data available for function %q",
			ErrLambdaUnavailable,
			fn.FunctionName,
		)
	}

	zipDir, extractErr := extractZip(zipData)
	if extractErr != nil {
		return "", "", fmt.Errorf("%w: zip extraction failed: %w", ErrLambdaUnavailable, extractErr)
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

	// Custom runtimes (provided.*) ship the executable as the zip's "bootstrap"
	// file at /var/task. The provided.al2 base image's default entrypoint runs
	// /var/runtime/bootstrap, so run the function's bootstrap directly instead
	// (matching real AWS, which execs /var/task/bootstrap for custom runtimes).
	if strings.HasPrefix(fn.Runtime, "provided") {
		spec.Entrypoint = []string{"/var/task/bootstrap"}
	} else if fn.Handler != "" {
		spec.Cmd = []string{fn.Handler}
	}

	containerID, err := b.docker.CreateAndStart(ctx, spec)
	if err != nil {
		_ = os.RemoveAll(zipDir) // #nosec G703

		return "", "", err
	}

	return zipDir, containerID, nil
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
				entries = append(
					entries,
					layerEntry{name: layerName, version: layerVersion, zipData: data},
				)

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

			return "", nil, fmt.Errorf(
				"extract layer %q v%d: %w",
				entry.name,
				entry.version,
				extractErr,
			)
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
func (b *InMemoryBackend) PublishLayerVersion(
	input *PublishLayerVersionInput,
) (*PublishLayerVersionOutput, error) {
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
func (b *InMemoryBackend) GetLayerVersion(
	layerName string,
	version int64,
) (*GetLayerVersionOutput, error) {
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

// ListLayers returns a paginated summary of all layers with their latest version.
// Marker is an opaque cursor; maxItems uses lambdaDefaultMaxItems when zero.
func (b *InMemoryBackend) ListLayers(compatibleRuntime, marker string, maxItems int) page.Page[*Layer] {
	b.mu.RLock("ListLayers")
	defer b.mu.RUnlock()

	result := make([]*Layer, 0, len(b.layers))

	names := collections.SortedKeys(b.layers)

	for _, name := range names {
		versions := b.layers[name]
		if len(versions) == 0 {
			continue
		}

		latest := versions[len(versions)-1]

		// Filter by CompatibleRuntime when provided.
		if compatibleRuntime != "" && !slices.Contains(latest.CompatibleRuntimes, compatibleRuntime) {
			continue
		}

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

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems)
}

// ListLayerVersions returns all versions of a specific layer in descending order.
func (b *InMemoryBackend) ListLayerVersions(layerName, compatibleRuntime string) ([]*LayerVersion, error) {
	b.mu.RLock("ListLayerVersions")
	defer b.mu.RUnlock()

	versions, ok := b.layers[layerName]
	if !ok {
		return nil, ErrLayerNotFound
	}

	// Return a copy in reverse order (newest first), applying optional runtime filter.
	result := make([]*LayerVersion, 0, len(versions))
	for _, lv := range slices.Backward(versions) {
		if compatibleRuntime != "" && !slices.Contains(lv.CompatibleRuntimes, compatibleRuntime) {
			continue
		}
		result = append(result, &LayerVersion{
			LayerVersionArn:    lv.LayerVersionArn,
			Description:        lv.Description,
			CreatedDate:        lv.CreatedDate,
			Content:            lv.Content,
			CompatibleRuntimes: lv.CompatibleRuntimes,
			LicenseInfo:        lv.LicenseInfo,
			Version:            lv.Version,
		})
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
func (b *InMemoryBackend) GetLayerVersionPolicy(
	layerName string,
	version int64,
) (*LayerVersionPolicy, error) {
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
func (b *InMemoryBackend) RemoveLayerVersionPermission(
	layerName string,
	version int64,
	statementID string,
) error {
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

	stmtIDs := collections.SortedKeys(stmts)

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
func (b *InMemoryBackend) GetFunctionEventInvokeConfig(
	name string,
) (*FunctionEventInvokeConfig, error) {
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
func (b *InMemoryBackend) PutFunctionConcurrency(
	name string,
	reserved int,
) (*FunctionConcurrency, error) {
	b.mu.Lock("PutFunctionConcurrency")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if reserved < 0 {
		return nil, fmt.Errorf(
			"%w: ReservedConcurrentExecutions must be >= 0",
			ErrInvalidParameterValue,
		)
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
		return nil, fmt.Errorf(
			"%w: ProvisionedConcurrentExecutions must be > 0",
			ErrInvalidParameterValue,
		)
	}

	if qualifier == versionLatest {
		return nil, fmt.Errorf(
			"%w: provisioned concurrency is not supported for $LATEST",
			ErrInvalidParameterValue,
		)
	}

	if _, exists := b.provisionedConcurrencies[name]; !exists {
		b.provisionedConcurrencies[name] = make(map[string]*ProvisionedConcurrencyConfig)
	}

	cfg := &ProvisionedConcurrencyConfig{
		AllocatedProvisionedConcurrentExecutions: requested,
		AvailableProvisionedConcurrentExecutions: requested,
		FunctionArn: buildAliasARN(
			b.region,
			b.accountID,
			fn.FunctionName,
			qualifier,
		),
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
func (b *InMemoryBackend) ListProvisionedConcurrencyConfigs(
	name string,
) ([]*ProvisionedConcurrencyConfig, error) {
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
	b.esmByFunctionARN = make(map[string]map[string]struct{})
	b.versionIndex = make(map[string]map[string]*FunctionVersion)
	b.eventInvokeConfigs = make(map[string]*FunctionEventInvokeConfig)
	b.functionConcurrencies = make(map[string]int)
	b.activeConcurrencies = make(map[string]int)
	b.provisionedConcurrencies = make(map[string]map[string]*ProvisionedConcurrencyConfig)
	b.fisFaults = make(map[string]*FISInvocationFault)
	b.runtimes = make(map[string]*functionRuntime)
	b.functionURLServers = make(map[string]*functionURLServer)
	b.functionURLConfigs = make(map[string]*FunctionURLConfig)
	b.permissions = make(map[string]map[string]*FunctionPermission)
	b.codeSigningConfigs = make(map[string]*CodeSigningConfig)
	b.fnCodeSigningConfigs = make(map[string]string)
	b.capacityProviders = make(map[string]*CapacityProvider)
	b.runtimeManagementConfigs = make(map[string]*RuntimeManagementConfig)
	b.functionRecursionConfigs = make(map[string]*FunctionRecursionConfig)
	b.functionScalingConfigs = make(map[string]*FunctionScalingConfig)
	b.durableExecs.reset()

	// Replace semaphore channels so that goroutines launched after Reset() use fresh
	// channels. Goroutines launched before Reset() captured the old channel references
	// (via the RLock capture pattern) and release correctly to those old channels.
	b.cleanupSem = make(chan struct{}, maxCleanupConcurrency)
	b.logSem = make(chan struct{}, maxConcurrentInvocationLogs)

	b.mu.Unlock()

	// Shut down URL servers and release ports outside the lock.
	ctx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
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
		wg.Go(func() { b.cleanupRuntime(ctx, rt) })
	}

	wg.Wait()
}

// Purge removes all functions older than the given cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}
	purgedFunctions, urlServers, rts := b.collectAndDeleteFunctions(cutoff)

	if len(purgedFunctions) == 0 {
		return
	}

	b.shutdownPurgedResources(urlServers, rts)
}

// collectAndDeleteFunctions removes functions older than cutoff under the lock and returns
// the names, URL servers, and runtimes that need external cleanup.
func (b *InMemoryBackend) collectAndDeleteFunctions(cutoff time.Time) (
	[]string, []*functionURLServer, []*functionRuntime,
) {
	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	var purgedFunctions []string
	var urlServers []*functionURLServer
	var rts []*functionRuntime

	for name, fn := range b.functions {
		if !fn.CreatedAt.Before(cutoff) {
			continue
		}
		purgedFunctions = append(purgedFunctions, name)
		if srv, ok := b.functionURLServers[name]; ok {
			urlServers = append(urlServers, srv)
		}
		if rt, ok := b.runtimes[name]; ok {
			rts = append(rts, rt)
		}
		b.deleteFunctionMapsLocked(name)
	}

	return purgedFunctions, urlServers, rts
}

// deleteFunctionMapsLocked removes all map entries for a function.
// Caller must hold b.mu.
func (b *InMemoryBackend) deleteFunctionMapsLocked(name string) {
	delete(b.functions, name)
	delete(b.runtimes, name)
	delete(b.functionURLServers, name)
	delete(b.functionURLConfigs, name)
	delete(b.aliases, name)
	delete(b.versionCounters, name)
	delete(b.versions, name)
	delete(b.eventInvokeConfigs, name)
	delete(b.functionConcurrencies, name)
	delete(b.activeConcurrencies, name)
	delete(b.provisionedConcurrencies, name)
	delete(b.fisFaults, name)
	delete(b.permissions, name)
	delete(b.fnCodeSigningConfigs, name)
	delete(b.runtimeManagementConfigs, name)
	delete(b.functionRecursionConfigs, name)
	delete(b.functionScalingConfigs, name)
	for id, m := range b.eventSourceMappings {
		if strings.HasSuffix(m.FunctionARN, ":function:"+name) {
			if ids, ok := b.esmByFunctionARN[m.FunctionARN]; ok {
				delete(ids, id)
				if len(ids) == 0 {
					delete(b.esmByFunctionARN, m.FunctionARN)
				}
			}
			delete(b.eventSourceMappings, id)
		}
	}
	delete(b.versionIndex, name)
}

// shutdownPurgedResources shuts down URL servers and runtimes outside the lock.
func (b *InMemoryBackend) shutdownPurgedResources(
	urlServers []*functionURLServer,
	rts []*functionRuntime,
) {
	ctx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
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
		wg.Go(func() { b.cleanupRuntime(ctx, rt) })
	}

	wg.Wait()
}

// --- AddPermission / resource-based policy ---

// AddPermission adds a permission statement to a function's resource-based policy.
func (b *InMemoryBackend) AddPermission(
	functionName string,
	input *AddPermissionInput,
) (*AddPermissionOutput, error) {
	b.mu.Lock("AddPermission")
	defer b.mu.Unlock()

	if strings.HasPrefix(functionName, "arn:aws:lambda:") {
		parts := strings.Split(functionName, ":")
		functionName = parts[len(parts)-1]
	}

	if _, ok := b.functions[functionName]; !ok {
		return nil, ErrFunctionNotFound
	}

	if b.permissions[functionName] == nil {
		b.permissions[functionName] = make(map[string]*FunctionPermission)
	}

	if _, exists := b.permissions[functionName][input.StatementID]; exists {
		return nil, ErrFunctionAlreadyExists
	}

	perm := &FunctionPermission{
		StatementID:   input.StatementID,
		Action:        input.Action,
		Principal:     input.Principal,
		SourceArn:     input.SourceArn,
		SourceAccount: input.SourceAccount,
		Effect:        "Allow",
		FunctionName:  functionName,
	}

	b.permissions[functionName][input.StatementID] = perm

	resourceArn := arn.Build(
		"lambda",
		b.region,
		b.accountID,
		fmt.Sprintf("function:%s", functionName),
	)
	stmtJSON := buildPermissionStatementJSON(perm, resourceArn)

	return &AddPermissionOutput{Statement: &stmtJSON}, nil
}

// RemovePermission removes a permission statement from a function's resource-based policy.
func (b *InMemoryBackend) RemovePermission(functionName, statementID string) error {
	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	if strings.HasPrefix(functionName, "arn:aws:lambda:") {
		parts := strings.Split(functionName, ":")
		functionName = parts[len(parts)-1]
	}

	if _, ok := b.functions[functionName]; !ok {
		return ErrFunctionNotFound
	}

	perms := b.permissions[functionName]
	if perms == nil {
		return ErrFunctionNotFound
	}

	if _, ok := perms[statementID]; !ok {
		return ErrFunctionNotFound
	}

	delete(perms, statementID)

	return nil
}

// GetPolicy returns the resource-based policy JSON for a function.
func (b *InMemoryBackend) GetPolicy(functionName string) (*GetPolicyOutput, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	if strings.HasPrefix(functionName, "arn:aws:lambda:") {
		parts := strings.Split(functionName, ":")
		functionName = parts[len(parts)-1]
	}

	if _, ok := b.functions[functionName]; !ok {
		return nil, ErrFunctionNotFound
	}

	perms := b.permissions[functionName]
	if len(perms) == 0 {
		return nil, ErrNoPolicyFound
	}

	stmts := make([]string, 0, len(perms))

	resourceArn := arn.Build(
		"lambda",
		b.region,
		b.accountID,
		fmt.Sprintf("function:%s", functionName),
	)

	// Sort statements for deterministic output.
	sortedPerms := make([]*FunctionPermission, 0, len(perms))
	for _, p := range perms {
		sortedPerms = append(sortedPerms, p)
	}
	sort.Slice(sortedPerms, func(i, j int) bool {
		return sortedPerms[i].StatementID < sortedPerms[j].StatementID
	})

	for _, p := range sortedPerms {
		stmts = append(stmts, buildPermissionStatementJSON(p, resourceArn))
	}

	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[%s]}`, strings.Join(stmts, ","))
	rev := "1"

	return &GetPolicyOutput{Policy: &policy, RevisionID: &rev}, nil
}

// buildPermissionStatementJSON builds the IAM policy statement JSON for a FunctionPermission.
// It includes a Condition block when SourceArn or SourceAccount are set, matching real AWS output.
func buildPermissionStatementJSON(p *FunctionPermission, resourceArn string) string {
	// Determine principal format: account IDs and "*" use root principal; services use Service key.
	var principalJSON string
	switch {
	case p.Principal == "*":
		principalJSON = `"*"`
	case strings.Contains(p.Principal, ".amazonaws.com") || strings.Contains(p.Principal, ".aws.amazon.com"):
		principalJSON = fmt.Sprintf(`{"Service":%q}`, p.Principal)
	default:
		// Account principal: arn:aws:iam::{account}:root
		principalJSON = fmt.Sprintf(`{"AWS":%q}`, p.Principal)
	}

	base := fmt.Sprintf(
		`{"Sid":%q,"Effect":"Allow","Principal":%s,"Action":%q,"Resource":%q`,
		p.StatementID, principalJSON, p.Action, resourceArn,
	)

	// Build Condition block for source constraints.
	var conditions []string
	if p.SourceArn != "" {
		conditions = append(conditions, fmt.Sprintf(`"ArnLike":{"AWS:SourceArn":%q}`, p.SourceArn))
	}
	if p.SourceAccount != "" {
		conditions = append(conditions, fmt.Sprintf(`"StringEquals":{"AWS:SourceAccount":%q}`, p.SourceAccount))
	}

	if len(conditions) > 0 {
		return base + `,"Condition":{` + strings.Join(conditions, ",") + `}}`
	}

	return base + "}"
}

// --- Code signing configs ---

// CreateCodeSigningConfig creates a new Lambda code signing configuration.
func (b *InMemoryBackend) CreateCodeSigningConfig(
	input *CreateCodeSigningConfigInput,
) (*CodeSigningConfig, error) {
	b.mu.Lock("CreateCodeSigningConfig")
	defer b.mu.Unlock()

	b.cscIDCounter++
	cscID := fmt.Sprintf("csc-%08d", b.cscIDCounter)
	cscARN := buildCodeSigningConfigARN(b.region, b.accountID, cscID)
	now := time.Now().UTC().Format(time.RFC3339)

	cfg := &CodeSigningConfig{
		CodeSigningConfigID:  cscID,
		CodeSigningConfigArn: cscARN,
		AllowedPublishers:    input.AllowedPublishers,
		CodeSigningPolicies:  input.CodeSigningPolicies,
		Description:          input.Description,
		LastModified:         now,
	}

	if cfg.AllowedPublishers == nil {
		cfg.AllowedPublishers = &AllowedPublishers{SigningProfileVersionArns: []string{}}
	}

	if cfg.CodeSigningPolicies == nil {
		cfg.CodeSigningPolicies = &CodeSigningPolicies{UntrustedArtifactOnDeployment: "Warn"}
	}

	b.codeSigningConfigs[cscARN] = cfg

	return cfg, nil
}

// GetCodeSigningConfig retrieves a code signing config by ARN.
func (b *InMemoryBackend) GetCodeSigningConfig(cscARN string) (*CodeSigningConfig, error) {
	b.mu.RLock("GetCodeSigningConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.codeSigningConfigs[cscARN]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	return cfg, nil
}

// DeleteCodeSigningConfig removes a code signing config by ARN.
func (b *InMemoryBackend) DeleteCodeSigningConfig(cscARN string) error {
	b.mu.Lock("DeleteCodeSigningConfig")
	defer b.mu.Unlock()

	if _, ok := b.codeSigningConfigs[cscARN]; !ok {
		return ErrFunctionNotFound
	}

	delete(b.codeSigningConfigs, cscARN)

	return nil
}

// UpdateCodeSigningConfig updates an existing code signing config.
func (b *InMemoryBackend) UpdateCodeSigningConfig(
	cscARN string,
	input *UpdateCodeSigningConfigInput,
) (*CodeSigningConfig, error) {
	b.mu.Lock("UpdateCodeSigningConfig")
	defer b.mu.Unlock()

	cfg, ok := b.codeSigningConfigs[cscARN]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if input.AllowedPublishers != nil {
		cfg.AllowedPublishers = input.AllowedPublishers
	}

	if input.CodeSigningPolicies != nil {
		cfg.CodeSigningPolicies = input.CodeSigningPolicies
	}

	if input.Description != "" {
		cfg.Description = input.Description
	}

	cfg.LastModified = time.Now().UTC().Format(time.RFC3339)
	b.codeSigningConfigs[cscARN] = cfg

	return cfg, nil
}

// ListCodeSigningConfigs returns all code signing configs.
func (b *InMemoryBackend) ListCodeSigningConfigs() []*CodeSigningConfig {
	b.mu.RLock("ListCodeSigningConfigs")
	defer b.mu.RUnlock()

	cfgs := make([]*CodeSigningConfig, 0, len(b.codeSigningConfigs))
	for _, cfg := range b.codeSigningConfigs {
		cfgs = append(cfgs, cfg)
	}

	sort.Slice(cfgs, func(i, j int) bool {
		return cfgs[i].CodeSigningConfigID < cfgs[j].CodeSigningConfigID
	})

	return cfgs
}

// PutFunctionCodeSigningConfig associates a code signing config with a function.
func (b *InMemoryBackend) PutFunctionCodeSigningConfig(functionName, cscARN string) error {
	b.mu.Lock("PutFunctionCodeSigningConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions[functionName]; !ok {
		return ErrFunctionNotFound
	}

	if _, ok := b.codeSigningConfigs[cscARN]; !ok {
		return ErrFunctionNotFound
	}

	b.fnCodeSigningConfigs[functionName] = cscARN

	return nil
}

// GetFunctionCodeSigningConfig returns the code signing config ARN associated with a function.
func (b *InMemoryBackend) GetFunctionCodeSigningConfig(functionName string) (string, error) {
	b.mu.RLock("GetFunctionCodeSigningConfig")
	defer b.mu.RUnlock()

	if _, ok := b.functions[functionName]; !ok {
		return "", ErrFunctionNotFound
	}

	cscARN, ok := b.fnCodeSigningConfigs[functionName]
	if !ok {
		return "", ErrCodeSigningConfigNotFound
	}

	return cscARN, nil
}

// DeleteFunctionCodeSigningConfig removes the code signing config association from a function.
func (b *InMemoryBackend) DeleteFunctionCodeSigningConfig(functionName string) error {
	b.mu.Lock("DeleteFunctionCodeSigningConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions[functionName]; !ok {
		return ErrFunctionNotFound
	}

	delete(b.fnCodeSigningConfigs, functionName)

	return nil
}

// ListFunctionsByCodeSigningConfig returns function ARNs associated with a code signing config.
func (b *InMemoryBackend) ListFunctionsByCodeSigningConfig(cscARN string) ([]string, error) {
	b.mu.RLock("ListFunctionsByCodeSigningConfig")
	defer b.mu.RUnlock()

	if _, ok := b.codeSigningConfigs[cscARN]; !ok {
		return nil, ErrFunctionNotFound
	}

	var arns []string

	for fnName, arn := range b.fnCodeSigningConfigs {
		if arn == cscARN {
			fn, ok := b.functions[fnName]
			if ok {
				arns = append(arns, fn.FunctionArn)
			}
		}
	}

	sort.Strings(arns)

	return arns, nil
}

// --- Capacity providers ---

// CreateCapacityProvider creates a new Lambda capacity provider.
func (b *InMemoryBackend) CreateCapacityProvider(
	input *CreateCapacityProviderInput,
) (*CapacityProvider, error) {
	b.mu.Lock("CreateCapacityProvider")
	defer b.mu.Unlock()

	if _, exists := b.capacityProviders[input.Name]; exists {
		return nil, ErrFunctionAlreadyExists
	}

	now := time.Now().UTC().Format(time.RFC3339)
	cp := &CapacityProvider{
		Name:                      input.Name,
		CapacityProviderArn:       buildCapacityProviderARN(b.region, b.accountID, input.Name),
		TargetOnDemandConcurrency: input.TargetOnDemandConcurrency,
		Status:                    "ACTIVE",
		LastModifiedTime:          now,
	}

	b.capacityProviders[input.Name] = cp

	return cp, nil
}

// GetCapacityProvider retrieves a capacity provider by name.
func (b *InMemoryBackend) GetCapacityProvider(name string) (*CapacityProvider, error) {
	b.mu.RLock("GetCapacityProvider")
	defer b.mu.RUnlock()

	cp, ok := b.capacityProviders[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	return cp, nil
}

// DeleteCapacityProvider removes a capacity provider by name.
func (b *InMemoryBackend) DeleteCapacityProvider(name string) error {
	b.mu.Lock("DeleteCapacityProvider")
	defer b.mu.Unlock()

	if _, ok := b.capacityProviders[name]; !ok {
		return ErrFunctionNotFound
	}

	delete(b.capacityProviders, name)

	return nil
}

// UpdateCapacityProvider updates an existing capacity provider.
func (b *InMemoryBackend) UpdateCapacityProvider(
	name string,
	input *UpdateCapacityProviderInput,
) (*CapacityProvider, error) {
	b.mu.Lock("UpdateCapacityProvider")
	defer b.mu.Unlock()

	cp, ok := b.capacityProviders[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if input.TargetOnDemandConcurrency > 0 {
		cp.TargetOnDemandConcurrency = input.TargetOnDemandConcurrency
	}

	cp.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)
	b.capacityProviders[name] = cp

	return cp, nil
}

// ListCapacityProviders returns all capacity providers.
func (b *InMemoryBackend) ListCapacityProviders() []*CapacityProvider {
	b.mu.RLock("ListCapacityProviders")
	defer b.mu.RUnlock()

	cps := make([]*CapacityProvider, 0, len(b.capacityProviders))
	for _, cp := range b.capacityProviders {
		cps = append(cps, cp)
	}

	sort.Slice(cps, func(i, j int) bool {
		return cps[i].Name < cps[j].Name
	})

	return cps
}

// SeedCapacityProviderFunctionVersions assigns the given function-version ARNs to
// the named capacity provider. AWS exposes no public assignment API in this
// emulator's surface, so this internal helper is the only way to populate the
// assignments observed by ListFunctionVersionsByCapacityProvider (primarily for
// tests). It returns ErrFunctionNotFound if the provider does not exist.
func (b *InMemoryBackend) SeedCapacityProviderFunctionVersions(
	name string,
	versions ...string,
) error {
	b.mu.Lock("SeedCapacityProviderFunctionVersions")
	defer b.mu.Unlock()

	cp, ok := b.capacityProviders[name]
	if !ok {
		return ErrFunctionNotFound
	}

	cp.AssignedFunctionVersions = append(cp.AssignedFunctionVersions, versions...)

	return nil
}

// ListFunctionVersionsByCapacityProvider returns a page of function-version ARNs
// assigned to the named capacity provider. It returns ErrFunctionNotFound if the
// provider does not exist. Assignments are populated only via the internal
// SeedCapacityProviderFunctionVersions helper, since AWS exposes no public
// assignment API in this emulator's surface.
func (b *InMemoryBackend) ListFunctionVersionsByCapacityProvider(
	name, marker string,
	maxItems int,
) (page.Page[string], error) {
	b.mu.RLock("ListFunctionVersionsByCapacityProvider")
	defer b.mu.RUnlock()

	cp, ok := b.capacityProviders[name]
	if !ok {
		return page.Page[string]{}, ErrFunctionNotFound
	}

	versions := make([]string, len(cp.AssignedFunctionVersions))
	copy(versions, cp.AssignedFunctionVersions)
	sort.Strings(versions)

	return page.New(versions, marker, maxItems, lambdaDefaultMaxItems), nil
}

// --- Account settings ---

// accountDefaultCodeSizeZipped is the default Lambda zip package size limit (50 MB).
const accountDefaultCodeSizeZipped = 50 * 1024 * 1024

// accountDefaultCodeSizeUnzipped is the default Lambda unzipped package size limit (250 MB).
const accountDefaultCodeSizeUnzipped = 250 * 1024 * 1024

// accountDefaultTotalCodeSize is the default Lambda total code storage limit (75 GB).
const accountDefaultTotalCodeSize = 75 * 1024 * 1024 * 1024

// accountDefaultConcurrentExecutions is the default Lambda concurrent execution limit.
const accountDefaultConcurrentExecutions = 1000

// GetAccountSettings returns the Lambda account settings for this in-memory backend.
func (b *InMemoryBackend) GetAccountSettings() *AccountSettingsOutput {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	fnCount := len(b.functions)
	totalCodeSize := int64(0)

	for _, fn := range b.functions {
		totalCodeSize += fn.CodeSize
	}

	// Compute unreserved concurrency: subtract sum of all per-function reserved values.
	totalReserved := 0
	for _, reserved := range b.functionConcurrencies {
		totalReserved += reserved
	}
	unreserved := max(0, accountDefaultConcurrentExecutions-totalReserved)

	return &AccountSettingsOutput{
		AccountLimit: &AccountLimit{
			CodeSizeUnzipped:               accountDefaultCodeSizeUnzipped,
			CodeSizeZipped:                 accountDefaultCodeSizeZipped,
			ConcurrentExecutions:           accountDefaultConcurrentExecutions,
			TotalCodeSize:                  accountDefaultTotalCodeSize,
			UnreservedConcurrentExecutions: unreserved,
		},
		AccountUsage: &AccountUsage{
			FunctionCount: fnCount,
			TotalCodeSize: totalCodeSize,
		},
	}
}

// UpdateFunctionURLConfig updates an existing function URL config.
func (b *InMemoryBackend) UpdateFunctionURLConfig(
	functionName, authType string,
	cors *FunctionURLCors,
) (*FunctionURLConfig, error) {
	b.mu.Lock("UpdateFunctionURLConfig")
	defer b.mu.Unlock()

	cfg, ok := b.functionURLConfigs[functionName]
	if !ok {
		return nil, ErrFunctionURLNotFound
	}

	if authType != "" {
		cfg.AuthType = authType
	}

	if cors != nil {
		cfg.Cors = cors
	}

	cfg.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)
	b.functionURLConfigs[functionName] = cfg

	return cfg, nil
}

// ListFunctionURLConfigs returns all function URL configs.
func (b *InMemoryBackend) ListFunctionURLConfigs() []*FunctionURLConfig {
	b.mu.RLock("ListFunctionURLConfigs")
	defer b.mu.RUnlock()

	cfgs := make([]*FunctionURLConfig, 0, len(b.functionURLConfigs))
	for _, cfg := range b.functionURLConfigs {
		cfgs = append(cfgs, cfg)
	}

	sort.Slice(cfgs, func(i, j int) bool {
		return cfgs[i].FunctionArn < cfgs[j].FunctionArn
	})

	return cfgs
}

// applyESMUpdate patches esm fields from input (non-zero / non-nil values only).
// Returns true if the mapping was enabled by this update.
func applyESMUpdate(esm *EventSourceMapping, input *UpdateEventSourceMappingInput) bool {
	var nowEnabled bool

	if input.Enabled != nil {
		if *input.Enabled {
			esm.State = ESMStateEnabled
			nowEnabled = true
		} else {
			esm.State = ESMStateDisabled
		}
	}

	if input.BatchSize > 0 {
		esm.BatchSize = input.BatchSize
	}

	if input.FilterCriteria != nil {
		esm.FilterCriteria = input.FilterCriteria
	}

	if input.DestinationConfig != nil {
		esm.DestinationConfig = input.DestinationConfig
	}

	if input.BisectBatchOnFunctionError != nil {
		esm.BisectBatchOnFunctionError = *input.BisectBatchOnFunctionError
	}

	applyESMWindowFields(esm, input)
	applyESMSourceFields(esm, input)

	esm.LastModified = time.Now()

	return nowEnabled
}

// applyESMWindowFields applies the windowing / retry fields from input.
func applyESMWindowFields(esm *EventSourceMapping, input *UpdateEventSourceMappingInput) {
	if input.MaximumBatchingWindowInSeconds > 0 {
		esm.MaximumBatchingWindowInSeconds = input.MaximumBatchingWindowInSeconds
	}

	if input.TumblingWindowInSeconds > 0 {
		esm.TumblingWindowInSeconds = input.TumblingWindowInSeconds
	}

	if input.MaximumRecordAgeInSeconds > 0 {
		esm.MaximumRecordAgeInSeconds = input.MaximumRecordAgeInSeconds
	}

	if input.MaximumRetryAttempts > 0 {
		esm.MaximumRetryAttempts = input.MaximumRetryAttempts
	}

	if input.ParallelizationFactor > 0 {
		esm.ParallelizationFactor = input.ParallelizationFactor
	}
}

// applyESMSourceFields applies source-access, topics, queues, and response types from input.
func applyESMSourceFields(esm *EventSourceMapping, input *UpdateEventSourceMappingInput) {
	if len(input.SourceAccessConfigurations) > 0 {
		esm.SourceAccessConfigurations = input.SourceAccessConfigurations
	}

	if len(input.Topics) > 0 {
		esm.Topics = input.Topics
	}

	if len(input.Queues) > 0 {
		esm.Queues = input.Queues
	}

	if len(input.FunctionResponseTypes) > 0 {
		esm.FunctionResponseTypes = input.FunctionResponseTypes
	}
}

// UpdateEventSourceMapping updates an existing event source mapping.
func (b *InMemoryBackend) UpdateEventSourceMapping(
	id string,
	input *UpdateEventSourceMappingInput,
) (*EventSourceMapping, error) {
	b.mu.Lock("UpdateEventSourceMapping")

	esm, ok := b.eventSourceMappings[id]
	if !ok {
		b.mu.Unlock()

		return nil, ErrESMNotFound
	}

	nowEnabled := applyESMUpdate(esm, input)

	poller := b.kinesisPoller
	b.mu.Unlock()

	if nowEnabled && poller != nil {
		poller.Notify()
	}

	return esm, nil
}

// GetRuntimeManagementConfig returns the runtime management config for a function.
func (b *InMemoryBackend) GetRuntimeManagementConfig(
	name string,
) (*RuntimeManagementConfig, error) {
	b.mu.RLock("GetRuntimeManagementConfig")
	defer b.mu.RUnlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.runtimeManagementConfigs[name]
	if !ok {
		return &RuntimeManagementConfig{UpdateRuntimeOn: "Auto", FunctionArn: fn.FunctionArn}, nil
	}

	out := *cfg
	out.FunctionArn = fn.FunctionArn

	return &out, nil
}

// PutRuntimeManagementConfig sets the runtime management config for a function.
func (b *InMemoryBackend) PutRuntimeManagementConfig(
	name string,
	input *PutRuntimeManagementConfigInput,
) (*RuntimeManagementConfig, error) {
	b.mu.Lock("PutRuntimeManagementConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if input.UpdateRuntimeOn == "" {
		return nil, ErrInvalidParameterValue
	}

	cfg := &RuntimeManagementConfig{
		UpdateRuntimeOn:   input.UpdateRuntimeOn,
		RuntimeVersionArn: input.RuntimeVersionArn,
	}
	b.runtimeManagementConfigs[name] = cfg

	out := *cfg
	out.FunctionArn = fn.FunctionArn

	return &out, nil
}

// GetFunctionRecursionConfig returns the recursion config for a function.
func (b *InMemoryBackend) GetFunctionRecursionConfig(
	name string,
) (*FunctionRecursionConfig, error) {
	b.mu.RLock("GetFunctionRecursionConfig")
	defer b.mu.RUnlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.functionRecursionConfigs[name]
	if !ok {
		return &FunctionRecursionConfig{RecursiveLoop: "Terminate"}, nil
	}

	return cfg, nil
}

// PutFunctionRecursionConfig sets the recursion config for a function.
func (b *InMemoryBackend) PutFunctionRecursionConfig(
	name string,
	input *PutFunctionRecursionConfigInput,
) (*FunctionRecursionConfig, error) {
	b.mu.Lock("PutFunctionRecursionConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions[name]; !ok {
		return nil, ErrFunctionNotFound
	}

	if input.RecursiveLoop == "" {
		return nil, ErrInvalidParameterValue
	}

	cfg := &FunctionRecursionConfig{RecursiveLoop: input.RecursiveLoop}
	b.functionRecursionConfigs[name] = cfg

	return cfg, nil
}

// GetFunctionScalingConfig returns the scaling config for a function.
func (b *InMemoryBackend) GetFunctionScalingConfig(name string) (*FunctionScalingConfig, error) {
	b.mu.RLock("GetFunctionScalingConfig")
	defer b.mu.RUnlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.functionScalingConfigs[name]
	if !ok {
		return &FunctionScalingConfig{FunctionArn: fn.FunctionArn}, nil
	}

	out := *cfg
	out.FunctionArn = fn.FunctionArn

	return &out, nil
}

// PutFunctionScalingConfig sets the scaling config for a function.
func (b *InMemoryBackend) PutFunctionScalingConfig(
	name string,
	input *PutFunctionScalingConfigInput,
) (*FunctionScalingConfig, error) {
	b.mu.Lock("PutFunctionScalingConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, ErrFunctionNotFound
	}

	cfg := &FunctionScalingConfig{MaximumConcurrency: input.MaximumConcurrency}
	b.functionScalingConfigs[name] = cfg

	out := *cfg
	out.FunctionArn = fn.FunctionArn

	return &out, nil
}

// GetLayerVersionByArn retrieves a layer version by its full ARN.
func (b *InMemoryBackend) GetLayerVersionByArn(
	layerVersionARN string,
) (*GetLayerVersionOutput, error) {
	layerName, version := parseLayerARN(layerVersionARN)
	if layerName == "" || version == 0 {
		return nil, ErrLayerVersionNotFound
	}

	return b.GetLayerVersion(layerName, version)
}

// TagResource applies the given tags to the named function, updating FunctionConfiguration.Tags.
func (b *InMemoryBackend) TagResource(functionName string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	fn, ok := b.functions[functionName]
	if !ok {
		return ErrFunctionNotFound
	}

	fn.Tags = maps.Clone(fn.Tags)
	maps.Copy(fn.Tags, tags)

	return nil
}

// UntagResource removes the specified tag keys from the named function's FunctionConfiguration.Tags.
func (b *InMemoryBackend) UntagResource(functionName string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	fn, ok := b.functions[functionName]
	if !ok {
		return ErrFunctionNotFound
	}

	fn.Tags = maps.Clone(fn.Tags)
	for _, k := range tagKeys {
		delete(fn.Tags, k)
	}

	return nil
}
