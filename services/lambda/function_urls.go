package lambda

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// functionURLServer holds a running HTTP listener for a Lambda function URL.
type functionURLServer struct {
	listener net.Listener
	server   *http.Server
	hostname string
	addr     string
	port     int
}

// urlConfigKey scopes a function URL config to its function, or to an alias
// when qualifier is set.
func urlConfigKey(name, qualifier string) string {
	if qualifier == "" {
		return name
	}

	return name + ":" + qualifier
}

// normalizeURLQualifier folds $LATEST into the unqualified URL.
func normalizeURLQualifier(qualifier string) string {
	if qualifier == versionLatest {
		return ""
	}

	return qualifier
}

// newFunctionURLHost mints the AWS-shaped host <32-char url-id>.lambda-url.<region>.on.aws.
func (b *InMemoryBackend) newFunctionURLHost() string {
	return awsid.NewN("", functionURLIDLen) + functionURLHostInfix + b.region + functionURLHostSuffix
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
	return b.CreateFunctionURLConfigQualified(ctx, functionName, "", authType, cors, invokeMode)
}

// CreateFunctionURLConfigQualified is CreateFunctionURLConfig scoped to an alias.
func (b *InMemoryBackend) CreateFunctionURLConfigQualified(
	ctx context.Context,
	functionName, qualifier, authType string,
	cors *FunctionURLCors,
	invokeMode string,
) (*FunctionURLConfig, error) {
	qualifier = normalizeURLQualifier(qualifier)
	key := urlConfigKey(functionName, qualifier)

	if err := validateFunctionURLSettings(authType, invokeMode, cors); err != nil {
		return nil, err
	}

	checkErr := func() error {
		b.mu.Lock("CreateFunctionURLConfig.check")
		defer b.mu.Unlock()

		if _, ok := b.functions.Get(functionName); !ok {
			return ErrFunctionNotFound
		}

		if qualifier != "" {
			if _, ok := b.aliases.Get(aliasKey(functionName, qualifier)); !ok {
				return ErrFunctionNotFound
			}
		}

		if _, exists := b.functionURLConfigs.Get(key); exists {
			return ErrFunctionAlreadyExists
		}

		return nil
	}()
	if checkErr != nil {
		return nil, checkErr
	}

	// Allocate port and start listener outside the lock (IO).
	urlStr, srv, startErr := b.doAllocateAndStart(ctx, functionName, qualifier)
	if startErr != nil {
		return nil, startErr
	}

	if invokeMode == "" {
		invokeMode = "BUFFERED"
	}

	now := time.Now().UTC().Format(time.RFC3339)
	cfg := &FunctionURLConfig{
		FunctionArn:      buildURLARN(b.region, b.accountID, key),
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

	if _, exists := b.functionURLConfigs.Get(key); exists {
		go b.releaseURLServer(context.WithoutCancel(ctx), srv)

		return nil, ErrFunctionAlreadyExists
	}

	b.functionURLServers[key] = srv
	b.functionURLConfigs.Put(cfg)

	return cloneFunctionURLConfig(cfg), nil
}

// doAllocateAndStart mints the function's public URL host and, when a port
// allocator is present, starts the per-function local listener.
func (b *InMemoryBackend) doAllocateAndStart(
	ctx context.Context,
	functionName, qualifier string,
) (string, *functionURLServer, error) {
	host := b.newFunctionURLHost()
	urlStr := "https://" + host + "/"
	srv := &functionURLServer{hostname: host}

	if b.portAlloc != nil {
		key := urlConfigKey(functionName, qualifier)

		port, allocErr := b.portAlloc.Acquire("lambda-url:" + key)
		if allocErr != nil {
			return "", nil, fmt.Errorf("%w: port allocation failed: %w", ErrLambdaUnavailable, allocErr)
		}

		ls, listenErr := b.startFunctionURLServer(ctx, functionName, qualifier, port)
		if listenErr != nil {
			_ = b.portAlloc.Release(port)

			return "", nil, fmt.Errorf(
				"%w: failed to start URL listener: %w",
				ErrLambdaUnavailable,
				listenErr,
			)
		}

		ls.hostname = host
		srv = ls
	}

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(host)
	}

	return urlStr, srv, nil
}

// cloneFunctionURLConfig copies cfg so a caller can't race UpdateFunctionURLConfig,
// which mutates the stored config's fields in place.
func cloneFunctionURLConfig(cfg *FunctionURLConfig) *FunctionURLConfig {
	cp := *cfg

	return &cp
}

// GetFunctionURLConfig returns the function URL config for a function.
func (b *InMemoryBackend) GetFunctionURLConfig(functionName string) (*FunctionURLConfig, error) {
	return b.GetFunctionURLConfigQualified(functionName, "")
}

// GetFunctionURLConfigQualified is GetFunctionURLConfig scoped to an alias.
func (b *InMemoryBackend) GetFunctionURLConfigQualified(functionName, qualifier string) (*FunctionURLConfig, error) {
	b.mu.RLock("GetFunctionURLConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.functionURLConfigs.Get(urlConfigKey(functionName, normalizeURLQualifier(qualifier)))
	if !ok {
		return nil, ErrFunctionURLNotFound
	}

	return cloneFunctionURLConfig(cfg), nil
}

// DeleteFunctionURLConfig removes the function URL config, stops the listener, and deregisters DNS.
func (b *InMemoryBackend) DeleteFunctionURLConfig(functionName string) error {
	return b.DeleteFunctionURLConfigQualified(functionName, "")
}

// DeleteFunctionURLConfigQualified is DeleteFunctionURLConfig scoped to an alias.
func (b *InMemoryBackend) DeleteFunctionURLConfigQualified(functionName, qualifier string) error {
	key := urlConfigKey(functionName, normalizeURLQualifier(qualifier))

	var srv *functionURLServer

	found := func() bool {
		b.mu.Lock("DeleteFunctionURLConfig")
		defer b.mu.Unlock()

		if _, ok := b.functionURLConfigs.Get(key); !ok {
			return false
		}

		b.functionURLConfigs.Delete(key)

		srv = b.functionURLServers[key]
		delete(b.functionURLServers, key)

		return true
	}()
	if !found {
		return ErrFunctionURLNotFound
	}

	b.releaseURLServer(b.ctx, srv)

	return nil
}

// releaseURLServer shuts a detached URL listener down, frees its port and
// deregisters its DNS name. Must not hold b.mu.
func (b *InMemoryBackend) releaseURLServer(ctx context.Context, srv *functionURLServer) {
	if srv == nil {
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), containerShutdownTimeout)
	defer cancel()

	b.stopURLListener(shutdownCtx, srv)

	if srv.hostname == "" {
		return
	}

	b.mu.RLock("releaseURLServer")
	dns := b.dnsRegistrar
	b.mu.RUnlock()

	if dns != nil {
		dns.Deregister(srv.hostname)
	}
}

// stopURLListener shuts the local listener down (when one runs) and frees its port.
func (b *InMemoryBackend) stopURLListener(ctx context.Context, srv *functionURLServer) {
	if srv.server != nil {
		_ = srv.server.Shutdown(ctx)
	}

	if srv.port != 0 && b.portAlloc != nil {
		_ = b.portAlloc.Release(srv.port)
	}
}

// FunctionURLListenAddr returns the local per-function listener address for a
// function URL (empty when no listener runs).
func (b *InMemoryBackend) FunctionURLListenAddr(functionName, qualifier string) string {
	b.mu.RLock("FunctionURLListenAddr")
	defer b.mu.RUnlock()

	if srv := b.functionURLServers[urlConfigKey(functionName, normalizeURLQualifier(qualifier))]; srv != nil {
		return srv.addr
	}

	return ""
}

// functionURLReadHeaderTimeout is the timeout for reading HTTP request headers on the function URL listener.
const functionURLReadHeaderTimeout = 30 * time.Second

// startFunctionURLServer starts an HTTP server on the given port that converts HTTP requests
// to Lambda invocation events and returns the function's response.
func (b *InMemoryBackend) startFunctionURLServer(
	ctx context.Context,
	functionName, qualifier string,
	port int,
) (*functionURLServer, error) {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", b.buildFunctionURLHandler(functionName, qualifier))

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

	return &functionURLServer{listener: ln, server: srv, port: port, addr: addr}, nil
}

// buildURLARN constructs an ARN for a Lambda function URL.
func buildURLARN(region, accountID, functionName string) string {
	return arn.Build("lambda", region, accountID, "function:"+functionName)
}

// UpdateFunctionURLConfig updates an existing function URL config.
func (b *InMemoryBackend) UpdateFunctionURLConfig(
	functionName, authType string,
	cors *FunctionURLCors,
	invokeMode string,
) (*FunctionURLConfig, error) {
	return b.UpdateFunctionURLConfigQualified(functionName, "", authType, cors, invokeMode)
}

// UpdateFunctionURLConfigQualified is UpdateFunctionURLConfig scoped to an alias.
func (b *InMemoryBackend) UpdateFunctionURLConfigQualified(
	functionName, qualifier, authType string,
	cors *FunctionURLCors,
	invokeMode string,
) (*FunctionURLConfig, error) {
	if err := validateFunctionURLSettings(authType, invokeMode, cors); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateFunctionURLConfig")
	defer b.mu.Unlock()

	cfg, ok := b.functionURLConfigs.Get(urlConfigKey(functionName, normalizeURLQualifier(qualifier)))
	if !ok {
		return nil, ErrFunctionURLNotFound
	}

	if authType != "" {
		cfg.AuthType = authType
	}

	if cors != nil {
		cfg.Cors = cors
	}

	if invokeMode != "" {
		cfg.InvokeMode = invokeMode
	}

	cfg.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)
	b.functionURLConfigs.Put(cfg)

	return cloneFunctionURLConfig(cfg), nil
}

// ListFunctionURLConfigs returns all function URL configs.
func (b *InMemoryBackend) ListFunctionURLConfigs() []*FunctionURLConfig {
	b.mu.RLock("ListFunctionURLConfigs")
	defer b.mu.RUnlock()

	return sortedURLConfigs(b.functionURLConfigs.All(), "")
}

// ListFunctionURLConfigsForFunction returns every URL config of one function
// (unqualified and alias-scoped), paginated by marker.
func (b *InMemoryBackend) ListFunctionURLConfigsForFunction(
	functionName, marker string,
	maxItems int,
) ([]*FunctionURLConfig, string, error) {
	b.mu.RLock("ListFunctionURLConfigsForFunction")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(functionName); !ok {
		return nil, "", ErrFunctionNotFound
	}

	p := page.New(sortedURLConfigs(b.functionURLConfigs.All(), functionName), marker, maxItems, lambdaDefaultMaxItems)

	return p.Data, p.Next, nil
}

// sortedURLConfigs clones cfgs (only functionName's when set), sorted by ARN.
func sortedURLConfigs(cfgs []*FunctionURLConfig, functionName string) []*FunctionURLConfig {
	out := make([]*FunctionURLConfig, 0, len(cfgs))

	for _, cfg := range cfgs {
		if name, _ := functionNameAndQualifierFromARN(cfg.FunctionArn); functionName == "" || name == functionName {
			out = append(out, cloneFunctionURLConfig(cfg))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].FunctionArn < out[j].FunctionArn
	})

	return out
}

// urlConfigKeysForFunctionLocked returns the keys of every URL config and server
// belonging to functionName. Caller holds b.mu.
func (b *InMemoryBackend) urlConfigKeysForFunctionLocked(functionName string) []string {
	seen := map[string]struct{}{}

	for _, cfg := range b.functionURLConfigs.All() {
		if name, qualifier := functionNameAndQualifierFromARN(cfg.FunctionArn); name == functionName {
			seen[urlConfigKey(name, qualifier)] = struct{}{}
		}
	}

	for key := range b.functionURLServers {
		if key == functionName || strings.HasPrefix(key, functionName+":") {
			seen[key] = struct{}{}
		}
	}

	return slices.Collect(maps.Keys(seen))
}

// detachURLServersLocked removes the function's URL configs and returns their
// listeners for shutdown outside the lock. Caller holds b.mu.
func (b *InMemoryBackend) detachURLServersLocked(functionName string) []*functionURLServer {
	var servers []*functionURLServer

	for _, key := range b.urlConfigKeysForFunctionLocked(functionName) {
		if srv, ok := b.functionURLServers[key]; ok {
			servers = append(servers, srv)
		}

		delete(b.functionURLServers, key)
		b.functionURLConfigs.Delete(key)
	}

	return servers
}
