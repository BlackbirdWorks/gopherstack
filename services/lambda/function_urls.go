package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/google/uuid"
)

// functionURLServer holds a running HTTP listener for a Lambda function URL.
type functionURLServer struct {
	listener net.Listener
	server   *http.Server
	port     int
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
	checkErr := func() error {
		b.mu.Lock("CreateFunctionURLConfig.check")
		defer b.mu.Unlock()

		if _, ok := b.functions.Get(functionName); !ok {
			return ErrFunctionNotFound
		}

		if _, exists := b.functionURLConfigs.Get(functionName); exists {
			return ErrFunctionAlreadyExists
		}

		return nil
	}()
	if checkErr != nil {
		return nil, checkErr
	}

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

	if _, exists := b.functionURLConfigs.Get(functionName); exists {
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

	b.functionURLConfigs.Put(cfg)

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
		defer b.mu.Unlock()

		b.functionURLServers[functionName] = srv
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

	cfg, ok := b.functionURLConfigs.Get(functionName)
	if !ok {
		return nil, ErrFunctionURLNotFound
	}

	return cfg, nil
}

// DeleteFunctionURLConfig removes the function URL config, stops the listener, and deregisters DNS.
func (b *InMemoryBackend) DeleteFunctionURLConfig(functionName string) error {
	var (
		found    bool
		srv      *functionURLServer
		dns      DNSRegistrar
		hostname string
	)

	func() {
		b.mu.Lock("DeleteFunctionURLConfig")
		defer b.mu.Unlock()

		if _, ok := b.functionURLConfigs.Get(functionName); !ok {
			return
		}

		found = true

		b.functionURLConfigs.Delete(functionName)

		srv = b.functionURLServers[functionName]
		delete(b.functionURLServers, functionName)
		dns = b.dnsRegistrar
		hostname = b.functionURLHostname(functionName)
	}()

	if !found {
		return ErrFunctionURLNotFound
	}

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
	Headers               map[string]string       `json:"headers"`
	QueryStringParameters map[string]string       `json:"queryStringParameters,omitempty"`
	PathParameters        map[string]string       `json:"pathParameters,omitempty"`
	RawPath               string                  `json:"rawPath"`
	RawQueryString        string                  `json:"rawQueryString"`
	Body                  string                  `json:"body,omitempty"`
	Version               string                  `json:"version"`
	RouteKey              string                  `json:"routeKey"`
	Cookies               []string                `json:"cookies,omitempty"`
	RequestContext        lambdaURLRequestContext `json:"requestContext"`
	IsBase64Encoded       bool                    `json:"isBase64Encoded"`
}

// lambdaURLResponse is a simplified Lambda Function URL response.
type lambdaURLResponse struct {
	Headers         map[string]string `json:"headers,omitempty"`
	Body            string            `json:"body,omitempty"`
	StatusCode      int               `json:"statusCode"`
	IsBase64Encoded bool              `json:"isBase64Encoded,omitempty"`
}

// buildFunctionURLHandler builds an [http.HandlerFunc] that invokes the Lambda function.
// It enforces the configured AuthType (AWS_IAM verifies SigV4, returning 403 on a bad
// or missing signature) and applies CORS: OPTIONS preflight requests are answered
// directly with the configured CORS headers, and those headers are echoed on real
// responses.
func (b *InMemoryBackend) buildFunctionURLHandler(functionName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg := b.lookupFunctionURLConfig(functionName)

		cors := functionURLCors(cfg)

		// Answer CORS preflight requests without invoking the function.
		if r.Method == http.MethodOptions && cors != nil {
			applyCORSHeaders(w, r, cors, true)
			w.WriteHeader(http.StatusOK)

			return
		}

		if authErr := b.enforceFunctionURLAuth(cfg, r); authErr != nil {
			if cors != nil {
				applyCORSHeaders(w, r, cors, false)
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"Message":"Forbidden"}`))

			return
		}

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

		if cors != nil {
			applyCORSHeaders(w, r, cors, false)
		}

		writeFunctionURLResponse(w, result)
	}
}

// lookupFunctionURLConfig returns a copy-free reference to the function's URL config.
func (b *InMemoryBackend) lookupFunctionURLConfig(functionName string) *FunctionURLConfig {
	b.mu.RLock("lookupFunctionURLConfig")
	defer b.mu.RUnlock()

	cfg, _ := b.functionURLConfigs.Get(functionName)

	return cfg
}

// functionURLCors returns the CORS config when present.
func functionURLCors(cfg *FunctionURLConfig) *FunctionURLCors {
	if cfg == nil {
		return nil
	}

	return cfg.Cors
}

// enforceFunctionURLAuth verifies the request against the URL's AuthType. For
// AWS_IAM it requires a valid SigV4 signature; NONE (or an unset config) is open.
func (b *InMemoryBackend) enforceFunctionURLAuth(cfg *FunctionURLConfig, r *http.Request) error {
	if cfg == nil || cfg.AuthType != functionURLAuthAWSIAM {
		return nil
	}

	var secret string

	func() {
		b.mu.RLock("functionURLSecret")
		defer b.mu.RUnlock()

		secret = b.sigV4Secret
	}()

	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
		return ErrFunctionURLForbidden
	}

	if sErr := httputils.NewSigV4Validator(secret).Verify(r); sErr != nil {
		return ErrFunctionURLForbidden
	}

	return nil
}

// functionURLAuthAWSIAM is the AuthType requiring SigV4 verification.
const functionURLAuthAWSIAM = "AWS_IAM"

// applyCORSHeaders writes the CORS response headers for a function URL request.
// When preflight is true the full preflight header set (methods, headers, max-age)
// is emitted; otherwise only the origin/credentials/expose-headers subset applies.
func applyCORSHeaders(w http.ResponseWriter, r *http.Request, cors *FunctionURLCors, preflight bool) {
	origin := r.Header.Get("Origin")
	h := w.Header()

	h.Set("Access-Control-Allow-Origin", corsAllowOrigin(cors.AllowOrigins, origin))

	if cors.AllowCredentials {
		h.Set("Access-Control-Allow-Credentials", "true")
	}

	if len(cors.ExposeHeaders) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(cors.ExposeHeaders, ","))
	}

	if !preflight {
		return
	}

	if len(cors.AllowMethods) > 0 {
		h.Set("Access-Control-Allow-Methods", strings.Join(cors.AllowMethods, ","))
	}

	if len(cors.AllowHeaders) > 0 {
		h.Set("Access-Control-Allow-Headers", strings.Join(cors.AllowHeaders, ","))
	}

	if cors.MaxAge > 0 {
		h.Set("Access-Control-Max-Age", strconv.Itoa(cors.MaxAge))
	}
}

// corsAllowOrigin resolves the Access-Control-Allow-Origin value. A wildcard or
// an empty allow-list echoes "*"; otherwise the request origin is echoed when it
// is in the allow-list, falling back to the first configured origin.
func corsAllowOrigin(allowed []string, origin string) string {
	if len(allowed) == 0 {
		return "*"
	}

	for _, a := range allowed {
		if a == "*" {
			return "*"
		}

		if a == origin {
			return origin
		}
	}

	return allowed[0]
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
		// AWS omits the Cookie header from `headers` and surfaces it via `cookies`.
		if strings.EqualFold(k, "Cookie") {
			continue
		}

		headers[strings.ToLower(k)] = strings.Join(vs, ",")
	}

	event := lambdaURLEvent{
		Version:               "2.0",
		RouteKey:              "$default",
		RawPath:               r.URL.Path,
		RawQueryString:        r.URL.RawQuery,
		Headers:               headers,
		Cookies:               requestCookies(r),
		QueryStringParameters: flattenQuery(r.URL.Query()),
		PathParameters:        map[string]string{},
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

// requestCookies extracts the request cookies as "name=value" strings, matching
// the `cookies` array AWS Lambda Function URLs place in the event payload.
func requestCookies(r *http.Request) []string {
	cookies := r.Cookies()
	if len(cookies) == 0 {
		return nil
	}

	out := make([]string, 0, len(cookies))
	for _, ck := range cookies {
		out = append(out, ck.Name+"="+ck.Value)
	}

	return out
}

// flattenQuery collapses a url.Values into the single-value map AWS uses for
// queryStringParameters. Repeated keys are comma-joined, as AWS does.
func flattenQuery(values url.Values) map[string]string {
	if len(values) == 0 {
		return nil
	}

	out := make(map[string]string, len(values))
	for k, vs := range values {
		out[k] = strings.Join(vs, ",")
	}

	return out
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

// UpdateFunctionURLConfig updates an existing function URL config.
func (b *InMemoryBackend) UpdateFunctionURLConfig(
	functionName, authType string,
	cors *FunctionURLCors,
) (*FunctionURLConfig, error) {
	b.mu.Lock("UpdateFunctionURLConfig")
	defer b.mu.Unlock()

	cfg, ok := b.functionURLConfigs.Get(functionName)
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
	b.functionURLConfigs.Put(cfg)

	return cfg, nil
}

// ListFunctionURLConfigs returns all function URL configs.
func (b *InMemoryBackend) ListFunctionURLConfigs() []*FunctionURLConfig {
	b.mu.RLock("ListFunctionURLConfigs")
	defer b.mu.RUnlock()

	cfgs := b.functionURLConfigs.All()

	sort.Slice(cfgs, func(i, j int) bool {
		return cfgs[i].FunctionArn < cfgs[j].FunctionArn
	})

	return cfgs
}
