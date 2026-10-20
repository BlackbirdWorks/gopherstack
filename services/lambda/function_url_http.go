package lambda

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	functionURLAuthAWSIAM    = "AWS_IAM"
	functionURLAuthNone      = "NONE"
	functionURLModeBuffered  = "BUFFERED"
	functionURLModeStreaming = "RESPONSE_STREAM"

	functionURLForbiddenMsg = "Forbidden. For troubleshooting Function URL authorization issues, " +
		"see: https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html"

	// maxFunctionURLBodyBytes caps the request body (the synchronous invoke payload limit).
	maxFunctionURLBodyBytes = 6 * 1024 * 1024

	urlExtraHeaders        = 4
	functionURLMaxAgeLimit = 86400
	functionURLMinStatus   = 100
	functionURLMaxStatus   = 599
	functionURLStreamSep   = "\x00\x00\x00\x00\x00\x00\x00\x00"
	functionURLTimeLayout  = "02/Jan/2006:15:04:05 -0700"
)

type lambdaURLHTTPContext struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	Protocol  string `json:"protocol"`
	SourceIP  string `json:"sourceIp"`
	UserAgent string `json:"userAgent"`
}

type lambdaURLIAM struct {
	AccessKey string `json:"accessKey"`
	AccountID string `json:"accountId"`
	CallerID  string `json:"callerId"`
	UserARN   string `json:"userArn"`
	UserID    string `json:"userId"`
}

type lambdaURLAuthorizer struct {
	IAM lambdaURLIAM `json:"iam"`
}

type lambdaURLRequestContext struct {
	Authorizer   *lambdaURLAuthorizer `json:"authorizer,omitempty"`
	HTTP         lambdaURLHTTPContext `json:"http"`
	AccountID    string               `json:"accountId"`
	APIID        string               `json:"apiId"`
	DomainName   string               `json:"domainName"`
	DomainPrefix string               `json:"domainPrefix"`
	RequestID    string               `json:"requestId"`
	Stage        string               `json:"stage"`
	Time         string               `json:"time"`
	TimeEpoch    int64                `json:"timeEpoch"`
}

// lambdaURLEvent is the payload format 2.0 event a function URL delivers.
type lambdaURLEvent struct {
	Headers               map[string]string       `json:"headers"`
	QueryStringParameters map[string]string       `json:"queryStringParameters,omitempty"`
	RawPath               string                  `json:"rawPath"`
	RawQueryString        string                  `json:"rawQueryString"`
	Body                  string                  `json:"body,omitempty"`
	Version               string                  `json:"version"`
	RouteKey              string                  `json:"routeKey"`
	Cookies               []string                `json:"cookies,omitempty"`
	RequestContext        lambdaURLRequestContext `json:"requestContext"`
	IsBase64Encoded       bool                    `json:"isBase64Encoded"`
}

// lambdaURLResponse is the payload format 2.0 response a function returns.
type lambdaURLResponse struct {
	Headers         map[string]string `json:"headers,omitempty"`
	Body            string            `json:"body,omitempty"`
	Cookies         []string          `json:"cookies,omitempty"`
	StatusCode      int               `json:"statusCode"`
	IsBase64Encoded bool              `json:"isBase64Encoded,omitempty"`
}

// urlRequestMeta is the per-URL context stamped on an event.
type urlRequestMeta struct {
	urlID     string
	accountID string
	iam       bool
}

// validateFunctionURLSettings checks AuthType, InvokeMode and Cors the way Lambda does.
func validateFunctionURLSettings(authType, invokeMode string, cors *FunctionURLCors) error {
	if authType != "" && authType != functionURLAuthNone && authType != functionURLAuthAWSIAM {
		return fmt.Errorf("%w: AuthType must be NONE or AWS_IAM", ErrInvalidParameterValue)
	}

	if invokeMode != "" && invokeMode != functionURLModeBuffered && invokeMode != functionURLModeStreaming {
		return fmt.Errorf("%w: InvokeMode must be BUFFERED or RESPONSE_STREAM", ErrInvalidParameterValue)
	}

	if cors != nil && (cors.MaxAge < 0 || cors.MaxAge > functionURLMaxAgeLimit) {
		return fmt.Errorf("%w: Cors.MaxAge must be between 0 and 86400", ErrInvalidParameterValue)
	}

	return nil
}

// buildFunctionURLHandler serves one function's local per-URL listener.
func (b *InMemoryBackend) buildFunctionURLHandler(functionName, qualifier string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b.serveFunctionURL(w, r, functionName, qualifier)
	}
}

// lookupFunctionURLConfig returns a copy of the config stored under key, or nil.
func (b *InMemoryBackend) lookupFunctionURLConfig(key string) *FunctionURLConfig {
	b.mu.RLock("lookupFunctionURLConfig")
	defer b.mu.RUnlock()

	if cfg, ok := b.functionURLConfigs.Get(key); ok {
		return cloneFunctionURLConfig(cfg)
	}

	return nil
}

// ServeFunctionURLHost serves r when its Host names one of this backend's function
// URLs. It reports false when the host is not a function URL host.
func (b *InMemoryBackend) ServeFunctionURLHost(w http.ResponseWriter, r *http.Request) bool {
	urlID, _, ok := parseFunctionURLHost(r.Host)
	if !ok {
		return false
	}

	name, qualifier, found := b.functionForURLID(urlID)
	if !found {
		writeFunctionURLError(w, http.StatusNotFound, "Not Found")

		return true
	}

	b.serveFunctionURL(w, r, name, qualifier)

	return true
}

// functionForURLID resolves a url-id to the function (and alias) that owns it.
func (b *InMemoryBackend) functionForURLID(urlID string) (string, string, bool) {
	b.mu.RLock("functionForURLID")
	defer b.mu.RUnlock()

	for _, cfg := range b.functionURLConfigs.All() {
		if id, _, ok := parseFunctionURLHost(urlHostname(cfg.FunctionURL)); ok && id == urlID {
			name, qualifier := functionNameAndQualifierFromARN(cfg.FunctionArn)

			return name, qualifier, true
		}
	}

	return "", "", false
}

// FunctionURLAuthTypeForHost returns the AuthType of the URL a host names, or "".
func (b *InMemoryBackend) FunctionURLAuthTypeForHost(host string) string {
	urlID, _, ok := parseFunctionURLHost(host)
	if !ok {
		return ""
	}

	name, qualifier, found := b.functionForURLID(urlID)
	if !found {
		return ""
	}

	if cfg := b.lookupFunctionURLConfig(urlConfigKey(name, qualifier)); cfg != nil {
		return cfg.AuthType
	}

	return ""
}

func urlHostname(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	return u.Host
}

// serveFunctionURL answers one HTTP request to a function URL: CORS preflight,
// auth, invoke, response mapping.
func (b *InMemoryBackend) serveFunctionURL(w http.ResponseWriter, r *http.Request, functionName, qualifier string) {
	cfg := b.lookupFunctionURLConfig(urlConfigKey(functionName, qualifier))
	if cfg == nil {
		writeFunctionURLError(w, http.StatusNotFound, "Not Found")

		return
	}

	if r.Method == http.MethodOptions && cfg.Cors != nil && r.Header.Get("Origin") != "" {
		applyCORSHeaders(w, r, cfg.Cors, true)
		w.WriteHeader(http.StatusOK)

		return
	}

	if authErr := b.enforceFunctionURLAuth(cfg, r); authErr != nil {
		applyCORSHeaders(w, r, cfg.Cors, false)
		w.Header().Set("X-Amzn-Errortype", "AccessDeniedException")
		writeFunctionURLError(w, http.StatusForbidden, functionURLForbiddenMsg)

		return
	}

	meta := urlRequestMeta{
		accountID: b.accountID,
		iam:       cfg.AuthType == functionURLAuthAWSIAM,
		urlID:     urlIDFromConfig(cfg),
	}

	payload, buildErr := b.buildURLEvent(r, meta)
	if buildErr != nil {
		http.Error(w, buildErr.Error(), http.StatusInternalServerError)

		return
	}

	result, _, fnErr, _, invokeErr := b.InvokeFunctionWithQualifier(
		r.Context(), functionName, qualifier, "", "", InvocationTypeRequestResponse, payload,
	)

	switch {
	case errors.Is(invokeErr, ErrFunctionNotFound):
		writeFunctionURLError(w, http.StatusNotFound, "Not Found")
	case invokeErr != nil:
		http.Error(w, invokeErr.Error(), http.StatusInternalServerError)
	case fnErr != "":
		writeFunctionURLError(w, http.StatusBadGateway, "Internal Server Error")
	default:
		applyCORSHeaders(w, r, cfg.Cors, false)
		writeFunctionURLResult(w, result, cfg.InvokeMode == functionURLModeStreaming)
	}
}

func urlIDFromConfig(cfg *FunctionURLConfig) string {
	id, _, _ := parseFunctionURLHost(urlHostname(cfg.FunctionURL))

	return id
}

func writeFunctionURLError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	body, _ := json.Marshal(map[string]string{"Message": message})
	_, _ = w.Write(body)
}

// enforceFunctionURLAuth verifies the request against the URL's AuthType. AWS_IAM
// requires a valid lambda-scoped SigV4 signature; NONE is open.
func (b *InMemoryBackend) enforceFunctionURLAuth(cfg *FunctionURLConfig, r *http.Request) error {
	if cfg == nil || cfg.AuthType != functionURLAuthAWSIAM {
		return nil
	}

	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") ||
		sigV4Service(r.Header.Get("Authorization")) != lambdaWorkerService {
		return ErrFunctionURLForbidden
	}

	b.mu.RLock("functionURLSecret")
	secret := b.sigV4Secret
	b.mu.RUnlock()

	if sErr := httputils.NewSigV4Validator(secret).Verify(r); sErr != nil {
		return ErrFunctionURLForbidden
	}

	return nil
}

// sigV4Credential returns the Credential= scope parts: access key, date, region, service.
func sigV4Credential(auth string) []string {
	_, after, ok := strings.Cut(auth, "Credential=")
	if !ok {
		return nil
	}

	cred, _, _ := strings.Cut(after, ",")

	return strings.Split(strings.TrimSpace(cred), "/")
}

const sigV4ServiceIdx = 3

func sigV4Service(auth string) string {
	if parts := sigV4Credential(auth); len(parts) > sigV4ServiceIdx {
		return parts[sigV4ServiceIdx]
	}

	return ""
}

func sigV4AccessKey(auth string) string {
	if parts := sigV4Credential(auth); len(parts) > 0 {
		return parts[0]
	}

	return ""
}

// applyCORSHeaders writes the CORS response headers when cors is set, the request
// carries an Origin and that origin is allowed. preflight adds methods/headers/max-age.
func applyCORSHeaders(w http.ResponseWriter, r *http.Request, cors *FunctionURLCors, preflight bool) {
	origin := r.Header.Get("Origin")
	if cors == nil || origin == "" {
		return
	}

	allow, ok := corsAllowOrigin(cors, origin)
	if !ok {
		return
	}

	h := w.Header()
	h.Set("Access-Control-Allow-Origin", allow)
	h.Add("Vary", "Origin")

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

// corsAllowOrigin resolves Access-Control-Allow-Origin: "*" for a wildcard
// without credentials, otherwise the echoed origin when it is allowed.
func corsAllowOrigin(cors *FunctionURLCors, origin string) (string, bool) {
	for _, a := range cors.AllowOrigins {
		switch {
		case a == "*" && !cors.AllowCredentials:
			return "*", true
		case a == "*" || a == origin:
			return origin, true
		}
	}

	return "", false
}

// buildURLEventPayload converts r to a function URL event with no per-URL context.
func (b *InMemoryBackend) buildURLEventPayload(r *http.Request) ([]byte, error) {
	return b.buildURLEvent(r, urlRequestMeta{accountID: b.accountID})
}

// buildURLEvent converts an HTTP request to a payload format 2.0 event.
func (b *InMemoryBackend) buildURLEvent(r *http.Request, meta urlRequestMeta) ([]byte, error) {
	var bodyBytes []byte

	if r.Body != nil {
		var readErr error

		bodyBytes, readErr = io.ReadAll(http.MaxBytesReader(nil, r.Body, maxFunctionURLBodyBytes))
		if readErr != nil {
			return nil, fmt.Errorf("failed to read request body: %w", readErr)
		}
	}

	ip := sourceIP(r)
	now := time.Now().UTC()
	path := r.URL.EscapedPath()

	if path == "" {
		path = "/"
	}

	event := lambdaURLEvent{
		Version:               "2.0",
		RouteKey:              "$default",
		RawPath:               path,
		RawQueryString:        r.URL.RawQuery,
		Headers:               urlEventHeaders(r, ip),
		Cookies:               requestCookies(r),
		QueryStringParameters: flattenQuery(r.URL.Query()),
		RequestContext: lambdaURLRequestContext{
			HTTP: lambdaURLHTTPContext{
				Method:    r.Method,
				Path:      path,
				Protocol:  r.Proto,
				SourceIP:  ip,
				UserAgent: r.UserAgent(),
			},
			AccountID:    meta.accountID,
			APIID:        meta.urlID,
			RequestID:    uuid.New().String(),
			Stage:        "$default",
			DomainName:   r.Host,
			DomainPrefix: domainPrefix(r.Host),
			TimeEpoch:    now.UnixMilli(),
			Time:         now.Format(functionURLTimeLayout),
		},
	}

	if meta.iam {
		event.RequestContext.Authorizer = urlIAMAuthorizer(r, meta.accountID)
	}

	event.Body, event.IsBase64Encoded = encodeURLBody(bodyBytes, r.Header.Get("Content-Type"))

	return json.Marshal(event)
}

func urlIAMAuthorizer(r *http.Request, accountID string) *lambdaURLAuthorizer {
	key := sigV4AccessKey(r.Header.Get("Authorization"))

	return &lambdaURLAuthorizer{IAM: lambdaURLIAM{
		AccessKey: key,
		AccountID: accountID,
		CallerID:  key,
		UserARN:   awsmeta.CallerArn(r.Context()),
		UserID:    key,
	}}
}

func domainPrefix(host string) string {
	prefix, _, _ := strings.Cut(host, ".")

	return prefix
}

func sourceIP(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || ip == "" {
		return r.RemoteAddr
	}

	return ip
}

// urlEventHeaders lowercases request headers, drops Cookie (surfaced as cookies)
// and adds the host and forwarding headers a real function URL front end sets.
func urlEventHeaders(r *http.Request, ip string) map[string]string {
	headers := make(map[string]string, len(r.Header)+urlExtraHeaders)

	for k, vs := range r.Header {
		if strings.EqualFold(k, "Cookie") {
			continue
		}

		headers[strings.ToLower(k)] = strings.Join(vs, ",")
	}

	headers["host"] = r.Host

	if _, ok := headers["x-forwarded-for"]; !ok {
		headers["x-forwarded-for"] = ip
	}

	if _, ok := headers["x-forwarded-proto"]; !ok {
		headers["x-forwarded-proto"] = "https"
	}

	if _, ok := headers["x-forwarded-port"]; !ok {
		headers["x-forwarded-port"] = "443"
	}

	return headers
}

// encodeURLBody returns the event body and whether it was base64-encoded: text
// content types pass through verbatim, everything else is base64.
func encodeURLBody(body []byte, contentType string) (string, bool) {
	if len(body) == 0 {
		return "", false
	}

	if isTextContentType(contentType) && utf8.Valid(body) {
		return string(body), false
	}

	return base64.StdEncoding.EncodeToString(body), true
}

func isTextContentType(contentType string) bool {
	if contentType == "" {
		return true
	}

	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}

	switch {
	case strings.HasPrefix(mt, "text/"),
		strings.HasSuffix(mt, "+json"),
		strings.HasSuffix(mt, "+xml"):
		return true
	}

	switch mt {
	case "application/json", "application/xml", "application/javascript",
		"application/ecmascript", "application/x-www-form-urlencoded":
		return true
	}

	return false
}

// requestCookies extracts the request cookies as "name=value" strings.
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

// flattenQuery collapses url.Values to AWS's single-value map (repeated keys comma-joined).
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

// writeFunctionURLResponse writes a buffered function result.
func writeFunctionURLResponse(w http.ResponseWriter, result []byte) {
	writeFunctionURLResult(w, result, false)
}

// writeFunctionURLResult maps a function's return value onto the HTTP response. In
// streaming mode a "<prelude JSON><8 NUL bytes><body>" result is honoured and the
// body is flushed to the client.
func writeFunctionURLResult(w http.ResponseWriter, result []byte, streaming bool) {
	var resp lambdaURLResponse

	if streaming {
		if prelude, rest, ok := bytes.Cut(result, []byte(functionURLStreamSep)); ok &&
			json.Unmarshal(prelude, &resp) == nil {
			writeURLResponseHead(w, &resp)
			writeFlushed(w, rest)

			return
		}
	}

	if json.Unmarshal(result, &resp) != nil || resp.StatusCode == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		writeBody(w, result, streaming)

		return
	}

	payload, err := urlResponseBody(&resp)
	if err != nil || resp.StatusCode < functionURLMinStatus || resp.StatusCode > functionURLMaxStatus {
		writeFunctionURLError(w, http.StatusBadGateway, "Internal Server Error")

		return
	}

	writeURLResponseHead(w, &resp)
	writeBody(w, payload, streaming)
}

func urlResponseBody(resp *lambdaURLResponse) ([]byte, error) {
	if resp.IsBase64Encoded {
		return base64.StdEncoding.DecodeString(resp.Body)
	}

	return []byte(resp.Body), nil
}

func writeURLResponseHead(w http.ResponseWriter, resp *lambdaURLResponse) {
	h := w.Header()

	for k, v := range resp.Headers {
		h.Set(k, v)
	}

	for _, ck := range resp.Cookies {
		h.Add("Set-Cookie", ck)
	}

	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/json")
	}

	status := resp.StatusCode
	if status == 0 {
		status = http.StatusOK
	}

	w.WriteHeader(status)
}

func writeBody(w http.ResponseWriter, body []byte, streaming bool) {
	if streaming {
		writeFlushed(w, body)

		return
	}

	_, _ = w.Write(body) //nolint:gosec // function output is passed through verbatim
}

func writeFlushed(w http.ResponseWriter, body []byte) {
	_, _ = w.Write(body) //nolint:gosec // function output is passed through verbatim
	_ = http.NewResponseController(w).Flush()
}
