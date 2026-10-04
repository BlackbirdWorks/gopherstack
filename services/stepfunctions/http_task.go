package stepfunctions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

const (
	httpService          = "http"
	httpTaskTimeout      = 60 * time.Second
	httpMaxResponseBytes = 262144
	httpMaxRedirects     = 10
	httpMaxTokenBytes    = 1 << 20
	httpStatusPrefix     = "States.Http.StatusCode."
	errCodeHTTPSocket    = "States.Http.Socket"
	errCodeDataLimit     = "States.DataLimitExceeded"
	connErrPrefix        = "Events.ConnectionResource."
	httpUserAgent        = "Amazon|StepFunctions|HttpInvoke|"
	httpRange            = "bytes=0-262144"
	bodyEncodingForm     = "URL_ENCODED"
	authAPIKey           = "API_KEY"
	authBasic            = "BASIC"
	authOAuth            = "OAUTH_CLIENT_CREDENTIALS"
)

// ErrConnectionNotFound is returned by a ConnectionResolver for an unknown connection ARN.
var ErrConnectionNotFound = errors.New("connection not found")

// ErrConnectionInvalidState is returned by a ConnectionResolver for a connection that is not AUTHORIZED.
var ErrConnectionInvalidState = errors.New("connection is not authorized")

// NameValue is one header, query-string or body parameter.
type NameValue struct{ Key, Value string }

// OAuthCredentials configures an OAuth client-credentials token request.
type OAuthCredentials struct {
	Endpoint, Method, ClientID, ClientSecret string
	Headers, Query, Body                     []NameValue
}

// ConnectionCredentials is an EventBridge connection's un-masked auth and invocation parameters.
type ConnectionCredentials struct {
	OAuth                             *OAuthCredentials
	AuthType, APIKeyName, APIKeyValue string
	Username, Password                string
	Headers, Query, Body              []NameValue
}

// ConnectionResolver reads an EventBridge connection's credentials by ARN.
type ConnectionResolver interface {
	ResolveConnectionCredentials(ctx context.Context, connectionARN string) (ConnectionCredentials, error)
}

// forbiddenHTTPHeader reports headers an HTTP Task definition may not set (AWS call-https-apis, Headers).
func forbiddenHTTPHeader(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "x-forwarded-") || strings.HasPrefix(lower, "x-amz-") ||
		strings.HasPrefix(lower, "x-amzn-") {
		return true
	}

	return slices.Contains([]string{
		"a-im", "accept-charset", "accept-datetime", "accept-encoding", "authorization", "cache-control",
		"connection", "content-encoding", "content-md5", "date", "expect", "forwarded", "from", "host",
		"http2-settings", "if-match", "if-modified-since", "if-none-match", "if-range", "if-unmodified-since",
		"max-forwards", "origin", "pragma", "proxy-authorization", "referer", "server", "te", "trailer",
		"transfer-encoding", "upgrade", "via", "warning",
	}, lower)
}

func validHTTPMethod(m string) bool {
	return slices.Contains([]string{
		http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete,
		http.MethodPatch, http.MethodOptions, http.MethodHead,
	}, m)
}

type httpSpec struct {
	body          any
	headers       map[string]string
	query         url.Values
	endpoint      string
	method        string
	connectionARN string
	arrayFormat   string
	formEncoded   bool
}

func runtimeFailure(format string, args ...any) error {
	return &asl.FailError{ErrCode: errCodeRuntime, Cause: fmt.Sprintf(format, args...)}
}

func (a *sdkAdapter) invokeHTTP(ctx context.Context, call asl.SDKCall) (any, error) {
	if call.Action != "invoke" {
		return nil, runtimeFailure("unknown HTTP Task action %q", call.Action)
	}

	spec, err := parseHTTPSpec(call.Params)
	if err != nil {
		return nil, err
	}

	creds, err := a.connectionCredentials(ctx, spec.connectionARN)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: httpTaskTimeout, CheckRedirect: redirectPolicy(creds.APIKeyName)}

	req, err := buildHTTPRequest(ctx, client, spec, creds, call.Region)
	if err != nil {
		return nil, err
	}

	return doHTTP(client, req)
}

func (a *sdkAdapter) connectionCredentials(ctx context.Context, arn string) (ConnectionCredentials, error) {
	if a.conns == nil {
		return ConnectionCredentials{}, &asl.FailError{
			ErrCode: connErrPrefix + "InternalError", Cause: "no EventBridge connection store is configured",
		}
	}

	creds, err := a.conns.ResolveConnectionCredentials(ctx, arn)

	switch {
	case err == nil:
		return creds, nil
	case errors.Is(err, ErrConnectionNotFound):
		return creds, &asl.FailError{ErrCode: connErrPrefix + "ResourceNotFound", Cause: "connection not found: " + arn}
	case errors.Is(err, ErrConnectionInvalidState):
		return creds, &asl.FailError{
			ErrCode: connErrPrefix + "InvalidConnectionState",
			Cause:   "connection is not AUTHORIZED",
		}
	default:
		return creds, &asl.FailError{ErrCode: connErrPrefix + "InternalError", Cause: "connection lookup failed"}
	}
}

func parseHTTPSpec(params any) (httpSpec, error) {
	m, _ := params.(map[string]any)
	spec := httpSpec{body: m["RequestBody"], headers: map[string]string{}, query: url.Values{}}

	spec.endpoint, _ = m["ApiEndpoint"].(string)
	if u, err := url.Parse(spec.endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return spec, runtimeFailure("ApiEndpoint must be an http(s) URL")
	}

	spec.method, _ = m["Method"].(string)
	if !validHTTPMethod(spec.method) {
		return spec, runtimeFailure("Method must be one of GET, POST, PUT, DELETE, PATCH, OPTIONS, HEAD")
	}

	for _, key := range []string{"InvocationConfig", "Authentication"} {
		if cfg, ok := m[key].(map[string]any); ok {
			spec.connectionARN, _ = cfg["ConnectionArn"].(string)
			if spec.connectionARN != "" {
				break
			}
		}
	}

	if spec.connectionARN == "" {
		return spec, runtimeFailure("an HTTP Task requires a ConnectionArn")
	}

	if err := parseHTTPHeaders(m["Headers"], spec.headers); err != nil {
		return spec, err
	}

	if err := parseHTTPQuery(m["QueryParameters"], spec.query); err != nil {
		return spec, err
	}

	return spec, parseHTTPTransform(m["Transform"], &spec)
}

func parseHTTPHeaders(raw any, out map[string]string) error {
	if raw == nil {
		return nil
	}

	h, ok := raw.(map[string]any)
	if !ok {
		return runtimeFailure("Headers must be an object")
	}

	for k, v := range h {
		if forbiddenHTTPHeader(k) {
			return runtimeFailure("header %q is not allowed in an HTTP Task", k)
		}

		out[k] = joinScalars(v)
	}

	return nil
}

func parseHTTPQuery(raw any, out url.Values) error {
	switch q := raw.(type) {
	case nil:
		return nil
	case string:
		parsed, err := url.ParseQuery(q)
		if err != nil {
			return runtimeFailure("QueryParameters is not a valid query string")
		}

		maps.Copy(out, parsed)
	case map[string]any:
		for k, v := range q {
			if items, isList := v.([]any); isList {
				for _, it := range items {
					out.Add(k, formScalar(it))
				}

				continue
			}

			out.Set(k, formScalar(v))
		}
	default:
		return runtimeFailure("QueryParameters must be an object or a string")
	}

	return nil
}

func parseHTTPTransform(raw any, spec *httpSpec) error {
	spec.arrayFormat = arrayIndices

	t, ok := raw.(map[string]any)
	if !ok {
		return nil
	}

	switch enc, _ := t["RequestBodyEncoding"].(string); enc {
	case "", "NONE":
	case bodyEncodingForm:
		spec.formEncoded = true
	default:
		return runtimeFailure("unsupported RequestBodyEncoding %q", enc)
	}

	opts, _ := t["RequestEncodingOptions"].(map[string]any)

	switch f, _ := opts["ArrayFormat"].(string); f {
	case "":
	case arrayIndices, arrayRepeat, arrayCommas, arrayBrackets:
		spec.arrayFormat = f
	default:
		return runtimeFailure("unsupported ArrayFormat %q", f)
	}

	return nil
}

func joinScalars(v any) string {
	if items, ok := v.([]any); ok {
		parts := make([]string, len(items))
		for i, it := range items {
			parts[i] = formScalar(it)
		}

		return strings.Join(parts, ",")
	}

	return formScalar(v)
}

func buildHTTPRequest(
	ctx context.Context, client *http.Client, spec httpSpec, creds ConnectionCredentials, region string,
) (*http.Request, error) {
	for _, h := range creds.Headers {
		spec.headers[h.Key] = h.Value
	}

	for _, q := range creds.Query {
		spec.query.Set(q.Key, q.Value)
	}

	body, err := mergeHTTPBody(spec, creds.Body)
	if err != nil {
		return nil, err
	}

	target, _ := url.Parse(spec.endpoint)

	query := target.Query()
	maps.Copy(query, spec.query)

	target.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, spec.method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, runtimeFailure("cannot build the HTTP request")
	}

	for k, v := range spec.headers {
		req.Header.Set(k, v)
	}

	if len(body) > 0 && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	}

	req.Header.Set("User-Agent", httpUserAgent+region)
	req.Header.Set("Range", httpRange)

	if err = applyConnectionAuth(ctx, client, req, creds); err != nil {
		return nil, err
	}

	return req, nil
}

// mergeHTTPBody overlays connection body parameters on RequestBody and serializes it.
func mergeHTTPBody(spec httpSpec, connBody []NameValue) ([]byte, error) {
	body := spec.body

	if len(connBody) > 0 {
		if _, isString := body.(string); isString {
			return nil, runtimeFailure("a string RequestBody cannot be merged with connection body parameters")
		}

		merged := map[string]any{}
		if m, ok := body.(map[string]any); ok {
			maps.Copy(merged, m)
		}

		for _, p := range connBody {
			merged[p.Key] = p.Value
		}

		body = merged
	}

	switch b := body.(type) {
	case nil:
		return nil, nil
	case string:
		return []byte(b), nil
	case map[string]any:
		if spec.formEncoded {
			return []byte(encodeForm(b, spec.arrayFormat)), nil
		}
	}

	out, err := json.Marshal(body)
	if err != nil {
		return nil, runtimeFailure("RequestBody is not serializable")
	}

	return out, nil
}

func applyConnectionAuth(ctx context.Context, client *http.Client, req *http.Request, c ConnectionCredentials) error {
	switch c.AuthType {
	case authAPIKey:
		if c.APIKeyName != "" {
			req.Header.Set(c.APIKeyName, c.APIKeyValue)
		}
	case authBasic:
		req.SetBasicAuth(c.Username, c.Password)
	case authOAuth:
		if c.OAuth == nil {
			return nil
		}

		token, err := fetchOAuthToken(ctx, client, c.OAuth)
		if err != nil {
			return &asl.FailError{
				ErrCode: connErrPrefix + "InvalidConnectionState", Cause: "OAuth token request failed",
			}
		}

		req.Header.Set("Authorization", "Bearer "+token)
	}

	return nil
}

var errOAuthToken = errors.New("OAuth token request failed")

func fetchOAuthToken(ctx context.Context, client *http.Client, o *OAuthCredentials) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	for _, p := range o.Body {
		form.Set(p.Key, p.Value)
	}

	method := o.Method
	if method == "" {
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(ctx, method, o.Endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(o.ClientID, o.ClientSecret)

	for _, h := range o.Headers {
		req.Header.Set(h.Key, h.Value)
	}

	q := req.URL.Query()
	for _, p := range o.Query {
		q.Set(p.Key, p.Value)
	}

	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, httpMaxTokenBytes))
	if err != nil || resp.StatusCode >= http.StatusBadRequest {
		return "", errOAuthToken
	}

	var tok struct {
		AccessToken string `json:"access_token"`
	}

	if json.Unmarshal(data, &tok) != nil || tok.AccessToken == "" {
		return "", errOAuthToken
	}

	return tok.AccessToken, nil
}

// redirectPolicy follows only http(s) redirects and drops the API-key header on a host change.
func redirectPolicy(secretHeader string) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= httpMaxRedirects || (req.URL.Scheme != "http" && req.URL.Scheme != "https") {
			return http.ErrUseLastResponse
		}

		if secretHeader != "" && req.URL.Host != via[0].URL.Host {
			req.Header.Del(secretHeader)
		}

		return nil
	}
}

func doHTTP(client *http.Client, req *http.Request) (any, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, transportFailure(req.Context(), err)
	}
	defer resp.Body.Close()

	if err = checkHTTPContentType(resp.Header.Get("Content-Type")); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, httpMaxResponseBytes+1))
	if err != nil {
		return nil, transportFailure(req.Context(), err)
	}

	if len(data) > httpMaxResponseBytes {
		return nil, &asl.FailError{ErrCode: errCodeDataLimit, Cause: "the HTTP response exceeds the payload size quota"}
	}

	if !utf8.Valid(data) {
		return nil, runtimeFailure("the HTTP response body is not valid text")
	}

	out := httpOutput(resp, data)
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return out, nil
	}

	cause, _ := json.Marshal(out)

	return nil, &asl.FailError{ErrCode: httpStatusPrefix + strconv.Itoa(resp.StatusCode), Cause: string(cause)}
}

func transportFailure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if urlErr, ok := errors.AsType[*url.Error](err); ok {
		err = urlErr.Err
	}

	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return &asl.FailError{ErrCode: errCodeHTTPSocket, Cause: "the HTTP request timed out"}
	}

	return &asl.FailError{ErrCode: errCodeTaskFailed, Cause: err.Error()}
}

func checkHTTPContentType(header string) error {
	media, _, _ := mime.ParseMediaType(header)

	if media == "application/octet-stream" || strings.HasPrefix(media, "image/") ||
		strings.HasPrefix(media, "video/") || strings.HasPrefix(media, "audio/") {
		return runtimeFailure("unsupported HTTP response content type %q", media)
	}

	return nil
}

func httpOutput(resp *http.Response, data []byte) map[string]any {
	headers := make(map[string]any, len(resp.Header))

	for k, vs := range resp.Header {
		items := make([]any, len(vs))
		for i, v := range vs {
			items[i] = v
		}

		headers[k] = items
	}

	var body any = string(data)

	media, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if media == "application/json" || strings.HasSuffix(media, "+json") {
		var parsed any
		if json.Unmarshal(data, &parsed) == nil {
			body = parsed
		}
	}

	return map[string]any{
		"Headers":      headers,
		"ResponseBody": body,
		"StatusCode":   resp.StatusCode,
		"StatusText":   strings.TrimSpace(strings.TrimPrefix(resp.Status, strconv.Itoa(resp.StatusCode))),
	}
}
