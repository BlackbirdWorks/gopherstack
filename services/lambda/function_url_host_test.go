package lambda_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

const urlTestFn = "url-host-fn"

type urlHostEnv struct {
	h      *lambda.Handler
	events chan []byte
	host   string
}

func freeLoopbackPort(t *testing.T) int {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer l.Close()

	tcp, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return tcp.Port
}

// runFakeRuntime answers every invocation on the runtime API at port with response.
func runFakeRuntime(t *testing.T, port int, response string, events chan<- []byte) {
	t.Helper()

	done := make(chan struct{})
	base := fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/", port)

	go func() {
		defer close(done)

		client := &http.Client{Timeout: 0}

		for t.Context().Err() == nil {
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"next", nil)

			resp, err := client.Do(req)
			if err != nil {
				return
			}

			raw, _ := io.ReadAll(resp.Body)
			id := resp.Header.Get("Lambda-Runtime-Aws-Request-Id")
			_ = resp.Body.Close()

			events <- raw

			post, _ := http.NewRequestWithContext(
				t.Context(), http.MethodPost, base+id+"/response", strings.NewReader(response),
			)
			if pr, perr := client.Do(post); perr == nil {
				_ = pr.Body.Close()
			}
		}
	}()

	t.Cleanup(func() { <-done })
}

func newURLHostEnv(
	t *testing.T, authType, invokeMode string, cors *lambda.FunctionURLCors, response string,
) *urlHostEnv {
	t.Helper()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, bk)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: urlTestFn,
		FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:" + urlTestFn,
		Runtime:      "python3.12",
		Handler:      "index.handler",
		Role:         "arn:aws:iam::000000000000:role/r",
		PackageType:  "Zip",
		State:        lambda.FunctionStateActive,
	}))

	port := freeLoopbackPort(t)
	srv := lambda.NewExportedRuntimeServer(port)
	require.NoError(t, srv.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Stop(ctx)
	})
	lambda.InjectRuntimeEntryWithSrv(bk, urlTestFn, srv)

	events := make(chan []byte, 8)
	runFakeRuntime(t, port, response, events)

	cfg, err := bk.CreateFunctionURLConfig(t.Context(), urlTestFn, authType, cors, invokeMode)
	require.NoError(t, err)

	h := lambda.NewHandler(bk)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	host := strings.TrimSuffix(strings.TrimPrefix(cfg.FunctionURL, "https://"), "/")

	return &urlHostEnv{h: h, host: host, events: events}
}

func (e *urlHostEnv) do(t *testing.T, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	require.NoError(t, e.h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func (e *urlHostEnv) request(t *testing.T, method, host, target, body string) *http.Request {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, "http://"+host+target, strings.NewReader(body))
	req.Host = host

	return req
}

func signLambdaRequest(t *testing.T, req *http.Request, secret, service string) {
	t.Helper()

	sum := sha256.Sum256(nil)
	creds := aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: secret}
	require.NoError(t, v4.NewSigner().SignHTTP(
		t.Context(), creds, req, hex.EncodeToString(sum[:]), service, "us-east-1", time.Now(),
	))
}

func TestFunctionURLHost_Match(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want bool
	}{
		{"on_aws", "abc123.lambda-url.us-east-1.on.aws", true},
		{"on_aws_port", "abc123.lambda-url.eu-west-2.on.aws:443", true},
		{"localhost_zone", "abc123.lambda-url.us-east-1.localhost:4566", true},
		{"upper_case", "ABC123.Lambda-URL.us-east-1.ON.AWS", true},
		{"plain_localhost", "localhost:4566", false},
		{"other_service", "sqs.us-east-1.amazonaws.com", false},
		{"missing_region", "abc123.lambda-url.on.aws", false},
		{"nested_id", "a.b.lambda-url.us-east-1.on.aws", false},
		{"wrong_infix", "abc123.lambdaurl.us-east-1.on.aws", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, lambda.IsFunctionURLHost(tt.host))

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			req.Host = tt.host
			c := echo.New().NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, lambda.NewHandler(nil).RouteMatcher()(c))
		})
	}
}

func TestFunctionURLHost_ResponseMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		response    string
		wantBody    string
		wantCookies []string
		wantStatus  int
	}{
		{
			name:        "structured",
			response:    `{"statusCode":201,"headers":{"X-A":"b"},"cookies":["a=1","b=2"],"body":"hi"}`,
			wantStatus:  http.StatusCreated,
			wantBody:    "hi",
			wantCookies: []string{"a=1", "b=2"},
		},
		{
			name: "base64",
			response: `{"statusCode":200,"isBase64Encoded":true,"body":"` +
				base64.StdEncoding.EncodeToString([]byte{0, 1, 2, 255}) + `"}`,
			wantStatus: http.StatusOK,
			wantBody:   string([]byte{0, 1, 2, 255}),
		},
		{name: "plain_json", response: `{"ok":true}`, wantStatus: http.StatusOK, wantBody: `{"ok":true}`},
		{name: "bad_status", response: `{"statusCode":999,"body":"x"}`, wantStatus: http.StatusBadGateway},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newURLHostEnv(t, "NONE", "", nil, tt.response)
			rec := env.do(t, env.request(t, http.MethodGet, env.host, "/", ""))

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusBadGateway {
				assert.Equal(t, tt.wantBody, rec.Body.String())
				assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			}

			assert.Equal(t, tt.wantCookies, valuesOrNil(rec.Header().Values("Set-Cookie")))
		})
	}
}

func valuesOrNil(v []string) []string {
	if len(v) == 0 {
		return nil
	}

	return v
}

func TestFunctionURLHost_EventShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		contentTyp string
		body       string
		wantBody   string
		hostSuffix string
		wantB64    bool
	}{
		{name: "text_body", contentTyp: "text/plain", body: "hello", wantBody: "hello"},
		{
			name: "binary_body", contentTyp: "application/octet-stream", body: "\x00\x01",
			wantBody: base64.StdEncoding.EncodeToString([]byte("\x00\x01")), wantB64: true,
		},
		{name: "localhost_host", contentTyp: "text/plain", body: "x", wantBody: "x", hostSuffix: ".localhost:4566"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newURLHostEnv(t, "NONE", "", nil, `{"statusCode":200,"body":"ok"}`)

			host := env.host
			if tt.hostSuffix != "" {
				host = strings.TrimSuffix(host, ".on.aws") + tt.hostSuffix
			}

			req := env.request(t, http.MethodPost, host, "/a%20b/c?x=1&y=2&y=3", tt.body)
			req.Header.Set("Content-Type", tt.contentTyp)
			req.Header.Set("Cookie", "s=1")

			rec := env.do(t, req)
			require.Equal(t, http.StatusOK, rec.Code)

			var ev struct {
				Headers        map[string]string `json:"headers"`
				Query          map[string]string `json:"queryStringParameters"`
				RequestContext struct {
					HTTP struct {
						Method string `json:"method"`
						Path   string `json:"path"`
					} `json:"http"`
					DomainName string `json:"domainName"`
					APIID      string `json:"apiId"`
				} `json:"requestContext"`
				Version        string   `json:"version"`
				RawPath        string   `json:"rawPath"`
				RawQueryString string   `json:"rawQueryString"`
				Body           string   `json:"body"`
				Cookies        []string `json:"cookies"`
				IsBase64       bool     `json:"isBase64Encoded"`
			}
			require.NoError(t, json.Unmarshal(<-env.events, &ev))

			assert.Equal(t, "2.0", ev.Version)
			assert.Equal(t, "/a%20b/c", ev.RawPath)
			assert.Equal(t, "x=1&y=2&y=3", ev.RawQueryString)
			assert.Equal(t, map[string]string{"x": "1", "y": "2,3"}, ev.Query)
			assert.Equal(t, http.MethodPost, ev.RequestContext.HTTP.Method)
			assert.Equal(t, host, ev.RequestContext.DomainName)
			assert.Equal(t, host, ev.Headers["host"])
			assert.Len(t, ev.RequestContext.APIID, 32)
			assert.Equal(t, []string{"s=1"}, ev.Cookies)
			assert.Equal(t, tt.wantBody, ev.Body)
			assert.Equal(t, tt.wantB64, ev.IsBase64)
		})
	}
}

func TestFunctionURLHost_Auth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		authType string
		secret   string
		service  string
		signed   bool
		want     int
	}{
		{name: "none_unsigned", authType: "NONE", want: http.StatusOK},
		{name: "none_signed", authType: "NONE", signed: true, secret: "test", service: "lambda", want: http.StatusOK},
		{name: "iam_unsigned", authType: "AWS_IAM", want: http.StatusForbidden},
		{name: "iam_signed", authType: "AWS_IAM", signed: true, secret: "test", service: "lambda", want: http.StatusOK},
		{name: "iam_bad_secret", authType: "AWS_IAM", signed: true, secret: "nope", service: "lambda", want: 403},
		{name: "iam_wrong_service", authType: "AWS_IAM", signed: true, secret: "test", service: "s3", want: 403},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newURLHostEnv(t, tt.authType, "", nil, `{"statusCode":200,"body":"ok"}`)
			req := env.request(t, http.MethodGet, env.host, "/", "")

			if tt.signed {
				signLambdaRequest(t, req, tt.secret, tt.service)
			}

			rec := env.do(t, req)
			assert.Equal(t, tt.want, rec.Code)

			if tt.want == http.StatusForbidden {
				assert.Contains(t, rec.Body.String(), "Forbidden")
			}
		})
	}
}

func TestFunctionURLHost_IAMAuthorizerContext(t *testing.T) {
	t.Parallel()

	env := newURLHostEnv(t, "AWS_IAM", "", nil, `{"statusCode":200}`)
	req := env.request(t, http.MethodGet, env.host, "/", "")
	signLambdaRequest(t, req, "test", "lambda")
	require.Equal(t, http.StatusOK, env.do(t, req).Code)

	var ev struct {
		RequestContext struct {
			Authorizer struct {
				IAM struct {
					AccessKey string `json:"accessKey"`
					AccountID string `json:"accountId"`
				} `json:"iam"`
			} `json:"authorizer"`
		} `json:"requestContext"`
	}
	require.NoError(t, json.Unmarshal(<-env.events, &ev))
	assert.Equal(t, "AKIDEXAMPLE", ev.RequestContext.Authorizer.IAM.AccessKey)
	assert.Equal(t, "000000000000", ev.RequestContext.Authorizer.IAM.AccountID)
}

func TestFunctionURLHost_CORS(t *testing.T) {
	t.Parallel()

	cors := &lambda.FunctionURLCors{
		AllowOrigins:  []string{"https://ok.example"},
		AllowMethods:  []string{"GET", "POST"},
		AllowHeaders:  []string{"content-type"},
		ExposeHeaders: []string{"x-id"},
		MaxAge:        600,
	}

	tests := []struct {
		name       string
		method     string
		origin     string
		wantOrigin string
		wantMeth   string
		wantStatus int
	}{
		{"preflight_allowed", http.MethodOptions, "https://ok.example", "https://ok.example", "GET,POST", 200},
		{"preflight_denied", http.MethodOptions, "https://evil.example", "", "", 200},
		{"actual_allowed", http.MethodGet, "https://ok.example", "https://ok.example", "", 200},
		{"actual_denied", http.MethodGet, "https://evil.example", "", "", 200},
		{"actual_no_origin", http.MethodGet, "", "", "", 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newURLHostEnv(t, "NONE", "", cors, `{"statusCode":200,"body":"ok"}`)
			req := env.request(t, tt.method, env.host, "/", "")

			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}

			rec := env.do(t, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantOrigin, rec.Header().Get("Access-Control-Allow-Origin"))
			assert.Equal(t, tt.wantMeth, rec.Header().Get("Access-Control-Allow-Methods"))

			if tt.method == http.MethodOptions && tt.wantOrigin != "" {
				assert.Equal(t, "600", rec.Header().Get("Access-Control-Max-Age"))
				assert.Empty(t, rec.Body.String(), "preflight must not invoke the function")
			}
		})
	}
}

func TestFunctionURLHost_ResponseStream(t *testing.T) {
	t.Parallel()

	prelude := `{"statusCode":202,"headers":{"Content-Type":"text/plain"},"cookies":["k=v"]}`

	tests := []struct {
		name     string
		mode     string
		response string
		want     string
		wantCode int
	}{
		{
			name: "prelude", mode: "RESPONSE_STREAM",
			response: prelude + strings.Repeat("\x00", 8) + "chunked body",
			want:     "chunked body", wantCode: http.StatusAccepted,
		},
		{
			name: "buffered_result", mode: "RESPONSE_STREAM",
			response: `{"statusCode":200,"body":"plain"}`, want: "plain", wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newURLHostEnv(t, "NONE", tt.mode, nil, tt.response)
			rec := env.do(t, env.request(t, http.MethodGet, env.host, "/", ""))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Equal(t, tt.want, rec.Body.String())
			assert.True(t, rec.Flushed)
		})
	}
}

func TestFunctionURLHost_UnknownURL(t *testing.T) {
	t.Parallel()

	env := newURLHostEnv(t, "NONE", "", nil, `{"statusCode":200}`)
	rec := env.do(t, env.request(t, http.MethodGet, strings.Repeat("a", 32)+".lambda-url.us-east-1.on.aws", "/", ""))

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFunctionURLConfig_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{"valid", `{"AuthType":"AWS_IAM","InvokeMode":"RESPONSE_STREAM"}`, http.StatusCreated},
		{"bad_auth", `{"AuthType":"BASIC"}`, http.StatusBadRequest},
		{"bad_mode", `{"AuthType":"NONE","InvokeMode":"STREAM"}`, http.StatusBadRequest},
		{"bad_max_age", `{"AuthType":"NONE","Cors":{"MaxAge":999999}}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			mustCreateFunctionViaHandler(t, h, "val-fn")

			rec := callHandler(t, h, http.MethodPost, "/2021-10-31/functions/val-fn/url", tt.body, nil)
			assert.Equal(t, tt.want, rec.Code)

			if tt.want == http.StatusCreated {
				var cfg lambda.FunctionURLConfig
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
				assert.Regexp(t, `^https://[0-9a-f]{32}\.lambda-url\.us-east-1\.on\.aws/$`, cfg.FunctionURL)

				up := callHandler(t, h, http.MethodPut, "/2021-10-31/functions/val-fn/url", `{"InvokeMode":"X"}`, nil)
				assert.Equal(t, http.StatusBadRequest, up.Code)
			}
		})
	}
}

func TestFunctionURLHost_IAMAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		authType string
		want     string
	}{
		{"iam", "AWS_IAM", "lambda:InvokeFunctionUrl"},
		{"none", "NONE", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newURLHostEnv(t, tt.authType, "", nil, `{"statusCode":200}`)
			req := env.request(t, http.MethodGet, env.host, "/", "")
			assert.Equal(t, tt.want, env.h.IAMAction(req))
		})
	}
}
