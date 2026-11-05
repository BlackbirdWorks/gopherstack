package lambdaweb_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

var errNeverListens = errors.New("application did not start listening")

// fakeApp is an in-process HTTP app standing in for a revision's execution environment.
type fakeApp struct {
	srv      *httptest.Server
	seen     chan *http.Request
	hold     chan struct{}
	closed   *atomic.Int32
	revision string
}

func (a *fakeApp) Addr() (string, int) {
	host, port, _ := net.SplitHostPort(a.srv.Listener.Addr().String())
	p, _ := strconv.Atoi(port)

	return host, p
}

func (a *fakeApp) LogStream() string { return "2026/11/01/[" + a.revision + "]abc" }

func (a *fakeApp) Close(context.Context) {
	a.closed.Add(1)
	a.srv.Close()
}

type fakeLauncher struct {
	err      error
	hold     chan struct{}
	apps     sync.Map
	all      []*fakeApp
	logs     func(spec lambdaweb.LaunchSpec, line func(stderr bool, msg string))
	holdRev  string
	once     sync.Once
	mu       sync.Mutex
	launches atomic.Int32
	closed   atomic.Int32
}

// release unblocks every held request once.
func (l *fakeLauncher) release() { l.once.Do(func() { close(l.hold) }) }

func (l *fakeLauncher) setHoldRev(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.holdRev = id
}

func (l *fakeLauncher) holdRevision() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.holdRev
}

// seenTotal counts the requests every launched app has received and not yet been read.
func (l *fakeLauncher) seenTotal() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := 0
	for _, a := range l.all {
		n += len(a.seen)
	}

	return n
}

func (l *fakeLauncher) Launch(_ context.Context, spec lambdaweb.LaunchSpec) (lambdaweb.Environment, error) {
	l.launches.Add(1)

	if l.err != nil {
		return nil, l.err
	}

	app := &fakeApp{revision: spec.Revision.ID, seen: make(chan *http.Request, 64), hold: l.hold, closed: &l.closed}
	if hr := l.holdRevision(); hr != "" && hr != spec.Revision.ID {
		app.hold = nil
	}

	app.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case app.seen <- r.Clone(context.Background()):
		default:
		}

		if l.logs != nil && spec.Output != nil {
			l.logs(spec, spec.Output)
		}

		if app.hold != nil {
			<-app.hold
		}

		switch r.URL.Path {
		case "/boom":
			w.WriteHeader(http.StatusInternalServerError)
		case "/missing":
			w.WriteHeader(http.StatusNotFound)
		}

		w.Header().Set("X-App-Revision", app.revision)
		w.Header().Set("Lambda-Web-Request-Id", "app-supplied")
		_, _ = io.WriteString(w, "hello from "+app.revision)
	}))
	l.apps.Store(spec.Revision.ID, app)
	l.mu.Lock()
	l.all = append(l.all, app)
	l.mu.Unlock()

	return app, nil
}

type dataEnv struct {
	*testEnv
	launcher *fakeLauncher
	http     *http.Client
	domain   string
	rev1     string
}

func newDataEnv(t *testing.T) *dataEnv {
	t.Helper()

	env := newTestEnv(t)
	l := &fakeLauncher{}
	env.backend.SetLauncher(l)

	created := env.createFunction(t, "app")
	env.settle()

	return &dataEnv{
		testEnv: env, launcher: l, domain: aws.ToString(created.Endpoint.DomainName),
		rev1: aws.ToString(created.Revision.RevisionId), http: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *dataEnv) do(t *testing.T, method, host, path string, mod func(*http.Request)) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, d.srvURL+path, nil)
	require.NoError(t, err)

	req.Host = host
	if mod != nil {
		mod(req)
	}

	resp, err := d.http.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func (d *dataEnv) get(t *testing.T) *http.Response {
	t.Helper()

	return d.do(t, http.MethodGet, d.domain, "/", nil)
}

func body(t *testing.T, resp *http.Response) string {
	t.Helper()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(b)
}

func TestDataPlane_ProxiesRequests(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	resp := d.do(t, http.MethodPost, d.domain, "/api/items?x=1", func(r *http.Request) {
		r.Header.Set("X-Custom", "yes")
		r.Header.Set("Lambda-Web-Request-Id", "client-supplied")
		r.Header.Set("X-Http-Method-Override", "DELETE")
	})

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "hello from "+d.rev1, body(t, resp))
	assert.NotEqual(
		t,
		"app-supplied",
		resp.Header.Get("Lambda-Web-Request-Id"),
		"the endpoint overwrites the app's value",
	)
	assert.Len(t, resp.Header.Get("Lambda-Web-Request-Id"), 26)
	assert.Contains(t, resp.Header.Get("Lambda-Web-Logstream"), "["+d.rev1+"]")

	app, _ := d.launcher.apps.Load(d.rev1)
	seen := <-app.(*fakeApp).seen
	assert.Equal(t, "/api/items", seen.URL.Path)
	assert.Equal(t, "x=1", seen.URL.RawQuery)
	assert.Equal(t, d.domain, seen.Host)
	assert.Equal(t, "yes", seen.Header.Get("X-Custom"))
	assert.Equal(t, "prod", seen.Header.Get("Lambda-Web-Endpoint-Name"))
	assert.NotEqual(t, "client-supplied", seen.Header.Get("Lambda-Web-Request-Id"))
	assert.Empty(t, seen.Header.Get("X-Http-Method-Override"))
	assert.NotEmpty(t, seen.Header.Get("X-Forwarded-For"))
}

func TestDataPlane_HostForms(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	tests := []struct {
		name string
		host string
		errT string
		want int
	}{
		{name: "on aws", host: d.domain, want: http.StatusOK},
		{
			name: "localhost zone with port",
			host: strings.TrimSuffix(d.domain, ".on.aws") + ".localhost:4566",
			want: http.StatusOK,
		},
		{
			name: "uppercase",
			host: strings.ToUpper(d.domain),
			want: http.StatusNotFound,
			errT: "Platform.InvalidEndpoint",
		},
		{
			name: "unknown endpoint",
			host: "zzzzzzzzzzzzzzzz.abc.lambda-web.us-east-1.on.aws",
			want: http.StatusNotFound,
			errT: "Platform.InvalidEndpoint",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp := d.do(t, http.MethodGet, tc.host, "/", nil)
			assert.Equal(t, tc.want, resp.StatusCode)
			assert.Equal(t, tc.errT, resp.Header.Get("Lambda-Web-Error-Type"))
		})
	}
}

func TestDataPlane_RejectsUnsupportedMethod(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	resp := d.do(t, http.MethodTrace, d.domain, "/", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "Platform.MalformedRequest", resp.Header.Get("Lambda-Web-Error-Type"))
	assert.Zero(t, d.launcher.launches.Load(), "rejected before the app is launched")
}

func TestDataPlane_EndpointStates(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	l := &fakeLauncher{}
	env.backend.SetLauncher(l)

	created := env.createFunction(t, "app")
	domain := aws.ToString(created.Endpoint.DomainName)
	d := &dataEnv{testEnv: env, launcher: l, domain: domain, http: &http.Client{Timeout: 30 * time.Second}}

	resp := d.get(t)
	assert.Equal(t, http.StatusConflict, resp.StatusCode, "a Pending endpoint is not served")
	assert.Equal(t, "Platform.EndpointNotActive", resp.Header.Get("Lambda-Web-Error-Type"))

	env.settle()
	assert.Equal(t, http.StatusOK, d.get(t).StatusCode)

	_, err := env.client.DeleteWebFunctionEndpoint(t.Context(), &lwsdk.DeleteWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)

	resp = d.get(t)
	assert.Equal(t, http.StatusConflict, resp.StatusCode, "a Deleting endpoint rejects new requests")

	env.settle()
	assert.Equal(t, http.StatusNotFound, d.get(t).StatusCode, "a deleted endpoint's domain is retired")
}

func TestDataPlane_WeightedRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		first bool
		roll  int
	}{
		{name: "first lower bound", roll: 0, first: true},
		{name: "first upper bound", roll: 89, first: true},
		{name: "second lower bound", roll: 90},
		{name: "second upper bound", roll: 99},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := newDataEnv(t)
			rev2 := aws.ToString(d.createRevision(t).RevisionId)
			d.settle()

			_, err := d.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
				FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
				RevisionWeights: weights(d.rev1, 90, rev2, 10),
			})
			require.NoError(t, err)
			d.settle()

			d.backend.SetRand(func(int) int { return tc.roll })

			want := rev2
			if tc.first {
				want = d.rev1
			}

			assert.Equal(t, want, d.get(t).Header.Get("X-App-Revision"))
		})
	}
}

func TestDataPlane_LatestRevisionFollowsNewest(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	l := &fakeLauncher{}
	env.backend.SetLauncher(l)

	env.createFunction(t, "app")
	env.settle()

	ep, err := env.client.CreateWebFunctionEndpoint(t.Context(), &lwsdk.CreateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("auto"), EndpointType: types.EndpointTypeHomeRegion,
		AuthType: types.AuthTypeApplicationManaged, AutoDeploymentMode: types.AutoDeploymentModeLatestRevision,
	})
	require.NoError(t, err)
	env.settle()

	d := &dataEnv{
		testEnv:  env,
		launcher: l,
		domain:   aws.ToString(ep.DomainName),
		http:     &http.Client{Timeout: 30 * time.Second},
	}
	newest := aws.ToString(env.createRevision(t).RevisionId)

	assert.NotEqual(t, newest, d.get(t).Header.Get("X-App-Revision"), "a Pending revision does not take traffic")

	env.settle()
	assert.Equal(t, newest, d.get(t).Header.Get("X-App-Revision"))
}

func TestDataPlane_RateLimit(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	_, err := d.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
		ThrottleConfig: &types.ThrottleConfig{RateLimit: aws.Int32(100)},
	})
	require.NoError(t, err)
	d.settle()

	for range 100 {
		require.Equal(t, http.StatusOK, d.get(t).StatusCode)
	}

	resp := d.get(t)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "Platform.EndpointRequestThrottle", resp.Header.Get("Lambda-Web-Error-Type"))

	d.clock.Advance(time.Second)
	assert.Equal(t, http.StatusOK, d.get(t).StatusCode, "the window rolls over each second")
}

func TestDataPlane_RateLimitZeroDisablesEndpoint(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	_, err := d.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
		ThrottleConfig: &types.ThrottleConfig{RateLimit: aws.Int32(0)},
	})
	require.NoError(t, err)
	d.settle()

	resp := d.get(t)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "Platform.EndpointDisabled", resp.Header.Get("Lambda-Web-Error-Type"))
}

func TestDataPlane_MaxEnvironmentsCapacity(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	l := &fakeLauncher{hold: make(chan struct{})}
	env.backend.SetLauncher(l)

	rc := revisionConfig()
	rc.ServiceConfig.MaxConcurrencyPerEnvironment = aws.Int32(1)
	ec := endpointConfig()
	ec.ScalingConfig = &types.ScalingConfig{MaxEnvironments: aws.Int32(2)}

	created, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("app"), RevisionConfig: rc, EndpointConfig: ec,
	})
	require.NoError(t, err)
	env.settle()

	d := &dataEnv{
		testEnv:  env,
		launcher: l,
		domain:   aws.ToString(created.Endpoint.DomainName),
		http:     &http.Client{Timeout: 30 * time.Second},
	}

	var wg sync.WaitGroup

	for range 2 {
		wg.Go(func() {
			resp := d.do(t, http.MethodGet, d.domain, "/", nil)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}

	require.Eventually(t, func() bool { return l.seenTotal() == 2 }, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, int32(2), l.launches.Load(), "each full environment scales out another")

	resp := d.get(t)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "Platform.EndpointMaxEnvThrottle", resp.Header.Get("Lambda-Web-Error-Type"))

	close(l.hold)
	wg.Wait()
}

func TestDataPlane_LaunchesOncePerRevision(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			assert.Equal(t, http.StatusOK, d.do(t, http.MethodGet, d.domain, "/", nil).StatusCode)
		})
	}

	wg.Wait()
	assert.Equal(t, int32(1), d.launcher.launches.Load())
}

func TestDataPlane_LaunchFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err        error
		name       string
		errType    string
		wantStatus int
	}{
		{
			name:       "never listens",
			err:        errNeverListens,
			wantStatus: http.StatusBadGateway,
			errType:    "Application.TimeoutListenError",
		},
		{
			name:       "code gone",
			err:        lambdaweb.ErrCodeUnavailable,
			wantStatus: http.StatusConflict,
			errType:    "Platform.RevisionNotActive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := newDataEnv(t)
			d.launcher.err = tc.err

			resp := d.get(t)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			assert.Equal(t, tc.errType, resp.Header.Get("Lambda-Web-Error-Type"))
		})
	}
}

func TestDataPlane_FailedRevisionIsRecorded(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)
	d.launcher.err = lambdaweb.ErrCodeUnavailable

	d.get(t)

	rev, err := d.client.GetWebFunctionRevision(t.Context(), &lwsdk.GetWebFunctionRevisionInput{
		FunctionName: aws.String("app"), RevisionId: aws.String(d.rev1),
	})
	require.NoError(t, err)
	assert.Equal(t, types.RevisionStateFailed, rev.State)
	require.Len(t, rev.Errors, 1)
	assert.Equal(t, "CodeUnavailable", aws.ToString(rev.Errors[0].ErrorCode))

	_, err = d.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"), RevisionWeights: weights(d.rev1, 100),
	})
	assertAPIErrorCode(t, err, "ConflictException")
}

func TestDataPlane_StubEngine(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	env.settle()

	d := &dataEnv{
		testEnv: env,
		domain:  aws.ToString(created.Endpoint.DomainName),
		http:    &http.Client{Timeout: 30 * time.Second},
	}

	resp := d.get(t)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, body(t, resp), "engine is stub")
}

func TestDataPlane_RequestTimeout(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	l := &fakeLauncher{hold: make(chan struct{})}
	env.backend.SetLauncher(l)
	t.Cleanup(func() { close(l.hold) })

	rc := revisionConfig()
	rc.ServiceConfig.TimeoutSeconds = aws.Int32(3)

	created, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("slow"), RevisionConfig: rc, EndpointConfig: endpointConfig(),
	})
	require.NoError(t, err)
	env.settle()

	d := &dataEnv{
		testEnv:  env,
		launcher: l,
		domain:   aws.ToString(created.Endpoint.DomainName),
		http:     &http.Client{Timeout: 30 * time.Second},
	}

	resp := d.get(t)
	assert.Equal(t, http.StatusGatewayTimeout, resp.StatusCode)
	assert.Equal(t, "Runtime.RequestTimeout", resp.Header.Get("Lambda-Web-Error-Type"))
}

func signedRequest(t *testing.T, service string) func(*http.Request) {
	t.Helper()

	return func(r *http.Request) {
		sum := sha256.Sum256(nil)
		hash := hex.EncodeToString(sum[:])
		r.Header.Set("X-Amz-Content-Sha256", hash)

		creds := aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test", SessionToken: "tok"}
		require.NoError(t, v4.NewSigner().SignHTTP(t.Context(), creds, r, hash, service, testRegion, time.Now()))
	}
}

func newIamEnv(t *testing.T) (*dataEnv, string) {
	t.Helper()

	env := newTestEnv(t)
	l := &fakeLauncher{}
	env.backend.SetLauncher(l)

	ec := endpointConfig()
	ec.AuthType = types.AuthTypeIamAuth

	created, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("secure"), RevisionConfig: revisionConfig(), EndpointConfig: ec,
	})
	require.NoError(t, err)
	env.settle()

	d := &dataEnv{
		testEnv: env, launcher: l, domain: aws.ToString(created.Endpoint.DomainName),
		http: &http.Client{Timeout: 30 * time.Second},
	}

	return d, aws.ToString(created.Revision.RevisionId)
}

func TestDataPlane_IamAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(*http.Request)
		name string
		want int
	}{
		{name: "unsigned", mod: nil, want: http.StatusForbidden},
		{name: "signed", mod: signedRequest(t, "lambda"), want: http.StatusOK},
		{name: "wrong service", mod: signedRequest(t, "s3"), want: http.StatusForbidden},
		{name: "missing content hash", mod: func(r *http.Request) {
			signedRequest(t, "lambda")(r)
			r.Header.Del("X-Amz-Content-Sha256")
		}, want: http.StatusForbidden},
		{name: "tampered", mod: func(r *http.Request) {
			signedRequest(t, "lambda")(r)
			r.Header.Set("X-Amz-Date", "20200101T000000Z")
		}, want: http.StatusForbidden},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, rev := newIamEnv(t)

			resp := d.do(t, http.MethodGet, d.domain, "/", tc.mod)
			assert.Equal(t, tc.want, resp.StatusCode)

			if tc.want != http.StatusOK {
				return
			}

			app, _ := d.launcher.apps.Load(rev)
			seen := <-app.(*fakeApp).seen
			assert.Empty(t, seen.Header.Get("Authorization"), "the AWS signature is stripped before the app sees it")
			assert.Empty(t, seen.Header.Get("X-Amz-Security-Token"))
		})
	}
}

func TestDataPlane_ApplicationManagedAuthHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth string
		want string
	}{
		{name: "bearer kept", auth: "Bearer abc", want: "Bearer abc"},
		{
			name: "aws signature removed",
			auth: "AWS4-HMAC-SHA256 Credential=x/20260101/us-east-1/lambda/aws4_request",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := newDataEnv(t)

			resp := d.do(
				t,
				http.MethodGet,
				d.domain,
				"/",
				func(r *http.Request) { r.Header.Set("Authorization", tc.auth) },
			)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			app, _ := d.launcher.apps.Load(d.rev1)
			seen := <-app.(*fakeApp).seen
			assert.Equal(t, tc.want, seen.Header.Get("Authorization"))
		})
	}
}

func TestIAMAction(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	iamDomain := aws.ToString(created.Endpoint.DomainName)

	tests := []struct {
		name   string
		host   string
		path   string
		method string
		want   string
	}{
		{name: "app managed endpoint is not gated", host: iamDomain, path: "/", method: http.MethodGet, want: ""},
		{
			name:   "control plane",
			host:   "localhost",
			path:   "/2025-03-07/web-functions",
			method: http.MethodPut,
			want:   "lambda:CreateWebFunction",
		},
		{name: "unknown path", host: "localhost", path: "/other", method: http.MethodGet, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Host = tc.host
			assert.Equal(t, tc.want, env.handler.IAMAction(req))
		})
	}
}
