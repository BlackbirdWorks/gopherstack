package lambdaweb_test

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
	testRoleARN   = "arn:aws:iam::123456789012:role/web-role"
)

// fakeClock is a manually advanced clock.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

// fakeCodes is an in-memory CodeStore.
type fakeCodes struct {
	objects map[string][]byte
	buckets map[string]bool
}

func newFakeCodes() *fakeCodes {
	return &fakeCodes{
		objects: map[string][]byte{"code/app.zip@v1": []byte("zip-v1")},
		buckets: map[string]bool{"code": true, "unversioned": false},
	}
}

func (f *fakeCodes) ResolveObject(_ context.Context, bucket, key, version string) (string, error) {
	versioned, ok := f.buckets[bucket]
	if !ok {
		return "", lambdaweb.ErrBucketNotFound
	}

	if !versioned {
		return "", lambdaweb.ErrBucketNotVersioned
	}

	if version == "" {
		version = "v1"
	}

	if _, found := f.objects[bucket+"/"+key+"@"+version]; !found {
		return "", lambdaweb.ErrObjectNotFound
	}

	return version, nil
}

func (f *fakeCodes) GetObject(_ context.Context, bucket, key, version string) ([]byte, error) {
	if b, ok := f.objects[bucket+"/"+key+"@"+version]; ok {
		return b, nil
	}

	return nil, lambdaweb.ErrObjectNotFound
}

type testEnv struct {
	clock   *fakeClock
	backend *lambdaweb.InMemoryBackend
	handler *lambdaweb.Handler
	client  *lwsdk.Client
	srvURL  string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	clock := newFakeClock()
	backend := lambdaweb.NewInMemoryBackend(testAccountID)
	backend.SetClock(clock.Now)
	backend.SetCodeStore(newFakeCodes())
	t.Cleanup(backend.Close)

	h := lambdaweb.NewHandler(backend)
	h.AccountID, h.DefaultRegion = testAccountID, testRegion

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := lwsdk.NewFromConfig(cfg, func(o *lwsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	return &testEnv{clock: clock, backend: backend, handler: h, client: client, srvURL: srv.URL}
}

// settle advances the fake clock past every lifecycle window.
func (e *testEnv) settle() { e.clock.Advance(10 * time.Second) }

func assertAPIErrorCode(t *testing.T, err error, code string) {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, code, apiErr.ErrorCode())
}

func revisionConfig() *types.RevisionConfig {
	return &types.RevisionConfig{
		BuildConfig: &types.BuildConfig{
			CodeConfig: &types.CodeConfig{
				S3Object: &types.S3Object{Bucket: aws.String("code"), Key: aws.String("app.zip")},
			},
			RuntimeConfig: &types.RuntimeConfig{Runtime: aws.String("nodejs24.x")},
		},
		ServiceConfig: &types.ServiceConfig{ExecutionRoleArn: aws.String(testRoleARN)},
	}
}

func endpointConfig() *types.EndpointConfig {
	return &types.EndpointConfig{
		EndpointName: aws.String("prod"), EndpointType: types.EndpointTypeHomeRegion,
		AuthType: types.AuthTypeApplicationManaged,
	}
}

func (e *testEnv) createFunction(t *testing.T, name string) *lwsdk.CreateWebFunctionOutput {
	t.Helper()

	out, err := e.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String(name), RevisionConfig: revisionConfig(), EndpointConfig: endpointConfig(),
	})
	require.NoError(t, err)

	return out
}

func (e *testEnv) createRevision(t *testing.T) *lwsdk.CreateWebFunctionRevisionOutput {
	t.Helper()

	rc := revisionConfig()
	out, err := e.client.CreateWebFunctionRevision(t.Context(), &lwsdk.CreateWebFunctionRevisionInput{
		FunctionName: aws.String("app"), BuildConfig: rc.BuildConfig, ServiceConfig: rc.ServiceConfig,
	})
	require.NoError(t, err)

	return out
}
