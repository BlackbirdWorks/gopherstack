package signin_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/url"
	"testing"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	signinsdk "github.com/aws/aws-sdk-go-v2/service/signin"
	"github.com/aws/smithy-go"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/signin"
)

const (
	testAccount = "123456789012"
	testRegion  = "us-east-1"
	testClient  = "arn:aws:signin:::devtools/same-device"
	testRedir   = "http://127.0.0.1:8080/callback"
	verifier    = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// fixedEndpoint pins every operation to the test server: the SDK's endpoint rules ignore
// BaseEndpoint for control-plane and OAuth operations.
type fixedEndpoint struct{ url string }

func (e fixedEndpoint) ResolveEndpoint(_ context.Context, _ signinsdk.EndpointParameters) (smithyendpoints.Endpoint, error) {
	u, err := url.Parse(e.url)
	if err != nil {
		return smithyendpoints.Endpoint{}, err
	}

	return smithyendpoints.Endpoint{URI: *u}, nil
}

type fixture struct {
	backend *signin.InMemoryBackend
	clock   *fakeClock
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	return newFixtureWith(t, signin.NewInMemoryBackend(testAccount, testRegion))
}

func newFixtureWith(t *testing.T, backend *signin.InMemoryBackend) *fixture {
	t.Helper()

	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	backend.SetClock(clock.now)

	return &fixture{backend: backend, clock: clock, url: sdktest.Serve(t, signin.NewHandler(backend))}
}

// client returns an SDK client acting as account (the X-Amz-Account-Id header selects the caller).
func (f *fixture) client(t *testing.T, account string) *signinsdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return signinsdk.NewFromConfig(cfg, func(o *signinsdk.Options) {
		o.EndpointResolverV2 = fixedEndpoint{url: f.url}
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			return s.Build.Add(middleware.BuildMiddlewareFunc("acct",
				func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
					middleware.BuildOutput, middleware.Metadata, error,
				) {
					if req, ok := in.Request.(*smithyhttp.Request); ok {
						req.Header.Set(awsmeta.AccountHeader, account)
					}

					return next.HandleBuild(ctx, in)
				}), middleware.After)
		})
	})
}

func (f *fixture) self(t *testing.T) *signinsdk.Client { return f.client(t, testAccount) }

func (f *fixture) ctxFor(account string) context.Context {
	return awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Account: account, Region: testRegion, Partition: awsmeta.DefaultPartition,
	})
}

func challenge() string {
	sum := sha256.Sum256([]byte(verifier))

	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (f *fixture) authCode(t *testing.T) string {
	t.Helper()

	code, err := f.backend.IssueAuthorizationCode(f.ctxFor(testAccount), testClient, testRedir, challenge(), "S256")
	require.NoError(t, err)

	return code
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
