package servicequotas_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/servicequotas"
)

const testAccount = "123456789012"

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fakeOrgs map[string]servicequotas.OrgInfo

func (f fakeOrgs) OrganizationOf(account string) (servicequotas.OrgInfo, bool) {
	o, ok := f[account]

	return o, ok
}

type fakeUsage map[string]float64

func (f fakeUsage) QuotaUsage(_, _, svc, code string) (float64, bool) {
	v, ok := f[svc+"/"+code]

	return v, ok
}

type fixture struct {
	client  *sqsdk.Client
	backend *servicequotas.InMemoryBackend
	clock   *fakeClock
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	return newFixtureFromBackend(t, servicequotas.NewInMemoryBackend(testAccount, "us-east-1"),
		&fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)})
}

func newFixtureFromBackend(t *testing.T, backend *servicequotas.InMemoryBackend, clock *fakeClock) *fixture {
	t.Helper()

	backend.SetClock(clock.now)

	url := sdktest.Serve(t, servicequotas.NewHandler(backend))

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := sqsdk.NewFromConfig(cfg, func(o *sqsdk.Options) { o.BaseEndpoint = aws.String(url) })

	return &fixture{client: client, backend: backend, clock: clock}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
