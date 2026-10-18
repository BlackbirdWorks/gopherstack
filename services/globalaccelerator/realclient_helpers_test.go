package globalaccelerator_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/globalaccelerator"
)

const testAccount = "123456789012"

type fakeSubnets map[string]string

func (f fakeSubnets) SubnetCIDR(_, id string) (string, bool) {
	c, ok := f[id]

	return c, ok
}

type fixture struct {
	client  *gasdk.Client
	backend *globalaccelerator.InMemoryBackend
	clock   *fakeClock
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newFixture(t *testing.T) *fixture {
	t.Helper()

	return newFixtureFromBackend(t, globalaccelerator.NewInMemoryBackend(testAccount, "us-west-2"),
		&fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)})
}

func newFixtureFromBackend(t *testing.T, backend *globalaccelerator.InMemoryBackend, clock *fakeClock) *fixture {
	t.Helper()

	backend.SetClock(clock.now)
	backend.SetSubnetResolver(fakeSubnets{"subnet-aaa": "10.0.0.0/28", "subnet-bbb": "10.0.1.0/28"})

	url := sdktest.Serve(t, globalaccelerator.NewHandler(backend))

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := gasdk.NewFromConfig(cfg, func(o *gasdk.Options) { o.BaseEndpoint = aws.String(url) })

	return &fixture{client: client, backend: backend, clock: clock}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
