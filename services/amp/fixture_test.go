package amp_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/amp"
)

const testAccount = "123456789012"

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fakeClusters map[string]bool

func (f fakeClusters) ClusterExists(region, name string) bool { return f[region+"/"+name] }

type fixture struct {
	client  *ampsdk.Client
	backend *amp.InMemoryBackend
	clock   *fakeClock
	http    *http.Client
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	return newFixtureFromBackend(t, amp.NewInMemoryBackend(testAccount, "us-east-1"),
		&fakeClock{t: time.Unix(0, 0).UTC()})
}

func newFixtureFromBackend(t *testing.T, backend *amp.InMemoryBackend, clock *fakeClock) *fixture {
	t.Helper()

	backend.SetClock(clock.now)

	url := sdktest.Serve(t, amp.NewHandler(backend))

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := ampsdk.NewFromConfig(cfg, func(o *ampsdk.Options) { o.BaseEndpoint = aws.String(url) })

	hc := httpclient.New(30 * time.Second)
	t.Cleanup(hc.CloseIdleConnections)

	return &fixture{client: client, backend: backend, clock: clock, http: hc, url: url}
}

func (f *fixture) settle() { f.clock.advance(time.Minute) }

func (f *fixture) workspace(t *testing.T) string {
	t.Helper()

	out, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{Alias: aws.String("ws")})
	require.NoError(t, err)
	f.settle()

	return aws.ToString(out.WorkspaceId)
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func b64(s string) []byte { return []byte(s) }

const validRules = `groups:
  - name: example
    rules:
      - record: job:up:sum
        expr: sum by (job) (up)
      - alert: Down
        expr: up == 0
        for: 5m
        labels:
          severity: page
`

const validAlertManager = `alertmanager_config: |
  route:
    receiver: default
  receivers:
    - name: default
`

const validScrape = `global:
  scrape_interval: 30s
scrape_configs:
  - job_name: app
    static_configs:
      - targets: ['localhost:9090']
`

func (f *fixture) regionClient(t *testing.T, region string) *ampsdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return ampsdk.NewFromConfig(cfg, func(o *ampsdk.Options) { o.BaseEndpoint = aws.String(f.url) })
}
