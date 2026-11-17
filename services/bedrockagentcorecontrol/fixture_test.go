package bedrockagentcorecontrol_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
)

const (
	testAccount = awsmeta.DefaultAccount
	testRole    = "arn:aws:iam::" + testAccount + ":role/agent"
	testImage   = testAccount + ".dkr.ecr.us-east-1.amazonaws.com/agent:latest"
)

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) advance() { c.step(time.Minute) }

func (c *fakeClock) step(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type fakeRefs struct {
	repos     map[string]bool
	functions map[string]bool
	secrets   map[string]bool
	objects   map[string][]byte
}

func (f fakeRefs) ECRRepositoryExists(_, _, repo string) bool { return f.repos[repo] }
func (f fakeRefs) LambdaFunctionExists(arn string) bool       { return f.functions[arn] }
func (f fakeRefs) SecretExists(arn string) bool               { return f.secrets[arn] }

func (f fakeRefs) ReadObjectUpTo(bucket, key string, limit int64) ([]byte, bool, bool) {
	b, ok := f.objects[bucket+"/"+key]
	if int64(len(b)) > limit {
		return nil, true, true
	}

	return b, ok, false
}

type fixture struct {
	backend *bedrockagentcorecontrol.InMemoryBackend
	clock   *fakeClock
	client  *sdk.Client
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := bedrockagentcorecontrol.NewInMemoryBackend(testAccount, "us-east-1")
	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	b.SetClock(clock.now)

	f := &fixture{backend: b, clock: clock, url: sdktest.Serve(t, bedrockagentcorecontrol.NewHandler(b))}
	f.client = f.clientIn(t, "us-east-1")

	return f
}

func (f *fixture) clientIn(t *testing.T, region string) *sdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(f.url) })
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
