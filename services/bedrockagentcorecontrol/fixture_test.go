package bedrockagentcorecontrol_test

import (
	"errors"
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

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) advance()       { c.t = c.t.Add(time.Minute) }

type fakeRefs struct {
	repos     map[string]bool
	functions map[string]bool
	secrets   map[string]bool
	objects   map[string][]byte
}

func (f fakeRefs) ECRRepositoryExists(_, _, repo string) bool { return f.repos[repo] }
func (f fakeRefs) LambdaFunctionExists(arn string) bool       { return f.functions[arn] }
func (f fakeRefs) SecretExists(arn string) bool               { return f.secrets[arn] }

func (f fakeRefs) ReadObject(bucket, key string) ([]byte, bool) {
	b, ok := f.objects[bucket+"/"+key]

	return b, ok
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
