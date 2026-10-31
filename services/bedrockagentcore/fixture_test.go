package bedrockagentcore_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

const (
	testAccount = awsmeta.DefaultAccount
	testRegion  = "us-east-1"
	testMemory  = "mem-abc"
	testRuntime = "agent_rt-0123456789"
	testWorkld  = "wl"
)

type fakeClock struct{ ns atomic.Int64 }

func newClock(t time.Time) *fakeClock {
	c := &fakeClock{}
	c.ns.Store(t.UnixNano())

	return c
}

func (c *fakeClock) now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

type fakeControl struct {
	runtimes  map[string]bedrockagentcore.RuntimeInfo
	memories  map[string]bedrockagentcore.MemoryInfo
	tools     map[string]bool
	workloads map[string]bool
	providers map[string]bedrockagentcore.ProviderInfo
	secrets   map[string]string
}

func (f fakeControl) Runtime(_ bedrockagentcore.Scope, ref, qualifier string) (bedrockagentcore.RuntimeInfo, error) {
	r, ok := f.runtimes[ref+"|"+qualifier]
	if !ok {
		return r, bedrockagentcore.ErrNotFound
	}

	return r, nil
}

func (f fakeControl) Memory(_ bedrockagentcore.Scope, id string) (bedrockagentcore.MemoryInfo, bool) {
	m, ok := f.memories[id]

	return m, ok
}

func (f fakeControl) Tool(_ bedrockagentcore.Scope, kind bedrockagentcore.ToolKind, id string) (
	bedrockagentcore.ToolInfo, bool,
) {
	return bedrockagentcore.ToolInfo{ID: id, Mode: "PUBLIC"}, f.tools[string(kind)+"|"+id]
}

func (f fakeControl) WorkloadIdentity(_ bedrockagentcore.Scope, name string) bool {
	return f.workloads[name]
}

func (f fakeControl) Provider(_ bedrockagentcore.Scope, kind bedrockagentcore.ProviderKind, name string) (
	bedrockagentcore.ProviderInfo, bool,
) {
	p, ok := f.providers[string(kind)+"|"+name]

	return p, ok
}

func (f fakeControl) SecretValue(arn, _ string) (string, bool) {
	v, ok := f.secrets[arn]

	return v, ok
}

func defaultControl() fakeControl {
	return fakeControl{
		runtimes: map[string]bedrockagentcore.RuntimeInfo{},
		memories: map[string]bedrockagentcore.MemoryInfo{
			testMemory: {
				ID: testMemory, ExpiryDays: 30,
				Strategies: []bedrockagentcore.StrategyInfo{{ID: "strat-1", Name: "facts", Type: "SEMANTIC"}},
			},
		},
		tools: map[string]bool{
			"codeInterpreter|aws.codeinterpreter.v1": true, "browser|aws.browser.v1": true,
		},
		workloads: map[string]bool{testWorkld: true},
		providers: map[string]bedrockagentcore.ProviderInfo{},
		secrets:   map[string]string{},
	}
}

type fixture struct {
	backend *bedrockagentcore.InMemoryBackend
	clock   *fakeClock
	client  *sdk.Client
	url     string
}

func newFixture(t *testing.T, ctl fakeControl) *fixture {
	t.Helper()

	b := bedrockagentcore.NewInMemoryBackend(testAccount, testRegion)
	clock := newClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	b.SetClock(clock.now)
	b.SetControl(ctl)
	t.Cleanup(b.Close)

	f := &fixture{backend: b, clock: clock, url: sdktest.Serve(t, bedrockagentcore.NewHandler(b))}
	f.client = f.newClient(t)

	return f
}

func (f *fixture) newClient(t *testing.T) *sdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) {
		o.BaseEndpoint = aws.String(f.url)
		o.Retryer = aws.NopRetryer{}
		o.HTTPClient = sdktest.PlainBodyHTTPClient()
	})
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
