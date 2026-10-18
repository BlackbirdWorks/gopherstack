package budgets_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/budgets"
)

const testAccount = "123456789012"

var errBoom = errors.New("boom")

type fakeSpend struct {
	mu     sync.Mutex
	amount float64
	ok     bool
}

func (f *fakeSpend) set(v float64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.amount, f.ok = v, true
}

func (f *fakeSpend) Spend(_, _ time.Time, _ map[string][]string, _ string) (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.amount, f.ok
}

type fakeNotifier struct {
	sent []budgets.Delivery
	mu   sync.Mutex
}

func (f *fakeNotifier) Notify(_ context.Context, d budgets.Delivery) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sent = append(f.sent, d)

	return nil
}

func (f *fakeNotifier) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.sent)
}

type fakeExecutor struct {
	runs  []string
	mu    sync.Mutex
	fails bool
}

func (f *fakeExecutor) Run(_ context.Context, _ string, a budgets.Action, reverse bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fails {
		return errBoom
	}

	verb := "apply"
	if reverse {
		verb = "reverse"
	}

	f.runs = append(f.runs, verb+":"+a.ActionType)

	return nil
}

type fixture struct {
	client   *bsdk.Client
	backend  *budgets.InMemoryBackend
	spend    *fakeSpend
	notifier *fakeNotifier
	executor *fakeExecutor
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := budgets.NewInMemoryBackend(testAccount, "us-east-1")
	b.SetClock(func() time.Time { return time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC) })

	f := &fixture{backend: b, spend: &fakeSpend{}, notifier: &fakeNotifier{}, executor: &fakeExecutor{}}
	b.SetSpendSource(f.spend)
	b.SetNotifier(f.notifier)
	b.SetActionExecutor(f.executor)

	url := sdktest.Serve(t, budgets.NewHandler(b))

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	f.client = bsdk.NewFromConfig(cfg, func(o *bsdk.Options) { o.BaseEndpoint = aws.String(url) })

	return f
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
