package appintegrations_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	ai "github.com/blackbirdworks/gopherstack/services/appintegrations"
)

const testAccount = "123456789012"

type fixture struct {
	backend *ai.InMemoryBackend
	handler *ai.Handler
	client  *sdk.Client
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := ai.NewInMemoryBackend(testAccount, "us-east-1")
	h := ai.NewHandler(b)
	url := sdktest.Serve(t, h)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testAccount, "test", "")),
	)
	require.NoError(t, err)

	return &fixture{
		backend: b, handler: h,
		client: sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

type fakeRules struct {
	buses map[string]bool
	rules map[string]string
	mu    sync.Mutex
}

func newFakeRules(buses ...string) *fakeRules {
	f := &fakeRules{buses: map[string]bool{}, rules: map[string]string{}}
	for _, b := range buses {
		f.buses[b] = true
	}

	return f
}

func (f *fakeRules) BusExists(_ context.Context, bus string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.buses[bus]
}

func (f *fakeRules) PutRule(_ context.Context, bus, name, source string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.rules[bus+"/"+name] = source

	return nil
}

func (f *fakeRules) DeleteRule(_ context.Context, bus, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.rules, bus+"/"+name)

	return nil
}

func (f *fakeRules) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.rules)
}
