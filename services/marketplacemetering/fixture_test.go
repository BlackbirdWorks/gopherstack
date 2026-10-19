package marketplacemetering_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	entsdk "github.com/aws/aws-sdk-go-v2/service/marketplaceentitlementservice"
	sdk "github.com/aws/aws-sdk-go-v2/service/marketplacemetering"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	ent "github.com/blackbirdworks/gopherstack/services/marketplaceentitlementservice"
	mm "github.com/blackbirdworks/gopherstack/services/marketplacemetering"
)

const (
	testAccount  = "000000000000"
	otherAccount = "210987654321"
	testProduct  = "prod-abcdefghijklm"
)

type fixture struct {
	backend  *mm.InMemoryBackend
	handler  *mm.Handler
	entBack  *ent.InMemoryBackend
	client   *sdk.Client
	entitled *entsdk.Client
	clock    *atomic.Int64
}

func (f *fixture) now() time.Time { return time.Unix(f.clock.Load(), 0).UTC() }

func (f *fixture) advance(d time.Duration) { f.clock.Add(int64(d / time.Second)) }

func newFixture(t *testing.T) *fixture {
	t.Helper()

	f := &fixture{clock: &atomic.Int64{}}
	f.clock.Store(time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC).Unix())

	f.backend = mm.NewInMemoryBackend(testAccount, "us-east-1")
	f.backend.SetClock(f.now)
	f.entBack = ent.NewInMemoryBackend(testAccount, "us-east-1")
	f.backend.SetEntitlementSink(f.entBack)
	f.handler = mm.NewHandler(f.backend)

	url := sdktest.Serve(t, f.handler, ent.NewHandler(f.entBack))

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testAccount, "test", "")),
	)
	require.NoError(t, err)

	f.client = sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) })
	f.entitled = entsdk.NewFromConfig(cfg, func(o *entsdk.Options) { o.BaseEndpoint = aws.String(url) })

	return f
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

//go:fix inline
func intValue(v int32) *ent.Value { return &ent.Value{IntegerValue: &v} }

func boolValue(v bool) *ent.Value { return &ent.Value{BooleanValue: &v} }
