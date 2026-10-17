package route53recoverycontrolconfig_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	cc "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
)

const testAccount = "123456789012"

// endpointConfig is the AppContext.Config stand-in; the URL is only known once the server is up.
type endpointConfig struct{ url atomic.Value }

func (c *endpointConfig) GetEndpoint() string { return c.url.Load().(string) }

type fixture struct {
	backend *cc.InMemoryBackend
	handler *cc.Handler
	client  *sdk.Client
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	ec := &endpointConfig{}
	ec.url.Store("")

	b := cc.NewInMemoryBackend(testAccount)
	b.SetAppConfig(ec)

	h := cc.NewHandler(b)
	url := sdktest.Serve(t, h)
	ec.url.Store(url)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return &fixture{
		backend: b, handler: h, url: url,
		client: sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func apiMessage(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorMessage()
	}

	return ""
}
