package oam_test

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	oamsdk "github.com/aws/aws-sdk-go-v2/service/oam"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/oam"
)

const sourceAccount = "111122223333"

type fakeDirectory map[string]oam.AccountInfo

func (f fakeDirectory) AccountInfo(id string) (oam.AccountInfo, bool) {
	i, ok := f[id]

	return i, ok
}

// fixture holds a monitoring-account client (default account) and a source-account client.
type fixture struct {
	monitoring *oamsdk.Client
	source     *oamsdk.Client
	backend    *oam.InMemoryBackend
}

func clientFor(t *testing.T, url, accessKey string) *oamsdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, "test", "")),
	)
	require.NoError(t, err)

	return oamsdk.NewFromConfig(cfg, func(o *oamsdk.Options) { o.BaseEndpoint = aws.String(url) })
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	backend := oam.NewInMemoryBackend("123456789012", "us-east-1")
	url := serveBackend(t, backend)

	return newFixtureOn(t, backend, url)
}

func serveBackend(t *testing.T, backend *oam.InMemoryBackend) string {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(oam.NewHandler(backend)))

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			c.SetRequest(req.WithContext(awsmeta.Set(req.Context(), awsmeta.FromRequest(req, "us-east-1"))))

			return next(c)
		}
	})
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv.URL
}

func newFixtureOn(t *testing.T, backend *oam.InMemoryBackend, url string) *fixture {
	t.Helper()

	return &fixture{
		monitoring: clientFor(t, url, "test"),
		source:     clientFor(t, url, sourceAccount),
		backend:    backend,
	}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
