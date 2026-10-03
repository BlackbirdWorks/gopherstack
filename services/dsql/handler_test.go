package dsql_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dsql"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"

	waitTimeout  = 5 * time.Second
	pollInterval = 50 * time.Millisecond
)

// assertAPIErrorCode fails the test unless err is a smithy API error with the given code.
func assertAPIErrorCode(t *testing.T, err error, code string) {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, code, apiErr.ErrorCode())
}

// newTestClient stands up the real aws-sdk-go-v2 dsql client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestClient(t *testing.T, h *dsql.Handler) *dsqlsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return dsqlsdk.NewFromConfig(cfg, func(o *dsqlsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestHandler() *dsql.Handler {
	backend := dsql.NewInMemoryBackend()
	h := dsql.NewHandler(backend)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}
