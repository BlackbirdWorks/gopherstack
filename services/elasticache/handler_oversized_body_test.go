package elasticache_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real ElastiCache
// client's DescribeCacheClusters with a Marker large enough to push the
// request body (form-urlencoded, Query/XML protocol) past
// httputils.MaxRequestBodyBytes (a real client can legitimately send this;
// aws-sdk-go-v2 imposes no client-side cap). It mounts Handler() directly
// (e.Any("/*", h.Handler()), the same bypass-RouteMatcher pattern used
// throughout this repo -- e.g. cleanrooms/handler_test.go, iot/handler_helpers_test.go)
// rather than going through service.NewServiceRouter, because RouteMatcher
// itself also calls httputils.ReadBody and silently returns false on the
// same oversized body (a separate, out-of-scope routing defect --
// RouteMatcher swallowing its own read error would otherwise misroute this
// request to a generic 404 before Handler() is ever reached, masking the
// bug this test targets).
//
// Before this fix, Handler()'s ReadBody-failure branch wrote a bare
// "cannot read body" text/plain body -- ElastiCache's Query/XML error
// deserializer expects the wrapped <ErrorResponse><Error>... XML envelope
// (elasticache@v1.56.4 deserializers.go awsAwsquery_deserializeError*), not
// plain text or a JSON body, so the client saw
// smithy.GenericAPIError{Code:"UnknownError"} instead of the typed
// InternalFailure xmlError already writes for every genuine backend error
// (gopherstack-o7gx). This is the Query/XML sibling of the JSON-protocol
// class fixed in gopherstack-wlo1 -- it needs the wrapped XML ErrorResponse
// form via the existing xmlError helper, not a JSON envelope.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1", nil)
	handler := elasticache.NewHandler(backend)

	e := echo.New()
	e.Any("/*", handler.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := elasticachesdk.NewFromConfig(cfg, func(o *elasticachesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err = client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
		Marker: huge,
	}, func(o *elasticachesdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
