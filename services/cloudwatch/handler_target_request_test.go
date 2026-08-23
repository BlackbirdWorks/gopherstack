package cloudwatch_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// handleTargetRequest (handler.go) is a second, header-based CBOR dispatch
// path alongside handleCBOR (rpcv2cbor.go). It is reached only when a
// request's URL path does NOT start with the rpc-v2-cbor service prefix but
// DOES carry an X-Amz-Target header -- a shape no cached aws-sdk-go-v2
// cloudwatch version (verified: v1.55.1 through the pinned v1.66.3) ever
// produces. smithy-go's rpcv2.Protocol.SerializeRequest
// (transport/http/protocol/rpcv2/rpcv2.go) unconditionally builds
// "/service/{name}/operation/{op}" and never sets X-Amz-Target -- that header
// belongs to the distinct awsjson protocol family, which cloudwatch has never
// used. So unlike handleCBOR (exercised by every real SDK client, see
// newTestHandlerAndClient's doc comment), this branch cannot be driven by any
// real SDK client; these tests drive it directly with a raw http.Client and
// a hand-set X-Amz-Target header, matching the precedent set by
// TestRouteMatcher_OversizedFormBodyRoutesInsteadOf404 for the analogous
// dead-to-real-clients form-urlencoded branch.
func TestHandleTargetRequest_DistinguishesReadFailureFromEmptyBody(t *testing.T) {
	t.Parallel()

	t.Run("oversized body surfaces a typed read-failure error", func(t *testing.T) {
		t.Parallel()

		srv := newTargetRequestTestServer(t)

		huge := bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))

		resp := doTargetRequest(t, srv, huge)
		defer resp.Body.Close()

		body := make([]byte, 4096)
		n, _ := resp.Body.Read(body)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
			"an unreadable body must not be silently treated as empty input")
		assert.Equal(t, "SerializationException", resp.Header.Get("X-Amzn-Errortype"))
		assert.Contains(t, string(body[:n]), "SerializationException",
			"rpc-v2-cbor clients read the exception name from the CBOR body, not just the header")
	})

	t.Run("empty body dispatches normally", func(t *testing.T) {
		t.Parallel()

		srv := newTargetRequestTestServer(t)

		resp := doTargetRequest(t, srv, nil)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode,
			"a genuinely empty body is a valid, empty-input ListMetrics call")
		assert.NotEqual(t, "SerializationException", resp.Header.Get("X-Amzn-Errortype"))
	})
}

func newTargetRequestTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func doTargetRequest(t *testing.T, srv *httptest.Server, body []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, srv.URL+"/", bytes.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", "GraniteServiceVersion20100801.ListMetrics")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

// TestHandleTargetRequest_ValidCBORBodyStillDispatches is the regression
// guard: a well-formed CBOR map on the target-header path must still reach
// dispatchCBOR and succeed, confirming the read-failure fix above did not
// disturb the success path.
func TestHandleTargetRequest_ValidCBORBodyStillDispatches(t *testing.T) {
	t.Parallel()

	srv := newTargetRequestTestServer(t)

	resp := doTargetRequest(t, srv, cbor.Encode(cbor.Map{}))
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
