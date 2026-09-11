package rdsdata_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

// TestHandler_MalformedBodyCarriesTypedErrorCode drives a raw POST with a
// syntactically invalid JSON body to ExecuteStatement's path (no typed SDK
// client ever emits malformed JSON itself, so this is exercised directly,
// matching how the iot malformed-body instance of this bug class was
// found). Before the fix, handleError's errInvalidRequest/errUnknownAction/
// syntax-or-type-error branch wrote only {"message": ...} with no __type
// key, so restjson.GetErrorInfo (aws-sdk-go-v2 aws/protocol/restjson/
// decoder_util.go:15) -- which reads the X-Amzn-ErrorType header first,
// then __type/code in the body -- found neither, and every real client
// decoded smithy.GenericAPIError{Code:"UnknownError"} instead of
// BadRequestException.
func TestHandler_MalformedBodyCarriesTypedErrorCode(t *testing.T) {
	t.Parallel()

	backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, srv.URL+"/Execute", strings.NewReader("{not valid json"),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/rds-data/aws4_request, "+
			"SignedHeaders=content-type;host, Signature=deadbeef")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, "BadRequestException", decoded["__type"],
		"raw response body must carry __type; got: %s", raw)
	require.NotEmpty(t, decoded["message"])
}
