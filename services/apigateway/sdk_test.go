package apigateway_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestAPIGateway_GetSdkTypes(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	rec := restCall(t, h, http.MethodGet, "/sdktypes", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["item"].([]any)
	require.NotEmpty(t, items)

	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.(map[string]any)["id"].(string))
	}

	assert.Contains(t, ids, "java")
	assert.Contains(t, ids, "javascript")
	assert.Contains(t, ids, "android")
}

func TestAPIGateway_GetSdkType(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	rec := restCall(t, h, http.MethodGet, "/sdktypes/java", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "java", resp["id"])
	assert.Equal(t, "Java", resp["friendlyName"])

	notFoundRec := restCall(t, h, http.MethodGet, "/sdktypes/cobol", "", "")
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)
}

// captureRawResponseHeader returns a client option that records the literal
// wire-level value of headerName into dst, bypassing whatever the SDK's
// typed Output struct does with it. Used to prove the HTTP response's
// Content-Type is not application/json -- a field-level assertion on the
// decoded GetSdkOutput can't show that, since ContentType there is only
// ever populated FROM that header (see
// awsRestjson1_deserializeOpHttpBindingsGetSdkOutput,
// apigateway@v1.42.4 deserializers.go).
func captureRawResponseHeader(headerName string, dst *string) func(*apigatewaysdk.Options) {
	return func(o *apigatewaysdk.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc(
				"CaptureRawResponseHeader",
				func(
					ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
				) (middleware.DeserializeOutput, middleware.Metadata, error) {
					out, metadata, err := next.HandleDeserialize(ctx, in)
					if resp, ok := out.RawResponse.(*smithyhttp.Response); ok {
						*dst = resp.Header.Get(headerName)
					}

					return out, metadata, err
				},
			), middleware.Before)
		})
	}
}

// TestAPIGateway_GetSdk_HeadersNotBody_RealClient covers gopherstack-eax4:
// GetSdkOutput's ContentType/ContentDisposition are HTTP response headers
// (awsRestjson1_deserializeOpHttpBindingsGetSdkOutput, apigateway@v1.42.4
// deserializers.go:13316) and Body is the raw payload
// (awsRestjson1_deserializeOpDocumentGetSdkOutput, deserializers.go:13333 --
// copies response.Body directly into Body, no JSON parsing at all) -- never
// JSON response fields. The old handler wrote a JSON object
// {"contentType":...,"contentDisposition":...,"body":...} with
// Content-Type: application/json, so a real client's ContentType/
// ContentDisposition fields decoded as the zero value and Body decoded as
// nil/garbage regardless of what the backend produced. Driven through the
// real SDK client since this is a header-vs-body confusion no field-level
// diff of a JSON response can detect -- the previous version of this test
// asserted exactly that JSON shape and passed against the broken handler.
func TestAPIGateway_GetSdk_HeadersNotBody_RealClient(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "sdk-api"})
	require.NoError(t, err)
	_, err = backend.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	client := newTestAPIGatewayClient(t, h)

	var rawContentType string

	out, err := client.GetSdk(t.Context(), &apigatewaysdk.GetSdkInput{
		RestApiId: aws.String(api.ID),
		StageName: aws.String("prod"),
		SdkType:   aws.String("java"),
	}, captureRawResponseHeader("Content-Type", &rawContentType))
	require.NoError(t, err)
	require.NotNil(t, out)

	assert.NotEmpty(t, rawContentType, "wire Content-Type header must be set")
	assert.NotEqual(t, "application/json", rawContentType,
		"GetSdk's HTTP response must not be a JSON envelope")

	require.NotNil(t, out.ContentType)
	assert.Equal(t, "application/octet-stream", *out.ContentType,
		"ContentType must round-trip via the real Content-Type response header")

	require.NotNil(t, out.ContentDisposition)
	assert.Contains(t, *out.ContentDisposition, "sdk.zip",
		"ContentDisposition must round-trip via the real Content-Disposition response header")

	require.NotEmpty(t, out.Body, "Body must round-trip as raw bytes, not a base64 JSON field")

	zr, err := zip.NewReader(bytes.NewReader(out.Body), int64(len(out.Body)))
	require.NoError(t, err, "GetSdk must return a real ZIP archive, not a fabricated blob")

	names := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		names = append(names, f.Name)
	}

	assert.Contains(t, names, "swagger.json")
	assert.Contains(t, names, "README.txt")
}

func TestAPIGateway_GetSdk_Errors(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "sdk-api"})
	require.NoError(t, err)
	_, err = backend.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	// Unknown sdkType must be rejected, not silently accepted.
	badRec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/prod/sdks/cobol", "", "")
	assert.Equal(t, http.StatusBadRequest, badRec.Code)

	// Unknown stage must 404.
	missingStageRec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/nope/sdks/java", "", "")
	assert.Equal(t, http.StatusNotFound, missingStageRec.Code)
}
