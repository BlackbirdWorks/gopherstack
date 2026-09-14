package lambda_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// captureTransport records the raw bytes of the last HTTP response, then
// replays them so the real SDK deserializer still sees the full body.
type captureTransport struct {
	body []byte
}

func (c *captureTransport) Do(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	b, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	resp.Body.Close()

	c.body = b
	resp.Body = io.NopCloser(bytes.NewReader(b))

	return resp, nil
}

// newCapturingWireTestLambdaClient is newWireTestLambdaClient plus a
// transport that stashes each response's raw bytes on capture.body, so a
// test can assert on the wire JSON while also proving the real client
// decodes it.
func newCapturingWireTestLambdaClient(t *testing.T, h *lambda.Handler) (*lambdasdk.Client, *captureTransport) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(lambdaWireTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	capture := &captureTransport{}

	client := lambdasdk.NewFromConfig(cfg, func(o *lambdasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = capture
	})

	return client, capture
}

// TestFunctionConfigurationWire_TagsStripped covers every wire path that
// marshals FunctionConfiguration directly (gopherstack-5dslv): none of them
// declare Tags on the real response, so the raw body must not carry the key
// even though the function has tags persisted. Each case also decodes the
// response through the real typed client to prove the strip didn't break
// the shape.
func TestFunctionConfigurationWire_TagsStripped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *lambdasdk.Client)
		name string
	}{
		{
			name: "create_function",
			run: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()

				out, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
					FunctionName: aws.String("wire-tags-create"),
					PackageType:  lambdatypes.PackageTypeImage,
					Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("img:latest")},
					Role:         aws.String("arn:aws:iam:::role/r"),
					Tags:         map[string]string{"team": "platform"},
				})
				require.NoError(t, err)
				require.NotNil(t, out)
				assert.Equal(t, "wire-tags-create", aws.ToString(out.FunctionName))
			},
		},
		{
			name: "create_function_publish",
			run: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()

				out, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
					FunctionName: aws.String("wire-tags-create-publish"),
					PackageType:  lambdatypes.PackageTypeImage,
					Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("img:latest")},
					Role:         aws.String("arn:aws:iam:::role/r"),
					Tags:         map[string]string{"team": "platform"},
					Publish:      true,
				})
				require.NoError(t, err)
				require.NotNil(t, out)
				assert.Equal(t, "1", aws.ToString(out.Version))
			},
		},
		{
			name: "update_function_code",
			run: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()

				createTaggedFunctionForWireTest(t, client, "wire-tags-update-code")

				out, err := client.UpdateFunctionCode(t.Context(), &lambdasdk.UpdateFunctionCodeInput{
					FunctionName: aws.String("wire-tags-update-code"),
					ImageUri:     aws.String("img:v2"),
				})
				require.NoError(t, err)
				require.NotNil(t, out)
			},
		},
		{
			name: "update_function_code_publish",
			run: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()

				createTaggedFunctionForWireTest(t, client, "wire-tags-update-code-publish")

				out, err := client.UpdateFunctionCode(t.Context(), &lambdasdk.UpdateFunctionCodeInput{
					FunctionName: aws.String("wire-tags-update-code-publish"),
					ImageUri:     aws.String("img:v2"),
					Publish:      true,
				})
				require.NoError(t, err)
				require.NotNil(t, out)
				assert.Equal(t, "1", aws.ToString(out.Version))
			},
		},
		{
			name: "update_function_configuration",
			run: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()

				createTaggedFunctionForWireTest(t, client, "wire-tags-update-config")

				out, err := client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
					FunctionName: aws.String("wire-tags-update-config"),
					Description:  aws.String("updated"),
				})
				require.NoError(t, err)
				require.NotNil(t, out)
			},
		},
		{
			name: "get_function_configuration",
			run: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()

				createTaggedFunctionForWireTest(t, client, "wire-tags-get-config")

				out, err := client.GetFunctionConfiguration(t.Context(), &lambdasdk.GetFunctionConfigurationInput{
					FunctionName: aws.String("wire-tags-get-config"),
				})
				require.NoError(t, err)
				require.NotNil(t, out)
			},
		},
		{
			name: "list_functions",
			run: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()

				createTaggedFunctionForWireTest(t, client, "wire-tags-list")

				out, err := client.ListFunctions(t.Context(), &lambdasdk.ListFunctionsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, out.Functions)
			},
		},
		{
			name: "list_functions_all_versions",
			run: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()

				createTaggedFunctionForWireTest(t, client, "wire-tags-list-all")

				out, err := client.ListFunctions(t.Context(), &lambdasdk.ListFunctionsInput{
					FunctionVersion: lambdatypes.FunctionVersionAll,
				})
				require.NoError(t, err)
				require.NotEmpty(t, out.Functions)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client, capture := newCapturingWireTestLambdaClient(t, h)

			tt.run(t, client)

			require.NotEmpty(t, capture.body)
			assert.NotContains(t, string(capture.body), `"Tags"`,
				"response must not carry the persisted-only Tags field on the wire")
		})
	}
}

// TestGetFunction_StillCarriesTagsAsSibling proves the fix stripped the
// right paths and not GetFunction: Tags is a real sibling of Configuration
// on GetFunctionOutput and must still appear on the wire.
func TestGetFunction_StillCarriesTagsAsSibling(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client, capture := newCapturingWireTestLambdaClient(t, h)

	createTaggedFunctionForWireTest(t, client, "wire-tags-get-function")

	out, err := client.GetFunction(t.Context(), &lambdasdk.GetFunctionInput{
		FunctionName: aws.String("wire-tags-get-function"),
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, map[string]string{"team": "platform"}, out.Tags)

	require.NotEmpty(t, capture.body)
	assert.Contains(t, string(capture.body), `"Tags":{"team":"platform"}`)
}

// createTaggedFunctionForWireTest creates a function with a non-empty Tags
// map through the real client, so later reads of it have something to leak
// if the wire strip regresses.
func createTaggedFunctionForWireTest(t *testing.T, client *lambdasdk.Client, name string) {
	t.Helper()

	_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String(name),
		PackageType:  lambdatypes.PackageTypeImage,
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("img:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
		Tags:         map[string]string{"team": "platform"},
	})
	require.NoError(t, err)
}
