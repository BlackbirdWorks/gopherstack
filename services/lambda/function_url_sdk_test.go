package lambda_test

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

var sdkURLPattern = regexp.MustCompile(`^https://[0-9a-f]{32}\.lambda-url\.us-east-1\.on\.aws/$`)

func TestFunctionURL_SDKOpsAndHostRouting(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	fn := aws.String("sdk-url-fn")
	h, bk := newInMemoryHandler(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = h.Handler()(echo.New().NewContext(r, w))
	}))
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	client := lambdasdk.NewFromConfig(cfg, func(o *lambdasdk.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.HTTPClient = sdktest.PlainBodyHTTPClient()
	})

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "sdk-url-fn", PackageType: lambda.PackageTypeImage, ImageURI: "test:latest",
	}))

	created, err := client.CreateFunctionUrlConfig(ctx, &lambdasdk.CreateFunctionUrlConfigInput{
		FunctionName: fn,
		AuthType:     lambdatypes.FunctionUrlAuthTypeAwsIam,
		InvokeMode:   lambdatypes.InvokeModeResponseStream,
		Cors: &lambdatypes.Cors{
			AllowOrigins: []string{"https://example.com"},
			MaxAge:       aws.Int32(600),
		},
	})
	require.NoError(t, err)
	assert.Regexp(t, sdkURLPattern, aws.ToString(created.FunctionUrl))
	assert.Equal(t, lambdatypes.InvokeModeResponseStream, created.InvokeMode)

	got, err := client.GetFunctionUrlConfig(ctx,
		&lambdasdk.GetFunctionUrlConfigInput{FunctionName: fn})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.FunctionUrl), aws.ToString(got.FunctionUrl))

	upd, err := client.UpdateFunctionUrlConfig(ctx, &lambdasdk.UpdateFunctionUrlConfigInput{
		FunctionName: fn,
		AuthType:     lambdatypes.FunctionUrlAuthTypeNone,
	})
	require.NoError(t, err)
	assert.Equal(t, lambdatypes.FunctionUrlAuthTypeNone, upd.AuthType)

	list, err := client.ListFunctionUrlConfigs(ctx, &lambdasdk.ListFunctionUrlConfigsInput{FunctionName: fn})
	require.NoError(t, err)
	require.Len(t, list.FunctionUrlConfigs, 1)

	_, err = client.CreateFunctionUrlConfig(ctx, &lambdasdk.CreateFunctionUrlConfigInput{
		FunctionName: fn,
		AuthType:     "BASIC",
	})
	require.Error(t, err)

	host := aws.ToString(created.FunctionUrl)[len("https://") : len(aws.ToString(created.FunctionUrl))-1]

	req, reqErr := http.NewRequestWithContext(ctx, http.MethodOptions, server.URL+"/", nil)
	require.NoError(t, reqErr)

	req.Host = host
	req.Header.Set("Origin", "https://example.com")

	resp, doErr := testclient.New(t).Do(req)
	require.NoError(t, doErr)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"))

	_, err = client.DeleteFunctionUrlConfig(ctx, &lambdasdk.DeleteFunctionUrlConfigInput{FunctionName: fn})
	require.NoError(t, err)

	_, err = client.GetFunctionUrlConfig(ctx,
		&lambdasdk.GetFunctionUrlConfigInput{FunctionName: fn})
	require.Error(t, err)
}
