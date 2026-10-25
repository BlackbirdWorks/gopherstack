package lambda_test

import (
	"archive/zip"
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func newLimitsClient(t *testing.T, st lambda.Settings) *lambdasdk.Client {
	t.Helper()

	bk := lambda.NewInMemoryBackend(nil, nil, st, "000000000000", "us-east-1")
	closeBackend(t, bk)

	h := lambda.NewHandler(bk)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = h.Handler()(echo.New().NewContext(r, w))
	}))
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(t.Context(), config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return lambdasdk.NewFromConfig(cfg, func(o *lambdasdk.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.HTTPClient = sdktest.PlainBodyHTTPClient()
	})
}

func zipWith(t *testing.T, size int) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := zip.NewWriter(&buf)
	f, err := w.Create("a.py")
	require.NoError(t, err)
	_, err = f.Write(bytes.Repeat([]byte("a"), size))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func errCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func createZip(t *testing.T, c *lambdasdk.Client, name, runtime string, code []byte, env map[string]string) error {
	t.Helper()

	in := &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String(name),
		Role:         aws.String("arn:aws:iam::000000000000:role/r"),
		Handler:      aws.String("index.handler"),
		Runtime:      lambdatypes.Runtime(runtime),
		Code:         &lambdatypes.FunctionCode{ZipFile: code},
	}
	if env != nil {
		in.Environment = &lambdatypes.Environment{Variables: env}
	}

	_, err := c.CreateFunction(t.Context(), in)

	return err
}

func TestLimits_CreateFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		env      map[string]string
		name     string
		runtime  string
		wantCode string
		code     []byte
		settings lambda.Settings
	}{
		{name: "defaults accept", runtime: "python3.12", code: zipWith(t, 10)},
		{
			name: "zipped too large", runtime: "python3.12", code: zipWith(t, 10),
			settings: lambda.Settings{CodeSizeZipped: 5}, wantCode: "RequestEntityTooLargeException",
		},
		{
			name: "unzipped too large", runtime: "python3.12", code: zipWith(t, 5000),
			settings: lambda.Settings{CodeSizeUnzipped: 1000}, wantCode: "InvalidParameterValueException",
		},
		{
			name: "total code storage", runtime: "python3.12", code: zipWith(t, 10),
			settings: lambda.Settings{TotalCodeSize: 10}, wantCode: "CodeStorageExceededException",
		},
		{
			name: "env too large", runtime: "python3.12", code: zipWith(t, 10),
			env:      map[string]string{"KEY_ONE": "0123456789"},
			settings: lambda.Settings{MaxEnvironmentBytes: 8}, wantCode: "InvalidParameterValueException",
		},
		{
			name: "request too large", runtime: "python3.12", code: zipWith(t, 10),
			settings: lambda.Settings{CreateFunctionRequestSize: 100}, wantCode: "RequestEntityTooLargeException",
		},
		{name: "deprecated accepted by default", runtime: "python3.7", code: zipWith(t, 10)},
		{
			name: "deprecated rejected strict", runtime: "python3.7", code: zipWith(t, 10),
			settings: lambda.Settings{StrictRuntimeValidation: true}, wantCode: "InvalidParameterValueException",
		},
		{
			name: "supported accepted strict", runtime: "python3.12", code: zipWith(t, 10),
			settings: lambda.Settings{StrictRuntimeValidation: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newLimitsClient(t, tt.settings)
			err := createZip(t, c, "fn", tt.runtime, tt.code, tt.env)

			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, errCode(err))
		})
	}
}

func TestLimits_AccountSettingsAndReservedConcurrency(t *testing.T) {
	t.Parallel()

	c := newLimitsClient(t, lambda.Settings{
		ConcurrentExecutions: 300, MinimumUnreservedConcurrency: 50, TotalCodeSize: 1 << 20,
		CodeSizeZipped: 2048, CodeSizeUnzipped: 4096,
	})

	out, err := c.GetAccountSettings(t.Context(), &lambdasdk.GetAccountSettingsInput{})
	require.NoError(t, err)
	assert.EqualValues(t, 300, out.AccountLimit.ConcurrentExecutions)
	assert.EqualValues(t, 300, aws.ToInt32(out.AccountLimit.UnreservedConcurrentExecutions))
	assert.EqualValues(t, 1<<20, out.AccountLimit.TotalCodeSize)
	assert.EqualValues(t, 2048, out.AccountLimit.CodeSizeZipped)
	assert.EqualValues(t, 4096, out.AccountLimit.CodeSizeUnzipped)

	require.NoError(t, createZip(t, c, "fn", "python3.12", zipWith(t, 10), nil))

	_, err = c.PutFunctionConcurrency(t.Context(), &lambdasdk.PutFunctionConcurrencyInput{
		FunctionName: aws.String("fn"), ReservedConcurrentExecutions: aws.Int32(251),
	})
	require.Error(t, err)
	assert.Equal(t, "InvalidParameterValueException", errCode(err))

	_, err = c.PutFunctionConcurrency(t.Context(), &lambdasdk.PutFunctionConcurrencyInput{
		FunctionName: aws.String("fn"), ReservedConcurrentExecutions: aws.Int32(250),
	})
	require.NoError(t, err)

	out, err = c.GetAccountSettings(t.Context(), &lambdasdk.GetAccountSettingsInput{})
	require.NoError(t, err)
	assert.EqualValues(t, 50, aws.ToInt32(out.AccountLimit.UnreservedConcurrentExecutions))
}
