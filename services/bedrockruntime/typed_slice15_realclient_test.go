package bedrockruntime_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bedrockruntimesdk "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// newTestBedrockRuntimeH2Client stands up an HTTP/2-over-TLS test server:
// InvokeModelWithBidirectionalStream's real client refuses its response
// over HTTP/1.1 ("operation requires minimum HTTP protocol of HTTP/2.0"),
// the same requirement polly's StartSpeechSynthesisStream has -- every
// other op in this package (including the response-only ConverseStream/
// InvokeModelWithResponseStream) round-trips fine over the usual plain
// httptest.Server via newTestBedrockRuntimeSDKClient.
func newTestBedrockRuntimeH2Client(t *testing.T, h *bedrockruntime.Handler) *bedrockruntimesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewUnstartedServer(e)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
		awscfg.WithHTTPClient(srv.Client()),
	)
	require.NoError(t, err)

	return bedrockruntimesdk.NewFromConfig(cfg, func(o *bedrockruntimesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestTypedSlice15RealClient drives bedrockruntime's typed-coverage-blind
// ops (gopherstack-n3zi slice 15) through the real aws-sdk-go-v2 client.
func TestTypedSlice15RealClient(t *testing.T) {
	t.Parallel()

	t.Run("converse", func(t *testing.T) {
		t.Parallel()

		backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
		h := bedrockruntime.NewHandler(backend)
		client := newTestBedrockRuntimeSDKClient(t, h)

		out, err := client.Converse(t.Context(), &bedrockruntimesdk.ConverseInput{
			ModelId: aws.String("amazon.nova-pro-v1:0"),
			Messages: []types.Message{
				{
					Role:    types.ConversationRoleUser,
					Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: "hello"}},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, out.Output)
		msgOut, ok := out.Output.(*types.ConverseOutputMemberMessage)
		require.True(t, ok, "expected a message output, got %T", out.Output)
		assert.Equal(t, types.ConversationRoleAssistant, msgOut.Value.Role)
		require.NotEmpty(t, msgOut.Value.Content)
		assert.Equal(t, types.StopReasonEndTurn, out.StopReason)
		require.NotNil(t, out.Usage)
		assert.Positive(t, aws.ToInt32(out.Usage.InputTokens))
	})

	t.Run("count tokens", func(t *testing.T) {
		t.Parallel()

		backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
		h := bedrockruntime.NewHandler(backend)
		client := newTestBedrockRuntimeSDKClient(t, h)

		out, err := client.CountTokens(t.Context(), &bedrockruntimesdk.CountTokensInput{
			ModelId: aws.String("amazon.nova-pro-v1:0"),
			Input: &types.CountTokensInputMemberConverse{
				Value: types.ConverseTokensRequest{
					Messages: []types.Message{
						{
							Role: types.ConversationRoleUser,
							Content: []types.ContentBlock{
								&types.ContentBlockMemberText{Value: "hello there, how are you today?"},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Positive(t, aws.ToInt32(out.InputTokens))
	})

	t.Run("async invoke lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
		h := bedrockruntime.NewHandler(backend)
		client := newTestBedrockRuntimeSDKClient(t, h)

		startOut, err := client.StartAsyncInvoke(t.Context(), &bedrockruntimesdk.StartAsyncInvokeInput{
			ModelId:    aws.String("amazon.nova-pro-v1:0"),
			ModelInput: document.NewLazyDocument(map[string]any{"prompt": "hello async"}),
			OutputDataConfig: &types.AsyncInvokeOutputDataConfigMemberS3OutputDataConfig{
				Value: types.AsyncInvokeS3OutputDataConfig{S3Uri: aws.String("s3://s15-bucket/output/")},
			},
		})
		require.NoError(t, err)
		invocationARN := aws.ToString(startOut.InvocationArn)
		require.NotEmpty(t, invocationARN)

		getOut, err := client.GetAsyncInvoke(t.Context(), &bedrockruntimesdk.GetAsyncInvokeInput{
			InvocationArn: aws.String(invocationARN),
		})
		require.NoError(t, err)
		assert.Equal(t, invocationARN, aws.ToString(getOut.InvocationArn))
		assert.NotEmpty(t, aws.ToString(getOut.ModelArn))
		require.NotNil(t, getOut.OutputDataConfig)
		s3Cfg, ok := getOut.OutputDataConfig.(*types.AsyncInvokeOutputDataConfigMemberS3OutputDataConfig)
		require.True(t, ok, "expected S3 output data config, got %T", getOut.OutputDataConfig)
		assert.Equal(t, "s3://s15-bucket/output/", aws.ToString(s3Cfg.Value.S3Uri))
		require.NotNil(t, getOut.SubmitTime)
	})

	t.Run("invoke guardrail checks", func(t *testing.T) {
		t.Parallel()

		backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
		h := bedrockruntime.NewHandler(backend)
		client := newTestBedrockRuntimeSDKClient(t, h)

		out, err := client.InvokeGuardrailChecks(t.Context(), &bedrockruntimesdk.InvokeGuardrailChecksInput{
			Checks: &types.GuardrailChecksConfig{
				SensitiveInformation: &types.GuardrailChecksSensitiveInformationConfig{
					Entities: []types.GuardrailChecksSensitiveInformationEntityConfig{
						{Type: types.GuardrailChecksSensitiveInformationEntityTypeEmail},
					},
				},
			},
			Messages: []types.GuardrailChecksMessage{
				{
					Role: types.GuardrailChecksRoleUser,
					Content: []types.GuardrailChecksContentBlock{
						&types.GuardrailChecksContentBlockMemberText{Value: "contact me at person@example.com"},
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, out.Results)
		require.NotNil(t, out.Results.SensitiveInformation)
		require.Len(t, out.Results.SensitiveInformation.Results, 1)
		assert.Equal(t, types.GuardrailChecksSensitiveInformationEntityTypeEmail,
			out.Results.SensitiveInformation.Results[0].Type)
		require.NotNil(t, out.Usage)
	})

	t.Run("invoke model with bidirectional stream", func(t *testing.T) {
		t.Parallel()

		backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
		h := bedrockruntime.NewHandler(backend)
		client := newTestBedrockRuntimeH2Client(t, h)
		ctx := t.Context()

		out, err := client.InvokeModelWithBidirectionalStream(
			ctx, &bedrockruntimesdk.InvokeModelWithBidirectionalStreamInput{
				ModelId: aws.String("amazon.nova-sonic-v1:0"),
			},
		)
		require.NoError(t, err)

		stream := out.GetStream()
		require.NoError(t, stream.Writer.Close())

		var sawChunk bool
		for event := range stream.Events() {
			if chunk, ok := event.(*types.InvokeModelWithBidirectionalStreamOutputMemberChunk); ok {
				assert.NotEmpty(t, chunk.Value.Bytes)
				sawChunk = true
			}
		}
		require.NoError(t, stream.Err())
		require.NoError(t, stream.Close())
		assert.True(t, sawChunk, "expected at least one chunk event")
	})
}
