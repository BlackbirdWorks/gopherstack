package bedrockruntime_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bedrockruntimesdk "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// newTestBedrockRuntimeSDKClient stands up the real aws-sdk-go-v2
// bedrockruntime client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production -- so event-stream framing is proven by the real client's own
// eventstream reader, not by hand-parsing the raw binary frames.
func newTestBedrockRuntimeSDKClient(
	t *testing.T,
	h *bedrockruntime.Handler,
) *bedrockruntimesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return bedrockruntimesdk.NewFromConfig(cfg, func(o *bedrockruntimesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestInvokeModelWithResponseStream_SDKRoundTrip proves the "chunk" event's
// payload really decodes through the real SDK's eventstream reader into a
// non-nil types.PayloadPart.Bytes. deserializeDocumentPayloadPart
// (bedrockruntime@v1.57.1 deserializers.go) requires the payload document to
// be exactly {"bytes":"<base64>"} -- any other top-level shape leaves Bytes
// nil with no client-visible error, which was the CRITICAL bug this service
// carried before the 2026-08-07 pass. This test drives the real client
// instead of hand-parsing the wire bytes, closing the gap the existing
// eventstream_test.go left (it only asserts frame length, never decodes a
// PayloadPart).
func TestInvokeModelWithResponseStream_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrockruntime.NewHandler(backend)
	client := newTestBedrockRuntimeSDKClient(t, h)

	out, err := client.InvokeModelWithResponseStream(
		t.Context(),
		&bedrockruntimesdk.InvokeModelWithResponseStreamInput{
			ModelId:     aws.String("amazon.nova-pro-v1:0"),
			ContentType: aws.String("application/json"),
			Accept:      aws.String("application/json"),
			Body:        []byte(`{"prompt":"hello"}`),
		},
	)
	require.NoError(t, err)

	stream := out.GetStream()
	t.Cleanup(func() { _ = stream.Close() })

	var chunks int

	for event := range stream.Events() {
		chunk, ok := event.(*types.ResponseStreamMemberChunk)
		require.True(t, ok, "expected a ResponseStreamMemberChunk event, got %T", event)
		require.NotNil(
			t,
			chunk.Value.Bytes,
			`PayloadPart.Bytes decoded nil -- chunk payload is not the {"bytes":...} shape the real deserializer requires`,
		)
		assert.NotEmpty(t, chunk.Value.Bytes)

		chunks++
	}

	require.NoError(t, stream.Err())
	assert.Positive(t, chunks, "expected at least one chunk event")
}

// TestApplyGuardrail_ActionEnum_SDKRoundTrip proves ApplyGuardrailOutput.Action
// decodes to a real types.GuardrailAction member. That enum has exactly two
// values -- "NONE" and "GUARDRAIL_INTERVENED" (types/enums.go) -- there is no
// "BLOCKED" member; "BLOCKED" belongs to the unrelated, nested
// types.GuardrailWordPolicyAction used only inside
// assessments[].wordPolicy.customWords[].action. Sending "BLOCKED" as the
// top-level action leaves ApplyGuardrailOutput.Action holding a string no
// real client code branches on (Go doesn't reject unknown enum strings at
// decode time, so this bug produced no client-side error -- just silent
// mis-detection of every blocked call).
func TestApplyGuardrail_ActionEnum_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrockruntime.NewHandler(backend)
	client := newTestBedrockRuntimeSDKClient(t, h)

	out, err := client.ApplyGuardrail(t.Context(), &bedrockruntimesdk.ApplyGuardrailInput{
		GuardrailIdentifier: aws.String("my-guardrail"),
		GuardrailVersion:    aws.String("1"),
		Source:              types.GuardrailContentSourceInput,
		Content: []types.GuardrailContentBlock{
			&types.GuardrailContentBlockMemberText{
				Value: types.GuardrailTextBlock{Text: aws.String("this is harmful content")},
			},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, types.GuardrailActionGuardrailIntervened, out.Action)
	require.Len(t, out.Assessments, 1)
	require.NotNil(t, out.Assessments[0].WordPolicy)
	require.Len(t, out.Assessments[0].WordPolicy.CustomWords, 1)
	assert.Equal(
		t,
		types.GuardrailWordPolicyActionBlocked,
		out.Assessments[0].WordPolicy.CustomWords[0].Action,
	)
}

// TestConverseStream_SDKRoundTrip drives ConverseStream through the real
// client and asserts every event in the union decodes to a known
// ConverseStreamOutput variant -- never types.UnknownUnionMember, which is
// what a caller sees when the mock emits a field the real deserializer's
// union switch doesn't recognize (e.g. the "start":{"text":""} fabricated
// field the 2026-08-07 pass removed from contentBlockStart).
func TestConverseStream_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrockruntime.NewHandler(backend)
	client := newTestBedrockRuntimeSDKClient(t, h)

	out, err := client.ConverseStream(t.Context(), &bedrockruntimesdk.ConverseStreamInput{
		ModelId: aws.String("amazon.nova-pro-v1:0"),
		Messages: []types.Message{
			{
				Role:    types.ConversationRoleUser,
				Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: "hello"}},
			},
		},
	})
	require.NoError(t, err)

	stream := out.GetStream()
	t.Cleanup(func() { _ = stream.Close() })

	var sawDelta, sawStop bool

	for event := range stream.Events() {
		switch e := event.(type) {
		case *types.ConverseStreamOutputMemberMessageStart:
			assert.Equal(t, types.ConversationRoleAssistant, e.Value.Role)
		case *types.ConverseStreamOutputMemberContentBlockStart:
			// types.ContentBlockStart only has image/toolResult/toolUse
			// variants (deserializeDocumentContentBlockStart); a plain-text
			// block must omit "start" so Start decodes nil, not
			// UnknownUnionMember for a fabricated "text" tag.
			if _, unk := e.Value.Start.(*types.UnknownUnionMember); unk {
				t.Fatalf(
					"contentBlockStart.start decoded as UnknownUnionMember -- gopherstack sent a " +
						"discriminator key the real union deserializer does not recognize",
				)
			}

			assert.Nil(t, e.Value.Start, "plain-text content block should omit start entirely")
		case *types.ConverseStreamOutputMemberContentBlockDelta:
			delta, ok := e.Value.Delta.(*types.ContentBlockDeltaMemberText)
			require.True(t, ok, "expected a text content-block delta, got %T", e.Value.Delta)
			assert.NotEmpty(t, delta.Value)

			sawDelta = true
		case *types.ConverseStreamOutputMemberMessageStop:
			assert.Equal(t, types.StopReasonEndTurn, e.Value.StopReason)

			sawStop = true
		case *types.UnknownUnionMember:
			t.Fatalf(
				"event decoded as UnknownUnionMember with tag %q -- gopherstack emitted a field/shape "+
					"the real SDK deserializer's union switch does not recognize", e.Tag,
			)
		}
	}

	require.NoError(t, stream.Err())
	assert.True(t, sawDelta, "expected a contentBlockDelta event")
	assert.True(t, sawStop, "expected a messageStop event")
}
