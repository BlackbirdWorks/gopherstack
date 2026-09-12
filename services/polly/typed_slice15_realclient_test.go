package polly_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"
	"github.com/aws/aws-sdk-go-v2/service/polly/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

// newTestPollyStreamClient stands up an HTTP/2-over-TLS test server: the
// real aws-sdk-go-v2 polly client refuses StartSpeechSynthesisStream's
// response over HTTP/1.1 with "operation requires minimum HTTP protocol of
// HTTP/2.0" (this op's event-stream RPC protocol is the only polly op with
// that requirement, confirmed live -- every other op here round-trips fine
// over the package's usual plain httptest.Server).
func newTestPollyStreamClient(t *testing.T, h *polly.Handler) *pollysdk.Client {
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

	return pollysdk.NewFromConfig(cfg, func(o *pollysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestTypedSlice15RealClient drives polly's typed-coverage-blind ops
// (gopherstack-n3zi slice 15) through the real aws-sdk-go-v2 client.
func TestTypedSlice15RealClient(t *testing.T) {
	t.Parallel()

	t.Run("describe voices", func(t *testing.T) {
		t.Parallel()

		client := newTestPollySDKClient(t, newHandler())
		ctx := t.Context()

		out, err := client.DescribeVoices(ctx, &pollysdk.DescribeVoicesInput{
			LanguageCode: types.LanguageCodeEnUs,
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.Voices)
		for _, v := range out.Voices {
			assert.Equal(t, types.LanguageCodeEnUs, v.LanguageCode)
			assert.NotEmpty(t, string(v.Id))
		}
	})

	t.Run("speech synthesis task lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestPollySDKClient(t, newHandler())
		ctx := t.Context()

		startOut, err := client.StartSpeechSynthesisTask(ctx, &pollysdk.StartSpeechSynthesisTaskInput{
			OutputFormat:       types.OutputFormatMp3,
			OutputS3BucketName: aws.String("polly-typed-slice15-bucket"),
			Text:               aws.String("hello from slice fifteen"),
			VoiceId:            types.VoiceIdJoanna,
		})
		require.NoError(t, err)
		require.NotNil(t, startOut.SynthesisTask)
		taskID := aws.ToString(startOut.SynthesisTask.TaskId)
		require.NotEmpty(t, taskID)
		assert.Equal(t, types.OutputFormatMp3, startOut.SynthesisTask.OutputFormat)
		assert.Equal(t, types.VoiceIdJoanna, startOut.SynthesisTask.VoiceId)

		getOut, err := client.GetSpeechSynthesisTask(ctx, &pollysdk.GetSpeechSynthesisTaskInput{
			TaskId: aws.String(taskID),
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.SynthesisTask)
		assert.Equal(t, taskID, aws.ToString(getOut.SynthesisTask.TaskId))
		assert.Equal(t, types.VoiceIdJoanna, getOut.SynthesisTask.VoiceId)

		listOut, err := client.ListSpeechSynthesisTasks(ctx, &pollysdk.ListSpeechSynthesisTasksInput{})
		require.NoError(t, err)
		found := false
		for _, task := range listOut.SynthesisTasks {
			if aws.ToString(task.TaskId) == taskID {
				found = true
			}
		}
		assert.True(t, found, "started task must appear in ListSpeechSynthesisTasks")
	})

	// StartSpeechSynthesisStream's real client signs every input event with
	// SigV4 event-stream chunk signing (smithy-go eventstream.SigningWriter):
	// each TextEvent/CloseStreamEvent is nested as the payload of an outer
	// frame carrying only ":date"/":chunk-signature" headers, which
	// decodeStreamText never unwrapped -- see the fix in handler.go. Only
	// the writer half is closed before draining Events(); calling the
	// combined stream.Close() first tears down the reader too and starves
	// the range loop before any response can arrive.
	t.Run("speech synthesis stream", func(t *testing.T) {
		t.Parallel()

		client := newTestPollyStreamClient(t, newHandler())
		ctx := t.Context()

		out, err := client.StartSpeechSynthesisStream(ctx, &pollysdk.StartSpeechSynthesisStreamInput{
			Engine:       types.EngineGenerative,
			OutputFormat: types.OutputFormatMp3,
			VoiceId:      types.VoiceIdJoanna,
		})
		require.NoError(t, err)

		stream := out.GetStream()
		require.NoError(t, stream.Send(ctx, &types.StartSpeechSynthesisStreamActionStreamMemberTextEvent{
			Value: types.TextEvent{Text: aws.String("hello world")},
		}))
		require.NoError(t, stream.Writer.Close())

		var audioLen int
		var closedEvent *types.StreamClosedEvent
		for event := range stream.Events() {
			switch e := event.(type) {
			case *types.StartSpeechSynthesisStreamEventStreamMemberAudioEvent:
				audioLen += len(e.Value.AudioChunk)
			case *types.StartSpeechSynthesisStreamEventStreamMemberStreamClosedEvent:
				closedEvent = &e.Value
			}
		}
		require.NoError(t, stream.Err())
		require.NoError(t, stream.Close())
		assert.Positive(t, audioLen)
		require.NotNil(t, closedEvent)
		assert.EqualValues(t, len("hello world"), closedEvent.RequestCharacters)
	})
}
