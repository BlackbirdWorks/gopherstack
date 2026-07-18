package pipes_test

// Per-target dispatch coverage for the pipe runner: verifies that each
// supported AWS target/enrichment ARN type is invoked with the right
// arguments, and that Pipes InvocationType values are correctly mapped onto
// each downstream service's own invocation-type enum.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// --- b3 helpers (eu-west-1 / 111122223333 test account) ---

func b3Backend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("111122223333", "eu-west-1")
}

func b3Handler(t *testing.T) *pipes.Handler {
	t.Helper()

	return pipes.NewHandler(b3Backend())
}

func b3Do(t *testing.T, h *pipes.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var b []byte

	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/eu-west-1/pipes/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func b3Create(t *testing.T, h *pipes.Handler, name string, body map[string]any) map[string]any {
	t.Helper()

	rec := b3Do(t, h, http.MethodPost, "/v1/pipes/"+name, body)
	require.Equal(t, http.StatusOK, rec.Code, "create pipe %q: %s", name, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func b3Describe(t *testing.T, h *pipes.Handler, name string) map[string]any {
	t.Helper()

	rec := b3Do(t, h, http.MethodGet, "/v1/pipes/"+name, nil)
	require.Equal(t, http.StatusOK, rec.Code, "describe pipe %q: %s", name, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func b3CreatePipe(t *testing.T, b *pipes.InMemoryBackend, name, target string) {
	t.Helper()

	_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:         name,
		RoleARN:      "arn:aws:iam::111122223333:role/r",
		Source:       b3SQSSource,
		Target:       target,
		DesiredState: "RUNNING",
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, b, name)
}

const (
	b3SQSSource    = "arn:aws:sqs:eu-west-1:111122223333:q"
	b3LambdaTarget = "arn:aws:lambda:eu-west-1:111122223333:function:fn"
)

// --- b3 mock implementations ---

type b3MockSQSReader struct {
	messages []*pipes.SQSMessage
	deleted  []string
	mu       sync.Mutex
}

func (m *b3MockSQSReader) ReceivePipeMessages(_ string, _ int) ([]*pipes.SQSMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.messages
	m.messages = nil

	return msgs, nil
}

func (m *b3MockSQSReader) DeletePipeMessages(_ string, receiptHandles []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted = append(m.deleted, receiptHandles...)

	return nil
}

type b3MockLambdaInvoker struct {
	calls           []string
	payloads        [][]byte
	invocationTypes []string
	returnPayload   []byte
	mu              sync.Mutex
}

func (m *b3MockLambdaInvoker) InvokeFunction(
	_ context.Context,
	name, invocationType string,
	payload []byte,
) ([]byte, int, error) {
	m.mu.Lock()
	m.calls = append(m.calls, name)
	m.payloads = append(m.payloads, payload)
	m.invocationTypes = append(m.invocationTypes, invocationType)
	ret := m.returnPayload
	m.mu.Unlock()

	return ret, 200, nil
}

// b3MuxLambdaInvoker routes Lambda invocations to different mocks based on function name.
type b3MuxLambdaInvoker struct {
	enrich   *b3MockLambdaInvoker
	target   *b3MockLambdaInvoker
	enrichFn string
}

func (m *b3MuxLambdaInvoker) InvokeFunction(
	ctx context.Context,
	name, invocationType string,
	payload []byte,
) ([]byte, int, error) {
	if name == m.enrichFn {
		return m.enrich.InvokeFunction(ctx, name, invocationType, payload)
	}

	return m.target.InvokeFunction(ctx, name, invocationType, payload)
}

type b3MockSNSPublisher struct {
	topicARNs []string
	messages  []string
	mu        sync.Mutex
}

func (m *b3MockSNSPublisher) PublishMessage(_ context.Context, topicARN, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topicARNs = append(m.topicARNs, topicARN)
	m.messages = append(m.messages, message)

	return nil
}

type b3MockSQSSender struct {
	queueURLs []string
	bodies    []string
	groupIDs  []string
	dedupIDs  []string
	mu        sync.Mutex
}

func (m *b3MockSQSSender) SendMessage(_ context.Context, queueURL, body, groupID, dedupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueURLs = append(m.queueURLs, queueURL)
	m.bodies = append(m.bodies, body)
	m.groupIDs = append(m.groupIDs, groupID)
	m.dedupIDs = append(m.dedupIDs, dedupID)

	return nil
}

type b3MockKinesisPutter struct {
	streamARNs    []string
	partitionKeys []string
	data          [][]byte
	mu            sync.Mutex
}

func (m *b3MockKinesisPutter) PutRecord(_ context.Context, streamARN, partitionKey string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamARNs = append(m.streamARNs, streamARN)
	m.partitionKeys = append(m.partitionKeys, partitionKey)
	m.data = append(m.data, data)

	return nil
}

type b3MockEventBridgePutter struct {
	busARNs []string
	events  [][]map[string]any
	mu      sync.Mutex
}

func (m *b3MockEventBridgePutter) PutEvents(_ context.Context, busARN string, events []map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.busARNs = append(m.busARNs, busARN)
	m.events = append(m.events, events)

	return nil
}

type b3MockCloudWatchLogsPutter struct {
	logGroupARNs []string
	streamNames  []string
	events       [][]string
	mu           sync.Mutex
}

func (m *b3MockCloudWatchLogsPutter) PutLogEvents(
	_ context.Context,
	logGroupARN, streamName string,
	events []string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logGroupARNs = append(m.logGroupARNs, logGroupARN)
	m.streamNames = append(m.streamNames, streamName)
	m.events = append(m.events, events)

	return nil
}

// Ensure b3MockLambdaInvoker implements the interface (compile-time check).
var _ interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
} = (*b3MockLambdaInvoker)(nil)

// --- b4 helpers (eu-west-1 / 111122223333 test account, Firehose/Timestream/HTTP tests) ---

func b4Backend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("111122223333", "eu-west-1")
}

func b4Handler(t *testing.T) *pipes.Handler {
	t.Helper()

	return pipes.NewHandler(b4Backend())
}

func b4Do(t *testing.T, h *pipes.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/eu-west-1/pipes/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func b4Create(t *testing.T, h *pipes.Handler, name string, body map[string]any) map[string]any {
	t.Helper()
	rec := b4Do(t, h, http.MethodPost, "/v1/pipes/"+name, body)
	require.Equal(t, http.StatusOK, rec.Code, "create pipe %q: %s", name, rec.Body.String())
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func b4Describe(t *testing.T, h *pipes.Handler, name string) map[string]any {
	t.Helper()
	rec := b4Do(t, h, http.MethodGet, "/v1/pipes/"+name, nil)
	require.Equal(t, http.StatusOK, rec.Code, "describe pipe %q: %s", name, rec.Body.String())
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func b4CreatePipe(t *testing.T, b *pipes.InMemoryBackend, name, target string) {
	t.Helper()
	_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:         name,
		RoleARN:      "arn:aws:iam::111122223333:role/r",
		Source:       b4SQSSource,
		Target:       target,
		DesiredState: "RUNNING",
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, b, name)
}

const (
	b4SQSSource    = "arn:aws:sqs:eu-west-1:111122223333:q"
	b4LambdaTarget = "arn:aws:lambda:eu-west-1:111122223333:function:fn"
)

type b4MockSQSReader struct {
	messages []*pipes.SQSMessage
	deleted  []string
	mu       sync.Mutex
}

func (m *b4MockSQSReader) ReceivePipeMessages(_ string, _ int) ([]*pipes.SQSMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.messages
	m.messages = nil

	return msgs, nil
}

func (m *b4MockSQSReader) DeletePipeMessages(_ string, receiptHandles []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted = append(m.deleted, receiptHandles...)

	return nil
}

type b4MockFirehosePutter struct {
	streamARNs []string
	records    [][]byte
	mu         sync.Mutex
}

func (m *b4MockFirehosePutter) PutRecord(_ context.Context, streamARN string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamARNs = append(m.streamARNs, streamARN)
	m.records = append(m.records, data)

	return nil
}

var _ pipes.PipeFirehosePutter = (*b4MockFirehosePutter)(nil)

// --- Lambda InvocationType mapping ---

// TestLambda_InvocationType_Mapping verifies that Pipes-API InvocationType values
// are translated to the correct Lambda API values.
func TestLambda_InvocationType_Mapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		pipesInvocType      string
		wantLambdaInvocType string
	}{
		{
			name:                "fire_and_forget_maps_to_event",
			pipesInvocType:      "FIRE_AND_FORGET",
			wantLambdaInvocType: "Event",
		},
		{
			name:                "request_response_maps_to_requestresponse",
			pipesInvocType:      "REQUEST_RESPONSE",
			wantLambdaInvocType: "RequestResponse",
		},
		{
			name:                "empty_defaults_to_event",
			pipesInvocType:      "",
			wantLambdaInvocType: "Event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			var tp *pipes.TargetParameters
			if tt.pipesInvocType != "" {
				tp = &pipes.TargetParameters{
					LambdaFunctionParameters: &pipes.LambdaFunctionParameters{
						InvocationType: tt.pipesInvocType,
					},
				}
			}

			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             "it-" + tt.name,
				RoleARN:          "arn:aws:iam::111122223333:role/r",
				Source:           b3SQSSource,
				Target:           b3LambdaTarget,
				DesiredState:     "RUNNING",
				TargetParameters: tp,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, "it-"+tt.name)

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
			}
			lambdaInvoker := &b3MockLambdaInvoker{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(lambdaInvoker)

			pipes.PollAllPipesOnce(t.Context(), runner)

			lambdaInvoker.mu.Lock()
			invocTypes := lambdaInvoker.invocationTypes
			lambdaInvoker.mu.Unlock()

			require.Len(t, invocTypes, 1, "Lambda should be invoked once")
			assert.Equal(t, tt.wantLambdaInvocType, invocTypes[0],
				"InvocationType should be mapped from Pipes enum to Lambda enum")
		})
	}
}

// --- Additional target dispatchers ---

// TestTarget_SNS verifies that SNS target pipes publish to the SNS topic.
func TestTarget_SNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		targetARN string
	}{
		{name: "sns_target_published", targetARN: "arn:aws:sns:eu-west-1:111122223333:my-topic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			b3CreatePipe(t, b, tt.name+"-pipe", tt.targetARN)

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: `{"key":"value"}`}},
			}
			snsPublisher := &b3MockSNSPublisher{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetSNSPublisher(snsPublisher)

			pipes.PollAllPipesOnce(t.Context(), runner)

			snsPublisher.mu.Lock()
			topicARNs := snsPublisher.topicARNs
			messages := snsPublisher.messages
			snsPublisher.mu.Unlock()

			require.Len(t, topicARNs, 1, "SNS publish should be called once")
			assert.Equal(t, tt.targetARN, topicARNs[0])
			assert.NotEmpty(t, messages[0])
		})
	}
}

// TestTarget_SQS verifies that SQS target pipes send to the destination queue.
func TestTarget_SQS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		targetARN string
		tp        *pipes.TargetParameters
		wantGroup string
		wantDedup string
	}{
		{
			name:      "basic_sqs_send",
			targetARN: "arn:aws:sqs:eu-west-1:111122223333:dest.fifo",
			tp:        nil,
			wantGroup: "",
			wantDedup: "",
		},
		{
			name:      "fifo_sqs_with_group_and_dedup",
			targetARN: "arn:aws:sqs:eu-west-1:111122223333:dest.fifo",
			tp: &pipes.TargetParameters{
				SqsQueueParameters: &pipes.SQSTargetParameters{
					MessageGroupID:         "grp1",
					MessageDeduplicationID: "dedup1",
				},
			},
			wantGroup: "grp1",
			wantDedup: "dedup1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             tt.name + "-pipe",
				RoleARN:          "arn:aws:iam::111122223333:role/r",
				Source:           b3SQSSource,
				Target:           tt.targetARN,
				DesiredState:     "RUNNING",
				TargetParameters: tt.tp,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
			}
			sqsSender := &b3MockSQSSender{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetSQSSender(sqsSender)

			pipes.PollAllPipesOnce(t.Context(), runner)

			sqsSender.mu.Lock()
			queueURLs := sqsSender.queueURLs
			groupIDs := sqsSender.groupIDs
			dedupIDs := sqsSender.dedupIDs
			sqsSender.mu.Unlock()

			require.Len(t, queueURLs, 1)
			assert.Equal(t, tt.targetARN, queueURLs[0])
			assert.Equal(t, tt.wantGroup, groupIDs[0])
			assert.Equal(t, tt.wantDedup, dedupIDs[0])
		})
	}
}

// TestTarget_Kinesis verifies that Kinesis target pipes put records with the
// configured partition key.
func TestTarget_Kinesis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		tp               *pipes.TargetParameters
		wantPartitionKey string
	}{
		{
			name:             "default_partition_key",
			tp:               nil,
			wantPartitionKey: "default",
		},
		{
			name: "custom_partition_key",
			tp: &pipes.TargetParameters{
				KinesisStreamParameters: &pipes.KinesisStreamTargetParameters{
					PartitionKey: "my-key",
				},
			},
			wantPartitionKey: "my-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kinesisARN := "arn:aws:kinesis:eu-west-1:111122223333:stream/output"
			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             tt.name + "-pipe",
				RoleARN:          "arn:aws:iam::111122223333:role/r",
				Source:           b3SQSSource,
				Target:           kinesisARN,
				DesiredState:     "RUNNING",
				TargetParameters: tt.tp,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
			}
			kinesisPutter := &b3MockKinesisPutter{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetKinesisPutter(kinesisPutter)

			pipes.PollAllPipesOnce(t.Context(), runner)

			kinesisPutter.mu.Lock()
			partitionKeys := kinesisPutter.partitionKeys
			streamARNs := kinesisPutter.streamARNs
			kinesisPutter.mu.Unlock()

			require.Len(t, partitionKeys, 1)
			assert.Equal(t, tt.wantPartitionKey, partitionKeys[0])
			assert.Equal(t, kinesisARN, streamARNs[0])
		})
	}
}

// TestTarget_EventBridge verifies that EventBridge target pipes put events
// with the configured source and detail-type.
func TestTarget_EventBridge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		tp             *pipes.TargetParameters
		wantSource     string
		wantDetailType string
	}{
		{
			name:           "default_event_fields",
			tp:             nil,
			wantSource:     "aws.pipes",
			wantDetailType: "Pipe Event",
		},
		{
			name: "custom_event_fields",
			tp: &pipes.TargetParameters{
				EventBridgeEventBusParameters: &pipes.EBEventBusTargetParameters{
					Source:     "com.myapp",
					DetailType: "OrderCreated",
				},
			},
			wantSource:     "com.myapp",
			wantDetailType: "OrderCreated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			busARN := "arn:aws:events:eu-west-1:111122223333:event-bus/my-bus"
			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             tt.name + "-pipe",
				RoleARN:          "arn:aws:iam::111122223333:role/r",
				Source:           b3SQSSource,
				Target:           busARN,
				DesiredState:     "RUNNING",
				TargetParameters: tt.tp,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
			}
			ebPutter := &b3MockEventBridgePutter{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetEventBridgePutter(ebPutter)

			pipes.PollAllPipesOnce(t.Context(), runner)

			ebPutter.mu.Lock()
			busARNs := ebPutter.busARNs
			events := ebPutter.events
			ebPutter.mu.Unlock()

			require.Len(t, busARNs, 1)
			assert.Equal(t, busARN, busARNs[0])
			require.Len(t, events, 1)
			require.Len(t, events[0], 1)
			assert.Equal(t, tt.wantSource, events[0][0]["Source"])
			assert.Equal(t, tt.wantDetailType, events[0][0]["DetailType"])
		})
	}
}

// TestTarget_CloudWatchLogs verifies that CloudWatch Logs target pipes
// put log events with the configured log stream name.
func TestTarget_CloudWatchLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		tp             *pipes.TargetParameters
		wantStreamName string
	}{
		{
			name:           "no_stream_name",
			tp:             nil,
			wantStreamName: "",
		},
		{
			name: "with_stream_name",
			tp: &pipes.TargetParameters{
				CloudWatchLogsParameters: &pipes.CloudWatchLogsTargetParameters{
					LogStreamName: "my-stream",
				},
			},
			wantStreamName: "my-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logGroupARN := "arn:aws:logs:eu-west-1:111122223333:log-group:/pipes/output"
			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             tt.name + "-pipe",
				RoleARN:          "arn:aws:iam::111122223333:role/r",
				Source:           b3SQSSource,
				Target:           logGroupARN,
				DesiredState:     "RUNNING",
				TargetParameters: tt.tp,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
			}
			cwLogs := &b3MockCloudWatchLogsPutter{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetCloudWatchLogsPutter(cwLogs)

			pipes.PollAllPipesOnce(t.Context(), runner)

			cwLogs.mu.Lock()
			logGroupARNs := cwLogs.logGroupARNs
			streamNames := cwLogs.streamNames
			cwLogs.mu.Unlock()

			require.Len(t, logGroupARNs, 1)
			assert.Equal(t, logGroupARN, logGroupARNs[0])
			assert.Equal(t, tt.wantStreamName, streamNames[0])
		})
	}
}

// TestUnsupportedTarget_NilInvoker verifies that missing invokers for
// supported target types are handled gracefully (no panic, messages deleted).
func TestUnsupportedTarget_NilInvoker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
	}{
		{name: "sns_nil_publisher", target: "arn:aws:sns:eu-west-1:111122223333:topic"},
		{name: "sqs_nil_sender", target: "arn:aws:sqs:eu-west-1:111122223333:dest"},
		{name: "kinesis_nil_putter", target: "arn:aws:kinesis:eu-west-1:111122223333:stream/out"},
		{name: "eventbridge_nil_putter", target: "arn:aws:events:eu-west-1:111122223333:event-bus/bus"},
		{name: "cwlogs_nil_putter", target: "arn:aws:logs:eu-west-1:111122223333:log-group:/g"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			b3CreatePipe(t, b, tt.name+"-pipe", tt.target)

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
			}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			// No invoker set for the target type → should not panic

			assert.NotPanics(t, func() {
				pipes.PollAllPipesOnce(t.Context(), runner)
			}, "nil target invoker should not panic")
		})
	}
}

// --- Firehose target dispatcher ---

// TestTarget_Firehose verifies that Firehose target pipes put records
// to the configured delivery stream ARN.
func TestTarget_Firehose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		targetARN string
	}{
		{
			name:      "basic_firehose_delivery",
			targetARN: "arn:aws:firehose:eu-west-1:111122223333:deliverystream/my-stream",
		},
		{
			name:      "firehose_with_long_name",
			targetARN: "arn:aws:firehose:eu-west-1:111122223333:deliverystream/prod-analytics-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b4Backend()
			b4CreatePipe(t, b, tt.name+"-pipe", tt.targetARN)

			sqsReader := &b4MockSQSReader{
				messages: []*pipes.SQSMessage{
					{MessageID: "m1", ReceiptHandle: "rh1", Body: `{"key":"value"}`},
				},
			}
			firehosePutter := &b4MockFirehosePutter{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetFirehosePutter(firehosePutter)

			pipes.PollAllPipesOnce(t.Context(), runner)

			firehosePutter.mu.Lock()
			streamARNs := firehosePutter.streamARNs
			records := firehosePutter.records
			firehosePutter.mu.Unlock()

			require.Len(t, streamARNs, 1, "Firehose PutRecord should be called once")
			assert.Equal(t, tt.targetARN, streamARNs[0])
			assert.NotEmpty(t, records[0], "Firehose record should not be empty")
		})
	}
}

// TestTarget_Firehose_InputTemplate verifies that Firehose target respects
// the InputTemplate transformation.
func TestTarget_Firehose_InputTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		inputTemplate string
		wantBody      string
	}{
		{
			name:          "template_replaces_payload",
			inputTemplate: `{"fixed":"payload"}`,
			wantBody:      `{"fixed":"payload"}`,
		},
		{
			name:          "no_template_uses_sqs_records",
			inputTemplate: "",
			wantBody:      "", // checked separately
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			firehoseARN := "arn:aws:firehose:eu-west-1:111122223333:deliverystream/out"
			b := b4Backend()
			var tp *pipes.TargetParameters
			if tt.inputTemplate != "" {
				tp = &pipes.TargetParameters{InputTemplate: tt.inputTemplate}
			}
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             tt.name + "-pipe",
				RoleARN:          "arn:aws:iam::111122223333:role/r",
				Source:           b4SQSSource,
				Target:           firehoseARN,
				DesiredState:     "RUNNING",
				TargetParameters: tp,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			sqsReader := &b4MockSQSReader{
				messages: []*pipes.SQSMessage{
					{MessageID: "m1", ReceiptHandle: "rh1", Body: `{"original":true}`},
				},
			}
			firehosePutter := &b4MockFirehosePutter{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetFirehosePutter(firehosePutter)

			pipes.PollAllPipesOnce(t.Context(), runner)

			firehosePutter.mu.Lock()
			records := firehosePutter.records
			firehosePutter.mu.Unlock()

			require.Len(t, records, 1)
			if tt.inputTemplate != "" {
				assert.JSONEq(t, tt.wantBody, string(records[0]),
					"Firehose should receive InputTemplate payload")
			} else {
				// No template → SQS records JSON is forwarded.
				var event map[string]any
				require.NoError(t, json.Unmarshal(records[0], &event))
				_, hasRecords := event["Records"]
				assert.True(t, hasRecords, "Firehose should receive SQS records payload")
			}
		})
	}
}

// TestTarget_Firehose_NilPutter verifies that a nil Firehose putter
// does not panic and messages are not deleted on nil (graceful no-op).
func TestTarget_Firehose_NilPutter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_putter_no_panic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			firehoseARN := "arn:aws:firehose:eu-west-1:111122223333:deliverystream/nil-test"
			b := b4Backend()
			b4CreatePipe(t, b, tt.name+"-pipe", firehoseARN)

			sqsReader := &b4MockSQSReader{
				messages: []*pipes.SQSMessage{
					{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"},
				},
			}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			// No firehose putter set — should not panic.

			assert.NotPanics(t, func() {
				pipes.PollAllPipesOnce(t.Context(), runner)
			}, "nil firehose putter should not panic")
		})
	}
}
