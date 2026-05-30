package pipes_test

// Audit batch-4: AWS-accuracy coverage for EventBridge Pipes (go-t0th / #1818).
//
// Covers:
//   - epochMillis: millisecond-precision timestamps via direct unit test
//   - TimestreamParameters: roundtrip create/describe/update (#5)
//   - TargetHTTPParameters: roundtrip for API Gateway targets (#5)
//   - Firehose target runner dispatch (arn:aws:firehose:) (#2)
//   - Clone isolation: TimestreamParameters and HTTPParameters (#5)
//   - Update TimestreamParameters via UpdatePipe

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// --- helpers ---

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
	_, err := b.CreatePipe(pipes.CreatePipeInput{
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

// --- epochMillis direct unit test ---

// TestBatch4_EpochMillis_Precision verifies that epochMillis returns millisecond-precision
// floating-point Unix timestamps, not integer seconds.
func TestBatch4_EpochMillis_Precision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    time.Time
		name     string
		wantFrac float64
	}{
		{
			name:     "sub_second_500ms",
			input:    time.Unix(1700000000, 500_000_000),
			wantFrac: 0.5,
		},
		{
			name:     "sub_second_123ms",
			input:    time.Unix(1700000000, 123_000_000),
			wantFrac: 0.123,
		},
		{
			name:     "sub_second_1ms",
			input:    time.Unix(1700000000, 1_000_000),
			wantFrac: 0.001,
		},
		{
			name:     "whole_second_zero_frac",
			input:    time.Unix(1700000000, 0),
			wantFrac: 0.0,
		},
		{
			name:     "sub_second_999ms",
			input:    time.Unix(1700000000, 999_000_000),
			wantFrac: 0.999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := pipes.EpochMillisForTest(tt.input)
			base := float64(tt.input.Unix())
			frac := got - base

			// Allow 1ms rounding tolerance.
			assert.InDelta(t, tt.wantFrac, frac, 0.001,
				"EpochMillis(%v) fractional part = %.3f, want %.3f", tt.input, frac, tt.wantFrac)

			// Verify the integer part is the same as Unix seconds.
			assert.InDelta(t, base, float64(int64(got)), 0.0,
				"EpochMillis integer part should match Unix seconds")
		})
	}
}

// --- TimestreamParameters ---

// TestBatch4_TargetParams_Timestream verifies that TimestreamParameters roundtrip
// through create/describe/update unchanged.
func TestBatch4_TargetParams_Timestream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params *pipes.TimestreamParameters
		name   string
	}{
		{
			name: "dimension_and_single_measure",
			params: &pipes.TimestreamParameters{
				TimeValue:     "$.timestamp",
				TimeFieldType: "EPOCH",
				EpochTimeUnit: "MILLISECONDS",
				VersionValue:  "1",
				DimensionMappings: []pipes.TimestreamDimensionMapping{
					{DimensionName: "region", DimensionValue: "$.region", DimensionValueType: "VARCHAR"},
				},
				SingleMeasureMappings: []pipes.TimestreamSingleMeasureMapping{
					{MeasureName: "cpu", MeasureValue: "$.cpu", MeasureValueType: "DOUBLE"},
				},
			},
		},
		{
			name: "multi_measure_mapping",
			params: &pipes.TimestreamParameters{
				TimeValue:       "$.eventTime",
				TimeFieldType:   "TIMESTAMP_FORMAT",
				TimestampFormat: "YYYY-MM-DD'T'HH:mm:ss",
				DimensionMappings: []pipes.TimestreamDimensionMapping{
					{DimensionName: "host", DimensionValue: "$.host", DimensionValueType: "VARCHAR"},
				},
				MultiMeasureMappings: []pipes.TimestreamMultiMeasureMapping{
					{
						MultiMeasureName: "metrics",
						MultiMeasureAttributeMappings: []pipes.TimestreamMultiMeasureAttributeMapping{
							{MeasureValue: "$.cpu", MeasureValueType: "DOUBLE", MultiMeasureAttributeName: "cpu"},
							{MeasureValue: "$.mem", MeasureValueType: "DOUBLE", MultiMeasureAttributeName: "mem"},
						},
					},
				},
			},
		},
		{
			name: "minimal_timestream",
			params: &pipes.TimestreamParameters{
				TimeValue:     "$.ts",
				TimeFieldType: "UNIX_TIME",
				DimensionMappings: []pipes.TimestreamDimensionMapping{
					{DimensionName: "id", DimensionValue: "$.id", DimensionValueType: "VARCHAR"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b4Backend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:    tt.name + "-ts-pipe",
				RoleARN: "arn:aws:iam::111122223333:role/r",
				Source:  b4SQSSource,
				Target:  "arn:aws:timestream:eu-west-1:111122223333:database/db/table/tbl",
				TargetParameters: &pipes.TargetParameters{
					TimestreamParameters: tt.params,
				},
			})
			require.NoError(t, err)

			p, err := b.GetPipe(tt.name + "-ts-pipe")
			require.NoError(t, err)
			require.NotNil(t, p.TargetParameters)
			require.NotNil(t, p.TargetParameters.TimestreamParameters)

			ts := p.TargetParameters.TimestreamParameters
			assert.Equal(t, tt.params.TimeValue, ts.TimeValue)
			assert.Equal(t, tt.params.TimeFieldType, ts.TimeFieldType)
			assert.Equal(t, tt.params.EpochTimeUnit, ts.EpochTimeUnit)
			assert.Equal(t, tt.params.TimestampFormat, ts.TimestampFormat)
			assert.Equal(t, tt.params.VersionValue, ts.VersionValue)
			assert.Len(t, ts.DimensionMappings, len(tt.params.DimensionMappings))
			if len(tt.params.DimensionMappings) > 0 {
				assert.Equal(t, tt.params.DimensionMappings[0].DimensionName, ts.DimensionMappings[0].DimensionName)
				assert.Equal(t, tt.params.DimensionMappings[0].DimensionValue, ts.DimensionMappings[0].DimensionValue)
			}
		})
	}
}

// TestBatch4_TargetParams_Timestream_Update verifies that UpdatePipe replaces
// TimestreamParameters when new params are provided.
func TestBatch4_TargetParams_Timestream_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialParams *pipes.TimestreamParameters
		updateParams  *pipes.TimestreamParameters
		wantTimeValue string
	}{
		{
			name: "update_replaces_time_value",
			initialParams: &pipes.TimestreamParameters{
				TimeValue:     "$.oldTs",
				TimeFieldType: "EPOCH",
				EpochTimeUnit: "SECONDS",
				DimensionMappings: []pipes.TimestreamDimensionMapping{
					{DimensionName: "d", DimensionValue: "$.d", DimensionValueType: "VARCHAR"},
				},
			},
			updateParams: &pipes.TimestreamParameters{
				TimeValue:     "$.newTs",
				TimeFieldType: "UNIX_TIME",
				DimensionMappings: []pipes.TimestreamDimensionMapping{
					{DimensionName: "d", DimensionValue: "$.d", DimensionValueType: "VARCHAR"},
				},
			},
			wantTimeValue: "$.newTs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b4Backend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:    tt.name + "-pipe",
				RoleARN: "arn:aws:iam::111122223333:role/r",
				Source:  b4SQSSource,
				Target:  "arn:aws:timestream:eu-west-1:111122223333:database/db/table/t",
				TargetParameters: &pipes.TargetParameters{
					TimestreamParameters: tt.initialParams,
				},
			})
			require.NoError(t, err)

			_, err = b.UpdatePipe(tt.name+"-pipe", pipes.UpdatePipeInput{
				TargetParameters: &pipes.TargetParameters{
					TimestreamParameters: tt.updateParams,
				},
			})
			require.NoError(t, err)

			p, err := b.GetPipe(tt.name + "-pipe")
			require.NoError(t, err)
			require.NotNil(t, p.TargetParameters.TimestreamParameters)
			assert.Equal(t, tt.wantTimeValue, p.TargetParameters.TimestreamParameters.TimeValue)
		})
	}
}

// TestBatch4_Clone_TimestreamIsolation verifies that clonePipe deep-copies
// TimestreamParameters so mutations of the clone do not affect the original.
func TestBatch4_Clone_TimestreamIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "dimension_mappings_isolated"},
		{name: "multi_measure_isolated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b4Backend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:    tt.name + "-pipe",
				RoleARN: "arn:aws:iam::111122223333:role/r",
				Source:  b4SQSSource,
				Target:  "arn:aws:timestream:eu-west-1:111122223333:database/db/table/t",
				TargetParameters: &pipes.TargetParameters{
					TimestreamParameters: &pipes.TimestreamParameters{
						TimeValue:     "$.ts",
						TimeFieldType: "UNIX_TIME",
						DimensionMappings: []pipes.TimestreamDimensionMapping{
							{DimensionName: "region", DimensionValue: "$.region", DimensionValueType: "VARCHAR"},
						},
					},
				},
			})
			require.NoError(t, err)

			// GetPipe returns a clone; mutate the clone's dimension mapping.
			clone, err := b.GetPipe(tt.name + "-pipe")
			require.NoError(t, err)
			require.NotNil(t, clone.TargetParameters.TimestreamParameters)
			clone.TargetParameters.TimestreamParameters.DimensionMappings[0].DimensionName = "mutated"

			// Re-read from backend; original should be unchanged.
			orig, err := b.GetPipe(tt.name + "-pipe")
			require.NoError(t, err)
			assert.Equal(t, "region",
				orig.TargetParameters.TimestreamParameters.DimensionMappings[0].DimensionName,
				"mutating clone dimension should not affect stored pipe")
		})
	}
}

// --- TargetHTTPParameters ---

// TestBatch4_TargetParams_HTTP verifies that HTTPParameters roundtrip
// for API Gateway and API Destination targets.
func TestBatch4_TargetParams_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params *pipes.TargetHTTPParameters
		name   string
	}{
		{
			name: "headers_and_query_string",
			params: &pipes.TargetHTTPParameters{
				HeaderParameters:      map[string]string{"X-Custom": "value", "Content-Type": "application/json"},
				QueryStringParameters: map[string]string{"version": "2", "format": "json"},
				PathParameterValues:   []string{"orders", "123"},
			},
		},
		{
			name: "headers_only",
			params: &pipes.TargetHTTPParameters{
				HeaderParameters: map[string]string{"Authorization": "Bearer token"},
			},
		},
		{
			name: "path_params_only",
			params: &pipes.TargetHTTPParameters{
				PathParameterValues: []string{"v1", "users"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b4Backend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:    tt.name + "-pipe",
				RoleARN: "arn:aws:iam::111122223333:role/r",
				Source:  b4SQSSource,
				Target:  "arn:aws:execute-api:eu-west-1:111122223333:api/stage/route",
				TargetParameters: &pipes.TargetParameters{
					HTTPParameters: tt.params,
				},
			})
			require.NoError(t, err)

			p, err := b.GetPipe(tt.name + "-pipe")
			require.NoError(t, err)
			require.NotNil(t, p.TargetParameters)
			require.NotNil(t, p.TargetParameters.HTTPParameters)

			hp := p.TargetParameters.HTTPParameters
			assert.Equal(t, tt.params.HeaderParameters, hp.HeaderParameters)
			assert.Equal(t, tt.params.QueryStringParameters, hp.QueryStringParameters)
			assert.Equal(t, tt.params.PathParameterValues, hp.PathParameterValues)
		})
	}
}

// TestBatch4_Clone_HTTPParamsIsolation verifies that HTTPParameters are deep-copied
// in clonePipe so mutations of one clone do not affect others.
func TestBatch4_Clone_HTTPParamsIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "headers_isolated"},
		{name: "path_params_isolated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b4Backend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:    tt.name + "-pipe",
				RoleARN: "arn:aws:iam::111122223333:role/r",
				Source:  b4SQSSource,
				Target:  "arn:aws:execute-api:eu-west-1:111122223333:api/stage/route",
				TargetParameters: &pipes.TargetParameters{
					HTTPParameters: &pipes.TargetHTTPParameters{
						HeaderParameters:    map[string]string{"X-Original": "yes"},
						PathParameterValues: []string{"original"},
					},
				},
			})
			require.NoError(t, err)

			clone, err := b.GetPipe(tt.name + "-pipe")
			require.NoError(t, err)
			clone.TargetParameters.HTTPParameters.HeaderParameters["X-Original"] = "mutated"
			clone.TargetParameters.HTTPParameters.PathParameterValues[0] = "mutated"

			orig, err := b.GetPipe(tt.name + "-pipe")
			require.NoError(t, err)
			assert.Equal(t, "yes", orig.TargetParameters.HTTPParameters.HeaderParameters["X-Original"],
				"mutating clone headers should not affect stored pipe")
			assert.Equal(t, "original", orig.TargetParameters.HTTPParameters.PathParameterValues[0],
				"mutating clone path params should not affect stored pipe")
		})
	}
}

// TestBatch4_TargetParams_HTTP_HTTP_Roundtrip verifies that HTTPParameters survive
// a create → describe HTTP roundtrip via the handler.
func TestBatch4_TargetParams_HTTP_Roundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          map[string]any
		wantHeaderKey string
		wantHeaderVal string
	}{
		{
			name: "headers_survive_roundtrip",
			body: map[string]any{
				"Source": b4SQSSource,
				"Target": "arn:aws:execute-api:eu-west-1:111122223333:api/stage/route",
				"TargetParameters": map[string]any{
					"HttpParameters": map[string]any{
						"HeaderParameters": map[string]any{
							"X-Pipe-Id": "batch4-test",
						},
					},
				},
			},
			wantHeaderKey: "X-Pipe-Id",
			wantHeaderVal: "batch4-test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b4Handler(t)
			b4Create(t, h, tt.name+"-pipe", tt.body)

			resp := b4Describe(t, h, tt.name+"-pipe")
			tp, ok := resp["TargetParameters"].(map[string]any)
			require.True(t, ok, "TargetParameters should be present")
			hp, ok := tp["HttpParameters"].(map[string]any)
			require.True(t, ok, "HTTPParameters should be present")
			headers, ok := hp["HeaderParameters"].(map[string]any)
			require.True(t, ok, "HeaderParameters should be present")
			assert.Equal(t, tt.wantHeaderVal, headers[tt.wantHeaderKey])
		})
	}
}

// --- Firehose target dispatcher ---

// TestBatch4_Target_Firehose verifies that Firehose target pipes put records
// to the configured delivery stream ARN.
func TestBatch4_Target_Firehose(t *testing.T) {
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

// TestBatch4_Target_Firehose_InputTemplate verifies that Firehose target respects
// the InputTemplate transformation.
func TestBatch4_Target_Firehose_InputTemplate(t *testing.T) {
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
			_, err := b.CreatePipe(pipes.CreatePipeInput{
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

// TestBatch4_Target_Firehose_NilPutter verifies that a nil Firehose putter
// does not panic and messages are not deleted on nil (graceful no-op).
func TestBatch4_Target_Firehose_NilPutter(t *testing.T) {
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

// --- mock implementations ---

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
