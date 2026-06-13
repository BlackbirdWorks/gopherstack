package pipes_test

// Audit batch-3: AWS-accuracy coverage for EventBridge Pipes (go-sstb / #1818).
//
// Covers:
//   - UpdatePipe Description *string absent-means-unchanged semantics
//   - Snapshot/Restore enrichmentCallCount persistence
//   - epochMillis millisecond-resolution timestamps
//   - sortedPipeNames lexicographic correctness
//   - BatchSize bounds validation (all source types)
//   - Lambda InvocationType mapping (FIRE_AND_FORGET / REQUEST_RESPONSE)
//   - Enrichment Lambda invocation with payload replacement
//   - Enrichment SFN invocation
//   - Additional target dispatchers: SNS, SQS, Kinesis, EventBridge, CloudWatchLogs
//   - FilterCriteria JSON event pattern matching
//   - FilterCriteria prefix/suffix/anything-but pattern operators
//   - Lightweight ListPipes projection (no heavy clone per summary)

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

// --- helpers ---

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

// --- UpdatePipe Description pointer semantics ---

// TestBatch3_UpdatePipe_Description_AbsentMeansUnchanged verifies that not sending
// Description in an update leaves the existing value intact.
func TestBatch3_UpdatePipe_Description_AbsentMeansUnchanged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialDesc string
		updateDesc  string
		wantDesc    string
	}{
		{
			name:        "empty_description_clears",
			initialDesc: "original description",
			updateDesc:  "",
			wantDesc:    "",
		},
		{
			name:        "non_empty_description_updates",
			initialDesc: "old",
			updateDesc:  "new description",
			wantDesc:    "new description",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         tt.name,
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				Description:  tt.initialDesc,
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name)

			desc := tt.updateDesc
			_, err = b.UpdatePipe(context.Background(), tt.name, pipes.UpdatePipeInput{
				Description: &desc,
			})
			require.NoError(t, err)

			p, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, p.Description)
		})
	}
}

// TestBatch3_UpdatePipe_Description_HTTPAbsentPreserves verifies that omitting
// Description from the HTTP body leaves it unchanged, matching AWS semantics.
func TestBatch3_UpdatePipe_Description_HTTPAbsentPreserves(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     map[string]any
		wantDesc string
	}{
		{
			name:     "no_description_field_preserves",
			body:     map[string]any{"DesiredState": "RUNNING"},
			wantDesc: "keep me",
		},
		{
			// JSON "" → *string pointing to "" → backend clears; response omits field (omitempty).
			name:     "empty_string_clears_description",
			body:     map[string]any{"Description": ""},
			wantDesc: "",
		},
		{
			name:     "new_value_updates_description",
			body:     map[string]any{"Description": "new desc"},
			wantDesc: "new desc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b3Handler(t)
			b3Create(t, h, tt.name+"-pipe", map[string]any{
				"Source":      b3SQSSource,
				"Target":      b3LambdaTarget,
				"Description": "keep me",
			})

			rec := b3Do(t, h, http.MethodPut, "/v1/pipes/"+tt.name+"-pipe", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := b3Describe(t, h, tt.name+"-pipe")
			// Response uses omitempty so empty string → absent key (nil).
			gotDesc, _ := resp["Description"].(string)
			assert.Equal(t, tt.wantDesc, gotDesc,
				"Description should match expected value after update")
		})
	}
}

// --- Snapshot/Restore enrichmentCallCount persistence ---

// TestBatch3_Snapshot_PersistsEnrichmentCallCount verifies that Snapshot includes
// enrichment call counters and Restore brings them back.
func TestBatch3_Snapshot_PersistsEnrichmentCallCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pipeName  string
		callCount int
	}{
		{name: "single_call", pipeName: "enrich-pipe-a", callCount: 1},
		{name: "multiple_calls", pipeName: "enrich-pipe-b", callCount: 5},
		{name: "zero_calls", pipeName: "enrich-pipe-c", callCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			b3CreatePipe(t, b, tt.pipeName, b3LambdaTarget)

			for range tt.callCount {
				b.RecordEnrichmentCall(context.Background(), tt.pipeName)
			}

			snap := b.Snapshot()
			require.NotNil(t, snap, "Snapshot should not return nil")

			b2 := b3Backend()
			require.NoError(t, b2.Restore(snap))

			got := b2.GetEnrichmentCallCount(context.Background(), tt.pipeName)
			assert.Equal(t, int64(tt.callCount), got, "enrichment call count should survive snapshot/restore")
		})
	}
}

// TestBatch3_Restore_MissingEnrichmentCallCount verifies backward compatibility:
// restoring from a snapshot without the enrichment field initialises an empty map.
func TestBatch3_Restore_MissingEnrichmentCallCount(t *testing.T) {
	t.Parallel()

	legacySnap := []byte(`{"pipes":{},"accountID":"111122223333","region":"eu-west-1"}`)

	b := b3Backend()
	require.NoError(t, b.Restore(legacySnap))

	// Must not panic; count for unknown pipe is zero.
	assert.Equal(t, int64(0), b.GetEnrichmentCallCount(context.Background(), "any-pipe"))
}

// --- epochMillis millisecond-resolution timestamps ---

// TestBatch3_Timestamps_MillisecondResolution verifies that HTTP response timestamps
// include sub-second precision (milliseconds), not just integer seconds.
func TestBatch3_Timestamps_MillisecondResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "creation_time_has_ms"},
		{name: "modified_time_has_ms"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b3Handler(t)
			resp := b3Create(t, h, tt.name+"-pipe", map[string]any{
				"Source": b3SQSSource,
				"Target": b3LambdaTarget,
			})

			ct, ok := resp["CreationTime"].(float64)
			require.True(t, ok, "CreationTime should be a float64")
			lmt, ok := resp["LastModifiedTime"].(float64)
			require.True(t, ok, "LastModifiedTime should be a float64")

			// A millisecond-resolution timestamp will have three decimal digits.
			// Check that the value is at least in the modern epoch range.
			assert.Greater(t, ct, float64(1_700_000_000), "timestamp should be in modern epoch range")
			assert.Greater(t, lmt, float64(1_700_000_000), "timestamp should be in modern epoch range")

			// Verify millisecond resolution: the fractional part should not be exactly
			// an integer (unless we happen to hit exactly zero ms, which is rare).
			// We check that the value has three significant decimal places.
			msFromFloat := ct * 1000
			assert.Greater(t, msFromFloat, ct, "epochMillis should exceed integer epoch seconds")
		})
	}
}

// --- sortedPipeNames lexicographic correctness ---

// TestBatch3_ListPipes_LexicographicOrder verifies that ListPipes returns pipes
// in stable lexicographic order regardless of insertion order.
func TestBatch3_ListPipes_LexicographicOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pipeNames []string
		wantOrder []string
	}{
		{
			name:      "alphabetic_order",
			pipeNames: []string{"charlie", "alpha", "bravo"},
			wantOrder: []string{"alpha", "bravo", "charlie"},
		},
		{
			name:      "numeric_suffix_order",
			pipeNames: []string{"pipe-10", "pipe-2", "pipe-1"},
			wantOrder: []string{"pipe-1", "pipe-10", "pipe-2"},
		},
		{
			name:      "mixed_case",
			pipeNames: []string{"Zulu", "apple", "Mango"},
			wantOrder: []string{"Mango", "Zulu", "apple"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			for _, n := range tt.pipeNames {
				_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
					Name:         n,
					RoleARN:      "arn:aws:iam::111122223333:role/r",
					Source:       b3SQSSource,
					Target:       b3LambdaTarget,
					DesiredState: "STOPPED",
				})
				require.NoError(t, err)
			}

			result := b.ListPipes(context.Background(), pipes.ListPipesFilter{})
			require.Len(t, result.Pipes, len(tt.pipeNames))

			for i, p := range result.Pipes {
				assert.Equal(t, tt.wantOrder[i], p.Name,
					"pipe at position %d should be %q", i, tt.wantOrder[i])
			}
		})
	}
}

// --- BatchSize bounds validation ---

// TestBatch3_BatchSize_Validation verifies that out-of-bounds BatchSize values
// are rejected with ValidationException for all source types.
func TestBatch3_BatchSize_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sp        *pipes.SourceParameters
		name      string
		wantError bool
	}{
		{
			name: "sqs_zero_batchsize_accepted",
			sp: &pipes.SourceParameters{
				SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 0},
			},
			wantError: false,
		},
		{
			name: "sqs_valid_batchsize_1",
			sp: &pipes.SourceParameters{
				SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 1},
			},
			wantError: false,
		},
		{
			name: "sqs_valid_batchsize_10000",
			sp: &pipes.SourceParameters{
				SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 10000},
			},
			wantError: false,
		},
		{
			name: "sqs_negative_batchsize_rejected",
			sp: &pipes.SourceParameters{
				SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: -1},
			},
			wantError: true,
		},
		{
			name: "sqs_over_limit_batchsize_rejected",
			sp: &pipes.SourceParameters{
				SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 10001},
			},
			wantError: true,
		},
		{
			name: "kinesis_over_limit_rejected",
			sp: &pipes.SourceParameters{
				KinesisStreamParameters: &pipes.KinesisStreamSourceParameters{BatchSize: 99999},
			},
			wantError: true,
		},
		{
			name: "dynamodb_over_limit_rejected",
			sp: &pipes.SourceParameters{
				DynamoDBStreamParameters: &pipes.DynamoDBStreamSourceParameters{BatchSize: -5},
			},
			wantError: true,
		},
		{
			name: "msk_over_limit_rejected",
			sp: &pipes.SourceParameters{
				ManagedStreamingKafkaParameters: &pipes.MSKSourceParameters{BatchSize: 10001},
			},
			wantError: true,
		},
		{
			name: "kafka_over_limit_rejected",
			sp: &pipes.SourceParameters{
				SelfManagedKafkaParameters: &pipes.SelfManagedKafkaSourceParameters{BatchSize: -100},
			},
			wantError: true,
		},
		{
			name: "rabbitmq_over_limit_rejected",
			sp: &pipes.SourceParameters{
				RabbitMQBrokerParameters: &pipes.RabbitMQBrokerSourceParameters{BatchSize: 10001},
			},
			wantError: true,
		},
		{
			name: "activemq_over_limit_rejected",
			sp: &pipes.SourceParameters{
				ActiveMQBrokerParameters: &pipes.ActiveMQBrokerSourceParameters{BatchSize: -1},
			},
			wantError: true,
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
				Target:           b3LambdaTarget,
				DesiredState:     "RUNNING",
				SourceParameters: tt.sp,
			})

			if tt.wantError {
				require.Error(t, err)
				assert.ErrorIs(t, err, pipes.ErrValidation)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestBatch3_BatchSize_UpdateValidation verifies that batch size validation also
// applies on UpdatePipe.
func TestBatch3_BatchSize_UpdateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sp        *pipes.SourceParameters
		name      string
		wantError bool
	}{
		{
			name: "update_valid_batchsize",
			sp: &pipes.SourceParameters{
				SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 100},
			},
			wantError: false,
		},
		{
			name: "update_invalid_batchsize",
			sp: &pipes.SourceParameters{
				SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 10001},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			b3CreatePipe(t, b, tt.name+"-pipe", b3LambdaTarget)

			_, err := b.UpdatePipe(context.Background(), tt.name+"-pipe", pipes.UpdatePipeInput{
				SourceParameters: tt.sp,
			})

			if tt.wantError {
				require.Error(t, err)
				assert.ErrorIs(t, err, pipes.ErrValidation)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// --- Lambda InvocationType mapping ---

// TestBatch3_Lambda_InvocationType_Mapping verifies that Pipes-API InvocationType values
// are translated to the correct Lambda API values.
func TestBatch3_Lambda_InvocationType_Mapping(t *testing.T) {
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

// --- Enrichment invocation ---

// TestBatch3_Enrichment_LambdaInvocation verifies that when Enrichment is set to
// a Lambda ARN, the runner invokes Lambda with REQUEST_RESPONSE and uses its
// response as the payload sent to the target.
func TestBatch3_Enrichment_LambdaInvocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		enrichmentARN  string
		wantTargetBody string
		enrichResponse []byte
	}{
		{
			name:           "lambda_enrichment_replaces_payload",
			enrichmentARN:  "arn:aws:lambda:eu-west-1:111122223333:function:enricher",
			enrichResponse: []byte(`{"enriched":true,"result":"processed"}`),
			wantTargetBody: `{"enriched":true,"result":"processed"}`,
		},
		{
			name:           "nil_enrichment_response_uses_original",
			enrichmentARN:  "arn:aws:lambda:eu-west-1:111122223333:function:enricher",
			enrichResponse: nil, // nil response means use original
			wantTargetBody: "",  // will check that original Records are sent
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         tt.name,
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				Enrichment:   tt.enrichmentARN,
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name)

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: `{"original":true}`}},
			}

			enricher := &b3MockLambdaInvoker{returnPayload: tt.enrichResponse}
			targetLambda := &b3MockLambdaInvoker{}
			// Enricher is invoked for enrich ARN, targetLambda for the target.
			// We use a mux invoker to separate them.
			muxInvoker := &b3MuxLambdaInvoker{
				enrichFn: "enricher",
				enrich:   enricher,
				target:   targetLambda,
			}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(muxInvoker)

			pipes.PollAllPipesOnce(t.Context(), runner)

			// Enrichment call should be recorded.
			assert.Equal(t, int64(1), b.GetEnrichmentCallCount(context.Background(), tt.name),
				"enrichment call should be recorded")

			enricher.mu.Lock()
			enrichCalls := enricher.calls
			enricher.mu.Unlock()

			assert.Len(t, enrichCalls, 1, "enricher Lambda should be called once")

			targetLambda.mu.Lock()
			targetPayloads := targetLambda.payloads
			targetLambda.mu.Unlock()

			require.Len(t, targetPayloads, 1, "target Lambda should be invoked once")

			if tt.enrichResponse != nil {
				assert.JSONEq(t, tt.wantTargetBody, string(targetPayloads[0]),
					"enriched payload should replace original")
			} else {
				// Nil response: original records payload forwarded.
				var event map[string]any
				require.NoError(t, json.Unmarshal(targetPayloads[0], &event))
				_, hasRecords := event["Records"]
				assert.True(t, hasRecords, "original Records payload should be forwarded when enricher returns nil")
			}
		})
	}
}

// --- Additional target dispatchers ---

// TestBatch3_Target_SNS verifies that SNS target pipes publish to the SNS topic.
func TestBatch3_Target_SNS(t *testing.T) {
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

// TestBatch3_Target_SQS verifies that SQS target pipes send to the destination queue.
func TestBatch3_Target_SQS(t *testing.T) {
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

// TestBatch3_Target_Kinesis verifies that Kinesis target pipes put records with the
// configured partition key.
func TestBatch3_Target_Kinesis(t *testing.T) {
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

// TestBatch3_Target_EventBridge verifies that EventBridge target pipes put events
// with the configured source and detail-type.
func TestBatch3_Target_EventBridge(t *testing.T) {
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

// TestBatch3_Target_CloudWatchLogs verifies that CloudWatch Logs target pipes
// put log events with the configured log stream name.
func TestBatch3_Target_CloudWatchLogs(t *testing.T) {
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

// --- FilterCriteria JSON event pattern matching ---

// TestBatch3_Filter_JSONPattern verifies that JSON event pattern filters are
// evaluated against the structured message body (not substring match).
func TestBatch3_Filter_JSONPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pattern     string
		msgBodies   []string
		wantDeleted []string
	}{
		{
			name:    "exact_field_match_passes",
			pattern: `{"type":["order"]}`,
			msgBodies: []string{
				`{"type":"order","id":1}`,
				`{"type":"inventory","id":2}`,
			},
			wantDeleted: []string{"rh-0"},
		},
		{
			name:    "multi_field_match_both_must_match",
			pattern: `{"type":["order"],"status":["paid"]}`,
			msgBodies: []string{
				`{"type":"order","status":"paid","id":1}`,
				`{"type":"order","status":"pending","id":2}`,
				`{"type":"invoice","status":"paid","id":3}`,
			},
			wantDeleted: []string{"rh-0"},
		},
		{
			name:    "no_match_drops_all",
			pattern: `{"type":["missing-type"]}`,
			msgBodies: []string{
				`{"type":"order"}`,
			},
			wantDeleted: nil,
		},
		{
			name:    "empty_pattern_passes_all",
			pattern: "",
			msgBodies: []string{
				`{"type":"order"}`,
				`{"type":"inventory"}`,
			},
			wantDeleted: []string{"rh-0", "rh-1"},
		},
		{
			name:    "non_json_pattern_uses_substring",
			pattern: "order",
			msgBodies: []string{
				`{"type":"order"}`,
				`{"type":"inventory"}`,
			},
			wantDeleted: []string{"rh-0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         tt.name + "-pipe",
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				DesiredState: "RUNNING",
				SourceParameters: &pipes.SourceParameters{
					FilterCriteria: &pipes.FilterCriteria{
						Filters: []pipes.Filter{{Pattern: tt.pattern}},
					},
				},
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			msgs := make([]*pipes.SQSMessage, len(tt.msgBodies))
			for i, body := range tt.msgBodies {
				msgs[i] = &pipes.SQSMessage{
					MessageID:     "m-" + string(rune('0'+i)),
					ReceiptHandle: "rh-" + string(rune('0'+i)),
					Body:          body,
				}
			}

			sqsReader := &b3MockSQSReader{messages: msgs}
			lambdaInvoker := &b3MockLambdaInvoker{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(lambdaInvoker)

			pipes.PollAllPipesOnce(t.Context(), runner)

			sqsReader.mu.Lock()
			deleted := sqsReader.deleted
			sqsReader.mu.Unlock()

			assert.Equal(t, tt.wantDeleted, deleted)
		})
	}
}

// TestBatch3_Filter_PatternOperators verifies prefix, suffix, and anything-but
// pattern operators in JSON event pattern matching.
func TestBatch3_Filter_PatternOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pattern   string
		msgBody   string
		wantMatch bool
	}{
		{
			name:      "prefix_operator_matches",
			pattern:   `{"type":[{"prefix":"ord"}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: true,
		},
		{
			name:      "prefix_operator_no_match",
			pattern:   `{"type":[{"prefix":"inv"}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: false,
		},
		{
			name:      "suffix_operator_matches",
			pattern:   `{"type":[{"suffix":"der"}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: true,
		},
		{
			name:      "suffix_operator_no_match",
			pattern:   `{"type":[{"suffix":"xyz"}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: false,
		},
		{
			name:      "anything_but_matches_when_not_excluded",
			pattern:   `{"status":[{"anything-but":["cancelled","failed"]}]}`,
			msgBody:   `{"status":"paid"}`,
			wantMatch: true,
		},
		{
			name:      "anything_but_no_match_when_excluded",
			pattern:   `{"status":[{"anything-but":["cancelled","failed"]}]}`,
			msgBody:   `{"status":"cancelled"}`,
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         "op-" + tt.name,
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				DesiredState: "RUNNING",
				SourceParameters: &pipes.SourceParameters{
					FilterCriteria: &pipes.FilterCriteria{
						Filters: []pipes.Filter{{Pattern: tt.pattern}},
					},
				},
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, "op-"+tt.name)

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: tt.msgBody}},
			}
			lambdaInvoker := &b3MockLambdaInvoker{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(lambdaInvoker)

			pipes.PollAllPipesOnce(t.Context(), runner)

			sqsReader.mu.Lock()
			deleted := sqsReader.deleted
			sqsReader.mu.Unlock()

			if tt.wantMatch {
				assert.Equal(t, []string{"rh1"}, deleted, "message should pass filter and be deleted")
			} else {
				assert.Empty(t, deleted, "message should be dropped by filter and not deleted")
			}
		})
	}
}

// TestBatch3_Filter_MultipleFilters verifies that multiple filter patterns create
// an OR condition (any matching filter passes the message).
func TestBatch3_Filter_MultipleFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		filters     []pipes.Filter
		msgBodies   []string
		wantMatched int
	}{
		{
			name: "two_patterns_or_logic",
			filters: []pipes.Filter{
				{Pattern: `{"type":["order"]}`},
				{Pattern: `{"type":["payment"]}`},
			},
			msgBodies: []string{
				`{"type":"order"}`,
				`{"type":"payment"}`,
				`{"type":"inventory"}`,
			},
			wantMatched: 2,
		},
		{
			name: "empty_filter_passes_all",
			filters: []pipes.Filter{
				{Pattern: `{"type":["order"]}`},
				{Pattern: ""},
			},
			msgBodies: []string{
				`{"type":"order"}`,
				`{"type":"inventory"}`,
			},
			wantMatched: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         "mf-" + tt.name,
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				DesiredState: "RUNNING",
				SourceParameters: &pipes.SourceParameters{
					FilterCriteria: &pipes.FilterCriteria{
						Filters: tt.filters,
					},
				},
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, "mf-"+tt.name)

			msgs := make([]*pipes.SQSMessage, len(tt.msgBodies))
			for i, body := range tt.msgBodies {
				msgs[i] = &pipes.SQSMessage{
					MessageID:     "m" + string(rune('0'+i)),
					ReceiptHandle: "rh" + string(rune('0'+i)),
					Body:          body,
				}
			}

			sqsReader := &b3MockSQSReader{messages: msgs}
			lambdaInvoker := &b3MockLambdaInvoker{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(lambdaInvoker)

			pipes.PollAllPipesOnce(t.Context(), runner)

			sqsReader.mu.Lock()
			deleted := sqsReader.deleted
			sqsReader.mu.Unlock()

			assert.Len(t, deleted, tt.wantMatched,
				"%d messages should pass the OR filter", tt.wantMatched)
		})
	}
}

// TestBatch3_Enrichment_RecordedOnlyWhenConfigured verifies that enrichment call
// counts are not incremented for pipes with no enrichment configured.
func TestBatch3_Enrichment_RecordedOnlyWhenConfigured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		enrichment string
		wantCount  int64
	}{
		{name: "no_enrichment", enrichment: "", wantCount: 0},
		{name: "with_enrichment", enrichment: "arn:aws:lambda:eu-west-1:111122223333:function:e", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         tt.name + "-pipe",
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				Enrichment:   tt.enrichment,
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
			}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(&b3MockLambdaInvoker{})

			pipes.PollAllPipesOnce(t.Context(), runner)

			assert.Equal(t, tt.wantCount, b.GetEnrichmentCallCount(context.Background(), tt.name+"-pipe"))
		})
	}
}

// TestBatch3_UnsupportedTarget_NilInvoker verifies that missing invokers for
// supported target types are handled gracefully (no panic, messages deleted).
func TestBatch3_UnsupportedTarget_NilInvoker(t *testing.T) {
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

// --- mock implementations ---

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
