package pipes_test

// Covers EnrichmentParameters CRUD/update round trips and the runner's
// enrichment invocation + call-count tracking behavior. Counter pruning on
// pipe deletion lives in enrichment_cleanup_test.go.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestEnrichmentParameters verifies EnrichmentParameters round-trip.
func TestEnrichmentParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		enrichmentARN string
		inputTemplate string
		httpHeaders   map[string]string
		queryParams   map[string]string
		pathValues    []string
	}{
		{
			name:          "lambda_enrichment_no_params",
			enrichmentARN: "arn:aws:lambda:us-west-2:123456789012:function:enricher",
			inputTemplate: `{"input": "$.body"}`,
		},
		{
			name:          "api_gateway_enrichment_with_http_params",
			enrichmentARN: "arn:aws:execute-api:us-west-2:123456789012:apiId/stage/POST/resource",
			httpHeaders:   map[string]string{"X-Custom-Header": "value"},
			queryParams:   map[string]string{"version": "2"},
			pathValues:    []string{"$.id"},
		},
		{
			name:          "sfn_enrichment",
			enrichmentARN: "arn:aws:states:us-west-2:123456789012:stateMachine:enricher",
			inputTemplate: `{"data": "$.detail"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			enrichParams := map[string]any{}
			if tt.inputTemplate != "" {
				enrichParams["InputTemplate"] = tt.inputTemplate
			}
			if len(tt.httpHeaders) > 0 || len(tt.queryParams) > 0 || len(tt.pathValues) > 0 {
				httpParams := map[string]any{}
				if len(tt.httpHeaders) > 0 {
					httpParams["HeaderParameters"] = tt.httpHeaders
				}
				if len(tt.queryParams) > 0 {
					httpParams["QueryStringParameters"] = tt.queryParams
				}
				if len(tt.pathValues) > 0 {
					httpParams["PathParameterValues"] = tt.pathValues
				}
				enrichParams["HttpParameters"] = httpParams
			}

			resp := auditCreate(t, h, tt.name+"-pipe", map[string]any{
				"Source":               "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":               "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"Enrichment":           tt.enrichmentARN,
				"DesiredState":         "RUNNING",
				"EnrichmentParameters": enrichParams,
			})

			assert.Equal(t, tt.enrichmentARN, resp["Enrichment"])
			ep, _ := resp["EnrichmentParameters"].(map[string]any)
			require.NotNil(t, ep, "EnrichmentParameters missing")
			if tt.inputTemplate != "" {
				assert.Equal(t, tt.inputTemplate, ep["InputTemplate"])
			}
			if len(tt.httpHeaders) > 0 {
				hp, _ := ep["HttpParameters"].(map[string]any)
				require.NotNil(t, hp, "HttpParameters missing")
				headers, _ := hp["HeaderParameters"].(map[string]any)
				for k, v := range tt.httpHeaders {
					assert.Equal(t, v, headers[k])
				}
			}
		})
	}
}

// TestEnrichmentParameters_Update verifies that EnrichmentParameters can be updated.
func TestEnrichmentParameters_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		initialTemplate string
		updatedTemplate string
	}{
		{
			name:            "add_enrichment_params",
			initialTemplate: "",
			updatedTemplate: `{"enriched": true}`,
		},
		{
			name:            "update_existing_enrichment",
			initialTemplate: `{"v": 1}`,
			updatedTemplate: `{"v": 2}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			inp := pipes.CreatePipeInput{
				Name:         tt.name + "-pipe",
				Source:       "arn:aws:sqs:us-west-2:123456789012:q",
				Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState: "RUNNING",
			}
			if tt.initialTemplate != "" {
				inp.EnrichmentParameters = &pipes.EnrichmentParameters{InputTemplate: tt.initialTemplate}
			}
			_, err := b.CreatePipe(context.Background(), inp)
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			updated, err := b.UpdatePipe(context.Background(), tt.name+"-pipe", pipes.UpdatePipeInput{
				EnrichmentParameters: &pipes.EnrichmentParameters{InputTemplate: tt.updatedTemplate},
			})
			require.NoError(t, err)
			require.NotNil(t, updated.EnrichmentParameters)
			assert.Equal(t, tt.updatedTemplate, updated.EnrichmentParameters.InputTemplate)
		})
	}
}

// TestPipeEnrichmentTracking verifies that enrichment invocations are counted.
func TestPipeEnrichmentTracking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		enrichmentARN string
		msgCount      int
		wantCount     int64
	}{
		{
			name:          "enrichment_counted_when_configured",
			enrichmentARN: "arn:aws:lambda:us-east-1:000000000000:function:enricher",
			msgCount:      2,
			wantCount:     1, // one poll = one enrichment call
		},
		{
			name:          "no_enrichment_no_count",
			enrichmentARN: "",
			msgCount:      2,
			wantCount:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newPipeBackend()
			r := pipes.NewRunner(b)

			sqsReader := &fakeSQSReader{}
			lambda := &fakeLambda{}

			for i := range tt.msgCount {
				id := string(rune('a' + i))
				sqsReader.messages = append(sqsReader.messages, &pipes.SQSMessage{
					MessageID:     id,
					ReceiptHandle: "rh-" + id,
					Body:          `{"type":"event"}`,
				})
			}

			r.SetSQSReader(sqsReader)
			r.SetLambdaInvoker(lambda)

			pipeName := "enrich-pipe-" + tt.name
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         pipeName,
				Source:       "arn:aws:sqs:us-east-1:000000000000:queue",
				Target:       "arn:aws:lambda:us-east-1:000000000000:function:fn",
				Enrichment:   tt.enrichmentARN,
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, pipeName)

			pipes.PollAllPipesOnce(context.Background(), r)

			count := b.GetEnrichmentCallCount(context.Background(), pipeName)
			assert.Equal(t, tt.wantCount, count, "enrichment call count mismatch")
		})
	}
}

// TestEnrichment_LambdaInvocation verifies that when Enrichment is set to
// a Lambda ARN, the runner invokes Lambda with REQUEST_RESPONSE and uses its
// response as the payload sent to the target.
func TestEnrichment_LambdaInvocation(t *testing.T) {
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

// TestEnrichment_RecordedOnlyWhenConfigured verifies that enrichment call
// counts are not incremented for pipes with no enrichment configured.
func TestEnrichment_RecordedOnlyWhenConfigured(t *testing.T) {
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
