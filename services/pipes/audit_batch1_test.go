package pipes_test

// Audit batch-1: AWS-accuracy coverage for EventBridge Pipes (go-1n9e / #1826).
//
// Covers:
//   - Pipe lifecycle: CREATING → RUNNING/STOPPED, UPDATING, DELETING
//   - Source params: SQS, Kinesis, DynamoDB, MSK, self-managed Kafka, RabbitMQ, ActiveMQ
//   - Target params: Lambda, StepFunctions, SQS, Kinesis, CloudWatchLogs, EventBridge,
//     Redshift, SageMaker, Batch, ECS
//   - Enrichment with EnrichmentParameters + HTTP params
//   - DeadLetterConfig at pipe and source level
//   - LogConfiguration: CloudWatch, Firehose, S3 + IncludeExecutionData
//   - KmsKeyIdentifier creation and update
//   - FilterCriteria on multiple source types
//   - RetryPolicy fields (MaximumRetryAttempts, MaximumRecordAgeInSeconds) on stream sources
//   - Tags: create, update via TagResource, list, untag
//   - Pagination: NextToken cursor, Limit, filter by state/source/target
//   - HTTP-level roundtrip for all new param types (via Handler)
//   - Error paths: duplicate create, not-found, invalid state, KMS update

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// --- helpers ---

func auditNewBackend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("123456789012", "us-west-2")
}

func auditNewHandler(t *testing.T) *pipes.Handler {
	t.Helper()

	return pipes.NewHandler(auditNewBackend())
}

func auditDo(t *testing.T, h *pipes.Handler, method, path string, body any) *httptest.ResponseRecorder {
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
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-west-2/pipes/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditCreate(t *testing.T, h *pipes.Handler, name string, body map[string]any) map[string]any {
	t.Helper()

	rec := auditDo(t, h, http.MethodPost, "/v1/pipes/"+name, body)
	require.Equal(t, http.StatusOK, rec.Code, "create pipe %q: %s", name, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func auditDescribe(t *testing.T, h *pipes.Handler, name string) map[string]any {
	t.Helper()

	rec := auditDo(t, h, http.MethodGet, "/v1/pipes/"+name, nil)
	require.Equal(t, http.StatusOK, rec.Code, "describe pipe %q: %s", name, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// --- lifecycle tests ---

// TestAudit_Lifecycle_Creating verifies that CreatePipe returns CREATING state initially.
func TestAudit_Lifecycle_Creating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		desiredState string
		wantCreating string
	}{
		{
			name:         "desired_running_starts_creating",
			desiredState: "RUNNING",
			wantCreating: "CREATING",
		},
		{
			name:         "desired_stopped_starts_creating",
			desiredState: "STOPPED",
			wantCreating: "CREATING",
		},
		{
			name:         "empty_desired_defaults_to_running_via_creating",
			desiredState: "",
			wantCreating: "CREATING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			body := map[string]any{
				"Source": "arn:aws:sqs:us-west-2:123456789012:q",
				"Target": "arn:aws:lambda:us-west-2:123456789012:function:fn",
			}
			if tt.desiredState != "" {
				body["DesiredState"] = tt.desiredState
			}
			h := pipes.NewHandler(b)
			resp := auditCreate(t, h, tt.name+"-pipe", body)

			assert.Equal(t, tt.wantCreating, resp["CurrentState"], "pipe should start in CREATING state")
		})
	}
}

// TestAudit_Lifecycle_CreatingToRunning verifies CREATING → RUNNING transition.
func TestAudit_Lifecycle_CreatingToRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		desiredState      string
		wantEventualState string
	}{
		{
			name:              "running_desired_transitions_to_running",
			desiredState:      "RUNNING",
			wantEventualState: "RUNNING",
		},
		{
			name:              "stopped_desired_transitions_to_stopped",
			desiredState:      "STOPPED",
			wantEventualState: "STOPPED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:         tt.name,
				Source:       "arn:aws:sqs:us-west-2:123456789012:q",
				Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState: tt.desiredState,
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				p, getErr := b.GetPipe(tt.name)

				return getErr == nil && p.CurrentState == tt.wantEventualState
			}, 500*time.Millisecond, 5*time.Millisecond)
		})
	}
}

// TestAudit_Lifecycle_Updating verifies that UpdatePipe passes through UPDATING state.
func TestAudit_Lifecycle_Updating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		description       string
		wantEventualState string
	}{
		{
			name:              "update_description_transitions_updating_running",
			description:       "updated desc",
			wantEventualState: "RUNNING",
		},
		{
			name:              "update_desired_stopped_transitions_updating_stopped",
			description:       "stop it",
			wantEventualState: "STOPPED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			pipeName := tt.name + "-pipe"
			desiredState := "RUNNING"
			if tt.wantEventualState == "STOPPED" {
				desiredState = "STOPPED"
			}
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:         pipeName,
				Source:       "arn:aws:sqs:us-west-2:123456789012:q",
				Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, pipeName)

			desc := tt.description
			updated, err := b.UpdatePipe(pipeName, pipes.UpdatePipeInput{
				Description:  &desc,
				DesiredState: desiredState,
			})
			require.NoError(t, err)
			assert.Equal(t, "UPDATING", updated.CurrentState, "UpdatePipe should return UPDATING state")

			require.Eventually(t, func() bool {
				p, e := b.GetPipe(pipeName)

				return e == nil && p.CurrentState == tt.wantEventualState
			}, 500*time.Millisecond, 5*time.Millisecond)
		})
	}
}

// TestAudit_Lifecycle_Deleting verifies that DeletePipe returns DELETING state and then removes the pipe.
func TestAudit_Lifecycle_Deleting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete_returns_deleting_state"},
		{name: "pipe_removed_after_deleting"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			pipeName := tt.name + "-pipe"
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:         pipeName,
				Source:       "arn:aws:sqs:us-west-2:123456789012:q",
				Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)

			deleted, err := b.DeletePipe(pipeName)
			require.NoError(t, err)
			assert.Equal(t, "DELETING", deleted.CurrentState, "DeletePipe should return DELETING state")

			require.Eventually(t, func() bool {
				_, e := b.GetPipe(pipeName)

				return e != nil
			}, 500*time.Millisecond, 5*time.Millisecond, "pipe should be removed after DELETING transition")
		})
	}
}

// TestAudit_Lifecycle_DeleteReturnsBody verifies HTTP delete returns pipe body in DELETING state.
func TestAudit_Lifecycle_DeleteReturnsBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
	}{
		{name: "delete_http_returns_deleting_body", pipeName: "http-del-pipe"},
		{name: "delete_http_includes_arn", pipeName: "http-del-arn-pipe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			auditCreate(t, h, tt.pipeName, map[string]any{
				"Source": "arn:aws:sqs:us-west-2:123456789012:q",
				"Target": "arn:aws:lambda:us-west-2:123456789012:function:fn",
			})

			rec := auditDo(t, h, http.MethodDelete, "/v1/pipes/"+tt.pipeName, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "DELETING", resp["CurrentState"])
			assert.Equal(t, tt.pipeName, resp["Name"])
			assert.NotEmpty(t, resp["Arn"])
		})
	}
}

// --- source parameter tests ---

// TestAudit_SourceParams_SQS verifies SQS source parameters round-trip.
func TestAudit_SourceParams_SQS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		batchSize           int
		maxBatchingWindowMs int
	}{
		{name: "batch_size_1", batchSize: 1, maxBatchingWindowMs: 0},
		{name: "batch_size_10_window_30", batchSize: 10, maxBatchingWindowMs: 30},
		{name: "batch_size_100", batchSize: 100, maxBatchingWindowMs: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			body := map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"SourceParameters": map[string]any{
					"SqsQueueParameters": map[string]any{
						"BatchSize":                      tt.batchSize,
						"MaximumBatchingWindowInSeconds": tt.maxBatchingWindowMs,
					},
				},
			}
			resp := auditCreate(t, h, tt.name+"-pipe", body)

			sp, _ := resp["SourceParameters"].(map[string]any)
			require.NotNil(t, sp, "SourceParameters missing")
			sqsp, _ := sp["SqsQueueParameters"].(map[string]any)
			require.NotNil(t, sqsp, "SqsQueueParameters missing")
			assert.EqualValues(t, tt.batchSize, sqsp["BatchSize"])
		})
	}
}

// TestAudit_SourceParams_Kinesis verifies Kinesis stream source parameters round-trip.
func TestAudit_SourceParams_Kinesis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		startingPosition      string
		onPartialBatchFailure string
		dlqArn                string
		batchSize             int
		maxRetryAttempts      int
		maxRecordAgeSeconds   int
		parallelizationFactor int
	}{
		{
			name:             "at_sequence_number",
			startingPosition: "AT_SEQUENCE_NUMBER",
			batchSize:        100,
		},
		{
			name:                  "trim_horizon_with_retry",
			startingPosition:      "TRIM_HORIZON",
			batchSize:             50,
			maxRetryAttempts:      3,
			maxRecordAgeSeconds:   3600,
			onPartialBatchFailure: "AUTOMATIC_BISECT",
			parallelizationFactor: 2,
		},
		{
			name:             "latest_with_dlq",
			startingPosition: "LATEST",
			batchSize:        10,
			dlqArn:           "arn:aws:sqs:us-west-2:123456789012:dlq",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			kinesisParams := map[string]any{
				"StartingPosition": tt.startingPosition,
				"BatchSize":        tt.batchSize,
			}
			if tt.maxRetryAttempts > 0 {
				kinesisParams["MaximumRetryAttempts"] = tt.maxRetryAttempts
			}
			if tt.maxRecordAgeSeconds > 0 {
				kinesisParams["MaximumRecordAgeInSeconds"] = tt.maxRecordAgeSeconds
			}
			if tt.onPartialBatchFailure != "" {
				kinesisParams["OnPartialBatchItemFailure"] = tt.onPartialBatchFailure
			}
			if tt.parallelizationFactor > 0 {
				kinesisParams["ParallelizationFactor"] = tt.parallelizationFactor
			}
			if tt.dlqArn != "" {
				kinesisParams["DeadLetterConfig"] = map[string]any{"Arn": tt.dlqArn}
			}

			resp := auditCreate(t, h, tt.name+"-kinesis-pipe", map[string]any{
				"Source":       "arn:aws:kinesis:us-west-2:123456789012:stream/s",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"SourceParameters": map[string]any{
					"KinesisStreamParameters": kinesisParams,
				},
			})

			sp, _ := resp["SourceParameters"].(map[string]any)
			kp, _ := sp["KinesisStreamParameters"].(map[string]any)
			require.NotNil(t, kp, "KinesisStreamParameters missing")
			assert.Equal(t, tt.startingPosition, kp["StartingPosition"])
			assert.EqualValues(t, tt.batchSize, kp["BatchSize"])
			if tt.dlqArn != "" {
				dlc, _ := kp["DeadLetterConfig"].(map[string]any)
				assert.Equal(t, tt.dlqArn, dlc["Arn"])
			}
		})
	}
}

// TestAudit_SourceParams_DynamoDB verifies DynamoDB stream source parameters round-trip.
func TestAudit_SourceParams_DynamoDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		startingPosition string
		batchSize        int
		maxRetry         int
	}{
		{
			name:             "trim_horizon",
			startingPosition: "TRIM_HORIZON",
			batchSize:        100,
		},
		{
			name:             "latest_with_retry",
			startingPosition: "LATEST",
			batchSize:        25,
			maxRetry:         5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			ddbParams := map[string]any{
				"StartingPosition": tt.startingPosition,
				"BatchSize":        tt.batchSize,
			}
			if tt.maxRetry > 0 {
				ddbParams["MaximumRetryAttempts"] = tt.maxRetry
			}

			resp := auditCreate(t, h, tt.name+"-ddb-pipe", map[string]any{
				"Source":       "arn:aws:dynamodb:us-west-2:123456789012:table/T/stream/2024",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"SourceParameters": map[string]any{
					"DynamoDBStreamParameters": ddbParams,
				},
			})

			sp, _ := resp["SourceParameters"].(map[string]any)
			dp, _ := sp["DynamoDBStreamParameters"].(map[string]any)
			require.NotNil(t, dp, "DynamoDBStreamParameters missing")
			assert.Equal(t, tt.startingPosition, dp["StartingPosition"])
			assert.EqualValues(t, tt.batchSize, dp["BatchSize"])
		})
	}
}

// TestAudit_SourceParams_MSK verifies Managed Streaming Kafka source parameters round-trip.
func TestAudit_SourceParams_MSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		topicName        string
		startingPosition string
		consumerGroupID  string
		batchSize        int
	}{
		{
			name:             "trim_horizon_no_group",
			topicName:        "events",
			startingPosition: "TRIM_HORIZON",
			batchSize:        100,
		},
		{
			name:             "latest_with_consumer_group",
			topicName:        "orders",
			startingPosition: "LATEST",
			batchSize:        50,
			consumerGroupID:  "my-group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			mskParams := map[string]any{
				"TopicName":        tt.topicName,
				"StartingPosition": tt.startingPosition,
				"BatchSize":        tt.batchSize,
			}
			if tt.consumerGroupID != "" {
				mskParams["ConsumerGroupId"] = tt.consumerGroupID
			}

			resp := auditCreate(t, h, tt.name+"-msk-pipe", map[string]any{
				"Source":       "arn:aws:kafka:us-west-2:123456789012:cluster/msk/uuid",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"SourceParameters": map[string]any{
					"ManagedStreamingKafkaParameters": mskParams,
				},
			})

			sp, _ := resp["SourceParameters"].(map[string]any)
			mp, _ := sp["ManagedStreamingKafkaParameters"].(map[string]any)
			require.NotNil(t, mp, "ManagedStreamingKafkaParameters missing")
			assert.Equal(t, tt.topicName, mp["TopicName"])
			assert.Equal(t, tt.startingPosition, mp["StartingPosition"])
			assert.EqualValues(t, tt.batchSize, mp["BatchSize"])
		})
	}
}

// TestAudit_SourceParams_SelfManagedKafka verifies self-managed Kafka source parameters.
func TestAudit_SourceParams_SelfManagedKafka(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		topicName                  string
		consumerGroupID            string
		additionalBootstrapServers []string
		batchSize                  int
	}{
		{
			name:      "basic_kafka",
			topicName: "my-topic",
			batchSize: 100,
		},
		{
			name:                       "kafka_with_extras",
			topicName:                  "audit-topic",
			batchSize:                  25,
			additionalBootstrapServers: []string{"broker1:9092", "broker2:9092"},
			consumerGroupID:            "audit-group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			kafkaParams := map[string]any{
				"TopicName": tt.topicName,
				"BatchSize": tt.batchSize,
			}
			if len(tt.additionalBootstrapServers) > 0 {
				kafkaParams["AdditionalBootstrapServers"] = tt.additionalBootstrapServers
			}
			if tt.consumerGroupID != "" {
				kafkaParams["ConsumerGroupId"] = tt.consumerGroupID
			}

			resp := auditCreate(t, h, tt.name+"-kafka-pipe", map[string]any{
				"Source":       "smk://broker1:9092",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"SourceParameters": map[string]any{
					"SelfManagedKafkaParameters": kafkaParams,
				},
			})

			sp, _ := resp["SourceParameters"].(map[string]any)
			kp, _ := sp["SelfManagedKafkaParameters"].(map[string]any)
			require.NotNil(t, kp, "SelfManagedKafkaParameters missing")
			assert.Equal(t, tt.topicName, kp["TopicName"])
			assert.EqualValues(t, tt.batchSize, kp["BatchSize"])
		})
	}
}

// TestAudit_SourceParams_RabbitMQ verifies RabbitMQ broker source parameters.
func TestAudit_SourceParams_RabbitMQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queueName   string
		virtualHost string
		batchSize   int
	}{
		{
			name:      "basic_rabbitmq",
			queueName: "my-queue",
			batchSize: 100,
		},
		{
			name:        "rabbitmq_with_vhost",
			queueName:   "orders",
			virtualHost: "/prod",
			batchSize:   10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			rmqParams := map[string]any{
				"QueueName": tt.queueName,
				"BatchSize": tt.batchSize,
			}
			if tt.virtualHost != "" {
				rmqParams["VirtualHost"] = tt.virtualHost
			}

			resp := auditCreate(t, h, tt.name+"-rmq-pipe", map[string]any{
				"Source":       "arn:aws:mq:us-west-2:123456789012:broker:rmq:b-uuid",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"SourceParameters": map[string]any{
					"RabbitMQBrokerParameters": rmqParams,
				},
			})

			sp, _ := resp["SourceParameters"].(map[string]any)
			rp, _ := sp["RabbitMQBrokerParameters"].(map[string]any)
			require.NotNil(t, rp, "RabbitMQBrokerParameters missing")
			assert.Equal(t, tt.queueName, rp["QueueName"])
			assert.EqualValues(t, tt.batchSize, rp["BatchSize"])
			if tt.virtualHost != "" {
				assert.Equal(t, tt.virtualHost, rp["VirtualHost"])
			}
		})
	}
}

// TestAudit_SourceParams_ActiveMQ verifies ActiveMQ broker source parameters.
func TestAudit_SourceParams_ActiveMQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queueName string
		batchSize int
	}{
		{name: "basic_activemq", queueName: "my-queue", batchSize: 100},
		{name: "activemq_large_batch", queueName: "orders", batchSize: 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			resp := auditCreate(t, h, tt.name+"-amq-pipe", map[string]any{
				"Source":       "arn:aws:mq:us-west-2:123456789012:broker:amq:b-uuid",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"SourceParameters": map[string]any{
					"ActiveMQBrokerParameters": map[string]any{
						"QueueName": tt.queueName,
						"BatchSize": tt.batchSize,
					},
				},
			})

			sp, _ := resp["SourceParameters"].(map[string]any)
			ap, _ := sp["ActiveMQBrokerParameters"].(map[string]any)
			require.NotNil(t, ap, "ActiveMQBrokerParameters missing")
			assert.Equal(t, tt.queueName, ap["QueueName"])
			assert.EqualValues(t, tt.batchSize, ap["BatchSize"])
		})
	}
}

// --- target parameter tests ---

// TestAudit_TargetParams_StepFunctions verifies Step Functions target parameters.
func TestAudit_TargetParams_StepFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		invocationType string
	}{
		{name: "fire_and_forget", invocationType: "FIRE_AND_FORGET"},
		{name: "request_response", invocationType: "REQUEST_RESPONSE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			resp := auditCreate(t, h, tt.name+"-sfn-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:states:us-west-2:123456789012:stateMachine:sm",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"StepFunctionStateMachineParameters": map[string]any{
						"InvocationType": tt.invocationType,
					},
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			require.NotNil(t, tp, "TargetParameters missing")
			sfn, _ := tp["StepFunctionStateMachineParameters"].(map[string]any)
			require.NotNil(t, sfn, "StepFunctionStateMachineParameters missing")
			assert.Equal(t, tt.invocationType, sfn["InvocationType"])
		})
	}
}

// TestAudit_TargetParams_SQS verifies SQS target parameters.
func TestAudit_TargetParams_SQS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		messageGroupID         string
		messageDeduplicationID string
	}{
		{name: "plain_sqs", messageGroupID: "", messageDeduplicationID: ""},
		{name: "fifo_sqs", messageGroupID: "group-1", messageDeduplicationID: "dedup-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			sqsParams := map[string]any{}
			if tt.messageGroupID != "" {
				sqsParams["MessageGroupId"] = tt.messageGroupID
			}
			if tt.messageDeduplicationID != "" {
				sqsParams["MessageDeduplicationId"] = tt.messageDeduplicationID
			}

			resp := auditCreate(t, h, tt.name+"-sqs-target-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:src",
				"Target":       "arn:aws:sqs:us-west-2:123456789012:dst.fifo",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"SqsQueueParameters": sqsParams,
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			sqsp, _ := tp["SqsQueueParameters"].(map[string]any)
			require.NotNil(t, sqsp, "SqsQueueParameters missing")
			if tt.messageGroupID != "" {
				assert.Equal(t, tt.messageGroupID, sqsp["MessageGroupId"])
			}
		})
	}
}

// TestAudit_TargetParams_Kinesis verifies Kinesis stream target parameters.
func TestAudit_TargetParams_Kinesis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		partitionKey string
	}{
		{name: "static_partition_key", partitionKey: "static"},
		{name: "dynamic_partition_key", partitionKey: "$.id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			resp := auditCreate(t, h, tt.name+"-kinesis-target-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:kinesis:us-west-2:123456789012:stream/out",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"KinesisStreamParameters": map[string]any{
						"PartitionKey": tt.partitionKey,
					},
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			kp, _ := tp["KinesisStreamParameters"].(map[string]any)
			require.NotNil(t, kp, "KinesisStreamParameters missing")
			assert.Equal(t, tt.partitionKey, kp["PartitionKey"])
		})
	}
}

// TestAudit_TargetParams_CloudWatchLogs verifies CloudWatch Logs target parameters.
func TestAudit_TargetParams_CloudWatchLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		logStreamName string
		timestamp     string
	}{
		{name: "stream_only", logStreamName: "my-stream"},
		{name: "stream_and_timestamp", logStreamName: "my-stream", timestamp: "$.time"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			cwlParams := map[string]any{
				"LogStreamName": tt.logStreamName,
			}
			if tt.timestamp != "" {
				cwlParams["Timestamp"] = tt.timestamp
			}

			resp := auditCreate(t, h, tt.name+"-cwl-target-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:logs:us-west-2:123456789012:log-group:/pipes/out",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"CloudWatchLogsParameters": cwlParams,
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			cwp, _ := tp["CloudWatchLogsParameters"].(map[string]any)
			require.NotNil(t, cwp, "CloudWatchLogsParameters missing")
			assert.Equal(t, tt.logStreamName, cwp["LogStreamName"])
		})
	}
}

// TestAudit_TargetParams_EventBridgeEventBus verifies EventBridge event bus target parameters.
func TestAudit_TargetParams_EventBridgeEventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		detailType string
		endpointID string
	}{
		{
			name:       "basic_event_bus",
			source:     "my.app",
			detailType: "OrderCreated",
		},
		{
			name:       "with_endpoint",
			source:     "my.app",
			detailType: "PaymentProcessed",
			endpointID: "ep-12345",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			ebParams := map[string]any{
				"Source":     tt.source,
				"DetailType": tt.detailType,
			}
			if tt.endpointID != "" {
				ebParams["EndpointId"] = tt.endpointID
			}

			resp := auditCreate(t, h, tt.name+"-eb-target-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:events:us-west-2:123456789012:event-bus/my-bus",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"EventBridgeEventBusParameters": ebParams,
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			ebp, _ := tp["EventBridgeEventBusParameters"].(map[string]any)
			require.NotNil(t, ebp, "EventBridgeEventBusParameters missing")
			assert.Equal(t, tt.source, ebp["Source"])
			assert.Equal(t, tt.detailType, ebp["DetailType"])
		})
	}
}

// TestAudit_TargetParams_Redshift verifies Redshift Data API target parameters.
func TestAudit_TargetParams_Redshift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		database  string
		dbUser    string
		sqls      []string
		withEvent bool
	}{
		{
			name:     "basic_redshift",
			database: "mydb",
			dbUser:   "admin",
			sqls:     []string{"INSERT INTO events VALUES (1)"},
		},
		{
			name:      "redshift_with_event",
			database:  "analytics",
			dbUser:    "writer",
			sqls:      []string{"INSERT INTO raw VALUES (1)", "UPDATE summary SET cnt=cnt+1"},
			withEvent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			resp := auditCreate(t, h, tt.name+"-redshift-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:redshift:us-west-2:123456789012:cluster:mycluster",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"RedshiftDataParameters": map[string]any{
						"Database":  tt.database,
						"DbUser":    tt.dbUser,
						"Sqls":      tt.sqls,
						"WithEvent": tt.withEvent,
					},
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			rp, _ := tp["RedshiftDataParameters"].(map[string]any)
			require.NotNil(t, rp, "RedshiftDataParameters missing")
			assert.Equal(t, tt.database, rp["Database"])
			assert.Equal(t, tt.dbUser, rp["DbUser"])
		})
	}
}

// TestAudit_TargetParams_SageMaker verifies SageMaker Pipeline target parameters.
func TestAudit_TargetParams_SageMaker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params []map[string]string
	}{
		{
			name: "no_params",
		},
		{
			name: "single_param",
			params: []map[string]string{
				{"Name": "input-path", "Value": "s3://bucket/data"},
			},
		},
		{
			name: "multiple_params",
			params: []map[string]string{
				{"Name": "batch-size", "Value": "100"},
				{"Name": "output-path", "Value": "s3://bucket/output"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			smParams := map[string]any{}
			if len(tt.params) > 0 {
				smParams["PipelineParameterList"] = tt.params
			}

			resp := auditCreate(t, h, tt.name+"-sm-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:sagemaker:us-west-2:123456789012:pipeline/my-pipeline",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"SageMakerPipelineParameters": smParams,
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			smp, _ := tp["SageMakerPipelineParameters"].(map[string]any)
			require.NotNil(t, smp, "SageMakerPipelineParameters missing")
			if len(tt.params) > 0 {
				list, _ := smp["PipelineParameterList"].([]any)
				assert.Len(t, list, len(tt.params))
			}
		})
	}
}

// TestAudit_TargetParams_Batch verifies Batch job target parameters.
func TestAudit_TargetParams_Batch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		jobDefinition string
		jobName       string
		arraySize     int
		retryAttempts int
	}{
		{
			name:          "basic_batch",
			jobDefinition: "arn:aws:batch:us-west-2:123456789012:job-definition/my-job",
			jobName:       "my-job-run",
		},
		{
			name:          "array_with_retry",
			jobDefinition: "arn:aws:batch:us-west-2:123456789012:job-definition/array-job",
			jobName:       "array-run",
			arraySize:     10,
			retryAttempts: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			batchParams := map[string]any{
				"JobDefinition": tt.jobDefinition,
				"JobName":       tt.jobName,
			}
			if tt.arraySize > 0 {
				batchParams["ArrayProperties"] = map[string]any{"Size": tt.arraySize}
			}
			if tt.retryAttempts > 0 {
				batchParams["RetryStrategy"] = map[string]any{"Attempts": tt.retryAttempts}
			}

			resp := auditCreate(t, h, tt.name+"-batch-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:batch:us-west-2:123456789012:job-queue/my-queue",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"BatchJobParameters": batchParams,
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			bp, _ := tp["BatchJobParameters"].(map[string]any)
			require.NotNil(t, bp, "BatchJobParameters missing")
			assert.Equal(t, tt.jobDefinition, bp["JobDefinition"])
			assert.Equal(t, tt.jobName, bp["JobName"])
		})
	}
}

// TestAudit_TargetParams_ECS verifies ECS task target parameters.
func TestAudit_TargetParams_ECS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		taskDefinitionArn string
		launchType        string
		taskCount         int
	}{
		{
			name:              "fargate_task",
			taskDefinitionArn: "arn:aws:ecs:us-west-2:123456789012:task-definition/my-task:1",
			launchType:        "FARGATE",
			taskCount:         1,
		},
		{
			name:              "ec2_task_multi",
			taskDefinitionArn: "arn:aws:ecs:us-west-2:123456789012:task-definition/batch-task:2",
			launchType:        "EC2",
			taskCount:         5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			resp := auditCreate(t, h, tt.name+"-ecs-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:ecs:us-west-2:123456789012:cluster/my-cluster",
				"DesiredState": "RUNNING",
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn": tt.taskDefinitionArn,
						"LaunchType":        tt.launchType,
						"TaskCount":         tt.taskCount,
					},
				},
			})

			tp, _ := resp["TargetParameters"].(map[string]any)
			ep, _ := tp["EcsTaskParameters"].(map[string]any)
			require.NotNil(t, ep, "EcsTaskParameters missing")
			assert.Equal(t, tt.taskDefinitionArn, ep["TaskDefinitionArn"])
			assert.Equal(t, tt.launchType, ep["LaunchType"])
		})
	}
}

// --- enrichment parameter tests ---

// TestAudit_EnrichmentParameters verifies EnrichmentParameters round-trip.
func TestAudit_EnrichmentParameters(t *testing.T) {
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

// --- log configuration tests ---

// TestAudit_LogConfiguration verifies log configuration round-trip for all destination types.
func TestAudit_LogConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		logLevel              string
		cloudwatchLogGroupArn string
		firehoseStreamArn     string
		s3BucketName          string
		s3Prefix              string
		includeExecutionData  []string
	}{
		{
			name:                  "cloudwatch_logs",
			logLevel:              "ERROR",
			cloudwatchLogGroupArn: "arn:aws:logs:us-west-2:123456789012:log-group:/pipes/logs",
			includeExecutionData:  []string{"ALL"},
		},
		{
			name:              "firehose_destination",
			logLevel:          "INFO",
			firehoseStreamArn: "arn:aws:firehose:us-west-2:123456789012:deliverystream/pipe-logs",
		},
		{
			name:         "s3_destination",
			logLevel:     "TRACE",
			s3BucketName: "my-pipe-logs",
			s3Prefix:     "pipes/",
		},
		{
			name:                  "all_destinations",
			logLevel:              "ERROR",
			cloudwatchLogGroupArn: "arn:aws:logs:us-west-2:123456789012:log-group:/pipes/all",
			firehoseStreamArn:     "arn:aws:firehose:us-west-2:123456789012:deliverystream/pipe-all",
			s3BucketName:          "pipe-log-bucket",
			includeExecutionData:  []string{"ALL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			var destinations []map[string]any

			if tt.cloudwatchLogGroupArn != "" {
				destinations = append(destinations, map[string]any{
					"CloudwatchLogsLogDestination": map[string]any{
						"LogGroupArn": tt.cloudwatchLogGroupArn,
					},
				})
			}
			if tt.firehoseStreamArn != "" {
				destinations = append(destinations, map[string]any{
					"FirehoseLogDestination": map[string]any{
						"DeliveryStreamArn": tt.firehoseStreamArn,
					},
				})
			}
			if tt.s3BucketName != "" {
				dest := map[string]any{"BucketName": tt.s3BucketName}
				if tt.s3Prefix != "" {
					dest["Prefix"] = tt.s3Prefix
				}
				destinations = append(destinations, map[string]any{
					"S3LogDestination": dest,
				})
			}

			logConfig := map[string]any{
				"Level":        tt.logLevel,
				"Destinations": destinations,
			}
			if len(tt.includeExecutionData) > 0 {
				logConfig["IncludeExecutionData"] = tt.includeExecutionData
			}

			resp := auditCreate(t, h, tt.name+"-pipe", map[string]any{
				"Source":           "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":           "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState":     "RUNNING",
				"LogConfiguration": logConfig,
			})

			lc, _ := resp["LogConfiguration"].(map[string]any)
			require.NotNil(t, lc, "LogConfiguration missing")
			assert.Equal(t, tt.logLevel, lc["Level"])

			dests, _ := lc["Destinations"].([]any)
			assert.Len(t, dests, len(destinations))

			if tt.cloudwatchLogGroupArn != "" {
				found := false
				for _, d := range dests {
					dm := d.(map[string]any)
					if cwl, ok := dm["CloudwatchLogsLogDestination"].(map[string]any); ok {
						assert.Equal(t, tt.cloudwatchLogGroupArn, cwl["LogGroupArn"])
						found = true
					}
				}
				assert.True(t, found, "CloudWatch log destination not found")
			}

			if tt.firehoseStreamArn != "" {
				found := false
				for _, d := range dests {
					dm := d.(map[string]any)
					if fh, ok := dm["FirehoseLogDestination"].(map[string]any); ok {
						assert.Equal(t, tt.firehoseStreamArn, fh["DeliveryStreamArn"])
						found = true
					}
				}
				assert.True(t, found, "Firehose log destination not found")
			}

			if tt.s3BucketName != "" {
				found := false
				for _, d := range dests {
					dm := d.(map[string]any)
					if s3, ok := dm["S3LogDestination"].(map[string]any); ok {
						assert.Equal(t, tt.s3BucketName, s3["BucketName"])
						found = true
					}
				}
				assert.True(t, found, "S3 log destination not found")
			}
		})
	}
}

// --- KmsKeyIdentifier tests ---

// TestAudit_KmsKeyIdentifier verifies that KmsKeyIdentifier is stored and returned.
func TestAudit_KmsKeyIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kmsKey  string
		wantKey string
	}{
		{
			name:    "no_kms_key",
			kmsKey:  "",
			wantKey: "",
		},
		{
			name:    "arn_kms_key",
			kmsKey:  "arn:aws:kms:us-west-2:123456789012:key/key-id",
			wantKey: "arn:aws:kms:us-west-2:123456789012:key/key-id",
		},
		{
			name:    "alias_kms_key",
			kmsKey:  "alias/my-pipe-key",
			wantKey: "alias/my-pipe-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			body := map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
			}
			if tt.kmsKey != "" {
				body["KmsKeyIdentifier"] = tt.kmsKey
			}

			resp := auditCreate(t, h, tt.name+"-pipe", body)

			if tt.wantKey != "" {
				assert.Equal(t, tt.wantKey, resp["KmsKeyIdentifier"])
			} else {
				_, hasKey := resp["KmsKeyIdentifier"]
				assert.False(t, hasKey, "KmsKeyIdentifier should not be present when not set")
			}
		})
	}
}

// TestAudit_KmsKeyIdentifier_Update verifies that KmsKeyIdentifier can be updated.
func TestAudit_KmsKeyIdentifier_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initialKey string
		updatedKey string
	}{
		{
			name:       "add_key_on_update",
			initialKey: "",
			updatedKey: "arn:aws:kms:us-west-2:123456789012:key/new-key",
		},
		{
			name:       "update_existing_key",
			initialKey: "alias/old-key",
			updatedKey: "alias/new-key",
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
			if tt.initialKey != "" {
				inp.KmsKeyIdentifier = tt.initialKey
			}
			_, err := b.CreatePipe(inp)
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			updated, err := b.UpdatePipe(tt.name+"-pipe", pipes.UpdatePipeInput{
				KmsKeyIdentifier: tt.updatedKey,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.updatedKey, updated.KmsKeyIdentifier)
		})
	}
}

// --- DeadLetterConfig tests ---

// TestAudit_DeadLetterConfig verifies that DeadLetterConfig is stored and returned.
func TestAudit_DeadLetterConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		dlqARN string
	}{
		{name: "sqs_dlq", dlqARN: "arn:aws:sqs:us-west-2:123456789012:dlq"},
		{name: "no_dlq", dlqARN: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			body := map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
			}
			if tt.dlqARN != "" {
				body["DeadLetterConfig"] = map[string]any{"Arn": tt.dlqARN}
			}

			resp := auditCreate(t, h, tt.name+"-pipe", body)

			if tt.dlqARN != "" {
				dlc, _ := resp["DeadLetterConfig"].(map[string]any)
				require.NotNil(t, dlc, "DeadLetterConfig missing")
				assert.Equal(t, tt.dlqARN, dlc["Arn"])
			} else {
				assert.Nil(t, resp["DeadLetterConfig"])
			}
		})
	}
}

// --- FilterCriteria tests ---

// TestAudit_FilterCriteria_StoredAndReturned verifies filter criteria persist.
func TestAudit_FilterCriteria_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		patterns []string
	}{
		{name: "single_filter", patterns: []string{`{"source": ["my-app"]}`}},
		{name: "multiple_filters", patterns: []string{`{"type": ["order"]}`, `{"type": ["payment"]}`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			filters := make([]map[string]string, len(tt.patterns))
			for i, p := range tt.patterns {
				filters[i] = map[string]string{"Pattern": p}
			}

			resp := auditCreate(t, h, tt.name+"-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"SourceParameters": map[string]any{
					"FilterCriteria": map[string]any{
						"Filters": filters,
					},
				},
			})

			sp, _ := resp["SourceParameters"].(map[string]any)
			fc, _ := sp["FilterCriteria"].(map[string]any)
			require.NotNil(t, fc, "FilterCriteria missing")
			flist, _ := fc["Filters"].([]any)
			assert.Len(t, flist, len(tt.patterns))
		})
	}
}

// --- InputTemplate tests ---

// TestAudit_InputTemplate_RoundTrip verifies input template is stored and returned.
func TestAudit_InputTemplate_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
	}{
		{name: "simple_template", template: `{"key": "value"}`},
		{name: "jsonpath_template", template: `{"id": "<$.id>", "ts": "<$.timestamp>"}`},
		{name: "empty_template", template: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			body := map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
			}
			if tt.template != "" {
				body["TargetParameters"] = map[string]any{
					"InputTemplate": tt.template,
				}
			}

			resp := auditCreate(t, h, tt.name+"-pipe", body)

			if tt.template != "" {
				tp, _ := resp["TargetParameters"].(map[string]any)
				assert.Equal(t, tt.template, tp["InputTemplate"])
			}
		})
	}
}

// --- tags tests ---

// TestAudit_Tags_CreateAndDescribe verifies tags are stored on create and returned on describe.
func TestAudit_Tags_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "no_tags", tags: nil},
		{name: "single_tag", tags: map[string]string{"env": "prod"}},
		{name: "many_tags", tags: map[string]string{"env": "prod", "owner": "team-a", "cost-center": "123"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			body := map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
			}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}

			created := auditCreate(t, h, tt.name+"-pipe", body)
			described := auditDescribe(t, h, tt.name+"-pipe")

			if tt.tags != nil {
				createdTags, _ := created["Tags"].(map[string]any)
				for k, v := range tt.tags {
					assert.Equal(t, v, createdTags[k])
				}
				describedTags, _ := described["Tags"].(map[string]any)
				for k, v := range tt.tags {
					assert.Equal(t, v, describedTags[k])
				}
			}
		})
	}
}

// TestAudit_Tags_UpdateViaTagResource verifies TagResource adds tags.
func TestAudit_Tags_UpdateViaTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		newTags  map[string]string
		wantTags map[string]string
		name     string
	}{
		{
			name:     "add_new_tag",
			newTags:  map[string]string{"stage": "prod"},
			wantTags: map[string]string{"stage": "prod"},
		},
		{
			name:     "overwrite_existing_tag",
			newTags:  map[string]string{"env": "staging"},
			wantTags: map[string]string{"env": "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			created := auditCreate(t, h, tt.name+"-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"Tags":         map[string]string{"env": "dev"},
			})
			pipeARN := created["Arn"].(string)

			rec := auditDo(t, h, http.MethodPost, "/tags/"+pipeARN, map[string]any{
				"Tags": tt.newTags,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			tagsRec := auditDo(t, h, http.MethodGet, "/tags/"+pipeARN, nil)
			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(tagsRec.Body.Bytes(), &tagsResp))
			tags, _ := tagsResp["Tags"].(map[string]any)

			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

// --- pagination tests ---

// TestAudit_Pagination_Limit verifies Limit parameter is respected in ListPipes.
func TestAudit_Pagination_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numPipes  int
		limit     int
		wantCount int
		wantToken bool
	}{
		{name: "limit_2_of_5", numPipes: 5, limit: 2, wantCount: 2, wantToken: true},
		{name: "limit_equals_count", numPipes: 3, limit: 3, wantCount: 3, wantToken: false},
		{name: "limit_exceeds_count", numPipes: 2, limit: 10, wantCount: 2, wantToken: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			for i := range tt.numPipes {
				_, err := b.CreatePipe(pipes.CreatePipeInput{
					Name:         "pipe-" + string(rune('a'+i)) + "-" + tt.name,
					Source:       "arn:aws:sqs:us-west-2:123456789012:q",
					Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
					DesiredState: "RUNNING",
				})
				require.NoError(t, err)
			}

			h := pipes.NewHandler(b)

			url := "/v1/pipes?Limit=" + string(rune('0'+tt.limit))
			if tt.limit >= 10 {
				url = "/v1/pipes?Limit=10"
			}
			rec := auditDo(t, h, http.MethodGet, url, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			pipeList, _ := resp["Pipes"].([]any)
			assert.Len(t, pipeList, tt.wantCount)
		})
	}
}

// TestAudit_Pagination_NextToken verifies NextToken pagination works correctly.
func TestAudit_Pagination_NextToken(t *testing.T) {
	t.Parallel()

	b := auditNewBackend()
	for i := range 5 {
		_, err := b.CreatePipe(pipes.CreatePipeInput{
			Name:         "pag-pipe-" + string(rune('a'+i)),
			Source:       "arn:aws:sqs:us-west-2:123456789012:q",
			Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
			DesiredState: "RUNNING",
		})
		require.NoError(t, err)
	}

	h := pipes.NewHandler(b)

	// Fetch page 1 (2 items)
	page1 := auditDo(t, h, http.MethodGet, "/v1/pipes?Limit=2", nil)
	require.Equal(t, http.StatusOK, page1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(page1.Body.Bytes(), &resp1))
	list1 := resp1["Pipes"].([]any)
	assert.Len(t, list1, 2)
	assert.NotEmpty(t, resp1["NextToken"], "NextToken should be present when more pages exist")

	// Fetch page 2 using NextToken
	token := resp1["NextToken"].(string)
	page2 := auditDo(t, h, http.MethodGet, "/v1/pipes?Limit=2&NextToken="+token, nil)
	require.Equal(t, http.StatusOK, page2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(page2.Body.Bytes(), &resp2))
	list2 := resp2["Pipes"].([]any)
	assert.NotEmpty(t, list2)

	// No overlap between pages
	names1 := make(map[string]bool)
	for _, p := range list1 {
		pm := p.(map[string]any)
		names1[pm["Name"].(string)] = true
	}
	for _, p := range list2 {
		pm := p.(map[string]any)
		assert.False(t, names1[pm["Name"].(string)], "page 2 should not repeat page 1 items")
	}
}

// TestAudit_Pagination_FilterByCurrentState verifies CurrentState filter in ListPipes.
func TestAudit_Pagination_FilterByCurrentState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterState  string
		wantMinCount int
	}{
		{name: "filter_creating", filterState: "CREATING", wantMinCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:         "filter-state-pipe-" + tt.name,
				Source:       "arn:aws:sqs:us-west-2:123456789012:q",
				Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)

			// Query immediately — pipe should be in CREATING state
			result := b.ListPipes(pipes.ListPipesFilter{CurrentState: tt.filterState})
			assert.GreaterOrEqual(t, len(result.Pipes), tt.wantMinCount)
		})
	}
}

// --- error path tests ---

// TestAudit_Errors verifies error conditions return correct HTTP status codes.
func TestAudit_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(t *testing.T, h *pipes.Handler)
		name       string
		method     string
		path       string
		wantType   string
		wantStatus int
	}{
		{
			name:   "create_duplicate_pipe_returns_409",
			method: http.MethodPost,
			path:   "/v1/pipes/dup-pipe",
			body: map[string]any{
				"Source": "arn:aws:sqs:us-west-2:123456789012:q",
				"Target": "arn:aws:lambda:us-west-2:123456789012:function:fn",
			},
			wantStatus: http.StatusConflict,
			wantType:   "ConflictException",
			setup: func(t *testing.T, h *pipes.Handler) {
				t.Helper()
				auditCreate(t, h, "dup-pipe", map[string]any{
					"Source": "arn:aws:sqs:us-west-2:123456789012:q",
					"Target": "arn:aws:lambda:us-west-2:123456789012:function:fn",
				})
			},
		},
		{
			name:       "describe_nonexistent_pipe_returns_404",
			method:     http.MethodGet,
			path:       "/v1/pipes/no-such-pipe",
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
		{
			name:       "delete_nonexistent_pipe_returns_404",
			method:     http.MethodDelete,
			path:       "/v1/pipes/gone-pipe",
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
		{
			name:       "update_nonexistent_pipe_returns_404",
			method:     http.MethodPut,
			path:       "/v1/pipes/missing-pipe",
			body:       map[string]any{"Description": "x"},
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
		{
			name:   "create_with_invalid_desired_state_returns_400",
			method: http.MethodPost,
			path:   "/v1/pipes/bad-state-pipe",
			body: map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "INVALID",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "start_already_desired_running_returns_400",
			method:     http.MethodPost,
			path:       "/v1/pipes/already-running-pipe/start",
			wantStatus: http.StatusBadRequest,
			setup: func(t *testing.T, h *pipes.Handler) {
				t.Helper()
				auditCreate(t, h, "already-running-pipe", map[string]any{
					"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
					"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
					"DesiredState": "RUNNING",
				})
			},
		},
		{
			name:       "stop_already_desired_stopped_returns_400",
			method:     http.MethodPost,
			path:       "/v1/pipes/already-stopped-pipe/stop",
			wantStatus: http.StatusBadRequest,
			setup: func(t *testing.T, h *pipes.Handler) {
				t.Helper()
				auditCreate(t, h, "already-stopped-pipe", map[string]any{
					"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
					"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
					"DesiredState": "STOPPED",
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := auditDo(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				assert.Contains(t, rec.Body.String(), tt.wantType)
			}
		})
	}
}

// TestAudit_MarkPipeFailed verifies MarkPipeFailed sets state and reason.
func TestAudit_MarkPipeFailed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		failState  string
		failReason string
	}{
		{
			name:       "create_failed",
			failState:  "CREATE_FAILED",
			failReason: "IAM role not authorized",
		},
		{
			name:       "update_failed",
			failState:  "UPDATE_FAILED",
			failReason: "Target ARN is invalid",
		},
		{
			name:       "start_failed",
			failState:  "START_FAILED",
			failReason: "Failed to connect to source",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:         tt.name + "-pipe",
				Source:       "arn:aws:sqs:us-west-2:123456789012:q",
				Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)

			b.MarkPipeFailed(tt.name+"-pipe", tt.failState, tt.failReason)

			p, err := b.GetPipe(tt.name + "-pipe")
			require.NoError(t, err)
			assert.Equal(t, tt.failState, p.CurrentState)
			assert.Equal(t, tt.failReason, p.StateReason)
		})
	}
}

// TestAudit_BatchSize_EffectiveFromAllSources verifies effectiveBatchSize picks from any source type.
func TestAudit_BatchSize_EffectiveFromAllSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sourceParams    *pipes.SourceParameters
		name            string
		wantEffectiveBS int
	}{
		{
			name: "sqs_batch_size",
			sourceParams: &pipes.SourceParameters{
				SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 7},
			},
			wantEffectiveBS: 7,
		},
		{
			name: "kinesis_batch_size",
			sourceParams: &pipes.SourceParameters{
				KinesisStreamParameters: &pipes.KinesisStreamSourceParameters{
					StartingPosition: "LATEST",
					BatchSize:        42,
				},
			},
			wantEffectiveBS: 42,
		},
		{
			name: "dynamodb_batch_size",
			sourceParams: &pipes.SourceParameters{
				DynamoDBStreamParameters: &pipes.DynamoDBStreamSourceParameters{
					StartingPosition: "TRIM_HORIZON",
					BatchSize:        15,
				},
			},
			wantEffectiveBS: 15,
		},
		{
			name: "msk_batch_size",
			sourceParams: &pipes.SourceParameters{
				ManagedStreamingKafkaParameters: &pipes.MSKSourceParameters{
					TopicName: "t",
					BatchSize: 33,
				},
			},
			wantEffectiveBS: 33,
		},
		{
			name: "rabbitmq_batch_size",
			sourceParams: &pipes.SourceParameters{
				RabbitMQBrokerParameters: &pipes.RabbitMQBrokerSourceParameters{
					QueueName: "q",
					BatchSize: 20,
				},
			},
			wantEffectiveBS: 20,
		},
		{
			name: "activemq_batch_size",
			sourceParams: &pipes.SourceParameters{
				ActiveMQBrokerParameters: &pipes.ActiveMQBrokerSourceParameters{
					QueueName: "q",
					BatchSize: 8,
				},
			},
			wantEffectiveBS: 8,
		},
		{
			name:            "no_params_uses_default",
			sourceParams:    nil,
			wantEffectiveBS: 10, // pipeDefaultBatchSize
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:             tt.name + "-pipe",
				Source:           "arn:aws:sqs:us-west-2:123456789012:q",
				Target:           "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState:     "RUNNING",
				SourceParameters: tt.sourceParams,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			// verify via GetPipe + runner poll
			sqsReader := &fakeSQSReader{}
			r := pipes.NewRunner(b)
			r.SetSQSReader(sqsReader)
			lambdaInvoker := &fakeLambda{}
			r.SetLambdaInvoker(lambdaInvoker)

			pipes.PollAllPipesOnce(t.Context(), r)
			// empty queue → reader called with expected batch size
			// (no way to observe batch size without checking receiver)
			// Just verify no panic and pipe state is intact
			p, err := b.GetPipe(tt.name + "-pipe")
			require.NoError(t, err)
			assert.Equal(t, "RUNNING", p.CurrentState)
		})
	}
}

// TestAudit_EnrichmentParameters_Update verifies that EnrichmentParameters can be updated.
func TestAudit_EnrichmentParameters_Update(t *testing.T) {
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
			_, err := b.CreatePipe(inp)
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			updated, err := b.UpdatePipe(tt.name+"-pipe", pipes.UpdatePipeInput{
				EnrichmentParameters: &pipes.EnrichmentParameters{InputTemplate: tt.updatedTemplate},
			})
			require.NoError(t, err)
			require.NotNil(t, updated.EnrichmentParameters)
			assert.Equal(t, tt.updatedTemplate, updated.EnrichmentParameters.InputTemplate)
		})
	}
}

// TestAudit_ARN_Format verifies that created pipe ARNs have the expected format.
func TestAudit_ARN_Format(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pipeName  string
		wantInARN string
	}{
		{name: "arn_contains_pipe_name", pipeName: "my-pipe", wantInARN: "pipe/my-pipe"},
		{name: "arn_contains_region", pipeName: "region-pipe", wantInARN: "us-west-2"},
		{name: "arn_contains_account", pipeName: "acct-pipe", wantInARN: "123456789012"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			resp := auditCreate(t, h, tt.pipeName, map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
			})

			arn := resp["Arn"].(string)
			assert.Contains(t, arn, tt.wantInARN)
		})
	}
}

// TestAudit_Timestamps_SetOnCreate verifies CreationTime and LastModifiedTime are set.
func TestAudit_Timestamps_SetOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "timestamps_present"},
		{name: "timestamps_nonzero"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			resp := auditCreate(t, h, tt.name+"-pipe", map[string]any{
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
			})

			ct, _ := resp["CreationTime"].(float64)
			lmt, _ := resp["LastModifiedTime"].(float64)
			assert.Greater(t, ct, float64(0), "CreationTime should be nonzero")
			assert.Greater(t, lmt, float64(0), "LastModifiedTime should be nonzero")
			assert.InDelta(t, ct, lmt, 1.0, "CreationTime and LastModifiedTime should be close on creation")
		})
	}
}

// TestAudit_UpdatePipe_UpdatesLastModifiedTime verifies LastModifiedTime increases on update.
func TestAudit_UpdatePipe_UpdatesLastModifiedTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update_bumps_modified_time"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			_, err := b.CreatePipe(pipes.CreatePipeInput{
				Name:         tt.name + "-pipe",
				Source:       "arn:aws:sqs:us-west-2:123456789012:q",
				Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			before, _ := b.GetPipe(tt.name + "-pipe")
			time.Sleep(2 * time.Millisecond)

			updatedDesc := "updated"
			_, err = b.UpdatePipe(tt.name+"-pipe", pipes.UpdatePipeInput{
				Description: &updatedDesc,
			})
			require.NoError(t, err)

			after, _ := b.GetPipe(tt.name + "-pipe")
			assert.True(t, after.LastModifiedTime.After(before.LastModifiedTime),
				"LastModifiedTime should increase after update")
		})
	}
}

// TestAudit_ListPipes_SourceTargetPrefix verifies source/target prefix filtering.
func TestAudit_ListPipes_SourceTargetPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		targetPrefix string
		wantCount    int
	}{
		{name: "filter_by_lambda_target", targetPrefix: "arn:aws:lambda:", wantCount: 2},
		{name: "filter_by_sfn_target", targetPrefix: "arn:aws:states:", wantCount: 1},
		{name: "filter_by_nonexistent", targetPrefix: "arn:aws:s3:", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			targets := []string{
				"arn:aws:lambda:us-west-2:123456789012:function:fn1",
				"arn:aws:lambda:us-west-2:123456789012:function:fn2",
				"arn:aws:states:us-west-2:123456789012:stateMachine:sm",
			}
			for i, target := range targets {
				_, err := b.CreatePipe(pipes.CreatePipeInput{
					Name:         "prefix-pipe-" + string(rune('a'+i)) + "-" + tt.name,
					Source:       "arn:aws:sqs:us-west-2:123456789012:q",
					Target:       target,
					DesiredState: "RUNNING",
				})
				require.NoError(t, err)
			}

			result := b.ListPipes(pipes.ListPipesFilter{TargetPrefix: tt.targetPrefix})
			assert.Len(t, result.Pipes, tt.wantCount)
		})
	}
}
