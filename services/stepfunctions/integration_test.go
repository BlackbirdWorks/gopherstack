package stepfunctions_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestNewSQSIntegration_SFNSendMessage(t *testing.T) {
	t.Parallel()

	sqsBackend := sqs.NewInMemoryBackend()
	out, setupErr := sqsBackend.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "test-queue",
		Endpoint:  "localhost",
	})
	require.NoError(t, setupErr)

	tests := []struct {
		name        string
		queueURL    string
		messageBody string
		wantErr     bool
	}{
		{
			name:        "valid_queue",
			queueURL:    out.QueueURL,
			messageBody: "hello body",
			wantErr:     false,
		},
		{
			name:        "invalid_queue",
			queueURL:    "http://localhost/000000000000/nonexistent",
			messageBody: "body",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			adapter := stepfunctions.NewSQSIntegration(sqsBackend)
			msgID, md5, callErr := adapter.SFNSendMessage(
				context.Background(), tt.queueURL, tt.messageBody, "", "", 0,
			)
			if tt.wantErr {
				require.Error(t, callErr)

				return
			}
			require.NoError(t, callErr)
			assert.NotEmpty(t, msgID)
			assert.NotEmpty(t, md5)
		})
	}
}

func TestNewSNSIntegration_SFNPublish(t *testing.T) {
	t.Parallel()

	snsBackend := sns.NewInMemoryBackend()
	topic, setupErr := snsBackend.CreateTopic("test-topic", nil)
	require.NoError(t, setupErr)

	tests := []struct {
		name     string
		topicARN string
		message  string
		wantErr  bool
	}{
		{
			name:     "valid_topic",
			topicARN: topic.TopicArn,
			message:  "test message",
			wantErr:  false,
		},
		{
			name:     "invalid_topic",
			topicARN: "arn:aws:sns:us-east-1:000000000000:nonexistent",
			message:  "msg",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			adapter := stepfunctions.NewSNSIntegration(snsBackend)
			msgID, callErr := adapter.SFNPublish(
				context.Background(), tt.topicARN, tt.message, "",
			)
			if tt.wantErr {
				require.Error(t, callErr)

				return
			}
			require.NoError(t, callErr)
			assert.NotEmpty(t, msgID)
		})
	}
}

func TestNewDynamoDBIntegration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ddbBackend := dynamodb.NewInMemoryDB()
	const tableName = "sfn-test-table"

	_, setupErr := ddbBackend.CreateTable(ctx, &awsdynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, setupErr)

	adapter := stepfunctions.NewDynamoDBIntegration(ddbBackend)

	tests := []struct {
		op      func() (any, error)
		name    string
		wantErr bool
	}{
		{
			name: "PutItem_missing_key",
			op: func() (any, error) {
				return adapter.SFNPutItem(ctx, map[string]any{
					"TableName": tableName,
					"Item":      map[string]any{},
				})
			},
			wantErr: true,
		},
		{
			name: "GetItem_missing_key",
			op: func() (any, error) {
				return adapter.SFNGetItem(ctx, map[string]any{
					"TableName": tableName,
					"Key":       map[string]any{},
				})
			},
			wantErr: true,
		},
		{
			name: "DeleteItem_missing_key",
			op: func() (any, error) {
				return adapter.SFNDeleteItem(ctx, map[string]any{
					"TableName": tableName,
					"Key":       map[string]any{},
				})
			},
			wantErr: true,
		},
		{
			name: "UpdateItem_missing_key",
			op: func() (any, error) {
				return adapter.SFNUpdateItem(ctx, map[string]any{
					"TableName": tableName,
					"Key":       map[string]any{},
				})
			},
			wantErr: true,
		},
		{
			name: "PutItem_attribute_value_marshal_error",
			op: func() (any, error) {
				return adapter.SFNPutItem(ctx, map[string]any{
					"TableName": "nonexistent",
					"Item":      map[string]any{"pk": map[string]any{"S": "k"}},
				})
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, callErr := tt.op()
			if tt.wantErr {
				require.Error(t, callErr)
			} else {
				require.NoError(t, callErr)
			}
		})
	}
}

func TestSetServiceIntegrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		set  func(b *stepfunctions.InMemoryBackend)
		name string
	}{
		{
			name: "SQS",
			set: func(b *stepfunctions.InMemoryBackend) {
				b.SetSQSIntegration(stepfunctions.NewSQSIntegration(sqs.NewInMemoryBackend()))
				b.SetSQSIntegration(nil)
			},
		},
		{
			name: "SNS",
			set: func(b *stepfunctions.InMemoryBackend) {
				b.SetSNSIntegration(stepfunctions.NewSNSIntegration(sns.NewInMemoryBackend()))
				b.SetSNSIntegration(nil)
			},
		},
		{
			name: "DynamoDB",
			set: func(b *stepfunctions.InMemoryBackend) {
				b.SetDynamoDBIntegration(
					stepfunctions.NewDynamoDBIntegration(dynamodb.NewInMemoryDB()),
				)
				b.SetDynamoDBIntegration(nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			tt.set(b)
		})
	}
}

// mockLambdaForBackend implements asl.LambdaInvoker for backend-level tests.
type mockLambdaForBackend struct {
	returnErr error
}

func (m *mockLambdaForBackend) InvokeFunction(
	_ context.Context, _, _ string, _ []byte,
) ([]byte, int, error) {
	if m.returnErr != nil {
		return nil, 500, m.returnErr
	}

	return []byte(`{"result": "ok"}`), 200, nil
}

func TestRecordTask_SucceededAndFailed(t *testing.T) {
	t.Parallel()

	const lambdaTaskDef = `{
"StartAt": "T",
"States": {
  "T": {
    "Type": "Task",
    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:myFn",
    "End": true
  }
}
}`

	tests := []struct {
		invoker        *mockLambdaForBackend
		name           string
		smName         string
		execName       string
		wantStatus     string
		wantEventTypes []string
	}{
		{
			name:           "TaskSucceeded",
			invoker:        &mockLambdaForBackend{},
			smName:         "task-success",
			execName:       "exec-success",
			wantStatus:     "SUCCEEDED",
			wantEventTypes: []string{"TaskScheduled", "TaskSucceeded"},
		},
		{
			name:           "TaskFailed",
			invoker:        &mockLambdaForBackend{returnErr: assert.AnError},
			smName:         "task-fail",
			execName:       "exec-fail",
			wantStatus:     "FAILED",
			wantEventTypes: []string{"TaskScheduled", "TaskFailed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			b.SetLambdaInvoker(tt.invoker)

			sm, err := b.CreateStateMachine(tt.smName, lambdaTaskDef, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, tt.execName, `{}`)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				desc, _ := b.DescribeExecution(exec.ExecutionArn)

				return desc != nil && desc.Status == tt.wantStatus
			}, 5*time.Second, 50*time.Millisecond)

			history, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 100, false)
			require.NoError(t, err)

			eventTypes := make([]string, 0, len(history))
			for _, ev := range history {
				eventTypes = append(eventTypes, ev.Type)
			}
			for _, wantType := range tt.wantEventTypes {
				assert.Contains(t, eventTypes, wantType)
			}
		})
	}
}
