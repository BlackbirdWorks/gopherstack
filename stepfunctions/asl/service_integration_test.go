package asl_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/stepfunctions/asl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errDynamoDBIntegrationNotConfigured is the sentinel used in DynamoDB integration error tests.
var errDynamoDBIntegrationNotConfigured = asl.ErrDynamoDBIntegrationNotConfigured

// --- Mock implementations ---

type mockSQS struct {
	returnErr               error
	capturedQueueURL        string
	capturedMessageBody     string
	capturedGroupID         string
	capturedDeduplicationID string
	returnMsgID             string
	returnMD5               string
	capturedDelaySeconds    int
}

func (m *mockSQS) SFNSendMessage(
	_ context.Context,
	queueURL, messageBody, groupID, deduplicationID string,
	delaySeconds int,
) (string, string, error) {
	m.capturedQueueURL = queueURL
	m.capturedMessageBody = messageBody
	m.capturedGroupID = groupID
	m.capturedDeduplicationID = deduplicationID
	m.capturedDelaySeconds = delaySeconds

	return m.returnMsgID, m.returnMD5, m.returnErr
}

type mockSNS struct {
	returnErr        error
	capturedTopicARN string
	capturedMessage  string
	capturedSubject  string
	returnMsgID      string
}

func (m *mockSNS) SFNPublish(_ context.Context, topicARN, message, subject string) (string, error) {
	m.capturedTopicARN = topicARN
	m.capturedMessage = message
	m.capturedSubject = subject

	return m.returnMsgID, m.returnErr
}

type mockDynamoDB struct {
	returnOutput any
	returnErr    error
	calledPut    bool
	calledGet    bool
	calledDelete bool
	calledUpdate bool
}

func (m *mockDynamoDB) SFNPutItem(_ context.Context, _ any) (any, error) {
	m.calledPut = true

	return m.returnOutput, m.returnErr
}

func (m *mockDynamoDB) SFNGetItem(_ context.Context, _ any) (any, error) {
	m.calledGet = true

	return m.returnOutput, m.returnErr
}

func (m *mockDynamoDB) SFNDeleteItem(_ context.Context, _ any) (any, error) {
	m.calledDelete = true

	return m.returnOutput, m.returnErr
}

func (m *mockDynamoDB) SFNUpdateItem(_ context.Context, _ any) (any, error) {
	m.calledUpdate = true

	return m.returnOutput, m.returnErr
}

// --- SQS tests ---

func TestExecutor_SQS_SendMessage(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Send",
		"States": {
			"Send": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage",
				"Parameters": {
					"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue",
					"MessageBody": "hello world"
				},
				"End": true
			}
		}
	}`

	mock := &mockSQS{returnMsgID: "msg-1", returnMD5: "abc123"}
	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetSQSIntegration(mock)

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)

	out, ok := result.Output.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "msg-1", out["MessageId"])
	assert.Equal(t, "abc123", out["MD5OfMessageBody"])

	assert.Equal(t, "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue", mock.capturedQueueURL)
	assert.Equal(t, "hello world", mock.capturedMessageBody)
}

func TestExecutor_SQS_SendMessage_WithFIFOFields(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Send",
		"States": {
			"Send": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage.sync",
				"Parameters": {
					"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123/myqueue.fifo",
					"MessageBody": "fifo msg",
					"MessageGroupId": "group1",
					"MessageDeduplicationId": "dedup1",
					"DelaySeconds": 5
				},
				"End": true
			}
		}
	}`

	mock := &mockSQS{returnMsgID: "msg-2", returnMD5: "def456"}
	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetSQSIntegration(mock)

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)

	assert.Equal(t, "group1", mock.capturedGroupID)
	assert.Equal(t, "dedup1", mock.capturedDeduplicationID)
	assert.Equal(t, 5, mock.capturedDelaySeconds)
}

func TestExecutor_SQS_NotConfigured(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Send",
		"States": {
			"Send": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:sendMessage",
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Contains(t, result.Cause, "SQS integration not configured")
}

func TestExecutor_SQS_UnsupportedAction(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Recv",
		"States": {
			"Recv": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sqs:receiveMessage",
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetSQSIntegration(&mockSQS{})

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Contains(t, result.Cause, "unsupported SQS action")
}

// --- SNS tests ---

func TestExecutor_SNS_Publish(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Publish",
		"States": {
			"Publish": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sns:publish",
				"Parameters": {
					"TopicArn": "arn:aws:sns:us-east-1:123456789012:MyTopic",
					"Message": "test message",
					"Subject": "test subject"
				},
				"End": true
			}
		}
	}`

	mock := &mockSNS{returnMsgID: "sns-msg-1"}
	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetSNSIntegration(mock)

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)

	out, ok := result.Output.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "sns-msg-1", out["MessageId"])

	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:MyTopic", mock.capturedTopicARN)
	assert.Equal(t, "test message", mock.capturedMessage)
	assert.Equal(t, "test subject", mock.capturedSubject)
}

func TestExecutor_SNS_NotConfigured(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Publish",
		"States": {
			"Publish": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sns:publish",
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Contains(t, result.Cause, "SNS integration not configured")
}

func TestExecutor_SNS_UnsupportedAction(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Sub",
		"States": {
			"Sub": {
				"Type": "Task",
				"Resource": "arn:aws:states:::sns:subscribe",
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetSNSIntegration(&mockSNS{})

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Contains(t, result.Cause, "unsupported SNS action")
}

// --- DynamoDB tests ---

func TestExecutor_DynamoDB_PutItem(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Put",
		"States": {
			"Put": {
				"Type": "Task",
				"Resource": "arn:aws:states:::dynamodb:putItem",
				"Parameters": {
					"TableName": "MyTable",
					"Item": {"pk": {"S": "val"}}
				},
				"End": true
			}
		}
	}`

	mock := &mockDynamoDB{returnOutput: map[string]any{}}
	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetDynamoDBIntegration(mock)

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)
	assert.True(t, mock.calledPut)
}

func TestExecutor_DynamoDB_GetItem(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Get",
		"States": {
			"Get": {
				"Type": "Task",
				"Resource": "arn:aws:states:::dynamodb:getItem",
				"Parameters": {
					"TableName": "MyTable",
					"Key": {"pk": {"S": "val"}}
				},
				"End": true
			}
		}
	}`

	mock := &mockDynamoDB{returnOutput: map[string]any{"Item": map[string]any{}}}
	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetDynamoDBIntegration(mock)

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)
	assert.True(t, mock.calledGet)
}

func TestExecutor_DynamoDB_DeleteItem(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Delete",
		"States": {
			"Delete": {
				"Type": "Task",
				"Resource": "arn:aws:states:::dynamodb:deleteItem",
				"Parameters": {
					"TableName": "MyTable",
					"Key": {"pk": {"S": "val"}}
				},
				"End": true
			}
		}
	}`

	mock := &mockDynamoDB{returnOutput: map[string]any{}}
	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetDynamoDBIntegration(mock)

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)
	assert.True(t, mock.calledDelete)
}

func TestExecutor_DynamoDB_UpdateItem(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Update",
		"States": {
			"Update": {
				"Type": "Task",
				"Resource": "arn:aws:states:::dynamodb:updateItem",
				"Parameters": {
					"TableName": "MyTable",
					"Key": {"pk": {"S": "val"}}
				},
				"End": true
			}
		}
	}`

	mock := &mockDynamoDB{returnOutput: map[string]any{}}
	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetDynamoDBIntegration(mock)

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)
	assert.True(t, mock.calledUpdate)
}

func TestExecutor_DynamoDB_NotConfigured(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Put",
		"States": {
			"Put": {
				"Type": "Task",
				"Resource": "arn:aws:states:::dynamodb:putItem",
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Contains(t, result.Cause, "DynamoDB integration not configured")
}

func TestExecutor_DynamoDB_UnsupportedAction(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Scan",
		"States": {
			"Scan": {
				"Type": "Task",
				"Resource": "arn:aws:states:::dynamodb:scan",
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetDynamoDBIntegration(&mockDynamoDB{})

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Contains(t, result.Cause, "unsupported DynamoDB action")
}

func TestExecutor_DynamoDB_IntegrationError(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Put",
		"States": {
			"Put": {
				"Type": "Task",
				"Resource": "arn:aws:states:::dynamodb:putItem",
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	exec.SetDynamoDBIntegration(&mockDynamoDB{returnErr: errDynamoDBIntegrationNotConfigured})

	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Contains(t, result.Cause, errDynamoDBIntegrationNotConfigured.Error())
}
