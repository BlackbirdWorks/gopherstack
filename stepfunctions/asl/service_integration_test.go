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

// --- SQS helpers ---

type sqsCaptureAssertion struct {
	wantCapturedQueueURL        string
	wantCapturedMessageBody     string
	wantCapturedGroupID         string
	wantCapturedDeduplicationID string
	wantCapturedDelaySeconds    int
}

func assertSQSCaptures(t *testing.T, mock *mockSQS, tt sqsCaptureAssertion) {
	t.Helper()

	if tt.wantCapturedQueueURL != "" {
		assert.Equal(t, tt.wantCapturedQueueURL, mock.capturedQueueURL)
	}

	if tt.wantCapturedMessageBody != "" {
		assert.Equal(t, tt.wantCapturedMessageBody, mock.capturedMessageBody)
	}

	if tt.wantCapturedGroupID != "" {
		assert.Equal(t, tt.wantCapturedGroupID, mock.capturedGroupID)
	}

	if tt.wantCapturedDeduplicationID != "" {
		assert.Equal(t, tt.wantCapturedDeduplicationID, mock.capturedDeduplicationID)
	}

	if tt.wantCapturedDelaySeconds != 0 {
		assert.Equal(t, tt.wantCapturedDelaySeconds, mock.capturedDelaySeconds)
	}
}

// --- SQS tests ---

func TestExecutor_SQS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCauseContains           string
		wantCapturedDeduplicationID string
		wantOutputMsgID             string
		mockReturnMsgID             string
		mockReturnMD5               string
		wantError                   string
		wantOutputMD5               string
		name                        string
		def                         string
		wantCapturedQueueURL        string
		wantCapturedMessageBody     string
		wantCapturedGroupID         string
		wantCapturedDelaySeconds    int
		setMock                     bool
	}{
		{
			name: "send_message",
			def: `{
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
			}`,
			setMock:                 true,
			mockReturnMsgID:         "msg-1",
			mockReturnMD5:           "abc123",
			wantOutputMsgID:         "msg-1",
			wantOutputMD5:           "abc123",
			wantCapturedQueueURL:    "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue",
			wantCapturedMessageBody: "hello world",
		},
		{
			name: "send_message_with_fifo_fields",
			def: `{
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
			}`,
			setMock:                     true,
			mockReturnMsgID:             "msg-2",
			mockReturnMD5:               "def456",
			wantCapturedGroupID:         "group1",
			wantCapturedDeduplicationID: "dedup1",
			wantCapturedDelaySeconds:    5,
		},
		{
			name: "not_configured",
			def: `{
				"StartAt": "Send",
				"States": {
					"Send": {
						"Type": "Task",
						"Resource": "arn:aws:states:::sqs:sendMessage",
						"End": true
					}
				}
			}`,
			setMock:           false,
			wantError:         "TaskFailed",
			wantCauseContains: "SQS integration not configured",
		},
		{
			name: "unsupported_action",
			def: `{
				"StartAt": "Recv",
				"States": {
					"Recv": {
						"Type": "Task",
						"Resource": "arn:aws:states:::sqs:receiveMessage",
						"End": true
					}
				}
			}`,
			setMock:           true,
			wantError:         "TaskFailed",
			wantCauseContains: "unsupported SQS action",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)

			exec := asl.NewExecutor(sm, nil, nil)

			var mock *mockSQS
			if tt.setMock {
				mock = &mockSQS{returnMsgID: tt.mockReturnMsgID, returnMD5: tt.mockReturnMD5}
				exec.SetSQSIntegration(mock)
			}

			result, err := exec.Execute(t.Context(), "test-exec", `{}`)
			require.NoError(t, err)

			assert.Equal(t, tt.wantError, result.Error)

			if tt.wantCauseContains != "" {
				assert.Contains(t, result.Cause, tt.wantCauseContains)
			}

			if tt.wantOutputMsgID != "" {
				out, ok := result.Output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantOutputMsgID, out["MessageId"])
				assert.Equal(t, tt.wantOutputMD5, out["MD5OfMessageBody"])
			}

			if mock != nil {
				assertSQSCaptures(t, mock, sqsCaptureAssertion{
					wantCapturedQueueURL:        tt.wantCapturedQueueURL,
					wantCapturedMessageBody:     tt.wantCapturedMessageBody,
					wantCapturedGroupID:         tt.wantCapturedGroupID,
					wantCapturedDeduplicationID: tt.wantCapturedDeduplicationID,
					wantCapturedDelaySeconds:    tt.wantCapturedDelaySeconds,
				})
			}
		})
	}
}

// --- SNS tests ---

func TestExecutor_SNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		def                  string
		mockReturnMsgID      string
		wantError            string
		wantCauseContains    string
		wantOutputMsgID      string
		wantCapturedTopicARN string
		wantCapturedMessage  string
		wantCapturedSubject  string
		setMock              bool
	}{
		{
			name: "publish",
			def: `{
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
			}`,
			setMock:              true,
			mockReturnMsgID:      "sns-msg-1",
			wantOutputMsgID:      "sns-msg-1",
			wantCapturedTopicARN: "arn:aws:sns:us-east-1:123456789012:MyTopic",
			wantCapturedMessage:  "test message",
			wantCapturedSubject:  "test subject",
		},
		{
			name: "not_configured",
			def: `{
				"StartAt": "Publish",
				"States": {
					"Publish": {
						"Type": "Task",
						"Resource": "arn:aws:states:::sns:publish",
						"End": true
					}
				}
			}`,
			setMock:           false,
			wantError:         "TaskFailed",
			wantCauseContains: "SNS integration not configured",
		},
		{
			name: "unsupported_action",
			def: `{
				"StartAt": "Sub",
				"States": {
					"Sub": {
						"Type": "Task",
						"Resource": "arn:aws:states:::sns:subscribe",
						"End": true
					}
				}
			}`,
			setMock:           true,
			wantError:         "TaskFailed",
			wantCauseContains: "unsupported SNS action",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)

			exec := asl.NewExecutor(sm, nil, nil)

			var mock *mockSNS
			if tt.setMock {
				mock = &mockSNS{returnMsgID: tt.mockReturnMsgID}
				exec.SetSNSIntegration(mock)
			}

			result, err := exec.Execute(t.Context(), "test-exec", `{}`)
			require.NoError(t, err)

			assert.Equal(t, tt.wantError, result.Error)

			if tt.wantCauseContains != "" {
				assert.Contains(t, result.Cause, tt.wantCauseContains)
			}

			if tt.wantOutputMsgID != "" {
				out, ok := result.Output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantOutputMsgID, out["MessageId"])
			}

			if mock != nil {
				if tt.wantCapturedTopicARN != "" {
					assert.Equal(t, tt.wantCapturedTopicARN, mock.capturedTopicARN)
				}
				if tt.wantCapturedMessage != "" {
					assert.Equal(t, tt.wantCapturedMessage, mock.capturedMessage)
				}
				if tt.wantCapturedSubject != "" {
					assert.Equal(t, tt.wantCapturedSubject, mock.capturedSubject)
				}
			}
		})
	}
}

// --- aws-sdk integration pattern tests ---

func TestExecutor_AwsSDKIntegrationPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mock          any
		name          string
		def           string
		setOnExecutor func(exec *asl.Executor, mock any)
		wantError     string
	}{
		{
			name: "sqs_aws-sdk_prefix",
			def: `{
				"StartAt": "Send",
				"States": {
					"Send": {
						"Type": "Task",
						"Resource": "arn:aws:states:::aws-sdk:sqs:sendMessage",
						"Parameters": {
							"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123/myqueue",
							"MessageBody": "hello"
						},
						"End": true
					}
				}
			}`,
			mock: &mockSQS{returnMsgID: "m1", returnMD5: "md5"},
			setOnExecutor: func(exec *asl.Executor, m any) {
				exec.SetSQSIntegration(m.(*mockSQS))
			},
		},
		{
			name: "sns_aws-sdk_prefix",
			def: `{
				"StartAt": "Pub",
				"States": {
					"Pub": {
						"Type": "Task",
						"Resource": "arn:aws:states:::aws-sdk:sns:publish",
						"Parameters": {
							"TopicArn": "arn:aws:sns:us-east-1:123:MyTopic",
							"Message": "hello"
						},
						"End": true
					}
				}
			}`,
			mock: &mockSNS{returnMsgID: "s1"},
			setOnExecutor: func(exec *asl.Executor, m any) {
				exec.SetSNSIntegration(m.(*mockSNS))
			},
		},
		{
			name: "dynamodb_aws-sdk_prefix",
			def: `{
				"StartAt": "Put",
				"States": {
					"Put": {
						"Type": "Task",
						"Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem",
						"End": true
					}
				}
			}`,
			mock: &mockDynamoDB{returnOutput: map[string]any{}},
			setOnExecutor: func(exec *asl.Executor, m any) {
				exec.SetDynamoDBIntegration(m.(*mockDynamoDB))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)

			exec := asl.NewExecutor(sm, nil, nil)
			tt.setOnExecutor(exec, tt.mock)

			result, err := exec.Execute(t.Context(), "exec-1", `{}`)
			require.NoError(t, err)
			assert.Equal(t, tt.wantError, result.Error, "execution should succeed via aws-sdk prefix")
		})
	}
}

func TestExecutor_DynamoDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		def               string
		mock              *mockDynamoDB
		wantError         string
		wantCauseContains string
		wantCalledPut     bool
		wantCalledGet     bool
		wantCalledDelete  bool
		wantCalledUpdate  bool
	}{
		{
			name: "put_item",
			def: `{
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
			}`,
			mock:          &mockDynamoDB{returnOutput: map[string]any{}},
			wantCalledPut: true,
		},
		{
			name: "get_item",
			def: `{
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
			}`,
			mock:          &mockDynamoDB{returnOutput: map[string]any{"Item": map[string]any{}}},
			wantCalledGet: true,
		},
		{
			name: "delete_item",
			def: `{
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
			}`,
			mock:             &mockDynamoDB{returnOutput: map[string]any{}},
			wantCalledDelete: true,
		},
		{
			name: "update_item",
			def: `{
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
			}`,
			mock:             &mockDynamoDB{returnOutput: map[string]any{}},
			wantCalledUpdate: true,
		},
		{
			name: "not_configured",
			def: `{
				"StartAt": "Put",
				"States": {
					"Put": {
						"Type": "Task",
						"Resource": "arn:aws:states:::dynamodb:putItem",
						"End": true
					}
				}
			}`,
			mock:              nil,
			wantError:         "TaskFailed",
			wantCauseContains: "DynamoDB integration not configured",
		},
		{
			name: "unsupported_action",
			def: `{
				"StartAt": "Scan",
				"States": {
					"Scan": {
						"Type": "Task",
						"Resource": "arn:aws:states:::dynamodb:scan",
						"End": true
					}
				}
			}`,
			mock:              &mockDynamoDB{},
			wantError:         "TaskFailed",
			wantCauseContains: "unsupported DynamoDB action",
		},
		{
			name: "integration_error",
			def: `{
				"StartAt": "Put",
				"States": {
					"Put": {
						"Type": "Task",
						"Resource": "arn:aws:states:::dynamodb:putItem",
						"End": true
					}
				}
			}`,
			mock:              &mockDynamoDB{returnErr: errDynamoDBIntegrationNotConfigured},
			wantError:         "TaskFailed",
			wantCauseContains: errDynamoDBIntegrationNotConfigured.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)

			exec := asl.NewExecutor(sm, nil, nil)
			if tt.mock != nil {
				exec.SetDynamoDBIntegration(tt.mock)
			}

			result, err := exec.Execute(t.Context(), "test-exec", `{}`)
			require.NoError(t, err)

			assert.Equal(t, tt.wantError, result.Error)

			if tt.wantCauseContains != "" {
				assert.Contains(t, result.Cause, tt.wantCauseContains)
			}

			if tt.mock != nil && tt.wantError == "" {
				assert.Equal(t, tt.wantCalledPut, tt.mock.calledPut)
				assert.Equal(t, tt.wantCalledGet, tt.mock.calledGet)
				assert.Equal(t, tt.wantCalledDelete, tt.mock.calledDelete)
				assert.Equal(t, tt.wantCalledUpdate, tt.mock.calledUpdate)
			}
		})
	}
}
