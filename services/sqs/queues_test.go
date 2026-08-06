package sqs_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStandardQueue_CreateDeleteCycle(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	// Create standard queue
	out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "std-queue", Endpoint: "localhost"})
	require.NoError(t, err)
	assert.Contains(t, out.QueueURL, "std-queue")

	// Idempotent create with same attrs returns same URL
	out2, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "std-queue", Endpoint: "localhost"})
	require.NoError(t, err)
	assert.Equal(t, out.QueueURL, out2.QueueURL)

	// Different attrs → QueueAlreadyExists
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "std-queue",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueAlreadyExists)

	// Delete
	require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: out.QueueURL}))

	// After delete, queue not found
	_, err = b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "std-queue"})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestQueueName_Validation(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		name    string
		wantErr bool
	}{
		{"valid-queue", false},
		{"valid_queue_123", false},
		{"a", false},
		{strings.Repeat("a", 80), false},
		{"", true},
		{strings.Repeat("a", 81), true},
		{"invalid queue", true},
		{"invalid.queue", true},
		{"invalid!queue", true},
	}
	for _, tc := range cases {
		t.Run(tc.name+"_valid="+strconv.FormatBool(!tc.wantErr), func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: tc.name, Endpoint: "localhost"})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestListQueues_Prefix(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	b2createQueue(t, b, "prefix-alpha")
	b2createQueue(t, b, "prefix-beta")
	b2createQueue(t, b, "other-gamma")

	out, err := b.ListQueues(&sqs.ListQueuesInput{QueueNamePrefix: "prefix-"})
	require.NoError(t, err)
	assert.Len(t, out.QueueURLs, 2)
	for _, u := range out.QueueURLs {
		assert.Contains(t, u, "prefix-")
	}
}

func TestListQueues_All(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	b2createQueue(t, b, "list-a")
	b2createQueue(t, b, "list-b")
	b2createQueue(t, b, "list-c")

	out, err := b.ListQueues(&sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.QueueURLs), 3)
}

func TestListQueues_Pagination(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	for i := range 5 {
		b2createQueue(t, b, fmt.Sprintf("page-queue-%02d", i))
	}

	// First page
	out1, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 3})
	require.NoError(t, err)
	assert.Len(t, out1.QueueURLs, 3)
	assert.NotEmpty(t, out1.NextToken)

	// Second page
	out2, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 3, NextToken: out1.NextToken})
	require.NoError(t, err)
	assert.NotEmpty(t, out2.QueueURLs)

	// No overlap
	all := make([]string, 0, len(out1.QueueURLs)+len(out2.QueueURLs))
	all = append(all, out1.QueueURLs...)
	all = append(all, out2.QueueURLs...)
	seen := map[string]bool{}
	for _, u := range all {
		assert.False(t, seen[u], "duplicate URL: %s", u)
		seen[u] = true
	}
}

func TestGetQueueURL_FoundAndNotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "url-lookup")
	out, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "url-lookup"})
	require.NoError(t, err)
	assert.Equal(t, qURL, out.QueueURL)

	_, err = b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "nonexistent"})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestDefaultAttributes_Standard(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "default-attrs")
	attrs := b2getAttrs(t, b, qURL, "All")

	assert.Equal(t, "30", attrs["VisibilityTimeout"])
	assert.Equal(t, "262144", attrs["MaximumMessageSize"])
	assert.Equal(t, "345600", attrs["MessageRetentionPeriod"])
	assert.Equal(t, "0", attrs["DelaySeconds"])
	assert.Equal(t, "0", attrs["ReceiveMessageWaitTimeSeconds"])
	assert.NotEmpty(t, attrs["QueueArn"])
	assert.Contains(t, attrs["QueueArn"], "default-attrs")
	assert.NotEmpty(t, attrs["CreatedTimestamp"])
	assert.NotEmpty(t, attrs["LastModifiedTimestamp"])
	assert.Equal(t, "true", attrs["SqsManagedSseEnabled"])
}

func TestPurgeQueue_RemovesAllMessages(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "purge-all")
	for i := range 5 {
		b2send(t, b, qURL, fmt.Sprintf("msg-%d", i))
	}

	require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL}))

	msgs := b2receive(t, b, qURL, 10)
	assert.Empty(t, msgs)
}

func TestPurgeQueue_60SecondCooldown(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "purge-cooldown")
	b2send(t, b, qURL, "msg")

	require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL}))

	// Second purge within 60s → PurgeQueueInProgress
	err := b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL})
	require.ErrorIs(t, err, sqs.ErrPurgeQueueInProgress)
}

func TestPurgeQueue_NotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	err := b.PurgeQueue(&sqs.PurgeQueueInput{
		QueueURL: "http://localhost/000000000000/nonexistent",
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestQueueARN_Format(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "arn-test")
	attrs := b2getAttrs(t, b, qURL, "QueueArn")

	arn := attrs["QueueArn"]
	assert.NotEmpty(t, arn)
	assert.True(t, strings.HasPrefix(arn, "arn:aws:sqs:"), "ARN should start with arn:aws:sqs: got: %s", arn)
	assert.Contains(t, arn, "arn-test")
}

// TestPurgeQueue_ClearsAllMessages verifies PurgeQueue clears all messages.
func TestPurgeQueue_ClearsAllMessages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "purge-q")

	for i := range 5 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: fmt.Sprintf("msg-%d", i),
		})
		require.NoError(t, err)
	}

	err := b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL})
	require.NoError(t, err)

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages, "PurgeQueue must empty the queue")

	attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"All"},
	})
	require.NoError(t, err)
	assert.Equal(t, "0", attrs.Attributes["ApproximateNumberOfMessages"],
		"count must be 0 after purge")
}

func TestCreateQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queueName string
		isFIFO    bool
	}{
		{name: "standard queue", queueName: "my-queue", isFIFO: false},
		{name: "fifo queue", queueName: "my-queue.fifo", isFIFO: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: tc.queueName,
				Endpoint:  testEndpoint,
			})

			require.NoError(t, err)
			assert.Equal(t, queueURL(tc.queueName), out.QueueURL)

			queues := b.ListAll()
			require.Len(t, queues, 1)
			assert.Equal(t, tc.isFIFO, queues[0].IsFIFO)
		})
	}
}

func TestCreateQueueDuplicate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestQueue(t, b, "my-queue")

	tests := []struct {
		wantErr error
		attrs   map[string]string
		name    string
	}{
		{
			name:    "same_attrs_idempotent",
			attrs:   nil,
			wantErr: nil,
		},
		{
			name:    "different_visibility_timeout",
			attrs:   map[string]string{"VisibilityTimeout": "60"},
			wantErr: sqs.ErrQueueAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "my-queue",
				Attributes: tt.attrs,
				Endpoint:   testEndpoint,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, out.QueueURL, "my-queue")
		})
	}
}

func TestCreateQueueNameValidation(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	tests := []struct {
		name    string
		qName   string
		wantErr bool
	}{
		{name: "valid_name", qName: "my-queue-1", wantErr: false},
		{name: "empty_name", qName: "", wantErr: true},
		{name: "too_long", qName: strings.Repeat("a", 81), wantErr: true},
		{name: "invalid_chars", qName: "my queue!", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: tt.qName,
				Endpoint:  testEndpoint,
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteQueue(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	err := b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL})
	require.NoError(t, err)

	assert.Empty(t, b.ListAll())
}

func TestDeleteQueueNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: queueURL("nonexistent")})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

// TestQueueDeletedRecently verifies AWS's real "you must wait 60 seconds
// after deleting a queue before you can create another with the same name"
// rule (aws-sdk-go-v2/service/sqs/types.QueueDeletedRecently), which this
// backend did not enforce at all prior to this change.
func TestQueueDeletedRecently(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "recreate-me")

	require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL}))

	// Immediate recreation with the same name must fail.
	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "recreate-me", Endpoint: testEndpoint})
	require.ErrorIs(t, err, sqs.ErrQueueDeletedRecently)

	// A different name is unaffected.
	_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "recreate-me-2", Endpoint: testEndpoint})
	require.NoError(t, err)

	// A different region is unaffected (same name, different region key).
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "recreate-me", Endpoint: testEndpoint, Region: "us-west-2",
	})
	require.NoError(t, err)

	// Once the 60-second window elapses, recreation succeeds. Simulate the
	// elapsed window by driving the janitor's prune pass with a future time
	// (RunJanitorOnceForTest) rather than sleeping in the test.
	b.RunJanitorOnceForTest(time.Now().Add(2 * time.Minute))

	out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "recreate-me", Endpoint: testEndpoint})
	require.NoError(t, err)
	assert.Contains(t, out.QueueURL, "recreate-me")
}

func TestListQueues(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestQueue(t, b, "alpha-queue")
	createTestQueue(t, b, "beta-queue")
	createTestQueue(t, b, "alpha-other")

	t.Run("no prefix", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListQueues(&sqs.ListQueuesInput{})
		require.NoError(t, err)
		assert.Len(t, out.QueueURLs, 3)
	})

	t.Run("with prefix", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListQueues(&sqs.ListQueuesInput{QueueNamePrefix: "alpha"})
		require.NoError(t, err)
		assert.Len(t, out.QueueURLs, 2)
	})

	t.Run("pagination first page", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 2})
		require.NoError(t, err)
		assert.Len(t, out.QueueURLs, 2)
		assert.NotEmpty(t, out.NextToken)
	})

	t.Run("pagination second page", func(t *testing.T) {
		t.Parallel()

		first, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 2})
		require.NoError(t, err)
		require.NotEmpty(t, first.NextToken)

		second, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 2, NextToken: first.NextToken})
		require.NoError(t, err)
		assert.Len(t, second.QueueURLs, 1)
		assert.Empty(t, second.NextToken)
	})

	t.Run("all pages combined equal full list", func(t *testing.T) {
		t.Parallel()

		var all []string
		var token string

		for {
			out, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 2, NextToken: token})
			require.NoError(t, err)
			all = append(all, out.QueueURLs...)
			token = out.NextToken

			if token == "" {
				break
			}
		}

		assert.Len(t, all, 3)
	})
}

func TestGetQueueURL(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestQueue(t, b, "my-queue")

	out, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "my-queue"})
	require.NoError(t, err)
	assert.Equal(t, queueURL("my-queue"), out.QueueURL)
}

func TestGetQueueURLNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "nonexistent"})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestPurgeQueue(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	for i := range 5 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: fmt.Sprintf("msg-%d", i),
		})
		require.NoError(t, err)
	}

	err := b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL})
	require.NoError(t, err)

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 10, WaitTimeSeconds: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages)
}

func TestQueueNameAttribute(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "test-queue")

	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"QueueArn"},
	})
	require.NoError(t, err)

	arn := out.Attributes["QueueArn"]
	assert.Contains(t, arn, "test-queue", "ARN should contain queue name")
}

func TestQueueNameFromInputEmpty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Passing an empty URL to SendMessage triggers queueNameFromInput("") -> ""
	// which means the queue won't be found.
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    "",
		MessageBody: "hello",
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

// TestDeleteQueueClosesNotifyChannel verifies that deleting a queue wakes any
// goroutine blocked on long-polling and that the goroutine receives an error.
func TestDeleteQueueClosesNotifyChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "long_poll_receiver_wakes_when_queue_deleted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newBackend(t)
				qURL := createTestQueue(t, b, "close-notify-queue")

				errCh := make(chan error, 1)
				go func() {
					_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
						QueueURL:            qURL,
						MaxNumberOfMessages: 1,
						WaitTimeSeconds:     10,
					})
					errCh <- err
				}()

				// Wait for the goroutine to enter the long-poll select.
				synctest.Wait()

				require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL}))

				select {
				case err := <-errCh:
					// The closed notify channel should cause the goroutine to wake up and
					// return ErrQueueNotFound from the next receiveOnce call.
					require.ErrorIs(t, err, sqs.ErrQueueNotFound, tt.name)
				case <-time.After(2 * time.Second):
					require.FailNow(t, "goroutine did not wake up after queue deletion")
				}
			})
		})
	}
}

// TestCreateQueue_NameValidation asserts InvalidParameterValue for bad queue names.
func TestCreateQueue_NameValidation(t *testing.T) {
	t.Parallel()

	const wantType = "com.amazonaws.sqs#InvalidParameterValue"

	tests := []struct {
		name      string
		queueName string
		wantType  string
		wantCode  int
	}{
		{name: "valid_name", queueName: "my-queue", wantCode: http.StatusOK},
		{name: "empty_name", queueName: "", wantCode: http.StatusBadRequest, wantType: wantType},
		{
			name:      "too_long_81_chars",
			queueName: strings.Repeat("a", 81),
			wantCode:  http.StatusBadRequest,
			wantType:  wantType,
		},
		{name: "invalid_chars_space", queueName: "bad queue", wantCode: http.StatusBadRequest, wantType: wantType},
		{name: "valid_80_chars", queueName: strings.Repeat("a", 80), wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateQueue", map[string]any{"QueueName": tt.queueName})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp jsonErr
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp.Type)
			}
		})
	}
}
