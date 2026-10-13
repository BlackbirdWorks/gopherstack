package sqs_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

type handleEnv struct {
	client *sqssdk.Client
	url    string
	other  string
}

func (e *handleEnv) send(t *testing.T, url string) {
	t.Helper()

	in := &sqssdk.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String("body")}
	if strings.HasSuffix(url, ".fifo") {
		in.MessageGroupId = aws.String("g")
		in.MessageDeduplicationId = aws.String("d")
	}

	_, err := e.client.SendMessage(t.Context(), in)
	require.NoError(t, err)
}

func (e *handleEnv) receive(t *testing.T, url string) string {
	t.Helper()

	out, err := e.client.ReceiveMessage(t.Context(), &sqssdk.ReceiveMessageInput{
		QueueUrl: aws.String(url), MaxNumberOfMessages: 1, VisibilityTimeout: 60,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	return aws.ToString(out.Messages[0].ReceiptHandle)
}

func (e *handleEnv) release(t *testing.T, url, handle string) {
	t.Helper()

	_, err := e.client.ChangeMessageVisibility(t.Context(), &sqssdk.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(url), ReceiptHandle: aws.String(handle), VisibilityTimeout: 0,
	})
	require.NoError(t, err)
}

func (e *handleEnv) remove(ctx context.Context, url, handle string) error {
	_, err := e.client.DeleteMessage(ctx, &sqssdk.DeleteMessageInput{
		QueueUrl: aws.String(url), ReceiptHandle: aws.String(handle),
	})

	return err
}

func (e *handleEnv) changeVisibility(ctx context.Context, url, handle string) error {
	_, err := e.client.ChangeMessageVisibility(ctx, &sqssdk.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(url), ReceiptHandle: aws.String(handle), VisibilityTimeout: 30,
	})

	return err
}

func (e *handleEnv) counts(t *testing.T, url string) (int, int) {
	t.Helper()

	out, err := e.client.GetQueueAttributes(t.Context(), &sqssdk.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	require.NoError(t, err)

	visible := out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)]
	inflight := out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]

	return atoiOrFail(t, visible), atoiOrFail(t, inflight)
}

func atoiOrFail(t *testing.T, s string) int {
	t.Helper()

	n := 0
	for _, c := range s {
		require.True(t, c >= '0' && c <= '9', s)
		n = n*10 + int(c-'0')
	}

	return n
}

func newHandleEnv(t *testing.T, fifo bool) *handleEnv {
	t.Helper()

	client, _ := newSQSTestServer(t)
	suffix := ""
	attrs := map[string]string{}

	if fifo {
		suffix = ".fifo"
		attrs["FifoQueue"] = "true"
	}

	mk := func(name string) string {
		out, err := client.CreateQueue(t.Context(), &sqssdk.CreateQueueInput{
			QueueName: aws.String(name + suffix), Attributes: attrs,
		})
		require.NoError(t, err)

		return aws.ToString(out.QueueUrl)
	}

	return &handleEnv{client: client, url: mk("rh-main"), other: mk("rh-other")}
}

func apiCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

func flipLast(s string) string {
	last := s[len(s)-1]
	repl := byte('A')

	if last == 'A' {
		repl = 'B'
	}

	return s[:len(s)-1] + string(repl)
}

func staleDeleteKeepsMessage(t *testing.T, e *handleEnv) error {
	t.Helper()

	e.send(t, e.url)
	first := e.receive(t, e.url)
	e.release(t, e.url, first)
	latest := e.receive(t, e.url)

	if err := e.remove(t.Context(), e.url, first); err != nil {
		return err
	}

	visible, inflight := e.counts(t, e.url)
	assert.Zero(t, visible)
	assert.Equal(t, 1, inflight, "stale handle must not delete the message")

	require.NoError(t, e.remove(t.Context(), e.url, latest))

	visible, inflight = e.counts(t, e.url)
	assert.Zero(t, visible)
	assert.Zero(t, inflight)

	return nil
}

func TestReceiptHandle_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act      func(t *testing.T, e *handleEnv) error
		name     string
		wantCode string
		fifo     bool
	}{
		{
			name: "delete garbage",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				return e.remove(t.Context(), e.url, "not-a-handle")
			},
			wantCode: "ReceiptHandleIsInvalid",
		},
		{
			name: "change visibility garbage",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				return e.changeVisibility(t.Context(), e.url, "not-a-handle")
			},
			wantCode: "ReceiptHandleIsInvalid",
		},
		{
			name: "delete tampered",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				e.send(t, e.url)

				return e.remove(t.Context(), e.url, flipLast(e.receive(t, e.url)))
			},
			wantCode: "ReceiptHandleIsInvalid",
		},
		{
			name: "change visibility tampered",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				e.send(t, e.url)

				return e.changeVisibility(t.Context(), e.url, flipLast(e.receive(t, e.url)))
			},
			wantCode: "ReceiptHandleIsInvalid",
		},
		{
			name: "delete on other queue",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				e.send(t, e.url)
				e.send(t, e.other)
				e.receive(t, e.other)

				return e.remove(t.Context(), e.other, e.receive(t, e.url))
			},
			wantCode: "ReceiptHandleIsInvalid",
		},
		{
			name: "stale change visibility",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				e.send(t, e.url)
				first := e.receive(t, e.url)
				e.release(t, e.url, first)
				e.receive(t, e.url)

				return e.changeVisibility(t.Context(), e.url, first)
			},
			wantCode: "ReceiptHandleIsInvalid",
		},
		{
			name: "change visibility after delete",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				e.send(t, e.url)
				h := e.receive(t, e.url)
				require.NoError(t, e.remove(t.Context(), e.url, h))

				return e.changeVisibility(t.Context(), e.url, h)
			},
			wantCode: "MessageNotInflight",
		},
		{
			name: "stale delete standard",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				return staleDeleteKeepsMessage(t, e)
			},
		},
		{
			name: "stale delete fifo",
			fifo: true,
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				return staleDeleteKeepsMessage(t, e)
			},
		},
		{
			name: "current delete",
			act: func(t *testing.T, e *handleEnv) error {
				t.Helper()

				e.send(t, e.url)

				return e.remove(t.Context(), e.url, e.receive(t, e.url))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.act(t, newHandleEnv(t, tc.fifo))
			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.True(t, strings.HasSuffix(apiCode(t, err), tc.wantCode), apiCode(t, err))

			var invalid *sqstypes.ReceiptHandleIsInvalid
			var notInflight *sqstypes.MessageNotInflight

			switch tc.wantCode {
			case "ReceiptHandleIsInvalid":
				require.ErrorAs(t, err, &invalid)
			case "MessageNotInflight":
				require.ErrorAs(t, err, &notInflight)
			}
		})
	}
}

func TestReceiptHandle_BatchErrorMessages_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(t *testing.T, e *handleEnv) (string, string)
		name     string
		wantCode string
	}{
		{
			name: "delete batch garbage",
			call: func(t *testing.T, e *handleEnv) (string, string) {
				t.Helper()

				out, err := e.client.DeleteMessageBatch(t.Context(), &sqssdk.DeleteMessageBatchInput{
					QueueUrl: aws.String(e.url),
					Entries: []sqstypes.DeleteMessageBatchRequestEntry{
						{Id: aws.String("a"), ReceiptHandle: aws.String("x")},
					},
				})
				require.NoError(t, err)
				require.Len(t, out.Failed, 1)

				return aws.ToString(out.Failed[0].Code), aws.ToString(out.Failed[0].Message)
			},
			wantCode: "ReceiptHandleIsInvalid",
		},
		{
			name: "change visibility batch garbage",
			call: func(t *testing.T, e *handleEnv) (string, string) {
				t.Helper()

				out, err := e.client.ChangeMessageVisibilityBatch(
					t.Context(),
					&sqssdk.ChangeMessageVisibilityBatchInput{
						QueueUrl: aws.String(e.url),
						Entries: []sqstypes.ChangeMessageVisibilityBatchRequestEntry{
							{Id: aws.String("a"), ReceiptHandle: aws.String("x"), VisibilityTimeout: 5},
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, out.Failed, 1)

				return aws.ToString(out.Failed[0].Code), aws.ToString(out.Failed[0].Message)
			},
			wantCode: "ReceiptHandleIsInvalid",
		},
		{
			name: "send batch invalid",
			call: func(t *testing.T, e *handleEnv) (string, string) {
				t.Helper()

				out, err := e.client.SendMessageBatch(t.Context(), &sqssdk.SendMessageBatchInput{
					QueueUrl: aws.String(e.url),
					Entries: []sqstypes.SendMessageBatchRequestEntry{
						{Id: aws.String("a"), MessageBody: aws.String("bad\x01"), DelaySeconds: 0},
					},
				})
				require.NoError(t, err)
				require.Len(t, out.Failed, 1)

				return aws.ToString(out.Failed[0].Code), aws.ToString(out.Failed[0].Message)
			},
			wantCode: "InvalidMessageContents",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			code, msg := tc.call(t, newHandleEnv(t, false))
			assert.True(t, strings.HasSuffix(code, tc.wantCode), code)
			assert.NotEmpty(t, msg)
			assert.NotEqual(t, code, msg, "message must describe the error, not repeat the code")
		})
	}
}

func TestFIFOSend_PurgeAndDuplicate_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		purge bool
	}{
		{name: "duplicate keeps sequence", purge: false},
		{name: "purge clears dedup cache", purge: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHandleEnv(t, true)
			send := func() *sqssdk.SendMessageOutput {
				out, err := e.client.SendMessage(t.Context(), &sqssdk.SendMessageInput{
					QueueUrl: aws.String(e.url), MessageBody: aws.String("same"),
					MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String("dup"),
				})
				require.NoError(t, err)

				return out
			}

			first := send()
			require.NotEmpty(t, aws.ToString(first.SequenceNumber))

			if tc.purge {
				_, err := e.client.PurgeQueue(t.Context(), &sqssdk.PurgeQueueInput{QueueUrl: aws.String(e.url)})
				require.NoError(t, err)
			}

			second := send()
			visible, _ := e.counts(t, e.url)

			if tc.purge {
				assert.NotEqual(t, aws.ToString(first.MessageId), aws.ToString(second.MessageId))
				assert.Equal(t, 1, visible)

				return
			}

			assert.Equal(t, aws.ToString(first.MessageId), aws.ToString(second.MessageId))
			assert.Equal(t, aws.ToString(first.SequenceNumber), aws.ToString(second.SequenceNumber))
			assert.Equal(t, 1, visible)
		})
	}
}

func TestPurgeQueue_ConcurrentWithSend(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	for i := range 100 {
		out, err := b.CreateQueue(&sqs.CreateQueueInput{
			QueueName:  "purge-race-" + strconv.Itoa(i) + ".fifo",
			Endpoint:   testEndpoint,
			Attributes: map[string]string{"FifoQueue": "true"},
		})
		require.NoError(t, err)

		var wg sync.WaitGroup

		started := make(chan struct{})

		wg.Go(func() {
			for j := range 200 {
				if j == 1 {
					close(started)
				}

				_, _ = b.SendMessage(&sqs.SendMessageInput{
					QueueURL: out.QueueURL, MessageBody: "m", MessageGroupID: "g",
					MessageDeduplicationID: strconv.Itoa(j),
				})
			}
		})

		<-started
		require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: out.QueueURL}))
		wg.Wait()
	}
}

func TestReceiptHandle_SurvivesRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rewrite func(snap, handle string) string
		name    string
	}{
		{name: "signed handle", rewrite: func(snap, _ string) string { return snap }},
		{
			name: "legacy handle",
			rewrite: func(snap, handle string) string {
				return strings.ReplaceAll(snap, handle, "00000000-0000-0000-0000-000000000000:1:legacy")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createQueueForTest(t, b, "restore-handle")

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "m"})
			require.NoError(t, err)

			recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 60,
			})
			require.NoError(t, err)
			require.Len(t, recv.Messages, 1)

			handle := recv.Messages[0].ReceiptHandle
			snap := tc.rewrite(string(b.Snapshot(t.Context())), handle)
			if tc.name == "legacy handle" {
				handle = "00000000-0000-0000-0000-000000000000:1:legacy"
			}

			fresh := newBackend(t)
			require.NoError(t, fresh.Restore(t.Context(), []byte(snap)))

			require.NoError(t, fresh.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueURL: qURL, ReceiptHandle: handle, VisibilityTimeout: 30,
			}))
			require.NoError(t, fresh.DeleteMessage(&sqs.DeleteMessageInput{QueueURL: qURL, ReceiptHandle: handle}))
		})
	}
}
