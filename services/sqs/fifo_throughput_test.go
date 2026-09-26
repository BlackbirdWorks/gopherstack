package sqs_test

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// newFIFOThroughputTestServer starts a real HTTP server fronting a fresh SQS
// backend and returns a real aws-sdk-go-v2 client pointed at it, plus the
// backend itself so the test can pin its clock via sqs.SetNowFunc.
func newFIFOThroughputTestServer(t *testing.T) (*sqssdk.Client, *sqs.InMemoryBackend) {
	t.Helper()

	backend := sqs.NewInMemoryBackend()
	t.Cleanup(backend.Close)

	h := sqs.NewHandler(backend)
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := sqssdk.NewFromConfig(cfg, func(o *sqssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client, backend
}

// TestFIFOThroughputLimit_PerQueueDefault_301stThrottled is a regression test
// for gopherstack-qgh: FifoThroughputLimit=perQueue (the AWS default, applied
// whenever the attribute is left unset) previously had no rate limiter at
// all, so a FIFO queue accepted unlimited SendMessage throughput. AWS caps
// unbatched SendMessage at 300 TPS per queue (https://docs.aws.amazon.com/
// AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-quotas.html#quotas-requests).
//
// Each send uses a distinct MessageGroupId so a (buggy) per-group-only
// limiter would never trip here — only a genuinely queue-scoped counter
// throttles the 301st call. The backend clock is pinned so the whole run
// happens at one instant, decoupling the test from real wall-clock timing.
func TestFIFOThroughputLimit_PerQueueDefault_301stThrottled(t *testing.T) {
	t.Parallel()

	client, backend := newFIFOThroughputTestServer(t)
	sqs.SetNowFunc(backend, newFixedThroughputClock())

	ctx := t.Context()

	out, err := client.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String("perqueue-default-throttle.fifo"),
	})
	require.NoError(t, err)

	for i := range 300 {
		_, sendErr := client.SendMessage(ctx, &sqssdk.SendMessageInput{
			QueueUrl:               out.QueueUrl,
			MessageBody:            aws.String(fmt.Sprintf("msg-%d", i)),
			MessageGroupId:         aws.String(fmt.Sprintf("group-%d", i)),
			MessageDeduplicationId: aws.String(fmt.Sprintf("dedup-%d", i)),
		})
		require.NoError(t, sendErr, "send %d of 300 must succeed within the 300 TPS budget", i)
	}

	_, err = client.SendMessage(ctx, &sqssdk.SendMessageInput{
		QueueUrl:               out.QueueUrl,
		MessageBody:            aws.String("msg-301"),
		MessageGroupId:         aws.String("group-301"),
		MessageDeduplicationId: aws.String("dedup-301"),
	})
	require.Error(t, err, "the 301st SendMessage within the same backend-clock second must be throttled")

	var target *sqstypes.RequestThrottled
	require.ErrorAs(t, err, &target,
		"expected the real RequestThrottled exception from the SDK deserializer, got %T: %v", err, err)
}

// TestFIFOThroughputLimit_PerMessageGroupId_TwoGroupsAt300_NotThrottled
// confirms that setting FifoThroughputLimit=perMessageGroupId keeps the
// queue-wide perQueue limiter out of the picture: two message groups each
// sending 300 messages (600 total, well over the queue-wide 300 TPS budget)
// must all succeed because each group has its own independent 300 TPS
// budget.
func TestFIFOThroughputLimit_PerMessageGroupId_TwoGroupsAt300_NotThrottled(t *testing.T) {
	t.Parallel()

	client, backend := newFIFOThroughputTestServer(t)
	sqs.SetNowFunc(backend, newFixedThroughputClock())

	ctx := t.Context()

	out, err := client.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String("permessagegroup-throttle.fifo"),
		Attributes: map[string]string{
			"FifoThroughputLimit": "perMessageGroupId",
			"DeduplicationScope":  "messageGroup",
		},
	})
	require.NoError(t, err)

	groups := []string{"group-a", "group-b"}
	for _, group := range groups {
		for i := range 300 {
			_, sendErr := client.SendMessage(ctx, &sqssdk.SendMessageInput{
				QueueUrl:               out.QueueUrl,
				MessageBody:            aws.String(fmt.Sprintf("%s-msg-%d", group, i)),
				MessageGroupId:         aws.String(group),
				MessageDeduplicationId: aws.String(fmt.Sprintf("%s-dedup-%d", group, i)),
			})
			require.NoError(t, sendErr,
				"%s send %d of 300 must succeed: each message group has its own 300 TPS budget", group, i)
		}
	}
}

// newFixedThroughputClock freezes now() at creation time (near-real, not an
// arbitrary date, since receiveOnce's retention sweep uses the real clock).
func newFixedThroughputClock() func() time.Time {
	now := time.Now()

	return func() time.Time { return now }
}

// newFIFOThroughputBackend drives the backend directly (no HTTP), for tests
// making hundreds of calls where SDK/httptest overhead would be unwieldy.
func newFIFOThroughputBackend(t *testing.T) *sqs.InMemoryBackend {
	t.Helper()

	backend := sqs.NewInMemoryBackend()
	t.Cleanup(backend.Close)
	sqs.SetNowFunc(backend, newFixedThroughputClock())

	return backend
}

// throughputBatchSize mirrors SendMessageBatch/ReceiveMessage's own 10-entry
// AWS batch cap.
const throughputBatchSize = 10

// sendAndReceiveDistinctGroups sends and receives n messages, one per distinct
// group, so nothing blocks on FIFO's one-in-flight-per-group limit.
func sendAndReceiveDistinctGroups(t *testing.T, b *sqs.InMemoryBackend, qURL string, n int) []string {
	t.Helper()

	for i := 0; i < n; i += throughputBatchSize {
		end := min(i+throughputBatchSize, n)

		entries := make([]sqs.SendMessageBatchEntry, 0, end-i)
		for j := i; j < end; j++ {
			entries = append(entries, sqs.SendMessageBatchEntry{
				ID:                     fmt.Sprintf("id-%d", j),
				MessageBody:            fmt.Sprintf("msg-%d", j),
				MessageGroupID:         fmt.Sprintf("group-%d", j),
				MessageDeduplicationID: fmt.Sprintf("dedup-%d", j),
			})
		}

		out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{QueueURL: qURL, Entries: entries})
		require.NoError(t, err)
		require.Empty(t, out.Failed)
	}

	handles := make([]string, 0, n)
	for len(handles) < n {
		out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            qURL,
			MaxNumberOfMessages: throughputBatchSize,
			VisibilityTimeout:   sqs.NoVisibilityTimeout,
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.Messages, "expected more messages available to receive")

		for _, m := range out.Messages {
			handles = append(handles, m.ReceiptHandle)
		}
	}

	return handles
}

// TestFIFOThroughputLimit_IndependentPerMethodBudgets confirms SendMessage,
// ReceiveMessage, and DeleteMessage each get their own 300-calls/sec budget.
func TestFIFOThroughputLimit_IndependentPerMethodBudgets(t *testing.T) {
	t.Parallel()

	b := newFIFOThroughputBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "independent-method-budgets.fifo",
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	qURL := out.QueueURL

	for i := range 300 {
		_, sendErr := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:               qURL,
			MessageBody:            fmt.Sprintf("msg-%d", i),
			MessageGroupID:         fmt.Sprintf("group-%d", i),
			MessageDeduplicationID: fmt.Sprintf("dedup-%d", i),
		})
		require.NoError(t, sendErr, "send %d of 300 must succeed", i)
	}

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "overflow",
		MessageGroupID:         "group-overflow",
		MessageDeduplicationID: "dedup-overflow",
	})
	require.ErrorIs(t, err, sqs.ErrRequestThrottled, "SendMessage's own budget must now be exhausted")

	// ReceiveMessage has its own independent budget: exhausting SendMessage
	// above must not throttle it.
	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 5,
		VisibilityTimeout:   sqs.NoVisibilityTimeout,
	})
	require.NoError(t, err, "ReceiveMessage must not be affected by SendMessage's exhausted budget")
	require.Len(t, recvOut.Messages, 5)

	// Likewise DeleteMessage.
	for i, msg := range recvOut.Messages {
		delErr := b.DeleteMessage(&sqs.DeleteMessageInput{QueueURL: qURL, ReceiptHandle: msg.ReceiptHandle})
		require.NoError(t, delErr, "delete %d must not be affected by SendMessage's exhausted budget", i)
	}
}

// TestFIFOThroughputLimit_SendMessageBatch_CallBudgetCountsOncePerCall: 300
// batches of 10 (3,000 messages) must succeed; a per-entry (not per-call)
// bug would throttle around the 30th batch instead of the 301st.
func TestFIFOThroughputLimit_SendMessageBatch_CallBudgetCountsOncePerCall(t *testing.T) {
	t.Parallel()

	b := newFIFOThroughputBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "batch-call-budget.fifo",
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	qURL := out.QueueURL

	for i := range 300 {
		entries := make([]sqs.SendMessageBatchEntry, throughputBatchSize)
		for j := range throughputBatchSize {
			n := i*throughputBatchSize + j
			entries[j] = sqs.SendMessageBatchEntry{
				ID:                     fmt.Sprintf("id-%d", n),
				MessageBody:            fmt.Sprintf("msg-%d", n),
				MessageGroupID:         fmt.Sprintf("group-%d", n),
				MessageDeduplicationID: fmt.Sprintf("dedup-%d", n),
			}
		}

		batchOut, batchErr := b.SendMessageBatch(&sqs.SendMessageBatchInput{QueueURL: qURL, Entries: entries})
		require.NoError(t, batchErr, "batch %d of 300 must succeed", i)
		require.Empty(t, batchOut.Failed, "batch %d of 300: all 10 entries must succeed", i)
	}

	overflowOut, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{{
			ID:                     "overflow",
			MessageBody:            "overflow",
			MessageGroupID:         "group-overflow",
			MessageDeduplicationID: "dedup-overflow",
		}},
	})
	require.NoError(t, err, "SendMessageBatch succeeds at the transport level even when every entry fails")
	require.Len(t, overflowOut.Failed, 1)
	require.Equal(t, sqs.ErrRequestThrottled.Error(), overflowOut.Failed[0].Code)
}

// TestFIFOThroughputLimit_ReceiveMessage_301stThrottled: an empty receive
// still counts as one API call, so no messages need to exist.
func TestFIFOThroughputLimit_ReceiveMessage_301stThrottled(t *testing.T) {
	t.Parallel()

	b := newFIFOThroughputBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "receive-budget-throttle.fifo",
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	qURL := out.QueueURL

	for i := range 300 {
		_, recvErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            qURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   sqs.NoVisibilityTimeout,
		})
		require.NoError(t, recvErr, "receive %d of 300 must succeed", i)
	}

	_, err = b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   sqs.NoVisibilityTimeout,
	})
	require.ErrorIs(t, err, sqs.ErrRequestThrottled, "the 301st ReceiveMessage must be throttled")
}

// TestFIFOThroughputLimit_DeleteMessage_ScopeSelection: perQueue shares one
// budget across all 301 distinct-group deletes (throttles at #301);
// perMessageGroupId gives each group its own (all 301 succeed).
func TestFIFOThroughputLimit_DeleteMessage_ScopeSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		fifoThroughputLimit string
		wantAllSucceed      bool
	}{
		{
			name:                "perqueue shares one budget across every group",
			fifoThroughputLimit: "perQueue",
			wantAllSucceed:      false,
		},
		{
			name:                "permessagegroupid gives each group its own budget",
			fifoThroughputLimit: "perMessageGroupId",
			wantAllSucceed:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newFIFOThroughputBackend(t)

			attrs := map[string]string{"FifoThroughputLimit": tc.fifoThroughputLimit}
			if tc.fifoThroughputLimit == "perMessageGroupId" {
				attrs["DeduplicationScope"] = "messageGroup"
			}

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "delete-scope-" + tc.fifoThroughputLimit + ".fifo",
				Endpoint:   testEndpoint,
				Attributes: attrs,
			})
			require.NoError(t, err)

			qURL := out.QueueURL

			const total = 301

			handles := sendAndReceiveDistinctGroups(t, b, qURL, total)

			failedAt := -1

			for i, handle := range handles {
				delErr := b.DeleteMessage(&sqs.DeleteMessageInput{QueueURL: qURL, ReceiptHandle: handle})
				if delErr != nil {
					require.ErrorIs(t, delErr, sqs.ErrRequestThrottled, "delete %d failed with an unexpected error", i)

					failedAt = i

					break
				}
			}

			if tc.wantAllSucceed {
				require.Equal(t, -1, failedAt, "every delete must succeed: each group makes only one call")
			} else {
				require.Equal(t, 300, failedAt,
					"the shared queue-wide budget must throttle exactly the 301st delete, regardless of grouping")
			}
		})
	}
}
