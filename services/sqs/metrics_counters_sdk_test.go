package sqs_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_MessageCountersViaSDKAndPoller(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		localPoller bool
	}{
		{name: "sdk receive and delete"},
		{name: "event source poller receive and delete", localPoller: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, backend := newFIFOThroughputTestServer(t)
			rec := &pointRecorder{}
			backend.SetMetricEmitter(rec)
			ctx := t.Context()

			q, err := client.CreateQueue(ctx, &sqssdk.CreateQueueInput{QueueName: aws.String("ctr-q")})
			require.NoError(t, err)
			_, err = client.SendMessage(ctx, &sqssdk.SendMessageInput{
				QueueUrl: q.QueueUrl, MessageBody: aws.String("hello"),
			})
			require.NoError(t, err)

			if tt.localPoller {
				msgs, rerr := backend.ReceiveMessagesLocal(aws.ToString(q.QueueUrl), 1)
				require.NoError(t, rerr)
				require.Len(t, msgs, 1)
				handles := []string{msgs[0].ReceiptHandle}
				require.NoError(t, backend.DeleteMessagesLocal(aws.ToString(q.QueueUrl), handles))
			} else {
				rcv, rerr := client.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{QueueUrl: q.QueueUrl})
				require.NoError(t, rerr)
				require.Len(t, rcv.Messages, 1)
				_, err = client.DeleteMessage(ctx, &sqssdk.DeleteMessageInput{
					QueueUrl: q.QueueUrl, ReceiptHandle: rcv.Messages[0].ReceiptHandle,
				})
				require.NoError(t, err)
			}

			counters := []string{"NumberOfMessagesSent", "NumberOfMessagesReceived", "NumberOfMessagesDeleted"}
			for _, name := range counters {
				require.Eventually(t, func() bool {
					return len(rec.find(name, "ctr-q")) > 0
				}, 2*time.Second, 5*time.Millisecond, name)
				assert.InDelta(t, 1, rec.find(name, "ctr-q")[0].Value, 0, name)
			}
		})
	}
}
