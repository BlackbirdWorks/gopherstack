package sns_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blockingLambdaInvoker is a LambdaInvoker double whose InvokeFunction blocks
// until release is closed, letting a test hold a replay delivery goroutine
// open long enough to observe whether WaitDeliveries actually waits for it.
type blockingLambdaInvoker struct {
	release chan struct{}
	called  chan struct{}
}

func newBlockingLambdaInvoker() *blockingLambdaInvoker {
	return &blockingLambdaInvoker{
		release: make(chan struct{}),
		called:  make(chan struct{}, 1),
	}
}

func (m *blockingLambdaInvoker) InvokeFunction(
	ctx context.Context,
	_, _ string,
	_ []byte,
) ([]byte, int, error) {
	select {
	case m.called <- struct{}{}:
	default:
	}

	select {
	case <-m.release:
	case <-ctx.Done():
	}

	return nil, 200, nil
}

// TestReplayMessagesToSubscription_TrackedByDeliveryWaitGroup verifies that a
// SetSubscriptionAttributes(ReplayPolicy) replay -- previously launched via a
// bare `go` untracked by deliveryWg (bd 1x2u0) -- is now waited on by
// WaitDeliveries/Shutdown instead of racing it.
func TestReplayMessagesToSubscription_TrackedByDeliveryWaitGroup(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	lambda := newBlockingLambdaInvoker()
	b.SetLambdaBackend(lambda)

	tp, err := b.CreateTopic("replay-shutdown-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	pastTime := time.Now().UTC().Add(-time.Hour)
	_, err = b.Publish(tp.TopicArn, "archived-message", "", "", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(
		tp.TopicArn, "lambda", "arn:aws:lambda:us-east-1:000000000000:function:replay-shutdown-fn", "",
	)
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
		fmt.Sprintf(`{"replayFromTimestamp":"%s"}`, pastTime.Format(time.RFC3339)))
	require.NoError(t, err)

	// Wait for the replay goroutine to actually reach the blocking Lambda
	// invocation before racing it against WaitDeliveries.
	select {
	case <-lambda.called:
	case <-time.After(2 * time.Second):
		t.Fatal("replay never invoked the Lambda double")
	}

	waitDone := make(chan struct{})
	go func() {
		b.WaitDeliveries()
		close(waitDone)
	}()

	// WaitDeliveries must NOT return while the replay delivery is still
	// blocked in InvokeFunction -- if it does, the goroutine was untracked.
	select {
	case <-waitDone:
		t.Fatal("WaitDeliveries returned before the in-flight replay delivery finished")
	case <-time.After(200 * time.Millisecond):
	}

	close(lambda.release)

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitDeliveries did not return after the replay delivery finished")
	}
}
