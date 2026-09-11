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
	sqs.SetNowFunc(backend, fixedThroughputTestTime)

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
	sqs.SetNowFunc(backend, fixedThroughputTestTime)

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

// fixedThroughputTestTime pins the backend clock to one instant so a whole
// test run happens in a single 1-second rate-limit window, decoupling the
// assertions from real wall-clock timing (see the no-time.Sleep-in-tests
// convention).
func fixedThroughputTestTime() time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
}
