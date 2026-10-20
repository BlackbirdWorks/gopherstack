package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/apptrace"
)

const fanoutQueueName = "fanout-queue"

type fanoutEnv struct {
	fx     *sfnFixture
	queue  string
	queueA string
}

func newFanoutEnv(t *testing.T) *fanoutEnv {
	t.Helper()

	fx := newSFNFixture(t)
	q, err := sqs.NewFromConfig(fx.cfg).CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String(fanoutQueueName),
	})
	require.NoError(t, err)

	return &fanoutEnv{
		fx:     fx,
		queue:  aws.ToString(q.QueueUrl),
		queueA: "arn:aws:sqs:us-east-1:000000000000:" + fanoutQueueName,
	}
}

func (f *fanoutEnv) spans(t *testing.T, query string) []apptrace.Span {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, appInspectorPath+"/requests?"+query, nil)
	rec := httptest.NewRecorder()
	f.fx.handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var res apptrace.ListResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &res))

	return res.Items
}

func (f *fanoutEnv) requireChild(t *testing.T, parentOp, childService, childOp string) {
	t.Helper()

	require.Eventually(t, func() bool {
		parents := f.spans(t, "kind=request&operation="+parentOp)
		if len(parents) == 0 {
			return false
		}

		for _, c := range f.spans(t, "kind=internal&service="+childService) {
			if c.Operation == childOp && c.ParentID == parents[0].ID && c.TraceID == parents[0].TraceID {
				assert.Equal(t, http.StatusOK, c.Status)

				return true
			}
		}

		return false
	}, 10*time.Second, 20*time.Millisecond, "no %s/%s child under %s", childService, childOp, parentOp)
}

func fanoutS3Queue(ctx context.Context, t *testing.T, f *fanoutEnv) {
	t.Helper()

	c := s3.NewFromConfig(f.fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("fanout-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String("fanout-bucket"),
		NotificationConfiguration: &s3types.NotificationConfiguration{
			QueueConfigurations: []s3types.QueueConfiguration{{
				QueueArn: aws.String(f.queueA),
				Events:   []s3types.Event{"s3:ObjectCreated:*"},
			}},
		},
	})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("fanout-bucket"), Key: aws.String("k"), Body: strings.NewReader("x"),
	})
	require.NoError(t, err)
}

func fanoutEventBridgeQueue(ctx context.Context, t *testing.T, f *fanoutEnv) {
	t.Helper()

	c := eventbridge.NewFromConfig(f.fx.cfg)
	_, err := c.PutRule(ctx, &eventbridge.PutRuleInput{
		Name: aws.String("fanout-rule"), EventPattern: aws.String(`{"source":["fanout.test"]}`),
	})
	require.NoError(t, err)

	_, err = c.PutTargets(ctx, &eventbridge.PutTargetsInput{
		Rule:    aws.String("fanout-rule"),
		Targets: []ebtypes.Target{{Id: aws.String("t1"), Arn: aws.String(f.queueA)}},
	})
	require.NoError(t, err)

	_, err = c.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
		Source: aws.String("fanout.test"), DetailType: aws.String("t"), Detail: aws.String(`{"a":1}`),
	}}})
	require.NoError(t, err)
}

func fanoutSNSQueue(ctx context.Context, t *testing.T, f *fanoutEnv) {
	t.Helper()

	c := sns.NewFromConfig(f.fx.cfg)
	topic, err := c.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("fanout-topic")})
	require.NoError(t, err)

	_, err = c.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(f.queueA),
	})
	require.NoError(t, err)

	_, err = c.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("hi")})
	require.NoError(t, err)
}

// TestAppInspectorFanoutChildSpans proves asynchronous cross-service
// deliveries are recorded as child spans of the request that triggered them,
// driven through the production router with real SDK clients.
func TestAppInspectorFanoutChildSpans(t *testing.T) {
	t.Parallel()

	tests := []struct {
		trigger      func(context.Context, *testing.T, *fanoutEnv)
		name         string
		parentOp     string
		childService string
		childOp      string
	}{
		{
			name: "s3 notification to sqs", trigger: fanoutS3Queue,
			parentOp: "PutObject", childService: "sqs", childOp: "SendMessage",
		},
		{
			name: "eventbridge rule target to sqs", trigger: fanoutEventBridgeQueue,
			parentOp: "PutEvents", childService: "sqs", childOp: "SendMessage",
		},
		{
			name: "sns publish to sqs subscription", trigger: fanoutSNSQueue,
			parentOp: "Publish", childService: "sqs", childOp: "SendMessage",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFanoutEnv(t)
			tc.trigger(t.Context(), t, f)
			f.requireChild(t, tc.parentOp, tc.childService, tc.childOp)
		})
	}
}
