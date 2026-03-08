package s3_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// captureQueue is a test SQSSender that records sent messages.
type captureQueue struct {
	lastARN  string
	messages []string
	mu       sync.Mutex
}

func (c *captureQueue) SendMessageToQueue(_ context.Context, queueARN, messageBody string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastARN = queueARN
	c.messages = append(c.messages, messageBody)

	return nil
}

// captureTopic is a test SNSPublisher that records published messages.
type captureTopic struct {
	messages []string
	mu       sync.Mutex
}

func (c *captureTopic) PublishToTopic(_ context.Context, _, message, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, message)

	return nil
}

func TestNotificationDispatcher_DispatchObjectCreated(t *testing.T) {
	t.Parallel()

	const (
		sqsCreatedXML = `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
</QueueConfiguration>
</NotificationConfiguration>`

		snsCreatedXML = `<NotificationConfiguration>
<TopicConfiguration>
  <Id>t1</Id>
  <Topic>arn:aws:sns:us-east-1:000000000000:my-topic</Topic>
  <Event>s3:ObjectCreated:*</Event>
</TopicConfiguration>
</NotificationConfiguration>`

		sqsExactMatchXML = `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>
</NotificationConfiguration>`

		sqsPrefixXML = `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
  <Filter>
    <S3Key>
      <FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule>
    </S3Key>
  </Filter>
</QueueConfiguration>
</NotificationConfiguration>`

		sqsSuffixXML = `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
  <Filter>
    <S3Key>
      <FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule>
    </S3Key>
  </Filter>
</QueueConfiguration>
</NotificationConfiguration>`

		sqsPrefixSuffixXML = `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
  <Filter>
    <S3Key>
      <FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule>
      <FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule>
    </S3Key>
  </Filter>
</QueueConfiguration>
</NotificationConfiguration>`

		snsPrefixXML = `<NotificationConfiguration>
<TopicConfiguration>
  <Id>t1</Id>
  <Topic>arn:aws:sns:us-east-1:000000000000:my-topic</Topic>
  <Event>s3:ObjectCreated:*</Event>
  <Filter>
    <S3Key>
      <FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule>
    </S3Key>
  </Filter>
</TopicConfiguration>
</NotificationConfiguration>`

		sqsUnknownFilterXML = `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
  <Filter>
    <S3Key>
      <FilterRule><Name>unknown</Name><Value>anything</Value></FilterRule>
    </S3Key>
  </Filter>
</QueueConfiguration>
</NotificationConfiguration>`
	)

	tests := []struct {
		name              string
		notifXML          string
		key               string
		etag              string
		wantQueueARN      string
		wantQueueContains []string
		wantTopicContains []string
		size              int64
		wantQueueCount    int
		wantTopicCount    int
	}{
		{
			name:              "SQS_basic",
			notifXML:          sqsCreatedXML,
			key:               "my-key",
			etag:              "abc123",
			size:              42,
			wantQueueCount:    1,
			wantQueueContains: []string{`"aws:s3"`, `"my-bucket"`, `"my-key"`, `"s3:ObjectCreated:Put"`},
			wantQueueARN:      "arn:aws:sqs:us-east-1:000000000000:my-queue",
		},
		{
			name:              "SNS_basic",
			notifXML:          snsCreatedXML,
			key:               "my-key",
			wantTopicCount:    1,
			wantTopicContains: []string{`"my-bucket"`},
		},
		{
			name:           "empty_config_no_dispatch",
			notifXML:       "",
			key:            "my-key",
			wantQueueCount: 0,
			wantTopicCount: 0,
		},
		{
			name:           "invalid_XML_no_dispatch",
			notifXML:       "<bad>xml",
			key:            "my-key",
			wantQueueCount: 0,
			wantTopicCount: 0,
		},
		{
			name:              "exact_event_match",
			notifXML:          sqsExactMatchXML,
			key:               "my-key",
			wantQueueCount:    1,
			wantQueueContains: []string{`"s3:ObjectCreated:Put"`},
		},
		{
			name:              "SQS_prefix_filter_match",
			notifXML:          sqsPrefixXML,
			key:               "images/photo.jpg",
			etag:              "abc",
			size:              10,
			wantQueueCount:    1,
			wantQueueContains: []string{`"images/photo.jpg"`},
		},
		{
			name:           "SQS_prefix_filter_no_match",
			notifXML:       sqsPrefixXML,
			key:            "docs/readme.txt",
			etag:           "abc",
			size:           10,
			wantQueueCount: 0,
		},
		{
			name:           "SQS_suffix_filter_match",
			notifXML:       sqsSuffixXML,
			key:            "images/photo.jpg",
			etag:           "abc",
			size:           10,
			wantQueueCount: 1,
		},
		{
			name:           "SQS_suffix_filter_no_match",
			notifXML:       sqsSuffixXML,
			key:            "images/photo.png",
			etag:           "abc",
			size:           10,
			wantQueueCount: 0,
		},
		{
			name:           "SQS_prefix_and_suffix_filter_match",
			notifXML:       sqsPrefixSuffixXML,
			key:            "images/photo.jpg",
			etag:           "abc",
			size:           10,
			wantQueueCount: 1,
		},
		{
			name:           "SQS_prefix_and_suffix_filter_prefix_no_match",
			notifXML:       sqsPrefixSuffixXML,
			key:            "docs/photo.jpg",
			etag:           "abc",
			size:           10,
			wantQueueCount: 0,
		},
		{
			name:           "SNS_prefix_filter_match",
			notifXML:       snsPrefixXML,
			key:            "images/photo.jpg",
			wantTopicCount: 1,
		},
		{
			name:           "SNS_prefix_filter_no_match",
			notifXML:       snsPrefixXML,
			key:            "docs/readme.txt",
			wantTopicCount: 0,
		},
		{
			name:           "unknown_filter_rule_no_dispatch",
			notifXML:       sqsUnknownFilterXML,
			key:            "any-key",
			etag:           "abc",
			size:           10,
			wantQueueCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			queue := &captureQueue{}
			topic := &captureTopic{}
			targets := &s3.NotificationTargets{SQSSender: queue, SNSPublisher: topic}
			d := s3.NewNotificationDispatcher(targets, "us-east-1")

			d.DispatchObjectCreated(t.Context(), "my-bucket", tt.key, tt.etag, tt.size, tt.notifXML)

			queue.mu.Lock()
			defer queue.mu.Unlock()
			assert.Len(t, queue.messages, tt.wantQueueCount)
			for _, c := range tt.wantQueueContains {
				require.NotEmpty(t, queue.messages)
				assert.Contains(t, queue.messages[0], c)
			}
			if tt.wantQueueARN != "" {
				assert.Equal(t, tt.wantQueueARN, queue.lastARN)
			}

			topic.mu.Lock()
			defer topic.mu.Unlock()
			assert.Len(t, topic.messages, tt.wantTopicCount)
			for _, c := range tt.wantTopicContains {
				require.NotEmpty(t, topic.messages)
				assert.Contains(t, topic.messages[0], c)
			}
		})
	}
}

func TestNotificationDispatcher_DispatchObjectDeleted(t *testing.T) {
	t.Parallel()

	const (
		deletedMatchXML = `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:del-queue</Queue>
  <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>
</NotificationConfiguration>`

		createdOnlyXML = `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
</QueueConfiguration>
</NotificationConfiguration>`
	)

	tests := []struct {
		name              string
		notifXML          string
		key               string
		wantQueueContains []string
		wantQueueCount    int
	}{
		{
			name:              "SQS_basic",
			notifXML:          deletedMatchXML,
			key:               "my-key",
			wantQueueCount:    1,
			wantQueueContains: []string{`"s3:ObjectRemoved:Delete"`},
		},
		{
			// Rule only matches ObjectCreated; dispatching ObjectDeleted must not deliver.
			name:           "event_filter_mismatch_no_dispatch",
			notifXML:       createdOnlyXML,
			key:            "my-key",
			wantQueueCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			queue := &captureQueue{}
			targets := &s3.NotificationTargets{SQSSender: queue}
			d := s3.NewNotificationDispatcher(targets, "us-east-1")

			d.DispatchObjectDeleted(t.Context(), "my-bucket", tt.key, tt.notifXML)

			queue.mu.Lock()
			defer queue.mu.Unlock()
			assert.Len(t, queue.messages, tt.wantQueueCount)
			for _, c := range tt.wantQueueContains {
				require.NotEmpty(t, queue.messages)
				assert.Contains(t, queue.messages[0], c)
			}
		})
	}
}

// captureLambda is a test LambdaInvoker that records invocations.
type captureLambda struct {
	invocations []string
	mu          sync.Mutex
}

func (c *captureLambda) InvokeFunction(_ context.Context, name, _ string, payload []byte) ([]byte, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invocations = append(c.invocations, name+":"+string(payload))

	return nil, 200, nil
}

func TestNotificationDispatcher_DispatchToLambda(t *testing.T) {
	t.Parallel()

	const lambdaXML = `<NotificationConfiguration>
<CloudFunctionConfiguration>
  <Id>fn1</Id>
  <CloudFunction>arn:aws:lambda:us-east-1:000000000000:function:my-fn</CloudFunction>
  <Event>s3:ObjectCreated:*</Event>
</CloudFunctionConfiguration>
</NotificationConfiguration>`

	tests := []struct {
		name             string
		notifXML         string
		wantInvokePrefix string
		wantInvokeCount  int
	}{
		{
			name:             "lambda_invoked_on_created",
			notifXML:         lambdaXML,
			wantInvokeCount:  1,
			wantInvokePrefix: "arn:aws:lambda:us-east-1:000000000000:function:my-fn:",
		},
		{
			name:            "no_lambda_config_no_invocation",
			notifXML:        `<NotificationConfiguration></NotificationConfiguration>`,
			wantInvokeCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn := &captureLambda{}
			targets := &s3.NotificationTargets{LambdaInvoker: fn}
			d := s3.NewNotificationDispatcher(targets, "us-east-1")

			d.DispatchObjectCreated(t.Context(), "my-bucket", "test-key", "etag123", 42, tt.notifXML)

			fn.mu.Lock()
			defer fn.mu.Unlock()
			assert.Len(t, fn.invocations, tt.wantInvokeCount)
			if tt.wantInvokeCount > 0 && tt.wantInvokePrefix != "" {
				assert.Greater(t, len(fn.invocations[0]), len(tt.wantInvokePrefix))
				assert.Contains(t, fn.invocations[0], tt.wantInvokePrefix)
			}
		})
	}
}

// captureEventBridge is a test EventBridgePublisher that records published events.
type captureEventBridge struct {
	events []struct{ source, detailType, detail string }
	mu     sync.Mutex
}

func (c *captureEventBridge) PublishS3Event(_ context.Context, source, detailType, detail string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, struct{ source, detailType, detail string }{source, detailType, detail})
}

func TestNotificationDispatcher_DispatchToEventBridge(t *testing.T) {
	t.Parallel()

	const (
		ebEnabledXML = `<NotificationConfiguration>
<EventBridgeConfiguration/>
</NotificationConfiguration>`

		ebDisabledXML = `<NotificationConfiguration>
</NotificationConfiguration>`

		ebWithSQSXML = `<NotificationConfiguration>
<EventBridgeConfiguration/>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
</QueueConfiguration>
</NotificationConfiguration>`
	)

	tests := []struct {
		name               string
		notifXML           string
		key                string
		etag               string
		wantDetailType     string
		wantDetailContains []string
		wantDetailAbsent   []string
		size               int64
		wantEventCount     int
		wantQueueCount     int
		dispatchDelete     bool
		dispatchCopy       bool
		dispatchComplete   bool
	}{
		{
			name:               "EventBridge_enabled_object_created",
			notifXML:           ebEnabledXML,
			key:                "my-key",
			etag:               "abc123",
			size:               42,
			wantEventCount:     1,
			wantDetailType:     "Object Created",
			wantDetailContains: []string{`"my-bucket"`, `"my-key"`, `"PutObject"`},
			wantDetailAbsent:   []string{`"source-ip-address"`, `"requester"`},
		},
		{
			name:               "EventBridge_enabled_object_copied",
			notifXML:           ebEnabledXML,
			key:                "my-key",
			etag:               "abc123",
			size:               42,
			dispatchCopy:       true,
			wantEventCount:     1,
			wantDetailType:     "Object Created",
			wantDetailContains: []string{`"my-bucket"`, `"my-key"`, `"CopyObject"`},
		},
		{
			name:               "EventBridge_enabled_object_completed",
			notifXML:           ebEnabledXML,
			key:                "my-key",
			etag:               "abc123",
			size:               42,
			dispatchComplete:   true,
			wantEventCount:     1,
			wantDetailType:     "Object Created",
			wantDetailContains: []string{`"my-bucket"`, `"my-key"`, `"CompleteMultipartUpload"`},
		},
		{
			name:               "EventBridge_enabled_object_deleted",
			notifXML:           ebEnabledXML,
			key:                "my-key",
			dispatchDelete:     true,
			wantEventCount:     1,
			wantDetailType:     "Object Deleted",
			wantDetailContains: []string{`"my-bucket"`, `"my-key"`, `"DeleteObject"`},
		},
		{
			name:           "EventBridge_disabled_no_event",
			notifXML:       ebDisabledXML,
			key:            "my-key",
			wantEventCount: 0,
		},
		{
			name:           "empty_config_no_event",
			notifXML:       "",
			key:            "my-key",
			wantEventCount: 0,
		},
		{
			name:           "EventBridge_and_SQS_both_delivered",
			notifXML:       ebWithSQSXML,
			key:            "my-key",
			etag:           "abc123",
			size:           10,
			wantEventCount: 1,
			wantDetailType: "Object Created",
			wantQueueCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eb := &captureEventBridge{}
			queue := &captureQueue{}
			targets := &s3.NotificationTargets{
				EventBridgePublisher: eb,
				SQSSender:            queue,
			}
			d := s3.NewNotificationDispatcher(targets, "us-east-1")

			switch {
			case tt.dispatchDelete:
				d.DispatchObjectDeleted(t.Context(), "my-bucket", tt.key, tt.notifXML)
			case tt.dispatchCopy:
				d.DispatchObjectCopied(t.Context(), "my-bucket", tt.key, tt.etag, tt.size, tt.notifXML)
			case tt.dispatchComplete:
				d.DispatchObjectCompleted(t.Context(), "my-bucket", tt.key, tt.etag, tt.size, tt.notifXML)
			default:
				d.DispatchObjectCreated(t.Context(), "my-bucket", tt.key, tt.etag, tt.size, tt.notifXML)
			}

			eb.mu.Lock()
			defer eb.mu.Unlock()
			assert.Len(t, eb.events, tt.wantEventCount)

			if tt.wantEventCount > 0 {
				assert.Equal(t, "aws.s3", eb.events[0].source)
				assert.Equal(t, tt.wantDetailType, eb.events[0].detailType)

				for _, c := range tt.wantDetailContains {
					assert.Contains(t, eb.events[0].detail, c)
				}

				for _, a := range tt.wantDetailAbsent {
					assert.NotContains(t, eb.events[0].detail, a)
				}
			}

			queue.mu.Lock()
			defer queue.mu.Unlock()
			assert.Len(t, queue.messages, tt.wantQueueCount)
		})
	}
}

func TestDetailTypeFromEventName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		eventName string
		want      string
	}{
		{name: "object_created_put", eventName: "s3:ObjectCreated:Put", want: "Object Created"},
		{name: "object_created_copy", eventName: "s3:ObjectCreated:Copy", want: "Object Created"},
		{name: "object_created_wildcard", eventName: "s3:ObjectCreated:*", want: "Object Created"},
		{name: "object_removed_delete", eventName: "s3:ObjectRemoved:Delete", want: "Object Deleted"},
		{name: "object_removed_wildcard", eventName: "s3:ObjectRemoved:*", want: "Object Deleted"},
		{name: "object_restore", eventName: "s3:ObjectRestore:Post", want: "Object Restore Initiated"},
		{name: "unknown_event", eventName: "s3:Replication:OperationMissedThreshold", want: "S3 Event"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s3.DetailTypeFromEventName(tt.eventName))
		})
	}
}

func TestReasonFromEventName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		eventName string
		want      string
	}{
		{name: "put", eventName: "s3:ObjectCreated:Put", want: "PutObject"},
		{name: "copy", eventName: "s3:ObjectCreated:Copy", want: "CopyObject"},
		{name: "multipart", eventName: "s3:ObjectCreated:CompleteMultipartUpload", want: "CompleteMultipartUpload"},
		{name: "delete", eventName: "s3:ObjectRemoved:Delete", want: "DeleteObject"},
		{name: "delete_marker", eventName: "s3:ObjectRemoved:DeleteMarkerCreated", want: "DeleteObject"},
		{name: "unknown", eventName: "s3:ObjectCreated:Unknown", want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s3.ReasonFromEventName(tt.eventName))
		})
	}
}
