package s3_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/s3"
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
		size              int64
		wantQueueCount    int
		wantTopicCount    int
		wantQueueContains []string
		wantTopicContains []string
		wantQueueARN      string
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
		wantQueueCount    int
		wantQueueContains []string
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
