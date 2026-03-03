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

func TestNotificationDispatcher_DispatchObjectCreated_SQS(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
</QueueConfiguration>
</NotificationConfiguration>`

	d.DispatchObjectCreated(t.Context(), "my-bucket", "my-key", "abc123", 42, notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Len(t, queue.messages, 1)
	assert.Contains(t, queue.messages[0], `"aws:s3"`)
	assert.Contains(t, queue.messages[0], `"my-bucket"`)
	assert.Contains(t, queue.messages[0], `"my-key"`)
	assert.Contains(t, queue.messages[0], `"s3:ObjectCreated:Put"`)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:my-queue", queue.lastARN)
}

func TestNotificationDispatcher_DispatchObjectDeleted_SQS(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:del-queue</Queue>
  <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>
</NotificationConfiguration>`

	d.DispatchObjectDeleted(t.Context(), "my-bucket", "my-key", notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Len(t, queue.messages, 1)
	assert.Contains(t, queue.messages[0], `"s3:ObjectRemoved:Delete"`)
}

func TestNotificationDispatcher_DispatchObjectCreated_SNS(t *testing.T) {
	t.Parallel()

	topic := &captureTopic{}
	targets := &s3.NotificationTargets{SNSPublisher: topic}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
<TopicConfiguration>
  <Id>t1</Id>
  <Topic>arn:aws:sns:us-east-1:000000000000:my-topic</Topic>
  <Event>s3:ObjectCreated:*</Event>
</TopicConfiguration>
</NotificationConfiguration>`

	d.DispatchObjectCreated(t.Context(), "my-bucket", "my-key", "", 0, notifXML)

	topic.mu.Lock()
	defer topic.mu.Unlock()
	require.Len(t, topic.messages, 1)
	assert.Contains(t, topic.messages[0], `"my-bucket"`)
}

func TestNotificationDispatcher_EventFilterMismatch(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	// Rule only matches ObjectCreated; we dispatch ObjectRemoved — should NOT be delivered.
	notifXML := `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:*</Event>
</QueueConfiguration>
</NotificationConfiguration>`

	d.DispatchObjectDeleted(t.Context(), "my-bucket", "my-key", notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	assert.Empty(t, queue.messages)
}

func TestNotificationDispatcher_EmptyConfig(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	// Empty notifXML — nothing should be dispatched.
	d.DispatchObjectCreated(t.Context(), "my-bucket", "my-key", "", 0, "")

	queue.mu.Lock()
	defer queue.mu.Unlock()
	assert.Empty(t, queue.messages)
}

func TestNotificationDispatcher_InvalidXML(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	// Malformed XML — should be handled gracefully.
	d.DispatchObjectCreated(t.Context(), "my-bucket", "my-key", "", 0, "<bad>xml")

	queue.mu.Lock()
	defer queue.mu.Unlock()
	assert.Empty(t, queue.messages)
}

func TestNotificationDispatcher_ExactEventMatch(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	// Exact event name match (no wildcard).
	notifXML := `<NotificationConfiguration>
<QueueConfiguration>
  <Id>q1</Id>
  <Queue>arn:aws:sqs:us-east-1:000000000000:my-queue</Queue>
  <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>
</NotificationConfiguration>`

	d.DispatchObjectCreated(t.Context(), "my-bucket", "my-key", "", 0, notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Len(t, queue.messages, 1)
	assert.Contains(t, queue.messages[0], `"s3:ObjectCreated:Put"`)
}

func TestNotificationDispatcher_PrefixFilter_Match(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
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

	d.DispatchObjectCreated(t.Context(), "my-bucket", "images/photo.jpg", "abc", 10, notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Len(t, queue.messages, 1)
	assert.Contains(t, queue.messages[0], `"images/photo.jpg"`)
}

func TestNotificationDispatcher_PrefixFilter_NoMatch(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
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

	d.DispatchObjectCreated(t.Context(), "my-bucket", "docs/readme.txt", "abc", 10, notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	assert.Empty(t, queue.messages)
}

func TestNotificationDispatcher_SuffixFilter_Match(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
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

	d.DispatchObjectCreated(t.Context(), "my-bucket", "images/photo.jpg", "abc", 10, notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Len(t, queue.messages, 1)
}

func TestNotificationDispatcher_SuffixFilter_NoMatch(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
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

	d.DispatchObjectCreated(t.Context(), "my-bucket", "images/photo.png", "abc", 10, notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	assert.Empty(t, queue.messages)
}

func TestNotificationDispatcher_PrefixAndSuffixFilter_Match(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
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

	d.DispatchObjectCreated(t.Context(), "my-bucket", "images/photo.jpg", "abc", 10, notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Len(t, queue.messages, 1)
}

func TestNotificationDispatcher_PrefixAndSuffixFilter_PrefixNoMatch(t *testing.T) {
	t.Parallel()

	queue := &captureQueue{}
	targets := &s3.NotificationTargets{SQSSender: queue}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
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

	// suffix matches but prefix does not
	d.DispatchObjectCreated(t.Context(), "my-bucket", "docs/photo.jpg", "abc", 10, notifXML)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	assert.Empty(t, queue.messages)
}

func TestNotificationDispatcher_SNS_PrefixFilter_Match(t *testing.T) {
	t.Parallel()

	topic := &captureTopic{}
	targets := &s3.NotificationTargets{SNSPublisher: topic}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
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

	d.DispatchObjectCreated(t.Context(), "my-bucket", "images/photo.jpg", "", 0, notifXML)

	topic.mu.Lock()
	defer topic.mu.Unlock()
	require.Len(t, topic.messages, 1)
}

func TestNotificationDispatcher_SNS_PrefixFilter_NoMatch(t *testing.T) {
	t.Parallel()

	topic := &captureTopic{}
	targets := &s3.NotificationTargets{SNSPublisher: topic}
	d := s3.NewNotificationDispatcher(targets, "us-east-1")

	notifXML := `<NotificationConfiguration>
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

	d.DispatchObjectCreated(t.Context(), "my-bucket", "docs/readme.txt", "", 0, notifXML)

	topic.mu.Lock()
	defer topic.mu.Unlock()
	assert.Empty(t, topic.messages)
}
