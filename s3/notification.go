package s3

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// notificationConfiguration mirrors the AWS S3 XML notification configuration
// stored in StoredBucket.NotificationConfig.
type notificationConfiguration struct {
	QueueConfigurations  []queueConfiguration  `xml:"QueueConfiguration"`
	TopicConfigurations  []topicConfiguration  `xml:"TopicConfiguration"`
	LambdaConfigurations []lambdaConfiguration `xml:"CloudFunctionConfiguration"`
}

type queueConfiguration struct {
	QueueID string   `xml:"Id"`
	Queue   string   `xml:"Queue"`
	Events  []string `xml:"Event"`
}

type topicConfiguration struct {
	TopicID string   `xml:"Id"`
	Topic   string   `xml:"Topic"`
	Events  []string `xml:"Event"`
}

type lambdaConfiguration struct {
	LambdaID  string   `xml:"Id"`
	CloudFunc string   `xml:"CloudFunction"`
	Events    []string `xml:"Event"`
}

// NotificationDispatcher delivers S3 event notifications to configured targets.
type NotificationDispatcher interface {
	// DispatchObjectCreated sends an s3:ObjectCreated notification for the given object.
	DispatchObjectCreated(ctx context.Context, bucket, key, etag string, size int64, notifXML string)
	// DispatchObjectDeleted sends an s3:ObjectRemoved notification for the given object.
	DispatchObjectDeleted(ctx context.Context, bucket, key, notifXML string)
}

// NotificationTargets holds concrete delivery clients for each supported target type.
type NotificationTargets struct {
	SQSSender    SQSSender
	SNSPublisher SNSPublisher
}

// SQSSender sends a message body to an SQS queue identified by ARN.
type SQSSender interface {
	SendMessageToQueue(ctx context.Context, queueARN, messageBody string) error
}

// SNSPublisher publishes a message to an SNS topic identified by ARN.
type SNSPublisher interface {
	PublishToTopic(ctx context.Context, topicARN, message, subject string) error
}

// s3EventRecord is the standard AWS S3 event notification record structure.
type s3EventRecord struct {
	EventTime    string          `json:"eventTime"`
	EventSource  string          `json:"eventSource"`
	AwsRegion    string          `json:"awsRegion"`
	EventName    string          `json:"eventName"`
	EventVersion string          `json:"eventVersion"`
	S3           s3EventRecordS3 `json:"s3"`
}

type s3EventRecordS3 struct {
	Bucket          s3EventBucket `json:"bucket"`
	S3SchemaVersion string        `json:"s3SchemaVersion"`
	ConfigurationID string        `json:"configurationId"`
	Object          s3EventObject `json:"object"`
}

type s3EventBucket struct {
	ARN  string `json:"arn"`
	Name string `json:"name"`
}

type s3EventObject struct {
	ETag      string `json:"eTag,omitempty"`
	Key       string `json:"key"`
	Sequencer string `json:"sequencer"`
	Size      int64  `json:"size,omitempty"`
}

// buildS3EventPayload builds the standard S3 event notification JSON payload.
func buildS3EventPayload(eventName, configID, region, bucket, key, etag string, size int64) (string, error) {
	record := s3EventRecord{
		EventVersion: "2.1",
		EventSource:  "aws:s3",
		AwsRegion:    region,
		EventTime:    time.Now().UTC().Format(time.RFC3339Nano),
		EventName:    eventName,
		S3: s3EventRecordS3{
			S3SchemaVersion: "1.0",
			ConfigurationID: configID,
			Bucket: s3EventBucket{
				Name: bucket,
				ARN:  arn.BuildS3(bucket),
			},
			Object: s3EventObject{
				Key:       key,
				ETag:      etag,
				Size:      size,
				Sequencer: fmt.Sprintf("%016X", time.Now().UnixNano()),
			},
		},
	}
	envelope := map[string]any{"Records": []s3EventRecord{record}}

	b, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// eventMatches returns true when the event name matches the rule event pattern.
// AWS event patterns use wildcards like "s3:ObjectCreated:*".
func eventMatches(pattern, eventName string) bool {
	if pattern == eventName {
		return true
	}
	// Support trailing wildcard: "s3:ObjectCreated:*" matches "s3:ObjectCreated:Put"
	if before, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(eventName, before)
	}

	return false
}

// inMemoryNotificationDispatcher delivers S3 event notifications using in-process targets.
type inMemoryNotificationDispatcher struct {
	targets *NotificationTargets
	region  string
}

// NewNotificationDispatcher creates a NotificationDispatcher that delivers
// events to the provided in-process targets (SQS, SNS, Lambda).
func NewNotificationDispatcher(targets *NotificationTargets, region string) NotificationDispatcher {
	return &inMemoryNotificationDispatcher{targets: targets, region: region}
}

func (d *inMemoryNotificationDispatcher) DispatchObjectCreated(
	ctx context.Context,
	bucket, key, etag string,
	size int64,
	notifXML string,
) {
	d.dispatch(ctx, "s3:ObjectCreated:Put", bucket, key, etag, size, notifXML)
}

func (d *inMemoryNotificationDispatcher) DispatchObjectDeleted(
	ctx context.Context,
	bucket, key, notifXML string,
) {
	d.dispatch(ctx, "s3:ObjectRemoved:Delete", bucket, key, "", 0, notifXML)
}

func (d *inMemoryNotificationDispatcher) dispatch(
	ctx context.Context,
	eventName, bucket, key, etag string,
	size int64,
	notifXML string,
) {
	if notifXML == "" {
		return
	}

	var cfg notificationConfiguration
	if err := xml.Unmarshal([]byte(notifXML), &cfg); err != nil {
		return
	}

	for _, qc := range cfg.QueueConfigurations {
		if !matchesAnyEvent(qc.Events, eventName) {
			continue
		}

		payload, err := buildS3EventPayload(eventName, qc.QueueID, d.region, bucket, key, etag, size)
		if err != nil {
			continue
		}

		if d.targets != nil && d.targets.SQSSender != nil {
			_ = d.targets.SQSSender.SendMessageToQueue(ctx, qc.Queue, payload)
		}
	}

	for _, tc := range cfg.TopicConfigurations {
		if !matchesAnyEvent(tc.Events, eventName) {
			continue
		}

		payload, err := buildS3EventPayload(eventName, tc.TopicID, d.region, bucket, key, etag, size)
		if err != nil {
			continue
		}

		if d.targets != nil && d.targets.SNSPublisher != nil {
			_ = d.targets.SNSPublisher.PublishToTopic(ctx, tc.Topic, payload, "S3Notification")
		}
	}
}

// matchesAnyEvent returns true if eventName matches any of the given event patterns.
func matchesAnyEvent(patterns []string, eventName string) bool {
	for _, p := range patterns {
		if eventMatches(p, eventName) {
			return true
		}
	}

	return false
}
