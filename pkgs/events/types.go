package events

// TableCreatedEvent is emitted when a DynamoDB table is created.
type TableCreatedEvent struct {
	Table string
}

func (e *TableCreatedEvent) EventType() string {
	return "dynamodb.table.created"
}

// TableDeletedEvent is emitted when a DynamoDB table is deleted.
type TableDeletedEvent struct {
	Table string
}

func (e *TableDeletedEvent) EventType() string {
	return "dynamodb.table.deleted"
}

// ItemCreatedEvent is emitted when an item is added to a DynamoDB table.
type ItemCreatedEvent struct {
	Key   map[string]any
	Table string
}

func (e *ItemCreatedEvent) EventType() string {
	return "dynamodb.item.created"
}

// ItemUpdatedEvent is emitted when an item in a DynamoDB table is updated.
type ItemUpdatedEvent struct {
	Key   map[string]any
	Table string
}

func (e *ItemUpdatedEvent) EventType() string {
	return "dynamodb.item.updated"
}

// ItemDeletedEvent is emitted when an item is deleted from a DynamoDB table.
type ItemDeletedEvent struct {
	Key   map[string]any
	Table string
}

func (e *ItemDeletedEvent) EventType() string {
	return "dynamodb.item.deleted"
}

// BucketCreatedEvent is emitted when an S3 bucket is created.
type BucketCreatedEvent struct {
	BucketName string
}

func (e *BucketCreatedEvent) EventType() string {
	return "s3.bucket.created"
}

// BucketDeletedEvent is emitted when an S3 bucket is deleted.
type BucketDeletedEvent struct {
	BucketName string
}

func (e *BucketDeletedEvent) EventType() string {
	return "s3.bucket.deleted"
}

// ObjectCreatedEvent is emitted when an object is added to an S3 bucket.
type ObjectCreatedEvent struct {
	BucketName string
	Key        string
	Size       int64
}

func (e *ObjectCreatedEvent) EventType() string {
	return "s3.object.created"
}

// ObjectDeletedEvent is emitted when an object is deleted from an S3 bucket.
type ObjectDeletedEvent struct {
	BucketName string
	Key        string
}

func (e *ObjectDeletedEvent) EventType() string {
	return "s3.object.deleted"
}

// SNSPublishedEvent is emitted whenever a message is published to an SNS topic.
// Listeners (e.g. SQS) can subscribe to deliver the message to the appropriate endpoints.
type SNSPublishedEvent struct {
	Attributes    map[string]SNSMessageAttributeSnapshot
	TopicARN      string
	MessageID     string
	Message       string
	Subject       string
	Subscriptions []SNSSubscriptionSnapshot
}

// SNSSubscriptionSnapshot holds subscription metadata at publish time.
type SNSSubscriptionSnapshot struct {
	// SubscriptionARN is the subscription ARN.
	SubscriptionARN string
	// Protocol is the delivery protocol (sqs, http, https, email, …).
	Protocol string
	// Endpoint is the delivery endpoint (queue ARN, URL, email address, …).
	Endpoint string
	// FilterPolicy is the JSON filter policy, or empty string if none.
	FilterPolicy string
}

// SNSMessageAttributeSnapshot holds a single message attribute value.
type SNSMessageAttributeSnapshot struct {
	// DataType is the attribute data type (String, Number, Binary, …).
	DataType string
	// StringValue is the string value (set when DataType is String/Number).
	StringValue string
}

func (e *SNSPublishedEvent) EventType() string {
	return "sns.message.published"
}

// S3NotificationEvent is emitted when an S3 notification must be delivered
// to external targets (SQS, SNS, Lambda) configured via PutBucketNotificationConfiguration.
// The Payload field contains the standard AWS S3 event notification JSON (Records array).
type S3NotificationEvent struct {
	// Payload is the JSON-encoded S3 event notification body (Records array).
	Payload string
	// TargetARN is the destination ARN (SQS queue ARN, SNS topic ARN, Lambda function ARN).
	TargetARN string
	// TargetType is the notification target type: "sqs", "sns", or "lambda".
	TargetType string
}

func (e *S3NotificationEvent) EventType() string {
	return "s3.notification"
}
