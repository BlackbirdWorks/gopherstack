package sns

import (
	"context"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend defines the interface for an SNS storage backend.
type StorageBackend interface {
	CreateTopic(name string, attributes map[string]string) (*Topic, error)
	CreateTopicInRegion(name, region string, attributes map[string]string) (*Topic, error)
	DeleteTopic(topicArn string) error
	ListTopics(nextToken string) ([]Topic, string, error)
	ListTopicsInRegion(region, nextToken string) ([]Topic, string, error)
	GetTopicAttributes(topicArn string) (map[string]string, error)
	SetTopicAttributes(topicArn, attrName, attrValue string) error
	Subscribe(topicArn, protocol, endpoint, filterPolicy string) (*Subscription, error)
	ConfirmSubscription(topicArn, token string) (*Subscription, error)
	Unsubscribe(subscriptionArn string) error
	ListSubscriptions(nextToken string) ([]Subscription, string, error)
	ListSubscriptionsByTopic(topicArn, nextToken string) ([]Subscription, string, error)
	GetSubscriptionAttributes(subscriptionArn string) (map[string]string, error)
	SetSubscriptionAttributes(subscriptionArn, attrName, attrValue string) error
	Publish(
		topicArn, message, subject, messageStructure string,
		attrs map[string]MessageAttribute,
	) (string, error)
	// PublishToTargetArn publishes directly to a platform endpoint ARN.
	// In the mock, this generates and returns a unique message ID without real delivery.
	PublishToTargetArn(
		targetArn, message, subject string,
		attrs map[string]MessageAttribute,
	) (string, error)
	// PublishSMS publishes directly to a phone number via SMS.
	// In the mock, this generates and returns a unique message ID without real delivery.
	PublishSMS(phoneNumber, message string) (string, error)
	ListAllTopics() []Topic
	ListAllSubscriptions() []Subscription
	ListAllPlatformApplications() []PlatformApplication
	GetTopicTags(arn string) map[string]string
	SetTopicTags(arn string, kv *svcTags.Tags)
	RemoveTopicTags(arn string, keys []string)
	// Platform application operations.
	CreatePlatformApplication(
		name, platform string,
		attributes map[string]string,
	) (*PlatformApplication, error)
	CreatePlatformApplicationInRegion(
		name, platform, region string,
		attributes map[string]string,
	) (*PlatformApplication, error)
	GetPlatformApplicationAttributes(platformApplicationArn string) (map[string]string, error)
	SetPlatformApplicationAttributes(
		platformApplicationArn string,
		attributes map[string]string,
	) error
	ListPlatformApplications(nextToken string) ([]PlatformApplication, string, error)
	DeletePlatformApplication(platformApplicationArn string) error
	// Platform endpoint operations.
	CreatePlatformEndpoint(
		platformApplicationArn, token string,
		attributes map[string]string,
	) (*PlatformEndpoint, error)
	GetEndpointAttributes(endpointArn string) (map[string]string, error)
	SetEndpointAttributes(endpointArn string, attributes map[string]string) error
	ListEndpointsByPlatformApplication(
		platformApplicationArn, nextToken string,
	) ([]PlatformEndpoint, string, error)
	DeleteEndpoint(endpointArn string) error
	// Permission operations.
	AddPermission(topicArn, label string, accounts, actions []string) error
	RemovePermission(topicArn, label string) error
	// SMS Sandbox operations.
	GetSMSSandboxAccountStatus() (bool, error)
	CreateSMSSandboxPhoneNumber(phoneNumber, languageCode string) error
	DeleteSMSSandboxPhoneNumber(phoneNumber string) error
	ListSMSSandboxPhoneNumbers(
		nextToken string,
		maxResults int,
	) ([]SandboxPhoneNumber, string, error)
	VerifySMSSandboxPhoneNumber(phoneNumber, oneTimePassword string) error
	// SMS opt-out operations.
	CheckIfPhoneNumberIsOptedOut(phoneNumber string) (bool, error)
	ListPhoneNumbersOptedOut(nextToken string, maxResults int) ([]string, string, error)
	OptInPhoneNumber(phoneNumber string) error
	// SMS attribute operations.
	GetSMSAttributes(names []string) (map[string]string, error)
	SetSMSAttributes(attributes map[string]string) error
	// Data protection policy operations.
	GetDataProtectionPolicy(resourceArn string) (string, error)
	PutDataProtectionPolicy(resourceArn, policy string) error
	// Origination number operations.
	ListOriginationNumbers(nextToken string, maxResults int) ([]XMLOriginationPhone, string, error)
}

// LambdaInvoker can invoke a Lambda function for SNS subscription delivery.
type LambdaInvoker interface {
	InvokeFunction(
		ctx context.Context,
		name, invocationType string,
		payload []byte,
	) ([]byte, int, error)
}

// FirehosePutter can put records to a Kinesis Firehose stream for SNS subscription delivery.
type FirehosePutter interface {
	// PutRecordBatch delivers a batch of records to the named delivery stream.
	PutRecordBatch(streamName string, records [][]byte) (int, error)
}

// SQSSender can send a message to an SQS queue identified by ARN, used for DLQ delivery.
type SQSSender interface {
	SendMessageToQueue(ctx context.Context, queueARN, messageBody string) error
}

// SQSQueueChecker can verify whether an SQS queue identified by ARN exists.
// Used to validate RedrivePolicy.deadLetterTargetArn during SetSubscriptionAttributes.
type SQSQueueChecker interface {
	QueueExists(ctx context.Context, queueARN string) (bool, error)
}

// deliveryWaiter is an optional interface implemented by backends that track
// in-flight HTTP/HTTPS delivery goroutines. Handlers check for this interface
// during graceful shutdown to drain pending deliveries.
type deliveryWaiter interface{ WaitDeliveries() }
