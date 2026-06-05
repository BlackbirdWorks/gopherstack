package sqs

import (
	"encoding/xml"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	sqsNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"
	fifoSuffix   = ".fifo"

	defaultVisibilityTimeout      = 30
	defaultMaxMessageSize         = 262144
	defaultMessageRetentionPeriod = 345600
	defaultDelaySeconds           = 0
	defaultWaitTimeSeconds        = 0
	maxBatchSize                  = 10
	deduplicationWindowSecs       = 300

	maxParseIterations = 20
	noVisibilitySet    = -1

	// msgAttrTransportTypeString is the SQS wire-format byte for String/Number message attributes.
	msgAttrTransportTypeString byte = 1
	// msgAttrTransportTypeBinary is the SQS wire-format byte for Binary message attributes.
	msgAttrTransportTypeBinary byte = 2

	attrVisibilityTimeout             = "VisibilityTimeout"
	attrMaximumMessageSize            = "MaximumMessageSize"
	attrMessageRetentionPeriod        = "MessageRetentionPeriod"
	attrDelaySeconds                  = "DelaySeconds"
	attrReceiveMessageWaitTimeSeconds = "ReceiveMessageWaitTimeSeconds"
	// AttrApproxMessages is the SQS attribute name for approximate number of visible messages.
	AttrApproxMessages = "ApproximateNumberOfMessages"
	// AttrApproxMessagesNotVisible is the SQS attribute name for messages currently in flight.
	AttrApproxMessagesNotVisible  = "ApproximateNumberOfMessagesNotVisible"
	attrCreatedTimestamp          = "CreatedTimestamp"
	attrLastModifiedTimestamp     = "LastModifiedTimestamp"
	attrQueueArn                  = "QueueArn"
	attrFifoQueue                 = "FifoQueue"
	attrContentBasedDeduplication = "ContentBasedDeduplication"
	attrRedrivePolicy             = "RedrivePolicy"
	attrPolicy                    = "Policy"
	attrApproxMessagesDelayed     = "ApproximateNumberOfMessagesDelayed"
	attrAll                       = "All"
	attrSqsManagedSseEnabled      = "SqsManagedSseEnabled"
	attrKmsMasterKeyID            = "KmsMasterKeyId"
	attrKmsDataKeyReusePeriodSecs = "KmsDataKeyReusePeriodSeconds"
	attrRedriveAllowPolicy        = "RedriveAllowPolicy"
	attrDeduplicationScope        = "DeduplicationScope"
	attrFifoThroughputLimit       = "FifoThroughputLimit"

	// fifoDedupScopeQueue / fifoDedupScopePerMessageGroup are the only valid
	// values for the DeduplicationScope FIFO attribute.
	fifoDedupScopeQueue           = "queue"
	fifoDedupScopePerMessageGroup = "messageGroup"
	// fifoThroughputLimitPerQueue / fifoThroughputLimitPerMessageGroupID are
	// the only valid values for the FifoThroughputLimit FIFO attribute.
	fifoThroughputLimitPerQueue          = "perQueue"
	fifoThroughputLimitPerMessageGroupID = "perMessageGroupId"

	attrApproxReceiveCount          = "ApproximateReceiveCount"
	attrSentTimestamp               = "SentTimestamp"
	attrApproxFirstReceiveTimestamp = "ApproximateFirstReceiveTimestamp"
	attrSenderID                    = "SenderId"
	attrMessageGroupIDSys           = "MessageGroupId"
	attrMessageDeduplicationIDSys   = "MessageDeduplicationId"
	attrSequenceNumber              = "SequenceNumber"

	// maxDelaySeconds is the AWS maximum for per-message or queue-level DelaySeconds.
	maxDelaySeconds = 900
	// minMessageRetentionPeriod is the AWS minimum for MessageRetentionPeriod.
	minMessageRetentionPeriod = 60
	// maxMessageRetentionPeriod is the AWS maximum for MessageRetentionPeriod.
	maxMessageRetentionPeriod = 1209600
	// minMaximumMessageSize is the AWS minimum for MaximumMessageSize.
	minMaximumMessageSize = 1024
	// purgeCooldownSecs is the AWS-mandated minimum interval between PurgeQueue calls.
	purgeCooldownSecs = 60

	attrValTrue  = "true"
	attrValFalse = "false"
	attrValZero  = "0"
)

// MessageAttributeValue holds a message attribute value.
type MessageAttributeValue struct {
	DataType    string `json:"dataType"`
	StringValue string `json:"stringValue"`
	BinaryValue []byte `json:"binaryValue,omitempty"`
}

// Message represents an SQS message.
type Message struct {
	MessageAttributes                map[string]MessageAttributeValue `json:"messageAttributes,omitempty"`
	Attributes                       map[string]string                `json:"attributes,omitempty"`
	VisibleAt                        time.Time                        `json:"visibleAt,omitzero"`
	Body                             string                           `json:"body"`
	MessageGroupID                   string                           `json:"messageGroupID,omitempty"`
	MessageDeduplicationID           string                           `json:"messageDeduplicationID,omitempty"`
	SequenceNumber                   string                           `json:"sequenceNumber,omitempty"`
	MessageID                        string                           `json:"messageID"`
	ReceiptHandle                    string                           `json:"receiptHandle"`
	MD5OfBody                        string                           `json:"md5OfBody"`
	MD5OfMessageAttributes           string                           `json:"md5OfMessageAttributes,omitempty"`
	SentTimestamp                    int64                            `json:"sentTimestamp"`
	ApproximateFirstReceiveTimestamp int64                            `json:"approximateFirstReceiveTimestamp"`
	ApproximateReceiveCount          int                              `json:"approximateReceiveCount"`
}

// InFlightMessage wraps a message that has been received but not deleted.
type InFlightMessage struct {
	VisibleAt     time.Time `json:"visibleAt"`
	Msg           *Message  `json:"msg"`
	ReceiptHandle string    `json:"receiptHandle"`
	// Generation matches the Queue.receiveGeneration at the time of receive.
	// Used to detect stale receipt handles when a message is re-received after
	// a visibility timeout expires and the generation counter advances.
	Generation uint64 `json:"generation"`
}

// receiveAttemptEntry caches the result of a ReceiveRequestAttemptID request
// for the FIFO exactly-once-retry window (5 minutes per AWS spec).
type receiveAttemptEntry struct {
	expiresAt time.Time
	msgs      []*Message
}

// QueuePermissionEntry represents a single AddPermission statement on a queue.
type QueuePermissionEntry struct {
	AWSAccountIDs []string `json:"awsAccountIDs"`
	Actions       []string `json:"actions"`
}

// Queue represents an SQS queue.
//
// Field ordering trades a few bytes of padding for grouping related state
// together (purge cooldown vs dedup vs FIFO send-rate).
//

type Queue struct {
	lastPurgedAt        time.Time
	notify              chan struct{}
	deduplicationMsgIDs map[string]string
	Attributes          map[string]string
	Permissions         map[string]*QueuePermissionEntry
	fifoSendTimes       map[string][]time.Time
	receiveAttempts     map[string]*receiveAttemptEntry
	// inFlightByHandle indexes in-flight messages by receipt handle for O(1) delete (#56).
	inFlightByHandle    map[string]*InFlightMessage
	Tags                *tags.Tags
	DeduplicationIDs    map[string]time.Time
	dlq                 *Queue
	Name                string
	URL                 string
	Region              string
	messages            []*Message
	inFlightMessages    []*InFlightMessage
	// mu guards queue-level state independently of the backend-global mu (#55).
	mu                  sync.Mutex
	fifoSeqCounter      uint64
	receiveGeneration   uint64
	// delayedCount tracks messages in q.messages with VisibleAt > now (#59).
	// Approximate: may overcount until next mutation reconciles it.
	delayedCount        int
	MaxReceiveCount     int
	IsFIFO              bool
}

// fifoPerGroupTPS is the AWS-documented per-message-group send rate when
// FifoThroughputLimit=perMessageGroupId. SDKs receiving more than this on a
// single group get OverLimit and back off.
const fifoPerGroupTPS = 300

// QueueInfo holds the immutable-after-creation fields of a queue, returned by ListAll.
type QueueInfo struct {
	Name   string
	URL    string
	IsFIFO bool
}

// CreateQueueInput is the input for CreateQueue.
type CreateQueueInput struct {
	Attributes map[string]string
	Tags       map[string]string
	QueueName  string
	Endpoint   string
	// Scheme is the URL scheme ("http" or "https") to use when constructing
	// the queue URL. Defaults to "http" when empty for backwards
	// compatibility, but callers should pass "https" when the request was
	// served over TLS so the returned QueueURL matches AWS conventions.
	Scheme string
	// Region is the AWS region for ARN construction (optional; defaults to backend region).
	Region string
}

// CreateQueueOutput is the output for CreateQueue.
type CreateQueueOutput struct {
	QueueURL string
}

// DeleteQueueInput is the input for DeleteQueue.
type DeleteQueueInput struct {
	QueueURL string
	Region   string
}

// ListQueuesInput is the input for ListQueues.
type ListQueuesInput struct {
	QueueNamePrefix string
	NextToken       string
	Region          string
	MaxResults      int
}

// ListQueuesOutput is the output for ListQueues.
type ListQueuesOutput struct {
	NextToken string
	QueueURLs []string
}

// GetQueueURLInput is the input for GetQueueURL.
type GetQueueURLInput struct {
	QueueName string
	Region    string
}

// GetQueueURLOutput is the output for GetQueueURL.
type GetQueueURLOutput struct {
	QueueURL string
}

// GetQueueAttributesInput is the input for GetQueueAttributes.
type GetQueueAttributesInput struct {
	QueueURL       string
	Region         string
	AttributeNames []string
}

// GetQueueAttributesOutput is the output for GetQueueAttributes.
type GetQueueAttributesOutput struct {
	Attributes map[string]string
}

// SetQueueAttributesInput is the input for SetQueueAttributes.
type SetQueueAttributesInput struct {
	Attributes map[string]string
	QueueURL   string
	Region     string
}

// SendMessageInput is the input for SendMessage.
type SendMessageInput struct {
	MessageAttributes map[string]MessageAttributeValue
	// MessageSystemAttributes carries reserved system attributes from the
	// caller (currently only AWSTraceHeader). They are stored on the message
	// and surfaced via ReceiveMessage when the consumer asks for them by name.
	MessageSystemAttributes map[string]MessageAttributeValue
	QueueURL                string
	Region                  string
	MessageBody             string
	MessageGroupID          string
	MessageDeduplicationID  string
	DelaySeconds            int
}

// attrAWSTraceHeader is the single AWS-reserved system attribute SQS accepts
// today. Carries X-Ray trace context end-to-end across producers and consumers.
const attrAWSTraceHeader = "AWSTraceHeader"

// SendMessageOutput is the output for SendMessage.
type SendMessageOutput struct {
	MessageID              string
	MD5OfBody              string
	MD5OfMessageAttributes string
	SequenceNumber         string
}

// ReceiveMessageInput is the input for ReceiveMessage.
type ReceiveMessageInput struct {
	QueueURL string
	Region   string
	// ReceiveRequestAttemptID enables FIFO exactly-once retry: repeating a receive
	// with the same ID within 5 minutes returns the original message set.
	ReceiveRequestAttemptID string
	AttributeNames          []string
	MessageAttributeNames   []string
	MaxNumberOfMessages     int
	VisibilityTimeout       int
	WaitTimeSeconds         int
}

// ReceiveMessageOutput is the output for ReceiveMessage.
type ReceiveMessageOutput struct {
	Messages []*Message
}

// DeleteMessageInput is the input for DeleteMessage.
type DeleteMessageInput struct {
	QueueURL      string
	Region        string
	ReceiptHandle string
}

// ChangeMessageVisibilityInput is the input for ChangeMessageVisibility.
type ChangeMessageVisibilityInput struct {
	QueueURL          string
	Region            string
	ReceiptHandle     string
	VisibilityTimeout int
}

// SendMessageBatchEntry is a single entry in a SendMessageBatch request.
type SendMessageBatchEntry struct {
	MessageAttributes       map[string]MessageAttributeValue
	MessageSystemAttributes map[string]MessageAttributeValue
	ID                      string
	MessageBody             string
	MessageGroupID          string
	MessageDeduplicationID  string
	DelaySeconds            int
}

// SendMessageBatchInput is the input for SendMessageBatch.
type SendMessageBatchInput struct {
	QueueURL string
	Region   string
	Entries  []SendMessageBatchEntry
}

// SendMessageBatchResultEntry is a successful entry in a SendMessageBatch result.
type SendMessageBatchResultEntry struct {
	ID                     string
	MessageID              string
	MD5OfBody              string
	MD5OfMessageAttributes string
	SequenceNumber         string
}

// BatchResultErrorEntry is a failed entry in a batch result.
type BatchResultErrorEntry struct {
	ID          string
	Code        string
	Message     string
	SenderFault bool
}

// SendMessageBatchOutput is the output for SendMessageBatch.
type SendMessageBatchOutput struct {
	Successful []SendMessageBatchResultEntry
	Failed     []BatchResultErrorEntry
}

// DeleteMessageBatchEntry is a single entry in a DeleteMessageBatch request.
type DeleteMessageBatchEntry struct {
	ID            string
	ReceiptHandle string
}

// DeleteMessageBatchInput is the input for DeleteMessageBatch.
type DeleteMessageBatchInput struct {
	QueueURL string
	Region   string
	Entries  []DeleteMessageBatchEntry
}

// DeleteMessageBatchResultEntry is a successful entry in a DeleteMessageBatch result.
type DeleteMessageBatchResultEntry struct {
	ID string
}

// DeleteMessageBatchOutput is the output for DeleteMessageBatch.
type DeleteMessageBatchOutput struct {
	Successful []DeleteMessageBatchResultEntry
	Failed     []BatchResultErrorEntry
}

// PurgeQueueInput is the input for PurgeQueue.
type PurgeQueueInput struct {
	QueueURL string
	Region   string
}

// XMLResponseMetadata holds the request ID for all SQS XML responses.
type XMLResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// XMLAttribute represents a Name/Value pair in SQS XML responses.
type XMLAttribute struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

// XMLErrorDetail is an empty element in SQS error responses.
type XMLErrorDetail struct{}

// XMLError holds error information in an SQS error response.
type XMLError struct {
	Detail  XMLErrorDetail `xml:"Detail"`
	Type    string         `xml:"Type"`
	Code    string         `xml:"Code"`
	Message string         `xml:"Message"`
}

// XMLErrorResponse is the top-level SQS error response.
type XMLErrorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Error     XMLError `xml:"Error"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"RequestId"`
}

// CreateQueueResult holds the result of a CreateQueue operation.
type CreateQueueResult struct {
	QueueURL string `xml:"QueueUrl"`
}

// CreateQueueResponse is the XML response for CreateQueue.
type CreateQueueResponse struct {
	XMLName           xml.Name            `xml:"CreateQueueResponse"`
	CreateQueueResult CreateQueueResult   `xml:"CreateQueueResult"`
	ResponseMetadata  XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns             string              `xml:"xmlns,attr"`
}

// DeleteQueueResponse is the XML response for DeleteQueue.
type DeleteQueueResponse struct {
	XMLName          xml.Name            `xml:"DeleteQueueResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
}

// GetQueueURLResult holds the result of a GetQueueUrl operation.
type GetQueueURLResult struct {
	QueueURL string `xml:"QueueUrl"`
}

// GetQueueURLResponse is the XML response for GetQueueUrl.
type GetQueueURLResponse struct {
	XMLName           xml.Name            `xml:"GetQueueUrlResponse"`
	GetQueueURLResult GetQueueURLResult   `xml:"GetQueueUrlResult"`
	ResponseMetadata  XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns             string              `xml:"xmlns,attr"`
}

// ListQueuesResult holds the result of a ListQueues operation.
type ListQueuesResult struct {
	NextToken string   `xml:"NextToken,omitempty"`
	QueueURLs []string `xml:"QueueUrl"`
}

// ListQueuesResponse is the XML response for ListQueues.
type ListQueuesResponse struct {
	XMLName          xml.Name            `xml:"ListQueuesResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
	ListQueuesResult ListQueuesResult    `xml:"ListQueuesResult"`
}

// GetQueueAttributesResult holds the result of a GetQueueAttributes operation.
type GetQueueAttributesResult struct {
	Attributes []XMLAttribute `xml:"Attribute"`
}

// GetQueueAttributesResponse is the XML response for GetQueueAttributes.
type GetQueueAttributesResponse struct {
	XMLName                  xml.Name                 `xml:"GetQueueAttributesResponse"`
	ResponseMetadata         XMLResponseMetadata      `xml:"ResponseMetadata"`
	Xmlns                    string                   `xml:"xmlns,attr"`
	GetQueueAttributesResult GetQueueAttributesResult `xml:"GetQueueAttributesResult"`
}

// SetQueueAttributesResponse is the XML response for SetQueueAttributes.
type SetQueueAttributesResponse struct {
	XMLName          xml.Name            `xml:"SetQueueAttributesResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
}

// SendMessageResult holds the result of a SendMessage operation.
type SendMessageResult struct {
	MD5OfMessageBody       string `xml:"MD5OfMessageBody"`
	MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes,omitempty"`
	MessageID              string `xml:"MessageId"`
}

// SendMessageResponse is the XML response for SendMessage.
type SendMessageResponse struct {
	XMLName           xml.Name            `xml:"SendMessageResponse"`
	SendMessageResult SendMessageResult   `xml:"SendMessageResult"`
	ResponseMetadata  XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns             string              `xml:"xmlns,attr"`
}

// XMLMessage represents a message in a ReceiveMessage XML response.
type XMLMessage struct {
	MessageID     string         `xml:"MessageId"`
	ReceiptHandle string         `xml:"ReceiptHandle"`
	MD5OfBody     string         `xml:"MD5OfBody"`
	Body          string         `xml:"Body"`
	Attributes    []XMLAttribute `xml:"Attribute"`
}

// ReceiveMessageResult holds the result of a ReceiveMessage operation.
type ReceiveMessageResult struct {
	Messages []XMLMessage `xml:"Message"`
}

// ReceiveMessageResponse is the XML response for ReceiveMessage.
type ReceiveMessageResponse struct {
	XMLName              xml.Name             `xml:"ReceiveMessageResponse"`
	ResponseMetadata     XMLResponseMetadata  `xml:"ResponseMetadata"`
	Xmlns                string               `xml:"xmlns,attr"`
	ReceiveMessageResult ReceiveMessageResult `xml:"ReceiveMessageResult"`
}

// DeleteMessageResponse is the XML response for DeleteMessage.
type DeleteMessageResponse struct {
	XMLName          xml.Name            `xml:"DeleteMessageResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
}

// ChangeMessageVisibilityResponse is the XML response for ChangeMessageVisibility.
type ChangeMessageVisibilityResponse struct {
	XMLName          xml.Name            `xml:"ChangeMessageVisibilityResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
}

// XMLSendMessageBatchResultEntry is a successful batch send entry.
type XMLSendMessageBatchResultEntry struct {
	ID                     string `xml:"Id"`
	MessageID              string `xml:"MessageId"`
	MD5OfMessageBody       string `xml:"MD5OfMessageBody"`
	MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes,omitempty"`
}

// XMLSendMessageBatchFailedEntry is a failed batch send entry.
type XMLSendMessageBatchFailedEntry struct {
	ID          string `xml:"Id"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
	SenderFault bool   `xml:"SenderFault"`
}

// XMLSendMessageBatchResult holds the result of a SendMessageBatch operation.
type XMLSendMessageBatchResult struct {
	Successful []XMLSendMessageBatchResultEntry `xml:"SendMessageBatchResultEntry"`
	Failed     []XMLSendMessageBatchFailedEntry `xml:"BatchResultErrorEntry"`
}

// SendMessageBatchResponse is the XML response for SendMessageBatch.
type SendMessageBatchResponse struct {
	XMLName                xml.Name                  `xml:"SendMessageBatchResponse"`
	ResponseMetadata       XMLResponseMetadata       `xml:"ResponseMetadata"`
	Xmlns                  string                    `xml:"xmlns,attr"`
	SendMessageBatchResult XMLSendMessageBatchResult `xml:"SendMessageBatchResult"`
}

// XMLDeleteMessageBatchResultEntry is a successful batch delete entry.
type XMLDeleteMessageBatchResultEntry struct {
	ID string `xml:"Id"`
}

// XMLDeleteMessageBatchFailedEntry is a failed batch delete entry.
type XMLDeleteMessageBatchFailedEntry struct {
	ID          string `xml:"Id"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
	SenderFault bool   `xml:"SenderFault"`
}

// XMLDeleteMessageBatchResult holds the result of a DeleteMessageBatch operation.
type XMLDeleteMessageBatchResult struct {
	Successful []XMLDeleteMessageBatchResultEntry `xml:"DeleteMessageBatchResultEntry"`
	Failed     []XMLDeleteMessageBatchFailedEntry `xml:"BatchResultErrorEntry"`
}

// DeleteMessageBatchResponse is the XML response for DeleteMessageBatch.
type DeleteMessageBatchResponse struct {
	XMLName                  xml.Name                    `xml:"DeleteMessageBatchResponse"`
	ResponseMetadata         XMLResponseMetadata         `xml:"ResponseMetadata"`
	Xmlns                    string                      `xml:"xmlns,attr"`
	DeleteMessageBatchResult XMLDeleteMessageBatchResult `xml:"DeleteMessageBatchResult"`
}

// PurgeQueueResponse is the XML response for PurgeQueue.
type PurgeQueueResponse struct {
	XMLName          xml.Name            `xml:"PurgeQueueResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
}

// TagQueueInput holds the input for TagQueue.
type TagQueueInput struct {
	Tags     *tags.Tags
	QueueURL string
	Region   string
}

// UntagQueueInput holds the input for UntagQueue.
type UntagQueueInput struct {
	QueueURL string
	Region   string
	TagKeys  []string
}

// ListQueueTagsInput holds the input for ListQueueTags.
type ListQueueTagsInput struct {
	QueueURL string
	Region   string
}

// ListQueueTagsOutput holds the result of ListQueueTags.
type ListQueueTagsOutput struct {
	Tags *tags.Tags
}

// ListDeadLetterSourceQueuesInput is the input for ListDeadLetterSourceQueues.
type ListDeadLetterSourceQueuesInput struct {
	QueueURL   string
	Region     string
	NextToken  string
	MaxResults int
}

// ListDeadLetterSourceQueuesOutput holds the result of ListDeadLetterSourceQueues.
type ListDeadLetterSourceQueuesOutput struct {
	NextToken string
	QueueURLs []string
}

// TagEntry is a single key/value tag pair in an XML response.
type TagEntry struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// ListQueueTagsResult is the XML body for ListQueueTagsResponse.
type ListQueueTagsResult struct {
	Tags []TagEntry `xml:"Tag"`
}

// ListQueueTagsResponse is the XML envelope for ListQueueTags.
type ListQueueTagsResponse struct {
	XMLName          xml.Name            `xml:"ListQueueTagsResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
	Result           ListQueueTagsResult `xml:"ListQueueTagsResult"`
}

// TagQueueResponse is the XML response for TagQueue.
type TagQueueResponse struct {
	XMLName          xml.Name            `xml:"TagQueueResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
}

// UntagQueueResponse is the XML response for UntagQueue.
type UntagQueueResponse struct {
	XMLName          xml.Name            `xml:"UntagQueueResponse"`
	ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
	Xmlns            string              `xml:"xmlns,attr"`
}

// ChangeMessageVisibilityBatchRequestEntry is one item in a batch visibility change.
type ChangeMessageVisibilityBatchRequestEntry struct {
	ID                string
	ReceiptHandle     string
	VisibilityTimeout int
}

// ChangeMessageVisibilityBatchInput holds input for ChangeMessageVisibilityBatch.
type ChangeMessageVisibilityBatchInput struct {
	QueueURL string
	Region   string
	Entries  []ChangeMessageVisibilityBatchRequestEntry
}

// BatchResultEntry is a successful batch result entry.
type BatchResultEntry struct {
	ID string `xml:"Id"`
}

// BatchErrorEntry is a failed batch result entry.
type BatchErrorEntry struct {
	ID          string `xml:"Id"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
	SenderFault bool   `xml:"SenderFault"`
}

// ChangeMessageVisibilityBatchOutput holds the result of ChangeMessageVisibilityBatch.
type ChangeMessageVisibilityBatchOutput struct {
	Successful []BatchResultEntry
	Failed     []BatchErrorEntry
}

// ChangeMessageVisibilityBatchResult is the XML body.
type ChangeMessageVisibilityBatchResult struct {
	Successful []BatchResultEntry `xml:"ChangeMessageVisibilityBatchResultEntry"`
	Failed     []BatchErrorEntry  `xml:"BatchResultErrorEntry"`
}

// ChangeMessageVisibilityBatchResponse is the XML envelope.
type ChangeMessageVisibilityBatchResponse struct {
	XMLName          xml.Name                           `xml:"ChangeMessageVisibilityBatchResponse"`
	ResponseMetadata XMLResponseMetadata                `xml:"ResponseMetadata"`
	Xmlns            string                             `xml:"xmlns,attr"`
	Result           ChangeMessageVisibilityBatchResult `xml:"ChangeMessageVisibilityBatchResult"`
}

// MoveTaskStatus represents the lifecycle state of a StartMessageMoveTask operation.
type MoveTaskStatus string

const (
	// MoveTaskStatusRunning indicates the task is actively moving messages.
	MoveTaskStatusRunning MoveTaskStatus = "RUNNING"
	// MoveTaskStatusCompleted indicates the task finished successfully.
	MoveTaskStatusCompleted MoveTaskStatus = "COMPLETED"
	// MoveTaskStatusCancelling indicates the task has been asked to cancel.
	MoveTaskStatusCancelling MoveTaskStatus = "CANCELLING"
	// MoveTaskStatusCancelled indicates the task was cancelled.
	MoveTaskStatusCancelled MoveTaskStatus = "CANCELLED"
	// MoveTaskStatusFailed indicates the task failed.
	MoveTaskStatusFailed MoveTaskStatus = "FAILED"
)

// MessageMoveTask describes the state of a single message move task.
type MessageMoveTask struct {
	// ApproximateNumberOfMessagesToMove is the total messages when the task started.
	// nil when not yet determined.
	ApproximateNumberOfMessagesToMove *int64
	// MaxNumberOfMessagesPerSecond is nil when no rate limit was set.
	MaxNumberOfMessagesPerSecond *int32
	// FailureReason is populated when Status is FAILED.
	FailureReason  *string
	TaskHandle     string
	SourceArn      string
	DestinationArn string
	Status         MoveTaskStatus
	// ApproximateNumberOfMessagesMoved is always present (not a pointer), matching
	// the AWS SDK ListMessageMoveTasksResultEntry.ApproximateNumberOfMessagesMoved.
	ApproximateNumberOfMessagesMoved int64
	// StartedTimestamp is always present (Unix epoch ms), matching the AWS SDK.
	StartedTimestamp int64
}

// AddPermissionInput is the input for AddPermission.
type AddPermissionInput struct {
	QueueURL      string
	Region        string
	Label         string
	Actions       []string
	AWSAccountIDs []string
}

// RemovePermissionInput is the input for RemovePermission.
type RemovePermissionInput struct {
	QueueURL string
	Region   string
	Label    string
}

// StartMessageMoveTaskInput is the input for StartMessageMoveTask.
type StartMessageMoveTaskInput struct {
	SourceArn                    string
	DestinationArn               string
	MaxNumberOfMessagesPerSecond int32
}

// StartMessageMoveTaskOutput is the output for StartMessageMoveTask.
type StartMessageMoveTaskOutput struct {
	TaskHandle string
}

// CancelMessageMoveTaskInput is the input for CancelMessageMoveTask.
type CancelMessageMoveTaskInput struct {
	TaskHandle string
}

// CancelMessageMoveTaskOutput is the output for CancelMessageMoveTask.
type CancelMessageMoveTaskOutput struct {
	ApproximateNumberOfMessagesMoved int64
}

// ListMessageMoveTasksInput is the input for ListMessageMoveTasks.
type ListMessageMoveTasksInput struct {
	SourceArn  string
	MaxResults int32
}

// ListMessageMoveTasksOutput is the output for ListMessageMoveTasks.
type ListMessageMoveTasksOutput struct {
	Results []MessageMoveTask
}
