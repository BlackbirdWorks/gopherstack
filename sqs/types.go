package sqs

import (
	"encoding/xml"
	"time"
)

const (
	sqsNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"
	accountID    = "000000000000"
	sqsRegion    = "us-east-1"
	fifoSuffix   = ".fifo"

	defaultVisibilityTimeout      = 30
	defaultMaxMessageSize         = 262144
	defaultMessageRetentionPeriod = 345600
	defaultDelaySeconds           = 0
	defaultWaitTimeSeconds        = 0
	maxBatchSize                  = 10
	deduplicationWindowSecs       = 300
	longPollIntervalMs            = 100

	maxParseIterations = 20
	noVisibilitySet    = -1

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
	attrApproxMessagesDelayed     = "ApproximateNumberOfMessagesDelayed"
	attrAll                       = "All"

	attrApproxReceiveCount          = "ApproximateReceiveCount"
	attrSentTimestamp               = "SentTimestamp"
	attrApproxFirstReceiveTimestamp = "ApproximateFirstReceiveTimestamp"

	attrValTrue  = "true"
	attrValFalse = "false"
	attrValZero  = "0"

	errTypeSender = "Sender"
)

// MessageAttributeValue holds a message attribute value.
type MessageAttributeValue struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

// Message represents an SQS message.
type Message struct {
	MessageAttributes                map[string]MessageAttributeValue
	Attributes                       map[string]string
	Body                             string
	MessageGroupID                   string
	MessageDeduplicationID           string
	MessageID                        string
	ReceiptHandle                    string
	MD5OfBody                        string
	SentTimestamp                    int64
	ApproximateFirstReceiveTimestamp int64 // Unix ms; 0 means never received
	ApproximateReceiveCount          int
}

// InFlightMessage wraps a message that has been received but not deleted.
type InFlightMessage struct {
	VisibleAt     time.Time
	Msg           *Message
	ReceiptHandle string
}

// Queue represents an SQS queue.
type Queue struct {
	deduplicationMsgIDs map[string]string
	DeduplicationIDs    map[string]time.Time
	Attributes          map[string]string
	Tags                map[string]string
	dlq                 *Queue // resolved DLQ queue pointer; nil = no DLQ
	Name                string
	URL                 string
	messages            []*Message
	inFlightMessages    []*InFlightMessage
	MaxReceiveCount     int // 0 = no DLQ
	IsFIFO              bool
}

// QueueInfo holds the immutable-after-creation fields of a queue, returned by ListAll.
type QueueInfo struct {
	Name   string
	URL    string
	IsFIFO bool
}

// CreateQueueInput is the input for CreateQueue.
type CreateQueueInput struct {
	Attributes map[string]string
	QueueName  string
	Endpoint   string
}

// CreateQueueOutput is the output for CreateQueue.
type CreateQueueOutput struct {
	QueueURL string
}

// DeleteQueueInput is the input for DeleteQueue.
type DeleteQueueInput struct {
	QueueURL string
}

// ListQueuesInput is the input for ListQueues.
type ListQueuesInput struct {
	QueueNamePrefix string
}

// ListQueuesOutput is the output for ListQueues.
type ListQueuesOutput struct {
	QueueURLs []string
}

// GetQueueURLInput is the input for GetQueueURL.
type GetQueueURLInput struct {
	QueueName string
}

// GetQueueURLOutput is the output for GetQueueURL.
type GetQueueURLOutput struct {
	QueueURL string
}

// GetQueueAttributesInput is the input for GetQueueAttributes.
type GetQueueAttributesInput struct {
	QueueURL       string
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
}

// SendMessageInput is the input for SendMessage.
type SendMessageInput struct {
	MessageAttributes      map[string]MessageAttributeValue
	QueueURL               string
	MessageBody            string
	MessageGroupID         string
	MessageDeduplicationID string
	DelaySeconds           int
}

// SendMessageOutput is the output for SendMessage.
type SendMessageOutput struct {
	MessageID string
	MD5OfBody string
}

// ReceiveMessageInput is the input for ReceiveMessage.
type ReceiveMessageInput struct {
	QueueURL            string
	AttributeNames      []string
	MaxNumberOfMessages int
	VisibilityTimeout   int
	WaitTimeSeconds     int
}

// ReceiveMessageOutput is the output for ReceiveMessage.
type ReceiveMessageOutput struct {
	Messages []*Message
}

// DeleteMessageInput is the input for DeleteMessage.
type DeleteMessageInput struct {
	QueueURL      string
	ReceiptHandle string
}

// ChangeMessageVisibilityInput is the input for ChangeMessageVisibility.
type ChangeMessageVisibilityInput struct {
	QueueURL          string
	ReceiptHandle     string
	VisibilityTimeout int
}

// SendMessageBatchEntry is a single entry in a SendMessageBatch request.
type SendMessageBatchEntry struct {
	MessageAttributes      map[string]MessageAttributeValue
	ID                     string
	MessageBody            string
	MessageGroupID         string
	MessageDeduplicationID string
	DelaySeconds           int
}

// SendMessageBatchInput is the input for SendMessageBatch.
type SendMessageBatchInput struct {
	QueueURL string
	Entries  []SendMessageBatchEntry
}

// SendMessageBatchResultEntry is a successful entry in a SendMessageBatch result.
type SendMessageBatchResultEntry struct {
	ID        string
	MessageID string
	MD5OfBody string
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
	MD5OfMessageBody string `xml:"MD5OfMessageBody"`
	MessageID        string `xml:"MessageId"`
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
	ID               string `xml:"Id"`
	MessageID        string `xml:"MessageId"`
	MD5OfMessageBody string `xml:"MD5OfMessageBody"`
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
	Tags     map[string]string
	QueueURL string
}

// UntagQueueInput holds the input for UntagQueue.
type UntagQueueInput struct {
	QueueURL string
	TagKeys  []string
}

// ListQueueTagsInput holds the input for ListQueueTags.
type ListQueueTagsInput struct {
	QueueURL string
}

// ListQueueTagsOutput holds the result of ListQueueTags.
type ListQueueTagsOutput struct {
	Tags map[string]string
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
