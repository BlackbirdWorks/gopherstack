package sns

import "encoding/xml"

// Topic represents an SNS topic.
type Topic struct {
	Attributes map[string]string `json:"attributes,omitempty"`
	TopicArn   string            `json:"topicArn"`
}

// Subscription represents an SNS subscription.
type Subscription struct {
	SubscriptionArn     string `json:"subscriptionArn"`
	TopicArn            string `json:"topicArn"`
	Protocol            string `json:"protocol"`
	Endpoint            string `json:"endpoint"`
	Owner               string `json:"owner"`
	FilterPolicy        string `json:"filterPolicy,omitempty"`
	PendingConfirmation bool   `json:"pendingConfirmation"`
}

// Message represents a published SNS message.
type Message struct {
	Attributes map[string]MessageAttribute
	MessageID  string
	Body       string
	Subject    string
	TopicArn   string
}

// MessageAttribute represents a single message attribute value.
type MessageAttribute struct {
	DataType    string
	StringValue string
}

// ResponseMetadata is included in all SNS XML responses.
type ResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// ErrorResponse represents an SNS XML error response.
type ErrorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Error     Error    `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

// Error contains the error details within an ErrorResponse.
type Error struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// XMLTopic is the XML representation of a topic in list responses.
type XMLTopic struct {
	TopicArn string `xml:"TopicArn"`
}

// XMLSubscription is the XML representation of a subscription in list responses.
type XMLSubscription struct {
	TopicArn        string `xml:"TopicArn"`
	Protocol        string `xml:"Protocol"`
	SubscriptionArn string `xml:"SubscriptionArn"`
	Owner           string `xml:"Owner"`
	Endpoint        string `xml:"Endpoint"`
}

// XMLAttributeEntry is a key-value attribute pair in XML attribute maps.
type XMLAttributeEntry struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

// XMLPublishBatchSuccessEntry represents a successfully published batch entry.
type XMLPublishBatchSuccessEntry struct {
	MessageID string `xml:"MessageId"`
	ID        string `xml:"Id"`
}

// XMLPublishBatchFailEntry represents a failed batch entry.
type XMLPublishBatchFailEntry struct {
	ID          string `xml:"Id"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
	SenderFault bool   `xml:"SenderFault"`
}

// CreateTopicResult holds the result of a CreateTopic operation.
type CreateTopicResult struct {
	TopicArn string `xml:"TopicArn"`
}

// CreateTopicResponse is the XML response for CreateTopic.
type CreateTopicResponse struct {
	XMLName           xml.Name          `xml:"https://sns.amazonaws.com/doc/2010-03-31/ CreateTopicResponse"`
	CreateTopicResult CreateTopicResult `xml:"CreateTopicResult"`
	ResponseMetadata  ResponseMetadata  `xml:"ResponseMetadata"`
}

// DeleteTopicResponse is the XML response for DeleteTopic.
type DeleteTopicResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ DeleteTopicResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListTopicsResult holds the result of a ListTopics operation.
type ListTopicsResult struct {
	NextToken string     `xml:"NextToken"`
	Topics    []XMLTopic `xml:"Topics>member"`
}

// ListTopicsResponse is the XML response for ListTopics.
type ListTopicsResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListTopicsResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	ListTopicsResult ListTopicsResult `xml:"ListTopicsResult"`
}

// GetTopicAttributesResult holds the result of a GetTopicAttributes operation.
type GetTopicAttributesResult struct {
	Attributes []XMLAttributeEntry `xml:"Attributes>entry"`
}

// GetTopicAttributesResponse is the XML response for GetTopicAttributes.
type GetTopicAttributesResponse struct {
	XMLName                  xml.Name                 `xml:"https://sns.amazonaws.com/doc/2010-03-31/ GetTopicAttributesResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata         ResponseMetadata         `xml:"ResponseMetadata"`
	GetTopicAttributesResult GetTopicAttributesResult `xml:"GetTopicAttributesResult"`
}

// SetTopicAttributesResponse is the XML response for SetTopicAttributes.
type SetTopicAttributesResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ SetTopicAttributesResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// SubscribeResult holds the result of a Subscribe operation.
type SubscribeResult struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
}

// SubscribeResponse is the XML response for Subscribe.
type SubscribeResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ SubscribeResponse"`
	SubscribeResult  SubscribeResult  `xml:"SubscribeResult"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// UnsubscribeResponse is the XML response for Unsubscribe.
type UnsubscribeResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ UnsubscribeResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListSubscriptionsResult holds the result of a ListSubscriptions operation.
type ListSubscriptionsResult struct {
	NextToken     string            `xml:"NextToken"`
	Subscriptions []XMLSubscription `xml:"Subscriptions>member"`
}

// ListSubscriptionsResponse is the XML response for ListSubscriptions.
type ListSubscriptionsResponse struct {
	XMLName                 xml.Name                `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListSubscriptionsResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata        ResponseMetadata        `xml:"ResponseMetadata"`
	ListSubscriptionsResult ListSubscriptionsResult `xml:"ListSubscriptionsResult"`
}

// ListSubscriptionsByTopicResult holds the result of a ListSubscriptionsByTopic operation.
type ListSubscriptionsByTopicResult struct {
	NextToken     string            `xml:"NextToken"`
	Subscriptions []XMLSubscription `xml:"Subscriptions>member"`
}

// ListSubscriptionsByTopicResponse is the XML response for ListSubscriptionsByTopic.
type ListSubscriptionsByTopicResponse struct {
	XMLName                        xml.Name                       `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListSubscriptionsByTopicResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
	ListSubscriptionsByTopicResult ListSubscriptionsByTopicResult `xml:"ListSubscriptionsByTopicResult"`
}

// PublishResult holds the result of a Publish operation.
type PublishResult struct {
	MessageID string `xml:"MessageId"`
}

// PublishResponse is the XML response for Publish.
type PublishResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ PublishResponse"`
	PublishResult    PublishResult    `xml:"PublishResult"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// PublishBatchResult holds the result of a PublishBatch operation.
type PublishBatchResult struct {
	Successful []XMLPublishBatchSuccessEntry `xml:"Successful>member"`
	Failed     []XMLPublishBatchFailEntry    `xml:"Failed>member"`
}

// PublishBatchResponse is the XML response for PublishBatch.
type PublishBatchResponse struct {
	XMLName            xml.Name           `xml:"https://sns.amazonaws.com/doc/2010-03-31/ PublishBatchResponse"`
	ResponseMetadata   ResponseMetadata   `xml:"ResponseMetadata"`
	PublishBatchResult PublishBatchResult `xml:"PublishBatchResult"`
}

// ConfirmSubscriptionResult holds the subscription ARN after confirmation.
type ConfirmSubscriptionResult struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
}

// ConfirmSubscriptionResponse is the XML response for ConfirmSubscription.
type ConfirmSubscriptionResponse struct {
	XMLName                   xml.Name                  `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ConfirmSubscriptionResponse"` //nolint:lll // XML namespace makes this line long.
	ConfirmSubscriptionResult ConfirmSubscriptionResult `xml:"ConfirmSubscriptionResult"`
	ResponseMetadata          ResponseMetadata          `xml:"ResponseMetadata"`
}

// GetSubscriptionAttributesResult holds the attributes of a subscription.
type GetSubscriptionAttributesResult struct {
	Attributes []XMLAttributeEntry `xml:"Attributes>entry"`
}

// GetSubscriptionAttributesResponse is the XML response for GetSubscriptionAttributes.
type GetSubscriptionAttributesResponse struct {
	XMLName                         xml.Name                        `xml:"https://sns.amazonaws.com/doc/2010-03-31/ GetSubscriptionAttributesResponse"` //nolint:lll // XML namespace
	ResponseMetadata                ResponseMetadata                `xml:"ResponseMetadata"`
	GetSubscriptionAttributesResult GetSubscriptionAttributesResult `xml:"GetSubscriptionAttributesResult"`
}
