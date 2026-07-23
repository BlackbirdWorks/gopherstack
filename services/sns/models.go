package sns

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TopicPermission represents a single permission (statement) added to an SNS topic policy.
type TopicPermission struct {
	Label      string   `json:"label"`
	AWSAccount []string `json:"awsAccount"`
	Actions    []string `json:"actions"`
}

// SandboxPhoneNumber represents a phone number registered in the SMS sandbox.
type SandboxPhoneNumber struct {
	CreationTimestamp time.Time `json:"creationTimestamp"`
	PhoneNumber       string    `json:"phoneNumber"`
	LanguageCode      string    `json:"languageCode,omitempty"`
	Status            string    `json:"status"` // "Pending" or "Verified"
}

// Topic represents an SNS topic.
type Topic struct {
	CreationTimestamp time.Time                   `json:"creationTimestamp"`
	Attributes        map[string]string           `json:"attributes,omitempty"`
	Permissions       map[string]*TopicPermission `json:"permissions,omitempty"`
	TopicArn          string                      `json:"topicArn"`
}

// Subscription represents an SNS subscription.
type Subscription struct {
	CreationTimestamp   time.Time `json:"creationTimestamp"`
	parsedFilterPolicy  parsedFilterPolicy
	RedrivePolicy       string `json:"redrivePolicy,omitempty"`
	DeliveryPolicy      string `json:"deliveryPolicy,omitempty"`
	ReplayPolicy        string `json:"replayPolicy,omitempty"`
	Endpoint            string `json:"endpoint"`
	Owner               string `json:"owner"`
	TopicArn            string `json:"topicArn"`
	Protocol            string `json:"protocol"`
	SubscriptionRoleArn string `json:"subscriptionRoleArn,omitempty"`
	FilterPolicyScope   string `json:"filterPolicyScope,omitempty"`
	SubscriptionArn     string `json:"subscriptionArn"`
	FilterPolicy        string `json:"filterPolicy,omitempty"`
	RawMessageDelivery  bool   `json:"rawMessageDelivery,omitempty"`
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
// SequenceNumber is only populated for FIFO topic batch entries.
type XMLPublishBatchSuccessEntry struct {
	MessageID      string `xml:"MessageId"`
	SequenceNumber string `xml:"SequenceNumber,omitempty"`
	ID             string `xml:"Id"`
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
	NextToken string     `xml:"NextToken,omitempty"`
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
	NextToken     string            `xml:"NextToken,omitempty"`
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
	NextToken     string            `xml:"NextToken,omitempty"`
	Subscriptions []XMLSubscription `xml:"Subscriptions>member"`
}

// ListSubscriptionsByTopicResponse is the XML response for ListSubscriptionsByTopic.
type ListSubscriptionsByTopicResponse struct {
	XMLName                        xml.Name                       `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListSubscriptionsByTopicResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
	ListSubscriptionsByTopicResult ListSubscriptionsByTopicResult `xml:"ListSubscriptionsByTopicResult"`
}

// PublishResult holds the result of a Publish operation.
// SequenceNumber is only populated for FIFO topics; it is a monotonically
// increasing 20-digit zero-padded integer unique within the topic.
type PublishResult struct {
	MessageID      string `xml:"MessageId"`
	SequenceNumber string `xml:"SequenceNumber,omitempty"`
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

// SetSubscriptionAttributesResponse is the XML response for SetSubscriptionAttributes.
type SetSubscriptionAttributesResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ SetSubscriptionAttributesResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// PlatformApplication represents an SNS platform application for mobile push.
type PlatformApplication struct {
	CreationTimestamp      time.Time         `json:"creationTimestamp"`
	Attributes             map[string]string `json:"attributes,omitempty"`
	PlatformApplicationArn string            `json:"platformApplicationArn"`
}

// PlatformEndpoint represents an SNS platform endpoint (device token registration).
type PlatformEndpoint struct {
	CreationTimestamp      time.Time         `json:"creationTimestamp"`
	Attributes             map[string]string `json:"attributes,omitempty"`
	EndpointArn            string            `json:"endpointArn"`
	PlatformApplicationArn string            `json:"platformApplicationArn"`
}

// XMLPlatformApplication is the XML representation of a platform application in list responses.
type XMLPlatformApplication struct {
	PlatformApplicationArn string              `xml:"PlatformApplicationArn"`
	Attributes             []XMLAttributeEntry `xml:"Attributes>entry"`
}

// XMLPlatformEndpoint is the XML representation of a platform endpoint in list responses.
type XMLPlatformEndpoint struct {
	EndpointArn string              `xml:"EndpointArn"`
	Attributes  []XMLAttributeEntry `xml:"Attributes>entry"`
}

// CreatePlatformApplicationResult holds the result of a CreatePlatformApplication operation.
type CreatePlatformApplicationResult struct {
	PlatformApplicationArn string `xml:"PlatformApplicationArn"`
}

// CreatePlatformApplicationResponse is the XML response for CreatePlatformApplication.
type CreatePlatformApplicationResponse struct {
	XMLName                         xml.Name                        `xml:"https://sns.amazonaws.com/doc/2010-03-31/ CreatePlatformApplicationResponse"` //nolint:lll // XML namespace makes this line long.
	CreatePlatformApplicationResult CreatePlatformApplicationResult `xml:"CreatePlatformApplicationResult"`
	ResponseMetadata                ResponseMetadata                `xml:"ResponseMetadata"`
}

// GetPlatformApplicationAttributesResult holds the result of GetPlatformApplicationAttributes.
type GetPlatformApplicationAttributesResult struct {
	Attributes []XMLAttributeEntry `xml:"Attributes>entry"`
}

// GetPlatformApplicationAttributesResponse is the XML response for GetPlatformApplicationAttributes.
type GetPlatformApplicationAttributesResponse struct {
	XMLName                                xml.Name                               `xml:"https://sns.amazonaws.com/doc/2010-03-31/ GetPlatformApplicationAttributesResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata                       ResponseMetadata                       `xml:"ResponseMetadata"`
	GetPlatformApplicationAttributesResult GetPlatformApplicationAttributesResult `xml:"GetPlatformApplicationAttributesResult"` //nolint:lll // Long type name.
}

// SetPlatformApplicationAttributesResponse is the XML response for SetPlatformApplicationAttributes.
type SetPlatformApplicationAttributesResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ SetPlatformApplicationAttributesResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListPlatformApplicationsResult holds the result of ListPlatformApplications.
type ListPlatformApplicationsResult struct {
	NextToken            string                   `xml:"NextToken,omitempty"`
	PlatformApplications []XMLPlatformApplication `xml:"PlatformApplications>member"`
}

// ListPlatformApplicationsResponse is the XML response for ListPlatformApplications.
type ListPlatformApplicationsResponse struct {
	XMLName                        xml.Name                       `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListPlatformApplicationsResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
	ListPlatformApplicationsResult ListPlatformApplicationsResult `xml:"ListPlatformApplicationsResult"`
}

// DeletePlatformApplicationResponse is the XML response for DeletePlatformApplication.
type DeletePlatformApplicationResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ DeletePlatformApplicationResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// CreatePlatformEndpointResult holds the result of CreatePlatformEndpoint.
type CreatePlatformEndpointResult struct {
	EndpointArn string `xml:"EndpointArn"`
}

// CreatePlatformEndpointResponse is the XML response for CreatePlatformEndpoint.
type CreatePlatformEndpointResponse struct {
	XMLName                      xml.Name                     `xml:"https://sns.amazonaws.com/doc/2010-03-31/ CreatePlatformEndpointResponse"` //nolint:lll // XML namespace makes this line long.
	CreatePlatformEndpointResult CreatePlatformEndpointResult `xml:"CreatePlatformEndpointResult"`
	ResponseMetadata             ResponseMetadata             `xml:"ResponseMetadata"`
}

// GetEndpointAttributesResult holds the result of GetEndpointAttributes.
type GetEndpointAttributesResult struct {
	Attributes []XMLAttributeEntry `xml:"Attributes>entry"`
}

// GetEndpointAttributesResponse is the XML response for GetEndpointAttributes.
type GetEndpointAttributesResponse struct {
	XMLName                     xml.Name                    `xml:"https://sns.amazonaws.com/doc/2010-03-31/ GetEndpointAttributesResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata            ResponseMetadata            `xml:"ResponseMetadata"`
	GetEndpointAttributesResult GetEndpointAttributesResult `xml:"GetEndpointAttributesResult"`
}

// SetEndpointAttributesResponse is the XML response for SetEndpointAttributes.
type SetEndpointAttributesResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ SetEndpointAttributesResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListEndpointsByPlatformApplicationResult holds the result of ListEndpointsByPlatformApplication.
type ListEndpointsByPlatformApplicationResult struct {
	NextToken string                `xml:"NextToken,omitempty"`
	Endpoints []XMLPlatformEndpoint `xml:"Endpoints>member"`
}

// ListEndpointsByPlatformApplicationResponse is the XML response for ListEndpointsByPlatformApplication.
type ListEndpointsByPlatformApplicationResponse struct {
	XMLName                                  xml.Name                                 `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListEndpointsByPlatformApplicationResponse"` //nolint:lll // XML namespace makes this line long.
	ResponseMetadata                         ResponseMetadata                         `xml:"ResponseMetadata"`
	ListEndpointsByPlatformApplicationResult ListEndpointsByPlatformApplicationResult `xml:"ListEndpointsByPlatformApplicationResult"` //nolint:lll // Long type name.
}

// DeleteEndpointResponse is the XML response for DeleteEndpoint.
type DeleteEndpointResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ DeleteEndpointResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// AddPermissionResponse is the XML response for AddPermission.
type AddPermissionResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ AddPermissionResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// CheckIfPhoneNumberIsOptedOutResult holds the result of CheckIfPhoneNumberIsOptedOut.
type CheckIfPhoneNumberIsOptedOutResult struct {
	IsOptedOut bool `xml:"isOptedOut"`
}

// CheckIfPhoneNumberIsOptedOutResponse is the XML response for CheckIfPhoneNumberIsOptedOut.
type CheckIfPhoneNumberIsOptedOutResponse struct {
	XMLName                            xml.Name                           `xml:"https://sns.amazonaws.com/doc/2010-03-31/ CheckIfPhoneNumberIsOptedOutResponse"` //nolint:lll // XML namespace.
	ResponseMetadata                   ResponseMetadata                   `xml:"ResponseMetadata"`
	CheckIfPhoneNumberIsOptedOutResult CheckIfPhoneNumberIsOptedOutResult `xml:"CheckIfPhoneNumberIsOptedOutResult"`
}

// CreateSMSSandboxPhoneNumberResponse is the XML response for CreateSMSSandboxPhoneNumber.
type CreateSMSSandboxPhoneNumberResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ CreateSMSSandboxPhoneNumberResponse"` //nolint:lll // XML namespace.
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DeleteSMSSandboxPhoneNumberResponse is the XML response for DeleteSMSSandboxPhoneNumber.
type DeleteSMSSandboxPhoneNumberResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ DeleteSMSSandboxPhoneNumberResponse"` //nolint:lll // XML namespace.
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetDataProtectionPolicyResult holds the result of GetDataProtectionPolicy.
type GetDataProtectionPolicyResult struct {
	DataProtectionPolicy string `xml:"DataProtectionPolicy"`
}

// GetDataProtectionPolicyResponse is the XML response for GetDataProtectionPolicy.
type GetDataProtectionPolicyResponse struct {
	XMLName                       xml.Name                      `xml:"https://sns.amazonaws.com/doc/2010-03-31/ GetDataProtectionPolicyResponse"` //nolint:lll // XML namespace.
	GetDataProtectionPolicyResult GetDataProtectionPolicyResult `xml:"GetDataProtectionPolicyResult"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
}

// GetSMSAttributesResult holds the result of GetSMSAttributes.
type GetSMSAttributesResult struct {
	Attributes []XMLAttributeEntry `xml:"attributes>entry"`
}

// GetSMSAttributesResponse is the XML response for GetSMSAttributes.
type GetSMSAttributesResponse struct {
	XMLName                xml.Name               `xml:"https://sns.amazonaws.com/doc/2010-03-31/ GetSMSAttributesResponse"` //nolint:lll // XML namespace.
	ResponseMetadata       ResponseMetadata       `xml:"ResponseMetadata"`
	GetSMSAttributesResult GetSMSAttributesResult `xml:"GetSMSAttributesResult"`
}

// GetSMSSandboxAccountStatusResult holds the result of GetSMSSandboxAccountStatus.
type GetSMSSandboxAccountStatusResult struct {
	IsInSandbox bool `xml:"IsInSandbox"`
}

// GetSMSSandboxAccountStatusResponse is the XML response for GetSMSSandboxAccountStatus.
type GetSMSSandboxAccountStatusResponse struct {
	XMLName                          xml.Name                         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ GetSMSSandboxAccountStatusResponse"` //nolint:lll // XML namespace.
	ResponseMetadata                 ResponseMetadata                 `xml:"ResponseMetadata"`
	GetSMSSandboxAccountStatusResult GetSMSSandboxAccountStatusResult `xml:"GetSMSSandboxAccountStatusResult"`
}

// XMLOriginationPhone is the XML representation of an origination phone number.
type XMLOriginationPhone struct {
	PhoneNumber        string   `xml:"PhoneNumber"`
	Iso2CountryCode    string   `xml:"Iso2CountryCode"`
	RouteType          string   `xml:"RouteType"`
	NumberCapabilities []string `xml:"NumberCapabilities>member"`
}

// ListOriginationNumbersResult holds the result of ListOriginationNumbers.
type ListOriginationNumbersResult struct {
	NextToken    string                `xml:"NextToken,omitempty"`
	PhoneNumbers []XMLOriginationPhone `xml:"PhoneNumbers>member"`
}

// ListOriginationNumbersResponse is the XML response for ListOriginationNumbers.
type ListOriginationNumbersResponse struct {
	XMLName                      xml.Name                     `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListOriginationNumbersResponse"` //nolint:lll // XML namespace.
	ResponseMetadata             ResponseMetadata             `xml:"ResponseMetadata"`
	ListOriginationNumbersResult ListOriginationNumbersResult `xml:"ListOriginationNumbersResult"`
}

// ListPhoneNumbersOptedOutResult holds the result of ListPhoneNumbersOptedOut.
type ListPhoneNumbersOptedOutResult struct {
	NextToken    string   `xml:"nextToken,omitempty"`
	PhoneNumbers []string `xml:"phoneNumbers>member"`
}

// ListPhoneNumbersOptedOutResponse is the XML response for ListPhoneNumbersOptedOut.
type ListPhoneNumbersOptedOutResponse struct {
	XMLName                        xml.Name                       `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListPhoneNumbersOptedOutResponse"` //nolint:lll // XML namespace.
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
	ListPhoneNumbersOptedOutResult ListPhoneNumbersOptedOutResult `xml:"ListPhoneNumbersOptedOutResult"`
}

// XMLSandboxPhoneNumber is the XML representation of a sandbox phone number entry.
type XMLSandboxPhoneNumber struct {
	PhoneNumber  string `xml:"PhoneNumber"`
	LanguageCode string `xml:"LanguageCode,omitempty"`
	Status       string `xml:"Status"`
}

// ListSMSSandboxPhoneNumbersResult holds the result of ListSMSSandboxPhoneNumbers.
type ListSMSSandboxPhoneNumbersResult struct {
	NextToken    string                  `xml:"NextToken,omitempty"`
	PhoneNumbers []XMLSandboxPhoneNumber `xml:"PhoneNumbers>member"`
}

// ListSMSSandboxPhoneNumbersResponse is the XML response for ListSMSSandboxPhoneNumbers.
type ListSMSSandboxPhoneNumbersResponse struct {
	XMLName                          xml.Name                         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListSMSSandboxPhoneNumbersResponse"` //nolint:lll // XML namespace.
	ResponseMetadata                 ResponseMetadata                 `xml:"ResponseMetadata"`
	ListSMSSandboxPhoneNumbersResult ListSMSSandboxPhoneNumbersResult `xml:"ListSMSSandboxPhoneNumbersResult"`
}

// RemovePermissionResponse is the XML response for RemovePermission.
type RemovePermissionResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ RemovePermissionResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// OptInPhoneNumberResponse is the XML response for OptInPhoneNumber.
type OptInPhoneNumberResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ OptInPhoneNumberResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// VerifySMSSandboxPhoneNumberResponse is the XML response for VerifySMSSandboxPhoneNumber.
type VerifySMSSandboxPhoneNumberResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ VerifySMSSandboxPhoneNumberResponse"` //nolint:lll // XML namespace.
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// SetSMSAttributesResponse is the XML response for SetSMSAttributes.
type SetSMSAttributesResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ SetSMSAttributesResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// PutDataProtectionPolicyResponse is the XML response for PutDataProtectionPolicy.
type PutDataProtectionPolicyResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ PutDataProtectionPolicyResponse"` //nolint:lll // XML namespace.
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// SMSDelivery records a single SMS message sent via PublishSMS.
type SMSDelivery struct {
	PhoneNumber string
	Message     string
	MessageID   string
}

// ApplicationDelivery records a single message delivered to an application-protocol
// (mobile push platform endpoint) subscription during a topic publish. AWS delivers
// these to a platform endpoint; gopherstack records the delivery here and exposes it
// via DrainApplicationDeliveries for inspection/testing.
type ApplicationDelivery struct {
	// EndpointARN is the target platform endpoint ARN.
	EndpointARN string
	// Message is the (per-protocol resolved) message body.
	Message string
	// MessageID is the generated message ID for the delivery.
	MessageID string
}

// EmailDelivery records a single message delivered to an email or email-json
// subscription. AWS sends these to a mailbox; gopherstack has no SMTP sink, so
// the delivery is recorded here and exposed via DrainEmailDeliveries for
// inspection/testing — the simulator equivalent of "the email was sent".
type EmailDelivery struct {
	// EndpointEmail is the subscriber's email address.
	EndpointEmail string
	// Protocol is "email" or "email-json".
	Protocol string
	// Subject is the optional message subject.
	Subject string
	// Message is the (per-protocol resolved) message body.
	Message string
	// MessageID is the publish MessageId.
	MessageID string
	// TopicARN is the originating topic.
	TopicARN string
}

// ArchivedMessage stores a published message in the per-topic archive.
// Messages are archived when the topic has an ArchivePolicy attribute set.
// They are replayed to subscriptions that have a ReplayPolicy set.
type ArchivedMessage struct {
	Attributes map[string]MessageAttribute
	Timestamp  time.Time
	MessageID  string
	Message    string
	Subject    string
}

// notificationSigner holds the RSA key pair and self-signed certificate used to
// sign SNS HTTP/HTTPS notification envelopes per AWS SignatureVersion=2 spec.
// The certificate is served at the URL stored in certURLValue so subscribers can
// verify signatures without contacting the real AWS endpoint. certURLValue is
// guarded by mu because SetSigningCertBaseURL may be called concurrently with
// in-flight deliveries that read the URL (e.g. a late server-address rewire
// racing a publish already in progress).
type notificationSigner struct {
	privateKey   *rsa.PrivateKey
	certURLValue string
	certPEM      []byte
	mu           sync.RWMutex
}

// InMemoryBackend implements StorageBackend using an in-memory concurrency-safe store.
type InMemoryBackend struct {
	emitter              events.EventEmitter[*events.SNSPublishedEvent]
	lambdaBackend        LambdaInvoker
	firehoseBackend      FirehosePutter
	sqsSender            SQSSender
	sqsChecker           SQSQueueChecker
	svcCtx               context.Context
	httpClient           *http.Client
	registry             *store.Registry
	topics               *store.Table[Topic]
	topicTags            map[string]*svcTags.Tags
	platformApplications *store.Table[PlatformApplication]
	smsSandbox           *store.Table[SandboxPhoneNumber]
	optedOutPhoneNumbers map[string]bool
	smsAttributes        map[string]string
	// originationNumbers holds origination phone numbers keyed by region. AWS SNS exposes no
	// public "create origination number" API (numbers are provisioned via Pinpoint / AWS
	// End User Messaging), so a fresh account legitimately has none. This map reflects real
	// state when populated via SeedOriginationNumber (used by tests / internal seeding).
	originationNumbers map[string][]XMLOriginationPhone
	mu                 *lockmetrics.RWMutex
	subscriptions      *store.Table[Subscription]
	// subscriptionsByTopic is a secondary index over subscriptions keyed by
	// TopicArn, replacing the old hand-maintained topicSubscriptions nested
	// map. It is kept consistent automatically by subscriptions.Put/Delete.
	subscriptionsByTopic  *store.Index[Subscription]
	platformEndpoints     *store.Table[PlatformEndpoint]
	topicMessageArchive   map[string][]*ArchivedMessage // populated when ArchivePolicy is set
	signer                *notificationSigner
	workerSem             chan struct{}
	accountID             string
	region                string
	smsDeliveries         []SMSDelivery
	emailDeliveries       []EmailDelivery
	applicationDeliveries []ApplicationDelivery
	deliveryWg            sync.WaitGroup
	closing               atomic.Bool
	smsSandboxEnabled     bool
	// subscriptionLimitPerTopic is the effective SubscriptionLimitExceeded
	// threshold for Subscribe on a single topic. Defaults to
	// defaultMaxSubscriptionsPerTopic (AWS's real 12,500,000 quota); overridable
	// via SetSubscriptionLimitPerTopicForTest so tests can exercise the limit
	// without creating millions of subscriptions.
	subscriptionLimitPerTopic int
}

// httpDelivery holds the endpoint and message body for an HTTP/HTTPS delivery.
type httpDelivery struct {
	signer               *notificationSigner // nil disables signing
	sqsSender            SQSSender           // optional; non-nil when subscription has a DLQ
	endpoint             string
	body                 string
	subject              string
	messageID            string
	topicARN             string
	subscriptionARN      string
	redrivePolicy        string // JSON RedrivePolicy; non-empty when DLQ is configured
	deliveryPolicy       string
	topicEffectivePolicy string
	rawDelivery          bool
}

// publishTargets holds the subscription snapshots and HTTP deliveries collected for a publish call.
type publishTargets struct {
	subs            []events.SNSSubscriptionSnapshot
	httpDeliveries  []httpDelivery
	emailDeliveries []EmailDelivery
}

type parsedFilterPolicy map[string][]json.RawMessage

// snsHTTPNotification is the SNS notification JSON envelope sent to HTTP/HTTPS subscribers.
// When RawMessageDelivery is false, this struct is serialised as the POST body.
// Field names use the exact casing required by AWS SNS.
type snsHTTPNotification struct {
	Subject          string `json:"Subject,omitempty"`
	Type             string `json:"Type"`
	MessageID        string `json:"MessageId"`
	TopicArn         string `json:"TopicArn"`
	Message          string `json:"Message"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SigningCertURL   string `json:"SigningCertURL"`
	UnsubscribeURL   string `json:"UnsubscribeURL"`
}

// TaggedTopicInfo contains a topic's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedTopicInfo struct {
	Tags map[string]string
	ARN  string
}

// batchEntry holds a single parsed PublishBatch entry.
type batchEntry struct {
	attrs            map[string]MessageAttribute
	id               string
	message          string
	subject          string
	messageGroupID   string
	messageStructure string
	dedupID          string
}

// snsListTagsResult is the inner result element for ListTagsForResource.
type snsListTagsResult struct {
	XMLName xml.Name     `xml:"ListTagsForResourceResult"`
	Tags    []svcTags.KV `xml:"Tags>Tag"`
}

// snsListTagsResponse is the XML response for ListTagsForResource.
type snsListTagsResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListTagsForResourceResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	Result           snsListTagsResult
}

// snsEmptyResponse is the XML response for tag mutation operations (TagResource, UntagResource).
// The XMLName field is set dynamically per action.
type snsEmptyResponse struct {
	XMLName xml.Name `xml:""`
}
