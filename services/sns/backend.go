package sns

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/events"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	boolFalseStr      = "false"
	eventTypeKey      = "EventType"
	endpointArnKey    = "EndpointArn"
	protocolEmailJSON = "email-json"
	protocolHTTPS     = "https"

	// topicArnKey is the SNS canonical attribute key for a topic ARN.
	topicArnKey = "TopicArn"

	// rsaKeyBits is the RSA key size used for notification signing.
	rsaKeyBits = 2048
)

const (
	messageTypeNotification = "Notification"
	protocolLambda          = "lambda"
	protocolFirehose        = "firehose"
	protocolEmail           = "email"
	protocolHTTP            = "http"
	// attrPendingConfirmation is the SNS subscription attribute key whose
	// value is "true" while a subscription awaits confirmation. The key uses
	// the PascalCase attribute name returned by GetSubscriptionAttributes.
	attrPendingConfirmation = "PendingConfirmation"
	// pendingConfirmationARN is the literal string AWS SNS returns in the
	// SubscriptionArn field of Subscribe for HTTP/HTTPS/email endpoints
	// before the subscriber confirms the subscription. Real AWS returns
	// the lowercase form "pending confirmation" (with a space), not the
	// attribute name.
	pendingConfirmationARN = "pending confirmation"
)

var (
	ErrTopicNotFound                    = errors.New("NotFound")
	ErrTopicAlreadyExists               = errors.New("TopicAlreadyExists")
	ErrSubscriptionNotFound             = errors.New("NotFound")
	ErrPlatformApplicationNotFound      = errors.New("NotFound")
	ErrPlatformApplicationAlreadyExists = errors.New("PlatformApplicationAlreadyExists")
	ErrEndpointNotFound                 = errors.New("NotFound")
	ErrEndpointDisabled                 = errors.New("EndpointDisabled")
	ErrInvalidParameter                 = errors.New("InvalidParameter")
	ErrPhoneNumberNotFound              = errors.New("ResourceNotFound")
	ErrSandboxPhoneAlreadyExists        = errors.New("AlreadyExists")
	ErrPermissionLabelExists            = errors.New("AuthorizationError")
	ErrPermissionLabelNotFound          = errors.New("AuthorizationError")
	ErrSandboxPhoneNotVerified          = errors.New("InvalidParameter")
	ErrOptedOut                         = errors.New("KMSOptInRequired")
)

const (
	// pageSize is the default page size for SNS List operations.
	// AWS SNS returns up to 100 items per page for most list operations.
	pageSize = 100

	attrFilterPolicy        = "FilterPolicy"
	attrRawMessageDelivery  = "RawMessageDelivery"
	attrRedrivePolicy       = "RedrivePolicy"
	attrDeliveryPolicy      = "DeliveryPolicy"
	attrReplayPolicy        = "ReplayPolicy"
	attrSubscriptionRoleArn = "SubscriptionRoleArn"
	attrFilterPolicyScope   = "FilterPolicyScope"

	// platformARNResourceParts is the expected number of slash-delimited parts
	// in a platform application ARN resource component: "app/{Platform}/{AppName}".
	platformARNResourceParts = 3

	// endpointExtraAttrs is the number of extra attributes added to a new endpoint
	// beyond what the caller provides: Token and Enabled.
	endpointExtraAttrs = 2

	// snsHTTPTimeout is the timeout applied to SNS HTTP/HTTPS endpoint deliveries.
	snsHTTPTimeout = 5 * time.Second

	// snsMaxConcurrentDeliveries caps the number of HTTP/HTTPS subscription
	// deliveries that may run concurrently for a single Publish call.
	snsMaxConcurrentDeliveries = 8

	// snsLambdaInvocationType is the Lambda invocation type for SNS delivery (fire-and-forget).
	snsLambdaInvocationType = "Event"

	// maxDeliveryResponseBytes is the maximum number of bytes read from an HTTP
	// delivery response body to prevent unbounded memory growth from large responses.
	maxDeliveryResponseBytes = 64 * 1024 // 64 KiB

	// maxFilterPolicySizeBytes is the maximum byte size of a FilterPolicy JSON string
	// that will be parsed. Policies exceeding this limit are treated as no filter.
	maxFilterPolicySizeBytes = 256 * 1024 // 256 KiB

	// maxPermissionLabelLen is the maximum character length of an AddPermission label.
	maxPermissionLabelLen = 80

	// defaultPolicyJSON is the default SNS topic access policy (empty statements).
	defaultPolicyJSON = `{"Version":"2012-10-17","Statement":[]}`

	// defaultEffectiveDeliveryPolicyJSON mirrors the AWS default SNS delivery
	// retry configuration returned in GetTopicAttributes when no custom
	// DeliveryPolicy has been set.
	defaultEffectiveDeliveryPolicyJSON = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,` +
		`"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,` +
		`"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false}}`

	// fifoTopicAttrValue is the attribute value indicating a topic is FIFO.
	fifoTopicAttrValue = "true"

	// maxListSMSSandboxResults is the maximum MaxResults value for ListSMSSandboxPhoneNumbers.
	maxListSMSSandboxResults = 100

	// defaultListSMSSandboxResults is the default page size for ListSMSSandboxPhoneNumbers.
	defaultListSMSSandboxResults = 100

	// maxListOptedOutResults is the maximum MaxResults value for ListPhoneNumbersOptedOut.
	maxListOptedOutResults = 100

	// defaultListOptedOutResults is the default page size for ListPhoneNumbersOptedOut.
	defaultListOptedOutResults = 100

	// maxPublishBatchEntries is the maximum number of entries per PublishBatch request.
	// This matches the AWS SNS service limit.
	maxPublishBatchEntries = 10

	// maxArchivedMessagesPerTopic caps the in-memory archive per topic.
	// When the cap is exceeded, the oldest messages are evicted.
	maxArchivedMessagesPerTopic = 100_000

	// maxTopicNameLen is the maximum length of an SNS topic name.
	maxTopicNameLen = 256

	// fifoTopicSuffix is the required suffix for FIFO topic names.
	fifoTopicSuffix = ".fifo"

	// maxMessageSizeBytes is the maximum byte size of an SNS message body (256 KB).
	// This matches the AWS SNS service limit.
	maxMessageSizeBytes = 256 * 1024

	// maxBatchEntryIDLen is the maximum character length of a PublishBatch entry ID.
	maxBatchEntryIDLen = 80

	// maxSubjectLen is the maximum character length of a Publish Subject.
	// AWS SNS rejects subjects longer than 100 characters.
	maxSubjectLen = 100

	// computedTopicAttrCount is the number of computed attributes added to GetTopicAttributes
	// output beyond stored attributes: Owner, TopicArn, EffectiveDeliveryPolicy,
	// SubscriptionsConfirmed, SubscriptionsPending, SubscriptionsDeleted.
	computedTopicAttrCount = 6
)

// isValidSMSAttributeName returns true if the attribute name is recognised by the AWS SNS API.
// Source: https://docs.aws.amazon.com/sns/latest/api/API_SetSMSAttributes.html
func isValidSMSAttributeName(name string) bool {
	switch name {
	case "MonthlySpendLimit",
		"DeliveryStatusIAMRole",
		"DeliveryStatusSuccessSamplingRate",
		"DefaultSenderID",
		"DefaultSMSType",
		"UsageReportS3Bucket":
		return true
	default:
		return false
	}
}

// isValidTopicName returns true if the topic name is non-empty, at most 256 characters,
// consists only of alphanumeric characters, hyphens, and underscores, and if it is a
// FIFO topic (ending in ".fifo") the base name (before the suffix) follows the same rules.
// Source: https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html
func isValidTopicName(name string) bool {
	if name == "" || len(name) > maxTopicNameLen {
		return false
	}

	base := name
	if strings.HasSuffix(name, fifoTopicSuffix) {
		base = name[:len(name)-len(fifoTopicSuffix)]
		if base == "" {
			return false
		}
	}

	for _, c := range base {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}

	return true
}

// StorageBackend defines the interface for an SNS storage backend.
type StorageBackend interface {
	CreateTopic(name string, attributes map[string]string) (*Topic, error)
	CreateTopicInRegion(name, region string, attributes map[string]string) (*Topic, error)
	DeleteTopic(topicArn string) error
	ListTopics(nextToken string) ([]Topic, string, error)
	GetTopicAttributes(topicArn string) (map[string]string, error)
	SetTopicAttributes(topicArn, attrName, attrValue string) error
	Subscribe(topicArn, protocol, endpoint, filterPolicy string) (*Subscription, error)
	ConfirmSubscription(topicArn, token string) (*Subscription, error)
	Unsubscribe(subscriptionArn string) error
	ListSubscriptions(nextToken string) ([]Subscription, string, error)
	ListSubscriptionsByTopic(topicArn, nextToken string) ([]Subscription, string, error)
	GetSubscriptionAttributes(subscriptionArn string) (map[string]string, error)
	SetSubscriptionAttributes(subscriptionArn, attrName, attrValue string) error
	Publish(topicArn, message, subject, messageStructure string, attrs map[string]MessageAttribute) (string, error)
	// PublishToTargetArn publishes directly to a platform endpoint ARN.
	// In the mock, this generates and returns a unique message ID without real delivery.
	PublishToTargetArn(targetArn, message, subject string, attrs map[string]MessageAttribute) (string, error)
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
	CreatePlatformApplication(name, platform string, attributes map[string]string) (*PlatformApplication, error)
	GetPlatformApplicationAttributes(platformApplicationArn string) (map[string]string, error)
	SetPlatformApplicationAttributes(platformApplicationArn string, attributes map[string]string) error
	ListPlatformApplications(nextToken string) ([]PlatformApplication, string, error)
	DeletePlatformApplication(platformApplicationArn string) error
	// Platform endpoint operations.
	CreatePlatformEndpoint(
		platformApplicationArn, token string,
		attributes map[string]string,
	) (*PlatformEndpoint, error)
	GetEndpointAttributes(endpointArn string) (map[string]string, error)
	SetEndpointAttributes(endpointArn string, attributes map[string]string) error
	ListEndpointsByPlatformApplication(platformApplicationArn, nextToken string) ([]PlatformEndpoint, string, error)
	DeleteEndpoint(endpointArn string) error
	// Permission operations.
	AddPermission(topicArn, label string, accounts, actions []string) error
	RemovePermission(topicArn, label string) error
	// SMS Sandbox operations.
	GetSMSSandboxAccountStatus() (bool, error)
	CreateSMSSandboxPhoneNumber(phoneNumber, languageCode string) error
	DeleteSMSSandboxPhoneNumber(phoneNumber string) error
	ListSMSSandboxPhoneNumbers(nextToken string, maxResults int) ([]SandboxPhoneNumber, string, error)
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
	ListOriginationNumbers(nextToken string) ([]XMLOriginationPhone, string, error)
}

// SMSDelivery records a single SMS message sent via PublishSMS.
type SMSDelivery struct {
	PhoneNumber string
	Message     string
	MessageID   string
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
// The certificate is served at the URL stored in certURL so subscribers can
// verify signatures without contacting the real AWS endpoint.
type notificationSigner struct {
	privateKey *rsa.PrivateKey
	certURL    string // URL where certPEM is accessible (configurable for tests)
	certPEM    []byte // PEM-encoded DER certificate, served at certURL
}

// newNotificationSigner generates a fresh RSA-2048 key pair and a self-signed
// x.509 certificate. The returned signer is valid for the lifetime of the
// backend instance.
func newNotificationSigner() *notificationSigner {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		// Key generation failure is unrecoverable; panic with a clear message
		// rather than silently falling back to mock signatures.
		panic("sns: failed to generate RSA key for notification signing: " + err.Error())
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Gopherstack SNS Mock"},
			CommonName:   "SimpleNotificationService",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic("sns: failed to create self-signed cert: " + err.Error())
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	return &notificationSigner{
		privateKey: key,
		certPEM:    certPEM,
		// certURL is set later via SetSigningCertBaseURL when the server address is known.
		certURL: "https://sns.us-east-1.amazonaws.com/SimpleNotificationService.pem",
	}
}

// sign computes the RSA-SHA256 signature of the canonical notification string
// per AWS SNS SignatureVersion=2 and returns it base64-encoded.
func (s *notificationSigner) sign(canonical string) string {
	h := sha256.Sum256([]byte(canonical))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, crypto.SHA256, h[:])
	if err != nil {
		return "SIGN-ERROR"
	}

	return base64.StdEncoding.EncodeToString(sig)
}

// canonicalNotificationString builds the string-to-sign for a Notification
// message per the AWS SNS message-signing specification. Fields are included
// in alphabetical order; Subject is omitted when empty.
func canonicalNotificationString(msgID, topicARN, subject, message, timestamp string) string {
	type field struct{ k, v string }
	fields := []field{
		{"Message", message},
		{"MessageId", msgID},
		{"Timestamp", timestamp},
		{topicArnKey, topicARN},
		{"Type", messageTypeNotification},
	}
	if subject != "" {
		fields = append(fields, field{"Subject", subject})
	}

	sort.Slice(fields, func(i, j int) bool { return fields[i].k < fields[j].k })

	var sb strings.Builder
	for _, f := range fields {
		sb.WriteString(f.k)
		sb.WriteByte('\n')
		sb.WriteString(f.v)
		sb.WriteByte('\n')
	}

	return sb.String()
}

// LambdaInvoker can invoke a Lambda function for SNS subscription delivery.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
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

// InMemoryBackend implements StorageBackend using an in-memory concurrency-safe store.
type InMemoryBackend struct {
	emitter              events.EventEmitter[*events.SNSPublishedEvent]
	lambdaBackend        LambdaInvoker
	firehoseBackend      FirehosePutter
	sqsSender            SQSSender
	svcCtx               context.Context
	topicSubscriptions   map[string]map[string]*Subscription
	httpClient           *http.Client
	topics               map[string]*Topic
	topicTags            map[string]*svcTags.Tags
	platformApplications map[string]*PlatformApplication
	smsSandbox           map[string]*SandboxPhoneNumber
	optedOutPhoneNumbers map[string]bool
	smsAttributes        map[string]string
	mu                   *lockmetrics.RWMutex
	subscriptions        map[string]*Subscription
	platformEndpoints    map[string]*PlatformEndpoint
	topicMessageArchive  map[string][]*ArchivedMessage // populated when ArchivePolicy is set
	signer               *notificationSigner
	workerSem            chan struct{}
	accountID            string
	region               string
	smsDeliveries        []SMSDelivery
	deliveryWg           sync.WaitGroup
	closing              atomic.Bool
}

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
// Use [NewInMemoryBackendWithContext] to bind it to a parent service context instead.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend bound to the given parent
// context. The context is used when emitting SNS publish events (e.g. to SQS delivery)
// so that event delivery is cancelled if the service is shut down.
// If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	return &InMemoryBackend{
		topics:               make(map[string]*Topic),
		subscriptions:        make(map[string]*Subscription),
		topicSubscriptions:   make(map[string]map[string]*Subscription),
		topicTags:            make(map[string]*svcTags.Tags),
		platformApplications: make(map[string]*PlatformApplication),
		platformEndpoints:    make(map[string]*PlatformEndpoint),
		topicMessageArchive:  make(map[string][]*ArchivedMessage),
		smsSandbox:           make(map[string]*SandboxPhoneNumber),
		optedOutPhoneNumbers: make(map[string]bool),
		smsAttributes:        make(map[string]string),
		accountID:            accountID,
		region:               region,
		svcCtx:               svcCtx,
		mu:                   lockmetrics.New("sns"),
		httpClient:           &http.Client{Timeout: snsHTTPTimeout},
		workerSem:            make(chan struct{}, snsMaxConcurrentDeliveries),
		signer:               newNotificationSigner(),
	}
}

// SetHTTPDeliveryClient configures the HTTP client used for HTTP/HTTPS subscription delivery.
// If not set, a dedicated client with a 5-second timeout is used.
func (b *InMemoryBackend) SetHTTPDeliveryClient(c *http.Client) {
	b.mu.Lock("SetHTTPDeliveryClient")
	defer b.mu.Unlock()

	b.httpClient = c
}

// SigningCertPEM returns the PEM-encoded self-signed certificate used to verify
// HTTP/HTTPS notification signatures. Tests can call this to verify that a
// notification's Signature field is a valid RSA-SHA1 signature over the
// canonical notification string.
func (b *InMemoryBackend) SigningCertPEM() []byte {
	return b.signer.certPEM
}

// SetSigningCertBaseURL configures the URL embedded in the SigningCertURL field
// of HTTP/HTTPS notification envelopes. Call this once the server address is
// known so that subscribers can retrieve the verification certificate.
// The URL should point to the mock server's /SimpleNotificationService.pem path.
func (b *InMemoryBackend) SetSigningCertBaseURL(baseURL string) {
	b.signer.certURL = strings.TrimRight(baseURL, "/") + "/SimpleNotificationService.pem"
}

// SetPublishEmitter registers an event emitter that fires when a message is published.
// This is used to wire SNS→SQS delivery at startup.
func (b *InMemoryBackend) SetPublishEmitter(emitter events.EventEmitter[*events.SNSPublishedEvent]) {
	b.mu.Lock("SetPublishEmitter")
	defer b.mu.Unlock()

	b.emitter = emitter
}

// SetLambdaBackend wires the Lambda backend for SNS → Lambda subscription delivery.
func (b *InMemoryBackend) SetLambdaBackend(lambda LambdaInvoker) {
	b.mu.Lock("SetLambdaBackend")
	defer b.mu.Unlock()

	b.lambdaBackend = lambda
}

// SetFirehoseBackend wires the Firehose backend for SNS → Firehose subscription delivery.
func (b *InMemoryBackend) SetFirehoseBackend(firehose FirehosePutter) {
	b.mu.Lock("SetFirehoseBackend")
	defer b.mu.Unlock()

	b.firehoseBackend = firehose
}

// SetSQSSender wires the SQS sender used to deliver failed messages to a subscription DLQ.
func (b *InMemoryBackend) SetSQSSender(sender SQSSender) {
	b.mu.Lock("SetSQSSender")
	defer b.mu.Unlock()

	b.sqsSender = sender
}

// CreateTopic creates a new SNS topic using the backend's default region.
func (b *InMemoryBackend) CreateTopic(name string, attributes map[string]string) (*Topic, error) {
	return b.CreateTopicInRegion(name, b.region, attributes)
}

// CreateTopicInRegion creates a new SNS topic in the specified region.
// If region is empty, the backend's default region is used.
func (b *InMemoryBackend) CreateTopicInRegion(name, region string, attributes map[string]string) (*Topic, error) {
	if !isValidTopicName(name) {
		return nil, fmt.Errorf(
			"%w: Topic name must be 1-256 characters and contain only alphanumeric characters, hyphens, and underscores",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTopicInRegion")
	defer b.mu.Unlock()

	if region == "" {
		region = b.region
	}

	topicArn := arn.Build("sns", region, b.accountID, name)
	if existing, exists := b.topics[topicArn]; exists {
		// AWS SNS CreateTopic is idempotent: calling it with an existing name
		// returns the existing topic ARN rather than an error.
		return existing, nil
	}

	attrs := make(map[string]string, len(attributes)+1)
	maps.Copy(attrs, attributes)
	attrs[topicArnKey] = topicArn
	// Ensure Policy is a valid JSON string with an empty Statement array so
	// Terraform's PolicyHasValidAWSPrincipals JMESPath check returns []any{}.
	if attrs["Policy"] == "" {
		attrs["Policy"] = defaultPolicyJSON
	}

	// Validate FifoTopic attribute consistency with topic name.
	// AWS rejects CreateTopic when FifoTopic=true but name doesn't end in ".fifo".
	if attrs["FifoTopic"] == fifoTopicAttrValue && !strings.HasSuffix(name, fifoTopicSuffix) {
		return nil, fmt.Errorf(
			"%w: Topic name must end with '.fifo' for FIFO topics",
			ErrInvalidParameter,
		)
	}

	// ContentBasedDeduplication is only valid on FIFO topics.
	if attrs["ContentBasedDeduplication"] != "" &&
		attrs["FifoTopic"] != fifoTopicAttrValue &&
		!strings.HasSuffix(name, fifoTopicSuffix) {
		return nil, fmt.Errorf(
			"%w: ContentBasedDeduplication is only applicable to FIFO topics",
			ErrInvalidParameter,
		)
	}

	// FIFO topics: auto-set FifoTopic=true and ContentBasedDeduplication if not already set.
	if strings.HasSuffix(name, fifoTopicSuffix) {
		attrs["FifoTopic"] = fifoTopicAttrValue
		if attrs["ContentBasedDeduplication"] == "" {
			attrs["ContentBasedDeduplication"] = boolFalseStr
		}
	}

	// Validate KmsMasterKeyId format if present (alias name, alias ARN, key ID, or key ARN).
	if v := attrs["KmsMasterKeyId"]; v != "" {
		if err := validateKmsMasterKeyID(v); err != nil {
			return nil, err
		}
	}

	topic := &Topic{
		TopicArn:          topicArn,
		Attributes:        attrs,
		CreationTimestamp: time.Now().UTC(),
	}
	b.topics[topicArn] = topic
	b.topicSubscriptions[topicArn] = make(map[string]*Subscription)

	return topic, nil
}

// DeleteTopic removes a topic by ARN.
func (b *InMemoryBackend) DeleteTopic(topicArn string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	if _, exists := b.topics[topicArn]; !exists {
		return ErrTopicNotFound
	}

	delete(b.topics, topicArn)

	// Close topic tags to prevent resource leak.
	if t := b.topicTags[topicArn]; t != nil {
		t.Close()
		delete(b.topicTags, topicArn)
	}

	// Remove any orphaned subscriptions for this topic.
	for subArn, sub := range b.subscriptions {
		if sub.TopicArn == topicArn {
			delete(b.subscriptions, subArn)
		}
	}
	delete(b.topicSubscriptions, topicArn)

	return nil
}

// ListTopics returns a page of topics and the next pagination token.
func (b *InMemoryBackend) ListTopics(nextToken string) ([]Topic, string, error) {
	b.mu.RLock("ListTopics")
	defer b.mu.RUnlock()

	all := b.sortedTopics()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	topics, next := paginate(all, offset, pageSize)

	return topics, next, nil
}

// GetTopicAttributes returns the attributes of a topic.
func (b *InMemoryBackend) GetTopicAttributes(topicArn string) (map[string]string, error) {
	b.mu.RLock("GetTopicAttributes")
	defer b.mu.RUnlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return nil, ErrTopicNotFound
	}

	attrs := make(map[string]string, len(topic.Attributes)+computedTopicAttrCount)
	maps.Copy(attrs, topic.Attributes)

	// Ensure Policy is always a valid JSON string with an empty Statement array so
	// Terraform's PolicyHasValidAWSPrincipals JMESPath check returns []any{}.
	if attrs["Policy"] == "" {
		attrs["Policy"] = defaultPolicyJSON
	}

	// Populate computed attributes that AWS returns but we store dynamically.
	if attrs["Owner"] == "" {
		attrs["Owner"] = b.accountID
	}

	// AWS always returns TopicArn as an attribute in GetTopicAttributes.
	if attrs[topicArnKey] == "" {
		attrs[topicArnKey] = topicArn
	}

	// AWS always returns FifoTopic in GetTopicAttributes: "true" for FIFO topics,
	// "false" for standard topics. Non-FIFO topics have no stored value — default to "false".
	if attrs["FifoTopic"] == "" {
		attrs["FifoTopic"] = boolFalseStr
	}

	// EffectiveDeliveryPolicy is the resolved delivery policy (defaults to
	// AWS standard retry configuration when no custom DeliveryPolicy is set).
	if attrs["EffectiveDeliveryPolicy"] == "" {
		if attrs["DeliveryPolicy"] != "" {
			attrs["EffectiveDeliveryPolicy"] = attrs["DeliveryPolicy"]
		} else {
			attrs["EffectiveDeliveryPolicy"] = defaultEffectiveDeliveryPolicyJSON
		}
	}

	// Count subscriptions for this topic.
	// SubscriptionsDeleted is not tracked in memory — AWS also resets this counter
	// periodically, so we report 0 for consistency with a fresh mock environment.
	confirmed, pending := 0, 0

	for _, sub := range b.topicSubscriptions[topicArn] {
		if sub.PendingConfirmation {
			pending++
		} else {
			confirmed++
		}
	}

	attrs["SubscriptionsConfirmed"] = strconv.Itoa(confirmed)
	attrs["SubscriptionsPending"] = strconv.Itoa(pending)
	attrs["SubscriptionsDeleted"] = "0"

	return attrs, nil
}

// isKnownTopicAttribute reports whether name is a settable SNS topic attribute.
// Includes the core attributes plus all delivery-status logging attributes.
func isKnownTopicAttribute(name string) bool {
	switch name {
	// Core topic attributes.
	case "DeliveryPolicy", "DisplayName", "FifoTopic", "ContentBasedDeduplication",
		"KmsMasterKeyId", "Policy", "TracingConfig", "FifoThroughputScope",
		"ArchivePolicy", "DataProtectionPolicy", "SignatureVersion":
		return true
	// HTTP/HTTPS delivery status logging.
	case "HTTPSuccessFeedbackRoleArn", "HTTPSuccessFeedbackSampleRate", "HTTPFailureFeedbackRoleArn",
		"HTTPSSuccessFeedbackRoleArn", "HTTPSSuccessFeedbackSampleRate", "HTTPSFailureFeedbackRoleArn":
		return true
	// SQS delivery status logging.
	case "SQSSuccessFeedbackRoleArn", "SQSSuccessFeedbackSampleRate", "SQSFailureFeedbackRoleArn":
		return true
	// Lambda delivery status logging.
	case "LambdaSuccessFeedbackRoleArn", "LambdaSuccessFeedbackSampleRate", "LambdaFailureFeedbackRoleArn":
		return true
	// Firehose delivery status logging.
	case "FirehoseSuccessFeedbackRoleArn", "FirehoseSuccessFeedbackSampleRate", "FirehoseFailureFeedbackRoleArn":
		return true
	// Mobile application (GCM/APNS/etc.) delivery status logging.
	case "ApplicationSuccessFeedbackRoleArn",
		"ApplicationSuccessFeedbackSampleRate",
		"ApplicationFailureFeedbackRoleArn":
		return true
	}

	return false
}

// SetTopicAttributes sets a single attribute on a topic.
func (b *InMemoryBackend) SetTopicAttributes(topicArn, attrName, attrValue string) error {
	b.mu.Lock("SetTopicAttributes")
	defer b.mu.Unlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return ErrTopicNotFound
	}

	// Reject read-only/computed attributes that cannot be mutated directly.
	if isReadOnlyTopicAttribute(attrName) {
		return fmt.Errorf(
			"%w: Invalid parameter: Attribute %s is a read-only attribute and cannot be set",
			ErrInvalidParameter,
			attrName,
		)
	}

	if !isKnownTopicAttribute(attrName) {
		return fmt.Errorf(
			"%w: Invalid parameter: Attribute name %s is not a known topic attribute",
			ErrInvalidParameter,
			attrName,
		)
	}

	// ContentBasedDeduplication is only valid on FIFO topics.
	if attrName == "ContentBasedDeduplication" && topic.Attributes["FifoTopic"] != fifoTopicAttrValue {
		return fmt.Errorf(
			"%w: Invalid parameter: ContentBasedDeduplication is only applicable to FIFO topics",
			ErrInvalidParameter,
		)
	}

	// FifoThroughputScope is only valid on FIFO topics.
	if attrName == "FifoThroughputScope" && topic.Attributes["FifoTopic"] != fifoTopicAttrValue {
		return fmt.Errorf(
			"%w: Invalid parameter: FifoThroughputScope is only applicable to FIFO topics",
			ErrInvalidParameter,
		)
	}

	// Validate KmsMasterKeyId format (alias name, alias ARN, key ID, or key ARN).
	if attrName == "KmsMasterKeyId" && attrValue != "" {
		if err := validateKmsMasterKeyID(attrValue); err != nil {
			return err
		}
	}

	// When clearing EffectiveDeliveryPolicy derived from DeliveryPolicy, reset it.
	if attrName == "DeliveryPolicy" {
		if attrValue == "" {
			delete(topic.Attributes, "EffectiveDeliveryPolicy")
		} else {
			topic.Attributes["EffectiveDeliveryPolicy"] = attrValue
		}
	}

	topic.Attributes[attrName] = attrValue

	return nil
}

// Subscribe creates a new subscription for the given topic, protocol, and endpoint.
// If a confirmed subscription for the same topic+protocol+endpoint already exists,
// the existing subscription ARN is returned (matching AWS deduplication behaviour).
func (b *InMemoryBackend) Subscribe(topicArn, protocol, endpoint, filterPolicy string) (*Subscription, error) {
	// Validate SMS endpoint is a valid E.164 phone number.
	if protocol == "sms" && !isValidE164(endpoint) {
		return nil, fmt.Errorf("%w: Endpoint must be in E.164 format for SMS protocol", ErrInvalidParameter)
	}

	// Validate email/email-json endpoints look like email addresses.
	if (protocol == protocolEmail || protocol == protocolEmailJSON) && !isValidEmail(endpoint) {
		return nil, fmt.Errorf(
			"%w: Invalid parameter: Endpoint must be a valid email address for %s protocol",
			ErrInvalidParameter,
			protocol,
		)
	}

	// Parse and validate the filter policy outside the backend lock so that
	// JSON parsing of large policies does not block other SNS operations.
	parsedPolicy, parseErr := parseFilterPolicy(filterPolicy)
	if parseErr != nil {
		return nil, parseErr
	}

	b.mu.Lock("Subscribe")
	defer b.mu.Unlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return nil, ErrTopicNotFound
	}

	// Dedup: return the existing subscription ARN when protocol+endpoint already
	// has a confirmed subscription on this topic (matches AWS behaviour).
	for _, existing := range b.topicSubscriptions[topicArn] {
		if !existing.PendingConfirmation &&
			existing.Protocol == protocol &&
			existing.Endpoint == endpoint {
			return existing, nil
		}
	}

	parts := strings.Split(topic.TopicArn, ":")
	topicName := parts[len(parts)-1]

	subArn := arn.Build("sns", b.region, b.accountID, topicName+":"+uuid.New().String())

	// HTTP and HTTPS subscriptions require out-of-band confirmation.
	// Email/email-json require the recipient to click a link.
	// SQS, Lambda, Firehose, and Application (mobile push) are auto-confirmed.
	pending := protocol == protocolHTTP || protocol == protocolHTTPS ||
		protocol == protocolEmail || protocol == protocolEmailJSON

	sub := &Subscription{
		SubscriptionArn:     subArn,
		TopicArn:            topicArn,
		Protocol:            protocol,
		Endpoint:            endpoint,
		Owner:               b.accountID,
		FilterPolicy:        filterPolicy,
		parsedFilterPolicy:  parsedPolicy,
		PendingConfirmation: pending,
		CreationTimestamp:   time.Now().UTC(),
	}

	b.subscriptions[subArn] = sub
	b.indexSubscription(sub)

	return sub, nil
}

// Unsubscribe removes a subscription by ARN.
func (b *InMemoryBackend) Unsubscribe(subscriptionArn string) error {
	b.mu.Lock("Unsubscribe")
	defer b.mu.Unlock()

	sub, exists := b.subscriptions[subscriptionArn]
	if !exists {
		return ErrSubscriptionNotFound
	}

	delete(b.subscriptions, subscriptionArn)
	b.removeIndexedSubscription(sub.TopicArn, subscriptionArn)

	return nil
}

// ConfirmSubscription "confirms" a pending subscription.
// In the mock, any non-empty token is accepted.
// The subscription must belong to the given topicArn; if found and pending,
// PendingConfirmation is cleared and the subscription ARN is returned.
func (b *InMemoryBackend) ConfirmSubscription(topicArn, token string) (*Subscription, error) {
	if token == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("ConfirmSubscription")
	defer b.mu.Unlock()

	// Use topic index for O(topic_subs) instead of O(all_subs).
	for _, sub := range b.topicSubscriptions[topicArn] {
		if sub.PendingConfirmation {
			sub.PendingConfirmation = false

			return sub, nil
		}
	}

	return nil, ErrSubscriptionNotFound
}

// GetSubscriptionAttributes returns the attributes of a subscription.
func (b *InMemoryBackend) GetSubscriptionAttributes(subscriptionArn string) (map[string]string, error) {
	b.mu.RLock("GetSubscriptionAttributes")
	defer b.mu.RUnlock()

	sub, exists := b.subscriptions[subscriptionArn]
	if !exists {
		return nil, ErrSubscriptionNotFound
	}

	attrs := map[string]string{
		"SubscriptionArn":              sub.SubscriptionArn,
		topicArnKey:                    sub.TopicArn,
		"Protocol":                     sub.Protocol,
		"Endpoint":                     sub.Endpoint,
		"Owner":                        sub.Owner,
		attrPendingConfirmation:        strconv.FormatBool(sub.PendingConfirmation),
		"ConfirmationWasAuthenticated": strconv.FormatBool(!sub.PendingConfirmation),
		attrRawMessageDelivery:         strconv.FormatBool(sub.RawMessageDelivery),
	}

	if sub.FilterPolicy != "" {
		attrs[attrFilterPolicy] = sub.FilterPolicy
	}

	if sub.RedrivePolicy != "" {
		attrs[attrRedrivePolicy] = sub.RedrivePolicy
	}

	if sub.DeliveryPolicy != "" {
		attrs[attrDeliveryPolicy] = sub.DeliveryPolicy
	}

	if sub.ReplayPolicy != "" {
		attrs[attrReplayPolicy] = sub.ReplayPolicy
	}

	if sub.SubscriptionRoleArn != "" {
		attrs[attrSubscriptionRoleArn] = sub.SubscriptionRoleArn
	}

	if sub.FilterPolicyScope != "" {
		attrs[attrFilterPolicyScope] = sub.FilterPolicyScope
	}

	return attrs, nil
}

// SetSubscriptionAttributes sets a single attribute on a subscription.
// When ReplayPolicy is set to a non-empty value, archived messages from the topic
// (published at or after replayFromTimestamp) are asynchronously delivered to this
// subscription. This mirrors AWS SNS archive replay behaviour.
func (b *InMemoryBackend) SetSubscriptionAttributes(subscriptionArn, attrName, attrValue string) error {
	// Parse the FilterPolicy outside the backend lock so JSON validation does
	// not serialize against unrelated SNS operations on large policies.
	var parsedPolicy parsedFilterPolicy

	if attrName == attrFilterPolicy {
		p, err := parseFilterPolicy(attrValue)
		if err != nil {
			return err
		}

		parsedPolicy = p
	}

	// Pre-validate redrive policy outside the backend lock for the same reason.
	if attrName == attrRedrivePolicy && attrValue != "" {
		if err := validateRedrivePolicy(attrValue); err != nil {
			return err
		}
	}

	// Validate and parse ReplayPolicy before acquiring the lock so JSON parsing
	// and RFC3339 timestamp validation don't hold the lock.
	var replayFromTime time.Time
	if attrName == attrReplayPolicy && attrValue != "" {
		ts, err := parseReplayFromTimestamp(attrValue)
		if err != nil {
			return err
		}

		replayFromTime = ts
	}

	b.mu.Lock("SetSubscriptionAttributes")

	sub, exists := b.subscriptions[subscriptionArn]
	if !exists {
		b.mu.Unlock()

		return ErrSubscriptionNotFound
	}

	if err := applySubscriptionAttr(sub, attrName, attrValue, parsedPolicy); err != nil {
		b.mu.Unlock()

		return err
	}

	// Capture a snapshot for replay (after the attribute is applied so RawMessageDelivery etc. are current).
	subSnap := *sub
	topicArn := sub.TopicArn

	b.mu.Unlock()

	// Trigger asynchronous replay when ReplayPolicy is set to a non-empty value.
	if attrName == attrReplayPolicy && attrValue != "" && !replayFromTime.IsZero() {
		go b.replayMessagesToSubscription(subSnap, topicArn, replayFromTime)
	}

	return nil
}

// applySubscriptionAttr mutates sub with the given attribute value.
// Extracted to keep SetSubscriptionAttributes under the cyclomatic complexity budget.
func applySubscriptionAttr(sub *Subscription, attrName, attrValue string, parsedPolicy parsedFilterPolicy) error {
	switch attrName {
	case attrRawMessageDelivery:
		sub.RawMessageDelivery = strings.EqualFold(attrValue, "true")
	case attrFilterPolicy:
		sub.FilterPolicy = attrValue
		sub.parsedFilterPolicy = parsedPolicy
	case attrRedrivePolicy:
		sub.RedrivePolicy = attrValue
	case attrDeliveryPolicy:
		sub.DeliveryPolicy = attrValue
	case attrReplayPolicy:
		sub.ReplayPolicy = attrValue
	case attrSubscriptionRoleArn:
		sub.SubscriptionRoleArn = attrValue
	case attrFilterPolicyScope:
		return applyFilterPolicyScope(sub, attrValue)
	default:
		return ErrInvalidParameter
	}

	return nil
}

// applyFilterPolicyScope validates and sets the FilterPolicyScope field.
func applyFilterPolicyScope(sub *Subscription, attrValue string) error {
	if attrValue != "MessageBody" && attrValue != "MessageAttributes" {
		return fmt.Errorf(
			"%w: FilterPolicyScope must be MessageBody or MessageAttributes",
			ErrInvalidParameter,
		)
	}

	sub.FilterPolicyScope = attrValue

	return nil
}

// ListSubscriptions returns a page of subscriptions and the next pagination token.
func (b *InMemoryBackend) ListSubscriptions(nextToken string) ([]Subscription, string, error) {
	b.mu.RLock("ListSubscriptions")
	defer b.mu.RUnlock()

	all := b.sortedSubscriptions()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	subs, next := paginate(all, offset, pageSize)

	return subs, next, nil
}

// ListSubscriptionsByTopic returns a page of subscriptions for a topic and the next pagination token.
func (b *InMemoryBackend) ListSubscriptionsByTopic(topicArn, nextToken string) ([]Subscription, string, error) {
	b.mu.RLock("ListSubscriptionsByTopic")
	defer b.mu.RUnlock()

	if _, exists := b.topics[topicArn]; !exists {
		return nil, "", ErrTopicNotFound
	}

	topicSubs := b.topicSubscriptions[topicArn]
	filtered := make([]Subscription, 0, len(topicSubs))
	for _, sub := range topicSubs {
		filtered = append(filtered, *sub)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].SubscriptionArn < filtered[j].SubscriptionArn
	})

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	subs, next := paginate(filtered, offset, pageSize)

	return subs, next, nil
}

// httpDelivery holds the endpoint and message body for an HTTP/HTTPS delivery.
type httpDelivery struct {
	signer          *notificationSigner // nil disables signing
	sqsSender       SQSSender           // optional; non-nil when subscription has a DLQ
	endpoint        string
	body            string
	subject         string
	messageID       string
	topicARN        string
	subscriptionARN string
	redrivePolicy   string // JSON RedrivePolicy; non-empty when DLQ is configured
	rawDelivery     bool
}

// publishTargets holds the subscription snapshots and HTTP deliveries collected for a publish call.
type publishTargets struct {
	subs           []events.SNSSubscriptionSnapshot
	httpDeliveries []httpDelivery
}

type parsedFilterPolicy map[string][]json.RawMessage

// parseFilterPolicy parses and validates a FilterPolicy JSON string. It returns
// an empty (non-nil) policy for an empty input, or an InvalidParameter-wrapped
// error for any malformed input. Validation enforces:
//   - JSON is well-formed and is an object whose values are arrays.
//   - Total encoded size ≤ maxFilterPolicySizeBytes (256 KiB).
//   - Total attribute conditions ≤ maxFilterPolicyConditions (150).
//   - Object-condition operator names are restricted to the AWS-supported set
//     (`prefix`, `suffix`, `equals-ignore-case`, `anything-but`, `exists`,
//     `numeric`).
//   - Numeric operand shape (operator/number pairs) is well-formed.
//
// Nesting depth (for nested-object filter policies) is not yet enforced —
// issue #1679 item 13.
// maxFilterPolicyConditions is the AWS SNS cap on total attribute conditions
// across all keys in a single FilterPolicy (≈150 in production).
const maxFilterPolicyConditions = 150

func parseFilterPolicy(filterPolicy string) (parsedFilterPolicy, error) {
	if filterPolicy == "" {
		return parsedFilterPolicy{}, nil
	}

	if len(filterPolicy) > maxFilterPolicySizeBytes {
		return nil, fmt.Errorf(
			"%w: FilterPolicy exceeds %d bytes",
			ErrInvalidParameter, maxFilterPolicySizeBytes,
		)
	}

	var rawPolicy map[string]json.RawMessage
	if err := json.Unmarshal([]byte(filterPolicy), &rawPolicy); err != nil {
		return nil, fmt.Errorf("%w: FilterPolicy is not valid JSON: %s", ErrInvalidParameter, err.Error())
	}

	parsed := make(parsedFilterPolicy, len(rawPolicy))

	totalConditions := 0

	for key, rawConditions := range rawPolicy {
		var conditions []json.RawMessage
		if err := json.Unmarshal(rawConditions, &conditions); err != nil {
			return nil, fmt.Errorf(
				"%w: FilterPolicy attribute %q must be a JSON array",
				ErrInvalidParameter, key,
			)
		}

		totalConditions += len(conditions)
		if totalConditions > maxFilterPolicyConditions {
			return nil, fmt.Errorf(
				"%w: FilterPolicy exceeds %d total attribute conditions",
				ErrInvalidParameter, maxFilterPolicyConditions,
			)
		}

		// Eagerly validate numeric operand shapes so that a malformed numeric
		// condition is rejected at Subscribe/SetSubscriptionAttributes time
		// rather than silently failing every match at evaluation.
		if err := validateConditionShapes(key, conditions); err != nil {
			return nil, err
		}

		parsed[key] = conditions
	}

	return parsed, nil
}

// knownFilterPolicyOperators is the set of object-condition keys recognised
// by AWS SNS subscription FilterPolicy. Conditions containing any other key
// are rejected at Subscribe / SetSubscriptionAttributes time so misconfigurations
// surface immediately rather than silently mis-routing messages.
//
//nolint:gochecknoglobals // read-only lookup
var knownFilterPolicyOperators = map[string]struct{}{
	"prefix":             {},
	"suffix":             {},
	"equals-ignore-case": {},
	"anything-but":       {},
	"exists":             {},
	"numeric":            {},
}

// validateConditionShapes inspects each condition under a single FilterPolicy
// attribute and rejects unknown operator names and malformed numeric operand
// structures. Scalar conditions (plain strings, numbers, booleans, null) and
// known object operators are tolerated as-is and matched lazily at evaluation.
func validateConditionShapes(key string, conditions []json.RawMessage) error {
	for _, raw := range conditions {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			// Scalar conditions (e.g. plain strings) are valid; skip object-only checks.
			continue
		}

		for opName := range obj {
			if _, ok := knownFilterPolicyOperators[opName]; !ok {
				return fmt.Errorf(
					"%w: FilterPolicy attribute %q uses unsupported operator %q",
					ErrInvalidParameter, key, opName,
				)
			}
		}

		numericRaw, ok := obj["numeric"]
		if !ok {
			continue
		}

		if err := validateNumericOperands(key, numericRaw); err != nil {
			return err
		}
	}

	return nil
}

// validateNumericOperands enforces that a "numeric" condition operand is a JSON
// array of even length where each pair is (operator-string, number).
func validateNumericOperands(key string, raw json.RawMessage) error {
	var operands []json.RawMessage
	if err := json.Unmarshal(raw, &operands); err != nil {
		return fmt.Errorf(
			"%w: FilterPolicy attribute %q numeric operand must be a JSON array",
			ErrInvalidParameter, key,
		)
	}

	if len(operands)%2 != 0 || len(operands) == 0 {
		return fmt.Errorf(
			"%w: FilterPolicy attribute %q numeric operand must contain operator/number pairs",
			ErrInvalidParameter, key,
		)
	}

	validNumericOps := map[string]struct{}{
		"=": {}, "<>": {}, ">": {}, ">=": {}, "<": {}, "<=": {},
	}

	for i := 0; i+1 < len(operands); i += 2 {
		var op string
		if err := json.Unmarshal(operands[i], &op); err != nil {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q numeric operator must be a string",
				ErrInvalidParameter, key,
			)
		}

		if _, ok := validNumericOps[op]; !ok {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q numeric operator %q is not supported",
				ErrInvalidParameter, key, op,
			)
		}

		var num json.Number
		if err := json.Unmarshal(operands[i+1], &num); err != nil {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q numeric threshold must be a number",
				ErrInvalidParameter, key,
			)
		}

		if _, err := strconv.ParseFloat(num.String(), 64); err != nil {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q numeric threshold %s is not a finite number",
				ErrInvalidParameter, key, num.String(),
			)
		}
	}

	return nil
}

// kmsKeyIDPattern matches a bare KMS key ID (UUID-ish: 8-4-4-4-12 hex, lowercase).
var kmsKeyIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

// validateKmsMasterKeyID validates that v is a syntactically plausible KMS key
// reference accepted by AWS SNS: a bare alias name, an alias ARN, a key ID, a
// key ARN, or the special "alias/aws/sns" managed key alias. The check rejects
// obviously malformed values; full ARN-resource validation is delegated to KMS.
func validateKmsMasterKeyID(v string) error {
	switch {
	case strings.HasPrefix(v, "alias/"):
		// Alias names must be at least one character after the prefix.
		if len(v) <= len("alias/") {
			return fmt.Errorf("%w: KmsMasterKeyId alias must not be empty", ErrInvalidParameter)
		}

		return nil
	case strings.HasPrefix(v, "arn:"):
		// Accept any well-formed KMS ARN (key or alias).
		parts := strings.Split(v, ":")
		if len(parts) < 6 || parts[2] != "kms" {
			return fmt.Errorf("%w: KmsMasterKeyId is not a valid KMS ARN: %s", ErrInvalidParameter, v)
		}

		return nil
	case kmsKeyIDPattern.MatchString(v):
		return nil
	default:
		return fmt.Errorf("%w: KmsMasterKeyId is not a valid key ID, ARN, or alias: %s", ErrInvalidParameter, v)
	}
}

// validateRedrivePolicy validates the JSON redrive policy attached to a
// subscription. AWS requires deadLetterTargetArn to be a valid SQS queue ARN.
func validateRedrivePolicy(policy string) error {
	var parsed struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}

	if err := json.Unmarshal([]byte(policy), &parsed); err != nil {
		return fmt.Errorf("%w: RedrivePolicy is not valid JSON: %s", ErrInvalidParameter, err.Error())
	}

	if parsed.DeadLetterTargetArn == "" {
		return fmt.Errorf("%w: RedrivePolicy must include deadLetterTargetArn", ErrInvalidParameter)
	}

	parts := strings.Split(parsed.DeadLetterTargetArn, ":")
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "sqs" {
		return fmt.Errorf(
			"%w: RedrivePolicy.deadLetterTargetArn must be a valid SQS queue ARN, got %s",
			ErrInvalidParameter, parsed.DeadLetterTargetArn,
		)
	}

	return nil
}

// validMessageAttributeDataType reports whether the given DataType prefix is
// one of the SNS-supported scalar message-attribute data types.
func validMessageAttributeDataType(base string) bool {
	switch base {
	case "String", "String.Array", "Number", "Binary":
		return true
	}

	return false
}

// maxMessageAttributeNameLen is the AWS-documented cap for a message-attribute name.
const maxMessageAttributeNameLen = 256

// validateMessageAttributes enforces SNS validation rules on the per-message
// attribute map: each name 1..256 chars, each DataType is one of the supported
// scalar types or a "<base>.<subtype>" specialization, and the cumulative
// payload size (names + data types + values) does not exceed 256 KiB.
func validateMessageAttributes(attrs map[string]MessageAttribute) error {
	const maxAttrPayloadBytes = 256 * 1024

	total := 0

	for name, a := range attrs {
		if name == "" || len(name) > maxMessageAttributeNameLen {
			return fmt.Errorf(
				"%w: message attribute name must be 1..%d characters",
				ErrInvalidParameter, maxMessageAttributeNameLen,
			)
		}

		base := a.DataType
		if i := strings.Index(base, "."); i >= 0 {
			base = base[:i]
		}

		if !validMessageAttributeDataType(a.DataType) && !validMessageAttributeDataType(base) {
			return fmt.Errorf(
				"%w: message attribute %q has unsupported DataType %q",
				ErrInvalidParameter, name, a.DataType,
			)
		}

		total += len(name) + len(a.DataType) + len(a.StringValue)
		if total > maxAttrPayloadBytes {
			return fmt.Errorf(
				"%w: aggregate message attribute payload exceeds %d bytes",
				ErrInvalidParameter, maxAttrPayloadBytes,
			)
		}
	}

	return nil
}

// buildMessageResolver returns a function that picks the correct message body for a given protocol,
// respecting MessageStructure "json" per-protocol map when provided.
func buildMessageResolver(defaultMsg string, perProtocol map[string]string) func(string) string {
	return func(protocol string) string {
		if perProtocol == nil {
			return defaultMsg
		}

		if msg, ok := perProtocol[protocol]; ok {
			return msg
		}

		if msg, ok := perProtocol["default"]; ok {
			return msg
		}

		return defaultMsg
	}
}

// collectPublishTargets scans b.subscriptions for a given topicArn and returns
// subscription snapshots and HTTP/HTTPS deliveries to dispatch.
// Must be called with at least RLock held.
func (b *InMemoryBackend) collectPublishTargets(
	topicArn, subject string,
	resolveMsg func(string) string,
	attrs map[string]MessageAttribute,
) publishTargets {
	var out publishTargets

	for _, sub := range b.topicSubscriptions[topicArn] {
		// Resolve the per-protocol message body for this subscription.
		// This must happen before filter evaluation when FilterPolicyScope=MessageBody,
		// because the body itself is the subject of the filter.
		msg := resolveMsg(sub.Protocol)

		// Apply filter policy. When FilterPolicyScope is "MessageBody", the filter
		// is evaluated against the message body parsed as a JSON object. The default
		// scope "MessageAttributes" (or unset) evaluates against message attributes.
		if sub.FilterPolicyScope == "MessageBody" {
			if !matchesFilterPolicyMessageBody(sub.parsedFilterPolicy, msg) {
				continue
			}
		} else {
			if !matchesParsedFilterPolicy(sub.parsedFilterPolicy, attrs) {
				continue
			}
		}

		if sub.Protocol == protocolHTTP || sub.Protocol == protocolHTTPS {
			out.httpDeliveries = append(out.httpDeliveries, httpDelivery{
				endpoint:        sub.Endpoint,
				body:            msg,
				subject:         subject,
				subscriptionARN: sub.SubscriptionArn,
				rawDelivery:     sub.RawMessageDelivery,
				redrivePolicy:   sub.RedrivePolicy,
				sqsSender:       b.sqsSender,
			})
		}

		out.subs = append(out.subs, events.SNSSubscriptionSnapshot{
			SubscriptionARN:    sub.SubscriptionArn,
			Protocol:           sub.Protocol,
			Endpoint:           sub.Endpoint,
			FilterPolicy:       sub.FilterPolicy,
			RawMessageDelivery: sub.RawMessageDelivery,
			RedrivePolicy:      sub.RedrivePolicy,
		})
	}

	return out
}

// matchesFilterPolicyMessageBody evaluates a parsed filter policy against the
// message body when FilterPolicyScope=MessageBody. The message must be a valid
// JSON object; if it is not, no subscription receives the message.
func matchesFilterPolicyMessageBody(policy parsedFilterPolicy, message string) bool {
	if len(policy) == 0 {
		return true
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal([]byte(message), &body); err != nil {
		return false
	}

	for key, conditions := range policy {
		rawVal, exists := body[key]
		if !exists {
			if !matchesConditions("", false, conditions) {
				return false
			}

			continue
		}

		// Try to decode the JSON field as a string value for condition matching.
		var strVal string
		if err := json.Unmarshal(rawVal, &strVal); err != nil {
			// Try number.
			var numVal json.Number
			if err2 := json.Unmarshal(rawVal, &numVal); err2 != nil {
				// Cannot extract a scalar — treat as not-existing for filter.
				if !matchesConditions("", false, conditions) {
					return false
				}

				continue
			}

			strVal = numVal.String()
		}

		if !matchesConditions(strVal, true, conditions) {
			return false
		}
	}

	return true
}

// Publish publishes a message to a topic and returns the message ID.
// HTTP/HTTPS subscriptions each receive an asynchronous best-effort delivery
// validateStructuredMessage validates a MessageStructure=json payload.
// Returns nil for non-json messageStructure values.
func validateStructuredMessage(message, messageStructure string) error {
	if messageStructure != "json" {
		return nil
	}

	var pm map[string]string
	if err := json.Unmarshal([]byte(message), &pm); err != nil {
		return fmt.Errorf(
			"%w: Invalid JSON in Message when MessageStructure is json: %s",
			ErrInvalidParameter,
			err.Error(),
		)
	}

	if _, ok := pm["default"]; !ok {
		return fmt.Errorf(
			"%w: Message must contain a 'default' key when MessageStructure is json",
			ErrInvalidParameter,
		)
	}

	return nil
}

// parsePerProtocolMessages parses a MessageStructure=json payload into a
// per-protocol map. Returns nil for non-json messageStructure values.
// Callers must have already validated the message with validateStructuredMessage.
func parsePerProtocolMessages(message, messageStructure string) map[string]string {
	if messageStructure != "json" {
		return nil
	}

	var pm map[string]string
	if err := json.Unmarshal([]byte(message), &pm); err != nil {
		return nil
	}

	return pm
}

// validatePublishMessage checks message size, subject format, structure, and
// attribute constraints before any backend lock is acquired.
func validatePublishMessage(message, subject, messageStructure string, attrs map[string]MessageAttribute) error {
	// AWS SNS counts the message body plus every attribute name + type + value
	// toward the 256 KiB cap.
	totalSize := len(message)
	for name, a := range attrs {
		totalSize += len(name) + len(a.DataType) + len(a.StringValue)
	}

	if totalSize > maxMessageSizeBytes {
		return fmt.Errorf(
			"%w: Message size exceeds SNS limit of %d bytes",
			ErrInvalidParameter,
			maxMessageSizeBytes,
		)
	}

	// AWS SNS rejects subjects longer than 100 characters or containing non-ASCII
	// printable characters (control characters, high-byte runes).
	if subject != "" {
		if len(subject) > maxSubjectLen {
			return fmt.Errorf(
				"%w: Subject must be no longer than %d characters",
				ErrInvalidParameter,
				maxSubjectLen,
			)
		}

		for _, r := range subject {
			if r < 0x20 || r > 0x7E {
				return fmt.Errorf(
					"%w: Subject contains invalid characters; must be printable ASCII",
					ErrInvalidParameter,
				)
			}
		}
	}

	if err := validateStructuredMessage(message, messageStructure); err != nil {
		return err
	}

	return validateMessageAttributes(attrs)
}

// dispatchHTTPDeliveries schedules asynchronous HTTP/HTTPS deliveries for each
// entry in deliveries. The closing check is evaluated once so that either all
// deliveries for a given Publish call are scheduled or none are, avoiding
// partial delivery when shutdown is in progress.
func (b *InMemoryBackend) dispatchHTTPDeliveries(deliveries []httpDelivery, client *http.Client) {
	if b.closing.Load() {
		return
	}

	ctx := b.svcCtx
	for _, d := range deliveries {
		b.deliveryWg.Go(func() {
			select {
			case b.workerSem <- struct{}{}:
				defer func() { <-b.workerSem }()
				deliverHTTPWithMeta(ctx, d, client)
			case <-ctx.Done():
				// Service is shutting down; drop this delivery rather than
				// blocking indefinitely on a full semaphore.
			}
		})
	}
}

// emitPublishedEvent fires an SNSPublishedEvent so other services (e.g. SQS)
// can react. It is a no-op when no emitter has been registered.
func (b *InMemoryBackend) emitPublishedEvent(
	topicArn, messageID, message, subject string,
	attrs map[string]MessageAttribute,
	subs []events.SNSSubscriptionSnapshot,
) {
	if b.emitter == nil {
		return
	}

	attrSnaps := make(map[string]events.SNSMessageAttributeSnapshot, len(attrs))
	for k, v := range attrs {
		attrSnaps[k] = events.SNSMessageAttributeSnapshot{
			DataType:    v.DataType,
			StringValue: v.StringValue,
		}
	}

	// Compute a fixed timestamp and RSA-SHA1 signature so SQS envelope
	// delivery can include a verifiable Signature field per AWS spec.
	ts := time.Now().UTC().Format(time.RFC3339)
	canonical := canonicalNotificationString(messageID, topicArn, subject, message, ts)
	sig := b.signer.sign(canonical)
	certURL := b.signer.certURL

	_ = b.emitter.Emit(b.svcCtx, &events.SNSPublishedEvent{
		TopicARN:       topicArn,
		MessageID:      messageID,
		Message:        message,
		Subject:        subject,
		Subscriptions:  subs,
		Attributes:     attrSnaps,
		Timestamp:      ts,
		Signature:      sig,
		SigningCertURL: certURL,
	})
}

// Publish delivers a message to all subscriptions of topicArn. HTTP/HTTPS
// subscriptions each receive an asynchronous best-effort delivery goroutine
// after the read lock is released to avoid lock starvation. Goroutines
// wait for a concurrency slot (up to snsMaxConcurrentDeliveries concurrent HTTP
// calls) or exit early if the backend is shutting down.
// All subscriptions are also broadcast via the publish emitter (e.g. to SQS).
func (b *InMemoryBackend) Publish(
	topicArn, message, subject, messageStructure string, attrs map[string]MessageAttribute,
) (string, error) {
	if err := validatePublishMessage(message, subject, messageStructure, attrs); err != nil {
		return "", err
	}

	b.mu.RLock("Publish")

	topic, exists := b.topics[topicArn]
	if !exists {
		b.mu.RUnlock()

		return "", ErrTopicNotFound
	}

	// Capture whether this topic archives messages (ArchivePolicy present).
	archivePolicy := topic.Attributes["ArchivePolicy"]

	messageID := uuid.New().String()

	// resolveMsg returns the appropriate message body for a given protocol.
	resolveMsg := buildMessageResolver(message, parsePerProtocolMessages(message, messageStructure))

	// Build subscription snapshot and collect HTTP deliveries — all under RLock.
	targets := b.collectPublishTargets(topicArn, subject, resolveMsg, attrs)

	// Annotate HTTP deliveries with messageID, topicARN, and signer for SNS envelope/headers.
	signer := b.signer
	for i := range targets.httpDeliveries {
		targets.httpDeliveries[i].messageID = messageID
		targets.httpDeliveries[i].topicARN = topicArn
		targets.httpDeliveries[i].signer = signer
	}

	// Capture httpClient under the read lock to avoid data races with
	// concurrent SetHTTPDeliveryClient calls.
	client := b.httpClient

	// Release the read lock before performing any network I/O so that slow or
	// unresponsive HTTP endpoints do not block write operations on the backend.
	b.mu.RUnlock()

	// Archive the message when the topic has an ArchivePolicy (e.g. FIFO topics
	// with message retention). Archived messages are used for subscription replay.
	if archivePolicy != "" {
		b.archivePublishedMessage(topicArn, messageID, message, subject, attrs)
	}

	b.dispatchHTTPDeliveries(targets.httpDeliveries, client)

	b.emitPublishedEvent(topicArn, messageID, message, subject, attrs, targets.subs)

	ev := &events.SNSPublishedEvent{
		TopicARN:      topicArn,
		MessageID:     messageID,
		Message:       message,
		Subject:       subject,
		Subscriptions: targets.subs,
	}
	b.deliverToLambdaSubscriptions(ev)
	b.deliverToFirehoseSubscriptions(ev)

	return messageID, nil
}

// PublishToTargetArn publishes a message directly to a platform endpoint ARN.
// Returns ErrEndpointDisabled when the endpoint has Enabled=false, matching
// the AWS EndpointDisabled error that triggers automatic endpoint disabling.
// In the mock, no actual push delivery occurs beyond generating the message ID.
func (b *InMemoryBackend) PublishToTargetArn(
	targetArn, _ /* message */, _ /* subject */ string,
	_ map[string]MessageAttribute,
) (string, error) {
	b.mu.RLock("PublishToTargetArn")
	defer b.mu.RUnlock()

	ep, exists := b.platformEndpoints[targetArn]
	if !exists {
		return "", ErrEndpointNotFound
	}

	if ep.Attributes["Enabled"] == boolFalseStr {
		return "", fmt.Errorf("%w: endpoint %s is disabled", ErrEndpointDisabled, targetArn)
	}

	return uuid.New().String(), nil
}

// PublishSMS publishes a message directly to a phone number via SMS.
// The delivery is recorded in smsDeliveries so tests can assert on it via DrainSMSDeliveries.
// Returns ErrOptedOut when the destination number has opted out of SMS messages.
// Returns ErrSandboxPhoneNotVerified when the number is in the SMS sandbox but not yet verified.
func (b *InMemoryBackend) PublishSMS(phoneNumber, message string) (string, error) {
	if !isValidE164(phoneNumber) {
		return "", fmt.Errorf("%w: Invalid phone number; must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.RLock("PublishSMS-check")
	sandboxEntry := b.smsSandbox[phoneNumber]
	optedOut := b.optedOutPhoneNumbers[phoneNumber]
	b.mu.RUnlock()

	// Opted-out numbers must not receive SMS regardless of sandbox state.
	if optedOut {
		return "", fmt.Errorf("%w: phone number %s has opted out of SMS messages", ErrOptedOut, phoneNumber)
	}

	// When the number is registered in the sandbox, it must be verified before
	// SMS can be sent to it. Unverified numbers in the sandbox mirror real AWS
	// sandbox behaviour where only verified destinations are allowed.
	if sandboxEntry != nil && sandboxEntry.Status != "Verified" {
		return "", fmt.Errorf(
			"%w: phone number %s is registered in the SMS sandbox but has not been verified",
			ErrSandboxPhoneNotVerified, phoneNumber,
		)
	}

	msgID := uuid.New().String()

	b.mu.Lock("PublishSMS")
	b.smsDeliveries = append(b.smsDeliveries, SMSDelivery{
		PhoneNumber: phoneNumber,
		Message:     message,
		MessageID:   msgID,
	})
	b.mu.Unlock()

	return msgID, nil
}

// DrainSMSDeliveries returns and clears all recorded SMS deliveries.
// This is intended for test assertions to verify SMS messages sent via PublishSMS.
func (b *InMemoryBackend) DrainSMSDeliveries() []SMSDelivery {
	b.mu.Lock("DrainSMSDeliveries")
	defer b.mu.Unlock()

	deliveries := b.smsDeliveries
	b.smsDeliveries = nil

	return deliveries
}

func matchesParsedFilterPolicy(policy parsedFilterPolicy, attrs map[string]MessageAttribute) bool {
	if policy == nil {
		return true
	}

	for key, conditions := range policy {
		attr, attrExists := attrs[key]
		if !matchesConditions(attr.StringValue, attrExists, conditions) {
			return false
		}
	}

	return true
}

// matchObjectCondition evaluates a single JSON-object SNS filter condition such as
// {"prefix": "order-"}, {"suffix": ".jpg"}, {"anything-but": [...]},
// {"equals-ignore-case": "OrderId"}, {"exists": true}, or {"numeric": [">", 0]}.
func matchObjectCondition(value string, attrExists bool, obj map[string]json.RawMessage) bool {
	if prefixRaw, ok := obj["prefix"]; ok {
		var prefix string
		if err := json.Unmarshal(prefixRaw, &prefix); err == nil {
			return attrExists && strings.HasPrefix(value, prefix)
		}

		return false
	}

	if suffixRaw, ok := obj["suffix"]; ok {
		var suffix string
		if err := json.Unmarshal(suffixRaw, &suffix); err == nil {
			return attrExists && strings.HasSuffix(value, suffix)
		}

		return false
	}

	if eqICaseRaw, ok := obj["equals-ignore-case"]; ok {
		var want string
		if err := json.Unmarshal(eqICaseRaw, &want); err == nil {
			return attrExists && strings.EqualFold(value, want)
		}

		return false
	}

	if existsRaw, ok := obj["exists"]; ok {
		var existsVal bool
		if err := json.Unmarshal(existsRaw, &existsVal); err == nil {
			return attrExists == existsVal
		}

		return false
	}

	if anythingButRaw, ok := obj["anything-but"]; ok {
		return matchAnythingBut(value, attrExists, anythingButRaw)
	}

	if numericRaw, ok := obj["numeric"]; ok {
		return attrExists && matchNumericCondition(value, numericRaw)
	}

	return false
}

// matchAnythingBut handles {"anything-but": value}, {"anything-but": [...]},
// and {"anything-but": {"prefix": "..."}} conditions.
func matchAnythingBut(value string, attrExists bool, raw json.RawMessage) bool {
	if !attrExists {
		return false
	}

	// Try as string literal.
	var s string
	if errStr := json.Unmarshal(raw, &s); errStr == nil {
		return value != s
	}

	// Try as number literal.
	var n json.Number
	if errNum := json.Unmarshal(raw, &n); errNum == nil {
		return value != n.String()
	}

	// Try as array of literals.
	var arr []json.RawMessage
	if errArr := json.Unmarshal(raw, &arr); errArr == nil {
		return matchAnythingButArray(value, arr)
	}

	// Try as nested prefix object: {"anything-but": {"prefix": "order-"}}.
	var prefixObj map[string]json.RawMessage
	if errObj := json.Unmarshal(raw, &prefixObj); errObj == nil {
		if prefixRaw, ok := prefixObj["prefix"]; ok {
			var prefix string
			if errP := json.Unmarshal(prefixRaw, &prefix); errP == nil {
				return !strings.HasPrefix(value, prefix)
			}
		}
	}

	return true
}

// matchAnythingButArray checks that value does not equal any element in the "anything-but" array.
func matchAnythingButArray(value string, arr []json.RawMessage) bool {
	for _, item := range arr {
		var sv string
		if errI := json.Unmarshal(item, &sv); errI == nil {
			if value == sv {
				return false
			}

			continue
		}

		var nv json.Number
		if errN := json.Unmarshal(item, &nv); errN == nil && value == nv.String() {
			return false
		}
	}

	return true
}

// matchNumericCondition evaluates {"numeric": [op, num, ...]} conditions.
// Conditions are pairs of [operator, number] and ALL pairs must be satisfied (AND semantics).
func matchNumericCondition(value string, raw json.RawMessage) bool {
	valFloat, errParse := strconv.ParseFloat(value, 64)
	if errParse != nil {
		return false
	}

	var conditions []json.RawMessage
	if errUnm := json.Unmarshal(raw, &conditions); errUnm != nil {
		return false
	}

	for i := 0; i+1 < len(conditions); i += 2 {
		var op string
		if errOp := json.Unmarshal(conditions[i], &op); errOp != nil {
			return false
		}

		var num json.Number
		if errNum := json.Unmarshal(conditions[i+1], &num); errNum != nil {
			return false
		}

		threshold, errThresh := strconv.ParseFloat(num.String(), 64)
		if errThresh != nil {
			return false
		}

		if !numericOpMatches(op, valFloat, threshold) {
			return false
		}
	}

	return true
}

// numericOpMatches evaluates a single numeric comparison operator.
func numericOpMatches(op string, value, threshold float64) bool {
	switch op {
	case "=":
		return value == threshold
	case "<>":
		return value != threshold
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	default:
		return false
	}
}

func matchCondition(value string, attrExists bool, raw json.RawMessage) bool {
	// Object conditions: prefix, exists, anything-but, numeric.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		return matchObjectCondition(value, attrExists, obj)
	}

	// String exact match — attribute must exist.
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return attrExists && value == s
	}

	// Number exact match — attribute must exist.
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return attrExists && value == n.String()
	}

	return false
}

// matchesConditions returns true if value/existence satisfies at least one condition in the list.
func matchesConditions(value string, attrExists bool, conditions []json.RawMessage) bool {
	for _, raw := range conditions {
		if matchCondition(value, attrExists, raw) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) indexSubscription(sub *Subscription) {
	topicSubs := b.topicSubscriptions[sub.TopicArn]
	if topicSubs == nil {
		topicSubs = make(map[string]*Subscription)
		b.topicSubscriptions[sub.TopicArn] = topicSubs
	}

	topicSubs[sub.SubscriptionArn] = sub
}

func (b *InMemoryBackend) removeIndexedSubscription(topicArn, subscriptionArn string) {
	topicSubs := b.topicSubscriptions[topicArn]
	if topicSubs == nil {
		return
	}

	delete(topicSubs, subscriptionArn)
	// Preserve the inner map even when empty so the next Subscribe call does not
	// need to re-allocate. Only remove it when the topic itself is deleted.
}

// ListAllTopics returns all topics sorted by ARN.
func (b *InMemoryBackend) ListAllTopics() []Topic {
	b.mu.RLock("ListAllTopics")
	defer b.mu.RUnlock()

	return b.sortedTopics()
}

// ListAllSubscriptions returns all subscriptions sorted by ARN.
func (b *InMemoryBackend) ListAllSubscriptions() []Subscription {
	b.mu.RLock("ListAllSubscriptions")
	defer b.mu.RUnlock()

	return b.sortedSubscriptions()
}

// ListAllPlatformApplications returns all platform applications sorted by ARN.
func (b *InMemoryBackend) ListAllPlatformApplications() []PlatformApplication {
	b.mu.RLock("ListAllPlatformApplications")
	defer b.mu.RUnlock()

	apps := make([]PlatformApplication, 0, len(b.platformApplications))
	for _, app := range b.platformApplications {
		apps = append(apps, *app)
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].PlatformApplicationArn < apps[j].PlatformApplicationArn
	})

	return apps
}

// sortedTopics returns topics sorted by TopicArn. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedTopics() []Topic {
	topics := make([]Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, *t)
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].TopicArn < topics[j].TopicArn
	})

	return topics
}

// sortedSubscriptions returns subscriptions sorted by SubscriptionArn. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedSubscriptions() []Subscription {
	subs := make([]Subscription, 0, len(b.subscriptions))
	for _, s := range b.subscriptions {
		subs = append(subs, *s)
	}

	sort.Slice(subs, func(i, j int) bool {
		return subs[i].SubscriptionArn < subs[j].SubscriptionArn
	})

	return subs
}

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

// deliverHTTPWithMeta sends a best-effort HTTP POST with SNS notification headers
// to the endpoint. Standard AWS SNS headers are added when metadata is available.
// When rawDelivery is false the body is wrapped in a SNS Notification JSON envelope
// (matching what AWS SNS sends to http/https subscribers by default).
// On network error or non-2xx response, the message is forwarded to the DLQ when
// a RedrivePolicy is configured.
func deliverHTTPWithMeta(parent context.Context, d httpDelivery, client *http.Client) {
	ctx, cancel := context.WithTimeout(parent, snsHTTPTimeout)
	defer cancel()

	body := d.body

	// When RawMessageDelivery is false (the default), wrap the message in the
	// standard SNS Notification JSON envelope. This matches what real AWS SNS
	// delivers to http/https subscribers so that notification handling libraries
	// (e.g. aws-sns-body-parser) can parse the payload correctly.
	if !d.rawDelivery && d.messageID != "" {
		timestamp := time.Now().UTC().Format(time.RFC3339)

		certURL := "https://sns.us-east-1.amazonaws.com/SimpleNotificationService.pem"
		signature := "MOCK-SIGNATURE"
		if d.signer != nil {
			certURL = d.signer.certURL
			canonical := canonicalNotificationString(
				d.messageID, d.topicARN, d.subject, d.body, timestamp,
			)
			signature = d.signer.sign(canonical)
		}

		env := snsHTTPNotification{
			Type:             messageTypeNotification,
			MessageID:        d.messageID,
			TopicArn:         d.topicARN,
			Message:          d.body,
			Timestamp:        timestamp,
			SignatureVersion: "2",
			Signature:        signature,
			SigningCertURL:   certURL,
			UnsubscribeURL:   "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=" + d.subscriptionARN,
		}
		if d.subject != "" {
			env.Subject = d.subject
		}

		if enc, err := json.Marshal(env); err == nil {
			body = string(enc)
		}
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		d.endpoint,
		strings.NewReader(body),
	)
	if err != nil {
		sendSubscriptionDLQ(parent, d)

		return
	}

	req.Header.Set("Content-Type", "application/json")

	// Add standard AWS SNS HTTP notification headers.
	req.Header.Set("X-Amz-Sns-Message-Type", messageTypeNotification)
	if d.messageID != "" {
		req.Header.Set("X-Amz-Sns-Message-Id", d.messageID)
	}
	if d.topicARN != "" {
		req.Header.Set("X-Amz-Sns-Topic-Arn", d.topicARN)
	}
	if d.subscriptionARN != "" {
		req.Header.Set("X-Amz-Sns-Subscription-Arn", d.subscriptionARN)
	}

	resp, err := client.Do(req)
	if err != nil {
		sendSubscriptionDLQ(parent, d)

		return
	}

	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDeliveryResponseBytes))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		sendSubscriptionDLQ(parent, d)
	}
}

// sendSubscriptionDLQ delivers the message body to the DLQ configured in d.redrivePolicy when
// d.sqsSender is non-nil. It is a no-op when either is absent.
func sendSubscriptionDLQ(ctx context.Context, d httpDelivery) {
	if d.sqsSender == nil || d.redrivePolicy == "" {
		return
	}

	var policy struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}

	if err := json.Unmarshal([]byte(d.redrivePolicy), &policy); err != nil {
		return
	}

	if policy.DeadLetterTargetArn == "" {
		return
	}

	_ = d.sqsSender.SendMessageToQueue(ctx, policy.DeadLetterTargetArn, d.body)
}

// decodeToken decodes a base64 pagination token into an integer offset.
// An empty token is treated as offset 0.
func decodeToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, err
	}

	offset, err := strconv.Atoi(string(decoded))
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// encodeToken encodes an integer offset as a base64 pagination token.
func encodeToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// paginate returns a page of items and the next token, or an empty token when exhausted.
func paginate[T any](items []T, offset, size int) ([]T, string) {
	if offset >= len(items) {
		return []T{}, ""
	}

	end := offset + size
	nextToken := ""

	if end < len(items) {
		nextToken = encodeToken(end)
	} else {
		end = len(items)
	}

	return items[offset:end], nextToken
}

// resolvePageSize returns the effective page size given a caller-requested size, a default,
// and a maximum. If requested is 0, defaultSize is used. If requested exceeds maxSize it is clamped.
func resolvePageSize(requested, defaultSize, maxSize int) int {
	if requested <= 0 {
		return defaultSize
	}

	if requested > maxSize {
		return maxSize
	}

	return requested
}

// GetTopicTags returns tags for the given topic ARN.
func (b *InMemoryBackend) GetTopicTags(arn string) map[string]string {
	b.mu.RLock("GetTopicTags")
	defer b.mu.RUnlock()
	if b.topicTags[arn] == nil {
		return map[string]string{}
	}

	return b.topicTags[arn].Clone()
}

// SetTopicTags stores tags for the given topic ARN.
func (b *InMemoryBackend) SetTopicTags(arn string, kv *svcTags.Tags) {
	b.mu.Lock("SetTopicTags")
	defer b.mu.Unlock()
	if kv == nil {
		return
	}
	if b.topicTags[arn] == nil {
		b.topicTags[arn] = svcTags.New("sns." + arn + ".tags")
	}
	b.topicTags[arn].Merge(kv.Clone())
}

// RemoveTopicTags removes specified tag keys for the given topic ARN.
func (b *InMemoryBackend) RemoveTopicTags(arn string, keys []string) {
	b.mu.Lock("RemoveTopicTags")
	defer b.mu.Unlock()
	if b.topicTags[arn] != nil {
		b.topicTags[arn].DeleteKeys(keys)
	}
}

// TaggedTopicInfo contains a topic's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedTopicInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedTopics returns a snapshot of all SNS topics with their tags.
// Intended for use by the Resource Groups Tagging API provider.
func (b *InMemoryBackend) TaggedTopics() []TaggedTopicInfo {
	b.mu.RLock("TaggedTopics")
	defer b.mu.RUnlock()

	result := make([]TaggedTopicInfo, 0, len(b.topics))

	for topicARN := range b.topics {
		var tagMap map[string]string
		if b.topicTags[topicARN] != nil {
			tagMap = b.topicTags[topicARN].Clone()
		}

		result = append(result, TaggedTopicInfo{ARN: topicARN, Tags: tagMap})
	}

	return result
}

// TagTopicByARN applies tags to the SNS topic identified by its ARN.
func (b *InMemoryBackend) TagTopicByARN(topicARN string, newTags map[string]string) error {
	b.mu.Lock("TagTopicByARN")
	defer b.mu.Unlock()

	if _, ok := b.topics[topicARN]; !ok {
		return fmt.Errorf("%w: topic %s", ErrTopicNotFound, topicARN)
	}

	if b.topicTags[topicARN] == nil {
		b.topicTags[topicARN] = svcTags.New("sns." + topicARN + ".tags")
	}

	b.topicTags[topicARN].Merge(newTags)

	return nil
}

// UntagTopicByARN removes the specified tag keys from the SNS topic identified by its ARN.
func (b *InMemoryBackend) UntagTopicByARN(topicARN string, tagKeys []string) error {
	b.mu.Lock("UntagTopicByARN")
	defer b.mu.Unlock()

	if _, ok := b.topics[topicARN]; !ok {
		return fmt.Errorf("%w: topic %s", ErrTopicNotFound, topicARN)
	}

	if b.topicTags[topicARN] != nil {
		b.topicTags[topicARN].DeleteKeys(tagKeys)
	}

	return nil
}

// CreatePlatformApplication creates a new SNS platform application (e.g. GCM, APNS).
func (b *InMemoryBackend) CreatePlatformApplication(
	name, platform string,
	attributes map[string]string,
) (*PlatformApplication, error) {
	if strings.ContainsAny(name, "/") || strings.ContainsAny(platform, "/") {
		return nil, fmt.Errorf("%w: Name and Platform must not contain '/'", ErrInvalidParameter)
	}

	// Validate platform is one of the known AWS SNS platforms.
	// FCM is the Firebase Cloud Messaging platform (successor to GCM).
	// APNS_VOIP and APNS_VOIP_SANDBOX support Apple VoIP push notifications.
	validPlatforms := map[string]bool{
		"GCM": true, "FCM": true, "APNS": true, "APNS_SANDBOX": true,
		"APNS_VOIP": true, "APNS_VOIP_SANDBOX": true,
		"ADM": true, "BAIDU": true, "WNS": true, "MPNS": true,
	}
	if !validPlatforms[platform] {
		return nil, fmt.Errorf(
			"%w: Platform must be one of GCM, FCM, APNS, APNS_SANDBOX, APNS_VOIP, APNS_VOIP_SANDBOX, ADM, BAIDU, WNS, MPNS",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreatePlatformApplication")
	defer b.mu.Unlock()

	appArn := arn.Build("sns", b.region, b.accountID, "app/"+platform+"/"+name)

	if _, exists := b.platformApplications[appArn]; exists {
		return nil, ErrPlatformApplicationAlreadyExists
	}

	attrs := make(map[string]string, len(attributes)+1)
	maps.Copy(attrs, attributes)

	// AWS always returns Enabled=true for newly created platform applications.
	if attrs["Enabled"] == "" {
		attrs["Enabled"] = "true"
	}

	app := &PlatformApplication{
		PlatformApplicationArn: appArn,
		Attributes:             attrs,
		CreationTimestamp:      time.Now().UTC(),
	}
	b.platformApplications[appArn] = app

	return app, nil
}

// GetPlatformApplicationAttributes returns the attributes of a platform application.
// In addition to stored attributes, computed statistics are returned:
//   - Enabled: always "true" for the application itself (not to be confused with endpoint Enabled).
//   - EndpointActive: the number of enabled platform endpoints for this application.
//   - EndpointDisabled: the number of disabled platform endpoints.
func (b *InMemoryBackend) GetPlatformApplicationAttributes(platformApplicationArn string) (map[string]string, error) {
	b.mu.RLock("GetPlatformApplicationAttributes")
	defer b.mu.RUnlock()

	app, exists := b.platformApplications[platformApplicationArn]
	if !exists {
		return nil, ErrPlatformApplicationNotFound
	}

	// Count active and disabled endpoints for this application.
	var activeCount, disabledCount int
	for _, ep := range b.platformEndpoints {
		if ep.PlatformApplicationArn != platformApplicationArn {
			continue
		}

		if ep.Attributes["Enabled"] == boolFalseStr {
			disabledCount++
		} else {
			activeCount++
		}
	}

	const computedCountFields = 2
	attrs := make(map[string]string, len(app.Attributes)+computedCountFields)
	maps.Copy(attrs, app.Attributes)

	// AWS always returns these computed counts.
	attrs["EndpointActive"] = strconv.Itoa(activeCount)
	attrs["EndpointDisabled"] = strconv.Itoa(disabledCount)

	return attrs, nil
}

// SetPlatformApplicationAttributes updates attributes on a platform application.
func (b *InMemoryBackend) SetPlatformApplicationAttributes(
	platformApplicationArn string,
	attributes map[string]string,
) error {
	b.mu.Lock("SetPlatformApplicationAttributes")
	defer b.mu.Unlock()

	app, exists := b.platformApplications[platformApplicationArn]
	if !exists {
		return ErrPlatformApplicationNotFound
	}

	maps.Copy(app.Attributes, attributes)

	return nil
}

// ListPlatformApplications returns a page of platform applications and the next pagination token.
func (b *InMemoryBackend) ListPlatformApplications(nextToken string) ([]PlatformApplication, string, error) {
	b.mu.RLock("ListPlatformApplications")
	defer b.mu.RUnlock()

	all := b.sortedPlatformApplications()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	apps, next := paginate(all, offset, pageSize)

	return apps, next, nil
}

// DeletePlatformApplication removes a platform application and its endpoints by ARN.
func (b *InMemoryBackend) DeletePlatformApplication(platformApplicationArn string) error {
	b.mu.Lock("DeletePlatformApplication")
	defer b.mu.Unlock()

	if _, exists := b.platformApplications[platformApplicationArn]; !exists {
		return ErrPlatformApplicationNotFound
	}

	delete(b.platformApplications, platformApplicationArn)

	// Remove all endpoints associated with this platform application.
	for endpointArn, ep := range b.platformEndpoints {
		if ep.PlatformApplicationArn == platformApplicationArn {
			delete(b.platformEndpoints, endpointArn)
		}
	}

	return nil
}

// CreatePlatformEndpoint registers a device token as an endpoint for a platform application.
// AWS deduplication behaviour: if an endpoint with the same token already exists under this
// platform application, the existing endpoint ARN is returned instead of creating a new one.
// After creation, an EventEndpointCreated event is fired to the platform application's configured
// event topic, if any.
func (b *InMemoryBackend) CreatePlatformEndpoint(
	platformApplicationArn, token string,
	attributes map[string]string,
) (*PlatformEndpoint, error) {
	b.mu.Lock("CreatePlatformEndpoint")

	app, exists := b.platformApplications[platformApplicationArn]
	if !exists {
		b.mu.Unlock()

		return nil, ErrPlatformApplicationNotFound
	}

	// Dedup: return the existing endpoint when the same token is already registered
	// under this platform application (mirrors AWS CreatePlatformEndpoint behaviour).
	for _, ep := range b.platformEndpoints {
		if ep.PlatformApplicationArn == platformApplicationArn &&
			ep.Attributes["Token"] == token {
			b.mu.Unlock()

			return ep, nil
		}
	}

	// Derive the platform and app name from the platform application ARN.
	// ARN format: arn:aws:sns:{region}:{accountID}:app/{Platform}/{AppName}
	parts := strings.Split(app.PlatformApplicationArn, ":")
	resource := parts[len(parts)-1] // "app/{Platform}/{AppName}"
	resourceParts := strings.SplitN(resource, "/", platformARNResourceParts)

	if len(resourceParts) != platformARNResourceParts {
		b.mu.Unlock()

		return nil, fmt.Errorf(
			"%w: malformed platform application ARN: %s",
			ErrInvalidParameter,
			platformApplicationArn,
		)
	}

	platform := resourceParts[1]
	appName := resourceParts[2]

	endpointArn := arn.Build("sns", b.region, b.accountID,
		"endpoint/"+platform+"/"+appName+"/"+uuid.New().String())

	// Allocate with room for Token and Enabled (endpointExtraAttrs) beyond caller-supplied attrs.
	attrs := make(map[string]string, len(attributes)+endpointExtraAttrs)
	maps.Copy(attrs, attributes)
	attrs["Token"] = token
	attrs["Enabled"] = "true"

	ep := &PlatformEndpoint{
		EndpointArn:            endpointArn,
		PlatformApplicationArn: platformApplicationArn,
		Attributes:             attrs,
		CreationTimestamp:      time.Now().UTC(),
	}
	b.platformEndpoints[endpointArn] = ep

	b.mu.Unlock()

	// Fire endpoint-created event to the configured topic (best-effort, non-blocking).
	b.fireEndpointEvent(platformApplicationArn, "EventEndpointCreated", map[string]string{
		eventTypeKey:   "EndpointCreated",
		endpointArnKey: endpointArn,
		"Token":        token,
	})

	return ep, nil
}

// GetEndpointAttributes returns the attributes of a platform endpoint.
func (b *InMemoryBackend) GetEndpointAttributes(endpointArn string) (map[string]string, error) {
	b.mu.RLock("GetEndpointAttributes")
	defer b.mu.RUnlock()

	ep, exists := b.platformEndpoints[endpointArn]
	if !exists {
		return nil, ErrEndpointNotFound
	}

	attrs := make(map[string]string, len(ep.Attributes))
	maps.Copy(attrs, ep.Attributes)

	return attrs, nil
}

// SetEndpointAttributes updates attributes on a platform endpoint.
// After the update, an EventEndpointUpdated event is fired to the platform
// application's configured event topic, if any.
func (b *InMemoryBackend) SetEndpointAttributes(endpointArn string, attributes map[string]string) error {
	b.mu.Lock("SetEndpointAttributes")

	ep, exists := b.platformEndpoints[endpointArn]
	if !exists {
		b.mu.Unlock()

		return ErrEndpointNotFound
	}

	maps.Copy(ep.Attributes, attributes)
	platformAppArn := ep.PlatformApplicationArn

	b.mu.Unlock()

	b.fireEndpointEvent(platformAppArn, "EventEndpointUpdated", map[string]string{
		eventTypeKey:   "EndpointUpdated",
		endpointArnKey: endpointArn,
	})

	return nil
}

// ListEndpointsByPlatformApplication returns a page of endpoints for a platform application.
func (b *InMemoryBackend) ListEndpointsByPlatformApplication(
	platformApplicationArn, nextToken string,
) ([]PlatformEndpoint, string, error) {
	b.mu.RLock("ListEndpointsByPlatformApplication")
	defer b.mu.RUnlock()

	if _, exists := b.platformApplications[platformApplicationArn]; !exists {
		return nil, "", ErrPlatformApplicationNotFound
	}

	all := b.sortedEndpoints()
	filtered := make([]PlatformEndpoint, 0, len(all))

	for _, ep := range all {
		if ep.PlatformApplicationArn == platformApplicationArn {
			filtered = append(filtered, ep)
		}
	}

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	eps, next := paginate(filtered, offset, pageSize)

	return eps, next, nil
}

// DeleteEndpoint removes a platform endpoint by ARN.
// After deletion, an EventEndpointDeleted event is fired to the platform
// application's configured event topic, if any.
func (b *InMemoryBackend) DeleteEndpoint(endpointArn string) error {
	b.mu.Lock("DeleteEndpoint")

	ep, exists := b.platformEndpoints[endpointArn]
	if !exists {
		b.mu.Unlock()

		return ErrEndpointNotFound
	}

	platformAppArn := ep.PlatformApplicationArn
	delete(b.platformEndpoints, endpointArn)

	b.mu.Unlock()

	b.fireEndpointEvent(platformAppArn, "EventEndpointDeleted", map[string]string{
		eventTypeKey:   "EndpointDeleted",
		endpointArnKey: endpointArn,
	})

	return nil
}

// sortedPlatformApplications returns platform applications sorted by ARN. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedPlatformApplications() []PlatformApplication {
	apps := make([]PlatformApplication, 0, len(b.platformApplications))
	for _, a := range b.platformApplications {
		apps = append(apps, *a)
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].PlatformApplicationArn < apps[j].PlatformApplicationArn
	})

	return apps
}

// sortedEndpoints returns platform endpoints sorted by ARN. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedEndpoints() []PlatformEndpoint {
	eps := make([]PlatformEndpoint, 0, len(b.platformEndpoints))
	for _, ep := range b.platformEndpoints {
		eps = append(eps, *ep)
	}

	sort.Slice(eps, func(i, j int) bool {
		return eps[i].EndpointArn < eps[j].EndpointArn
	})

	return eps
}

// fireEndpointEvent publishes an endpoint lifecycle event notification to the
// SNS topic configured in the platform application's event attribute (e.g.
// "EventEndpointCreated"). This is best-effort and non-blocking; errors are
// silently discarded so that endpoint operations always succeed regardless of
// whether the event topic exists.
func (b *InMemoryBackend) fireEndpointEvent(appArn, eventAttr string, payload map[string]string) {
	b.mu.RLock("fireEndpointEvent")
	app, exists := b.platformApplications[appArn]
	var topicArn string
	if exists {
		topicArn = app.Attributes[eventAttr]
	}
	b.mu.RUnlock()

	if topicArn == "" {
		return
	}

	msg, _ := json.Marshal(payload)
	_, _ = b.Publish(topicArn, string(msg), "", "", nil)
}

// parseReplayFromTimestamp parses the replayFromTimestamp field from a ReplayPolicy JSON string.
// Returns the zero time and an error when the policy is malformed or the timestamp is missing.
func parseReplayFromTimestamp(replayPolicy string) (time.Time, error) {
	if replayPolicy == "" {
		return time.Time{}, nil
	}

	var p struct {
		ReplayFromTimestamp string `json:"replayFromTimestamp"`
	}

	if err := json.Unmarshal([]byte(replayPolicy), &p); err != nil {
		return time.Time{}, fmt.Errorf("%w: ReplayPolicy is not valid JSON: %s", ErrInvalidParameter, err.Error())
	}

	if p.ReplayFromTimestamp == "" {
		return time.Time{}, fmt.Errorf("%w: ReplayPolicy must include replayFromTimestamp", ErrInvalidParameter)
	}

	ts, err := time.Parse(time.RFC3339, p.ReplayFromTimestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"%w: ReplayPolicy.replayFromTimestamp is not a valid RFC3339 timestamp: %s",
			ErrInvalidParameter, err.Error(),
		)
	}

	return ts, nil
}

// replayMessagesToSubscription delivers archived messages published at or after
// fromTime to the given subscription. This supports the ReplayPolicy subscription
// attribute: when set, a subscriber receives historical messages from the topic's
// archive. Delivery uses the same mechanisms as a normal Publish (HTTP/HTTPS goroutines
// and the event emitter for SQS/Lambda/Firehose).
func (b *InMemoryBackend) replayMessagesToSubscription(sub Subscription, topicArn string, fromTime time.Time) {
	b.mu.RLock("replayMessages")

	archive := b.topicMessageArchive[topicArn]
	var toReplay []*ArchivedMessage
	for _, msg := range archive {
		if !msg.Timestamp.Before(fromTime) {
			toReplay = append(toReplay, msg)
		}
	}

	emitter := b.emitter
	client := b.httpClient
	signer := b.signer
	sqsSender := b.sqsSender
	b.mu.RUnlock()

	for _, msg := range toReplay {
		subSnap := events.SNSSubscriptionSnapshot{
			SubscriptionARN:    sub.SubscriptionArn,
			Protocol:           sub.Protocol,
			Endpoint:           sub.Endpoint,
			FilterPolicy:       sub.FilterPolicy,
			RawMessageDelivery: sub.RawMessageDelivery,
			RedrivePolicy:      sub.RedrivePolicy,
		}

		if sub.Protocol == protocolHTTP || sub.Protocol == protocolHTTPS {
			d := httpDelivery{
				endpoint:        sub.Endpoint,
				body:            msg.Message,
				subject:         msg.Subject,
				messageID:       msg.MessageID,
				topicARN:        topicArn,
				subscriptionARN: sub.SubscriptionArn,
				rawDelivery:     sub.RawMessageDelivery,
				redrivePolicy:   sub.RedrivePolicy,
				sqsSender:       sqsSender,
				signer:          signer,
			}
			deliverHTTPWithMeta(b.svcCtx, d, client)
		}

		if emitter != nil {
			attrSnaps := make(map[string]events.SNSMessageAttributeSnapshot, len(msg.Attributes))
			for k, v := range msg.Attributes {
				attrSnaps[k] = events.SNSMessageAttributeSnapshot{
					DataType:    v.DataType,
					StringValue: v.StringValue,
				}
			}

			_ = emitter.Emit(b.svcCtx, &events.SNSPublishedEvent{
				TopicARN:      topicArn,
				MessageID:     msg.MessageID,
				Message:       msg.Message,
				Subject:       msg.Subject,
				Subscriptions: []events.SNSSubscriptionSnapshot{subSnap},
				Attributes:    attrSnaps,
			})
		}
	}
}

// GetArchivedMessages returns a snapshot of all messages archived for the given
// topic ARN. Returns nil when no messages have been archived (ArchivePolicy not set
// or no messages published yet). Intended for test assertions.
func (b *InMemoryBackend) GetArchivedMessages(topicArn string) []ArchivedMessage {
	b.mu.RLock("GetArchivedMessages")
	defer b.mu.RUnlock()

	archive := b.topicMessageArchive[topicArn]
	if len(archive) == 0 {
		return nil
	}

	result := make([]ArchivedMessage, len(archive))
	for i, msg := range archive {
		result[i] = *msg
	}

	return result
}

// isValidE164 returns true if the phone number string is a valid E.164 number
// (starts with '+' followed by 1–15 digits).
func isValidE164(phone string) bool {
	if len(phone) < 2 || len(phone) > 16 || phone[0] != '+' {
		return false
	}

	for _, c := range phone[1:] {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

// isValidEmail performs a basic check that the string contains exactly one '@'
// with a non-empty local part and a domain containing at least one '.'.
// This mirrors the lightweight check AWS SNS applies; full RFC 5322 is not enforced.
func isValidEmail(email string) bool {
	atIdx := strings.Index(email, "@")
	if atIdx <= 0 {
		return false
	}

	domain := email[atIdx+1:]

	return strings.Contains(domain, ".")
}

// isValidBatchEntryID returns true if the batch entry ID is non-empty, at most
// maxBatchEntryIDLen characters, and contains only alphanumeric characters, hyphens,
// or underscores. Matches the AWS SNS batch entry ID constraints.
func isValidBatchEntryID(id string) bool {
	if id == "" || len(id) > maxBatchEntryIDLen {
		return false
	}

	for _, c := range id {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') &&
			c != '-' && c != '_' {
			return false
		}
	}

	return true
}

// isReadOnlyTopicAttribute returns true if the attribute name is a computed/read-only
// topic attribute that must not be set via SetTopicAttributes.
func isReadOnlyTopicAttribute(name string) bool {
	switch name {
	case "Owner", topicArnKey, "SubscriptionsConfirmed", "SubscriptionsPending",
		"SubscriptionsDeleted", "EffectiveDeliveryPolicy":
		return true
	}

	return false
}

// isValidPermissionLabel returns true if the label is non-empty, not longer than
// maxPermissionLabelLen, and contains only alphanumeric characters or hyphens.
func isValidPermissionLabel(label string) bool {
	if label == "" || len(label) > maxPermissionLabelLen {
		return false
	}

	for _, c := range label {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}

	return true
}

// AddPermission adds a permission statement to an SNS topic's access policy.
// Duplicate labels are rejected with ErrPermissionLabelExists.
// Labels must be non-empty, at most 80 characters, and consist only of alphanumeric
// characters or hyphens; invalid labels are rejected with ErrInvalidParameter.
func (b *InMemoryBackend) AddPermission(topicArn, label string, accounts, actions []string) error {
	if !isValidPermissionLabel(label) {
		return fmt.Errorf(
			"%w: Label must be non-empty, max 80 chars, alphanumeric or hyphen",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("AddPermission")
	defer b.mu.Unlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return ErrTopicNotFound
	}

	if topic.Permissions == nil {
		topic.Permissions = make(map[string]*TopicPermission)
	}

	if _, alreadyExists := topic.Permissions[label]; alreadyExists {
		return ErrPermissionLabelExists
	}

	topic.Permissions[label] = &TopicPermission{
		Label:      label,
		AWSAccount: accounts,
		Actions:    actions,
	}

	return nil
}

// RemovePermission removes a permission statement (identified by label) from an SNS topic.
func (b *InMemoryBackend) RemovePermission(topicArn, label string) error {
	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return ErrTopicNotFound
	}

	if topic.Permissions == nil {
		return ErrPermissionLabelNotFound
	}

	if _, labelExists := topic.Permissions[label]; !labelExists {
		return ErrPermissionLabelNotFound
	}

	delete(topic.Permissions, label)

	return nil
}

// GetSMSSandboxAccountStatus always returns true (sandbox mode) for the mock backend.
func (b *InMemoryBackend) GetSMSSandboxAccountStatus() (bool, error) {
	return true, nil
}

// CreateSMSSandboxPhoneNumber adds a phone number to the SMS sandbox.
// The phone number must be in E.164 format. Numbers start with status "Pending"
// and must be verified via VerifySMSSandboxPhoneNumber before they can receive SMS.
func (b *InMemoryBackend) CreateSMSSandboxPhoneNumber(phoneNumber, languageCode string) error {
	if !isValidE164(phoneNumber) {
		return fmt.Errorf("%w: phone number must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSMSSandboxPhoneNumber")
	defer b.mu.Unlock()

	if _, exists := b.smsSandbox[phoneNumber]; exists {
		return ErrSandboxPhoneAlreadyExists
	}

	b.smsSandbox[phoneNumber] = &SandboxPhoneNumber{
		PhoneNumber:       phoneNumber,
		LanguageCode:      languageCode,
		Status:            "Pending",
		CreationTimestamp: time.Now().UTC(),
	}

	return nil
}

// DeleteSMSSandboxPhoneNumber removes a phone number from the SMS sandbox.
// The phone number must be in E.164 format.
func (b *InMemoryBackend) DeleteSMSSandboxPhoneNumber(phoneNumber string) error {
	if !isValidE164(phoneNumber) {
		return fmt.Errorf("%w: phone number must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSMSSandboxPhoneNumber")
	defer b.mu.Unlock()

	if _, exists := b.smsSandbox[phoneNumber]; !exists {
		return ErrPhoneNumberNotFound
	}

	delete(b.smsSandbox, phoneNumber)

	return nil
}

// VerifySMSSandboxPhoneNumber marks a sandbox phone number as Verified.
// In the mock backend, any non-empty one-time password is accepted.
func (b *InMemoryBackend) VerifySMSSandboxPhoneNumber(phoneNumber, oneTimePassword string) error {
	if oneTimePassword == "" {
		return fmt.Errorf("%w: OneTimePassword is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifySMSSandboxPhoneNumber")
	defer b.mu.Unlock()

	entry, exists := b.smsSandbox[phoneNumber]
	if !exists {
		return ErrPhoneNumberNotFound
	}

	entry.Status = "Verified"

	return nil
}

// ListSMSSandboxPhoneNumbers returns a paginated list of SMS sandbox phone numbers,
// a next-page token (empty when the last page is reached), and any error.
// maxResults controls the page size; 0 means the default (100). Values exceeding 100 are clamped.
func (b *InMemoryBackend) ListSMSSandboxPhoneNumbers(
	nextToken string,
	maxResults int,
) ([]SandboxPhoneNumber, string, error) {
	b.mu.RLock("ListSMSSandboxPhoneNumbers")
	defer b.mu.RUnlock()

	all := b.sortedSandboxNumbers()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	size := resolvePageSize(maxResults, defaultListSMSSandboxResults, maxListSMSSandboxResults)
	nums, next := paginate(all, offset, size)

	return nums, next, nil
}

// sortedSandboxNumbers returns sandbox phone numbers sorted by phone number.
// Must be called with at least RLock held.
func (b *InMemoryBackend) sortedSandboxNumbers() []SandboxPhoneNumber {
	nums := make([]SandboxPhoneNumber, 0, len(b.smsSandbox))
	for _, n := range b.smsSandbox {
		nums = append(nums, *n)
	}

	sort.Slice(nums, func(i, j int) bool {
		return nums[i].PhoneNumber < nums[j].PhoneNumber
	})

	return nums
}

// CheckIfPhoneNumberIsOptedOut returns whether a phone number has opted out of SMS messages.
func (b *InMemoryBackend) CheckIfPhoneNumberIsOptedOut(phoneNumber string) (bool, error) {
	if !isValidE164(phoneNumber) {
		return false, fmt.Errorf("%w: phone number must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.RLock("CheckIfPhoneNumberIsOptedOut")
	defer b.mu.RUnlock()

	return b.optedOutPhoneNumbers[phoneNumber], nil
}

// ListPhoneNumbersOptedOut returns a paginated list of phone numbers opted out of SMS,
// a next-page token (empty when the last page is reached), and any error.
// maxResults controls the page size; 0 means the default (100). Values exceeding 100 are clamped.
func (b *InMemoryBackend) ListPhoneNumbersOptedOut(nextToken string, maxResults int) ([]string, string, error) {
	b.mu.RLock("ListPhoneNumbersOptedOut")
	defer b.mu.RUnlock()

	all := b.sortedOptedOutNumbers()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	size := resolvePageSize(maxResults, defaultListOptedOutResults, maxListOptedOutResults)
	nums, next := paginate(all, offset, size)

	return nums, next, nil
}

// sortedOptedOutNumbers returns opted-out phone numbers sorted lexicographically.
// Must be called with at least RLock held.
func (b *InMemoryBackend) sortedOptedOutNumbers() []string {
	nums := make([]string, 0, len(b.optedOutPhoneNumbers))
	for phone, optedOut := range b.optedOutPhoneNumbers {
		if optedOut {
			nums = append(nums, phone)
		}
	}

	sort.Strings(nums)

	return nums
}

// OptInPhoneNumber removes a phone number from the opt-out list so it can receive SMS messages.
// The phone number must be in E.164 format.
func (b *InMemoryBackend) OptInPhoneNumber(phoneNumber string) error {
	if !isValidE164(phoneNumber) {
		return fmt.Errorf("%w: phone number must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.Lock("OptInPhoneNumber")
	defer b.mu.Unlock()

	delete(b.optedOutPhoneNumbers, phoneNumber)

	return nil
}

// GetSMSAttributes returns the current SMS account attributes, optionally filtered by name.
// If names is empty all attributes are returned.
func (b *InMemoryBackend) GetSMSAttributes(names []string) (map[string]string, error) {
	b.mu.RLock("GetSMSAttributes")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		result := make(map[string]string, len(b.smsAttributes))
		maps.Copy(result, b.smsAttributes)

		return result, nil
	}

	result := make(map[string]string, len(names))
	for _, name := range names {
		result[name] = b.smsAttributes[name]
	}

	return result, nil
}

// SetSMSAttributes stores global SMS account attributes.
// Existing attribute keys are updated; unspecified keys are left unchanged.
// Only known AWS attribute names are accepted; unknown names are rejected with ErrInvalidParameter.
func (b *InMemoryBackend) SetSMSAttributes(attributes map[string]string) error {
	for k := range attributes {
		if !isValidSMSAttributeName(k) {
			return fmt.Errorf("%w: unknown SMS attribute name %q", ErrInvalidParameter, k)
		}
	}

	b.mu.Lock("SetSMSAttributes")
	defer b.mu.Unlock()

	maps.Copy(b.smsAttributes, attributes)

	return nil
}

// GetDataProtectionPolicy returns the data protection policy JSON for the given topic ARN.
// The policy is stored as the "DataProtectionPolicy" attribute on the topic.
func (b *InMemoryBackend) GetDataProtectionPolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetDataProtectionPolicy")
	defer b.mu.RUnlock()

	topic, exists := b.topics[resourceArn]
	if !exists {
		return "", ErrTopicNotFound
	}

	return topic.Attributes["DataProtectionPolicy"], nil
}

// PutDataProtectionPolicy stores a data protection policy JSON string on the given topic ARN.
// The policy must be valid JSON; invalid JSON is rejected with ErrInvalidParameter.
func (b *InMemoryBackend) PutDataProtectionPolicy(resourceArn, policy string) error {
	if policy != "" && !json.Valid([]byte(policy)) {
		return fmt.Errorf("%w: DataProtectionPolicy must be valid JSON", ErrInvalidParameter)
	}

	b.mu.Lock("PutDataProtectionPolicy")
	defer b.mu.Unlock()

	topic, exists := b.topics[resourceArn]
	if !exists {
		return ErrTopicNotFound
	}

	topic.Attributes["DataProtectionPolicy"] = policy

	return nil
}

// ListOriginationNumbers returns a paginated list of origination phone numbers.
// The mock backend maintains no origination numbers by default; callers receive an empty list.
func (b *InMemoryBackend) ListOriginationNumbers(_ string) ([]XMLOriginationPhone, string, error) {
	return []XMLOriginationPhone{}, "", nil
}

// WaitDeliveries stops accepting new HTTP/HTTPS delivery goroutines and blocks
// until all currently in-flight delivery goroutines have finished. It is called
// during graceful shutdown by Handler.Shutdown.
//
// Goroutines that were already launched (before WaitDeliveries is called) always
// complete their delivery. Subsequent calls are safe (idempotent via closing flag).
func (b *InMemoryBackend) WaitDeliveries() {
	// Mark as closing so that Publish does not schedule new delivery goroutines.
	// This must happen before Wait() to ensure no goroutine can be added to
	// deliveryWg once Wait() has returned. In practice, Shutdowner is called
	// after the HTTP server stops accepting requests, so no new Publish calls
	// can race with WaitDeliveries.
	b.closing.Store(true)
	b.deliveryWg.Wait()
}

// Purge removes all SNS resources created before the given cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	b.purgeTopics(ctx, cutoff)
	b.purgeSubscriptions(ctx, cutoff)
	b.purgePlatformApplications(ctx, cutoff)
	b.purgePlatformEndpoints(ctx, cutoff)
	b.purgeSMSSandbox(ctx, cutoff)
}

func (b *InMemoryBackend) purgeTopics(ctx context.Context, cutoff time.Time) {
	for arn, topic := range b.topics {
		if ctx.Err() != nil {
			return
		}

		if topic.CreationTimestamp.Before(cutoff) {
			delete(b.topics, arn)
			delete(b.topicMessageArchive, arn)

			if t := b.topicTags[arn]; t != nil {
				t.Close()
				delete(b.topicTags, arn)
			}
		}
	}

	// Evict archived messages whose timestamp predates the cutoff, for topics that remain.
	for topicArn, archive := range b.topicMessageArchive {
		if ctx.Err() != nil {
			return
		}

		kept := archive[:0]
		for _, msg := range archive {
			if !msg.Timestamp.Before(cutoff) {
				kept = append(kept, msg)
			}
		}

		if len(kept) == 0 {
			delete(b.topicMessageArchive, topicArn)
		} else {
			b.topicMessageArchive[topicArn] = kept
		}
	}
}

func (b *InMemoryBackend) purgeSubscriptions(ctx context.Context, cutoff time.Time) {
	for subArn, sub := range b.subscriptions {
		if ctx.Err() != nil {
			return
		}
		if sub.CreationTimestamp.Before(cutoff) {
			delete(b.subscriptions, subArn)
			// Also clean up the topic subscription index to prevent stale entries.
			b.removeIndexedSubscription(sub.TopicArn, subArn)
		}
	}
}

func (b *InMemoryBackend) purgePlatformApplications(ctx context.Context, cutoff time.Time) {
	for arn, app := range b.platformApplications {
		if ctx.Err() != nil {
			return
		}
		if app.CreationTimestamp.Before(cutoff) {
			delete(b.platformApplications, arn)
		}
	}
}

func (b *InMemoryBackend) purgePlatformEndpoints(ctx context.Context, cutoff time.Time) {
	for arn, ep := range b.platformEndpoints {
		if ctx.Err() != nil {
			return
		}
		if ep.CreationTimestamp.Before(cutoff) {
			delete(b.platformEndpoints, arn)
		}
	}
}

func (b *InMemoryBackend) purgeSMSSandbox(ctx context.Context, cutoff time.Time) {
	for phone, entry := range b.smsSandbox {
		if ctx.Err() != nil {
			return
		}
		if entry.CreationTimestamp.Before(cutoff) {
			delete(b.smsSandbox, phone)
		}
	}
}

// Reset clears all in-memory state from the database. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close all topic tag stores to prevent resource leaks.
	for _, t := range b.topicTags {
		if t != nil {
			t.Close()
		}
	}

	b.topics = make(map[string]*Topic)
	b.subscriptions = make(map[string]*Subscription)
	// topicSubscriptions must be reset alongside subscriptions to avoid stale index entries.
	b.topicSubscriptions = make(map[string]map[string]*Subscription)
	b.topicTags = make(map[string]*svcTags.Tags)
	b.platformApplications = make(map[string]*PlatformApplication)
	b.platformEndpoints = make(map[string]*PlatformEndpoint)
	b.topicMessageArchive = make(map[string][]*ArchivedMessage)
	b.smsSandbox = make(map[string]*SandboxPhoneNumber)
	b.optedOutPhoneNumbers = make(map[string]bool)
	b.smsAttributes = make(map[string]string)
	b.smsDeliveries = nil
}

func (b *InMemoryBackend) archivePublishedMessage(
	topicArn, messageID, message, subject string,
	attrs map[string]MessageAttribute,
) {
	attrsCopy := make(map[string]MessageAttribute, len(attrs))
	maps.Copy(attrsCopy, attrs)

	b.mu.Lock("archiveMessage")
	defer b.mu.Unlock()

	archive := b.topicMessageArchive[topicArn]
	if len(archive) >= maxArchivedMessagesPerTopic {
		overage := len(archive) - maxArchivedMessagesPerTopic + 1
		archive = archive[overage:]
	}

	b.topicMessageArchive[topicArn] = append(archive, &ArchivedMessage{
		MessageID:  messageID,
		Message:    message,
		Subject:    subject,
		Attributes: attrsCopy,
		Timestamp:  time.Now().UTC(),
	})
}
