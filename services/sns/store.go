package sns

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/events"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	protocolSMS             = "sms"
	protocolApplication     = "application"
	protocolSQS             = "sqs"
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

	// maxFilterPoliciesPerTopic is the AWS SNS default quota on the number of
	// subscriptions with a non-empty FilterPolicy on a single topic (200,
	// adjustable via a Support case). Exceeding it returns FilterPolicyLimitExceeded.
	maxFilterPoliciesPerTopic = 200

	// maxFilterPoliciesPerAccount is the AWS SNS default quota on the number of
	// subscriptions with a non-empty FilterPolicy across the whole account
	// (10,000, adjustable). Exceeding it returns FilterPolicyLimitExceeded.
	maxFilterPoliciesPerAccount = 10_000

	// defaultMaxSubscriptionsPerTopic is the AWS SNS fixed quota on the number of
	// subscriptions a single topic may have (12,500,000). Exceeding it returns
	// SubscriptionLimitExceeded. Stored on InMemoryBackend.subscriptionLimitPerTopic
	// (rather than used as a bare constant) so tests can lower it without creating
	// millions of subscriptions.
	defaultMaxSubscriptionsPerTopic = 12_500_000

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

	// maxListOriginationNumbersResults is the maximum MaxResults value for ListOriginationNumbers.
	// AWS SNS caps MaxResults for this operation at 30.
	maxListOriginationNumbersResults = 30

	// defaultListOriginationNumbersResults is the default page size for ListOriginationNumbers.
	defaultListOriginationNumbersResults = 30

	// maxPublishBatchEntries is the maximum number of entries per PublishBatch request.
	// This matches the AWS SNS service limit.
	maxPublishBatchEntries = 10

	// maxArchivedMessagesPerTopic caps the in-memory archive per topic.
	// When the cap is exceeded, the oldest messages are evicted.
	maxArchivedMessagesPerTopic = 100_000

	// maxRecordedDeliveries caps each in-memory delivery-observation buffer
	// (smsDeliveries, emailDeliveries, applicationDeliveries). Unlike AWS's real
	// SMS/email/mobile-push delivery paths, this mock has no external sink for
	// those protocols, so it records deliveries for later inspection via the
	// Drain* methods. Without a cap, sustained publish traffic to a topic whose
	// subscribers never drain these buffers grows them without bound.
	maxRecordedDeliveries = 100_000

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

	// arnPartCount is the number of colon-delimited fields in an AWS ARN:
	// arn:{partition}:{service}:{region}:{account}:{resource}.
	arnPartCount = 6
)

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
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		registry:                  store.NewRegistry(),
		topicTags:                 make(map[string]*svcTags.Tags),
		topicMessageArchive:       make(map[string][]*ArchivedMessage),
		optedOutPhoneNumbers:      make(map[string]bool),
		smsAttributes:             make(map[string]string),
		originationNumbers:        make(map[string][]XMLOriginationPhone),
		accountID:                 accountID,
		region:                    region,
		smsSandboxEnabled:         true,
		svcCtx:                    svcCtx,
		mu:                        lockmetrics.New("sns"),
		httpClient:                &http.Client{Timeout: snsHTTPTimeout},
		workerSem:                 make(chan struct{}, snsMaxConcurrentDeliveries),
		signer:                    newNotificationSigner(region),
		subscriptionLimitPerTopic: defaultMaxSubscriptionsPerTopic,
	}

	registerAllTables(b)

	return b
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
	b.signer.setCertURL(strings.TrimRight(baseURL, "/") + "/SimpleNotificationService.pem")
}

// SetPublishEmitter registers an event emitter that fires when a message is published.
// This is used to wire SNS→SQS delivery at startup.
func (b *InMemoryBackend) SetPublishEmitter(
	emitter events.EventEmitter[*events.SNSPublishedEvent],
) {
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

// SetSQSChecker wires the SQS queue checker used to verify deadLetterTargetArn existence
// during SetSubscriptionAttributes. When nil, the existence check is skipped.
func (b *InMemoryBackend) SetSQSChecker(checker SQSQueueChecker) {
	b.mu.Lock("SetSQSChecker")
	defer b.mu.Unlock()

	b.sqsChecker = checker
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
	// Collect eligible-for-deletion ARNs first rather than deleting from
	// within Table.Range's own iteration.
	var expired []string

	b.topics.Range(func(topic *Topic) bool {
		if ctx.Err() != nil {
			return false
		}

		if topic.CreationTimestamp.Before(cutoff) {
			expired = append(expired, topic.TopicArn)
		}

		return true
	})

	for _, arn := range expired {
		b.topics.Delete(arn)
		delete(b.topicMessageArchive, arn)

		if t := b.topicTags[arn]; t != nil {
			t.Close()
			delete(b.topicTags, arn)
		}
	}

	if ctx.Err() != nil {
		return
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
	var expired []string

	b.subscriptions.Range(func(sub *Subscription) bool {
		if ctx.Err() != nil {
			return false
		}
		if sub.CreationTimestamp.Before(cutoff) {
			expired = append(expired, sub.SubscriptionArn)
		}

		return true
	})

	for _, subArn := range expired {
		// Table.Delete also removes the entry from subscriptionsByTopic, so no
		// separate index cleanup (formerly removeIndexedSubscription) is needed.
		b.subscriptions.Delete(subArn)
	}
}

func (b *InMemoryBackend) purgePlatformApplications(ctx context.Context, cutoff time.Time) {
	var expired []string

	b.platformApplications.Range(func(app *PlatformApplication) bool {
		if ctx.Err() != nil {
			return false
		}
		if app.CreationTimestamp.Before(cutoff) {
			expired = append(expired, app.PlatformApplicationArn)
		}

		return true
	})

	for _, arn := range expired {
		b.platformApplications.Delete(arn)
	}
}

func (b *InMemoryBackend) purgePlatformEndpoints(ctx context.Context, cutoff time.Time) {
	var expired []string

	b.platformEndpoints.Range(func(ep *PlatformEndpoint) bool {
		if ctx.Err() != nil {
			return false
		}
		if ep.CreationTimestamp.Before(cutoff) {
			expired = append(expired, ep.EndpointArn)
		}

		return true
	})

	for _, arn := range expired {
		b.platformEndpoints.Delete(arn)
	}
}

func (b *InMemoryBackend) purgeSMSSandbox(ctx context.Context, cutoff time.Time) {
	var expired []string

	b.smsSandbox.Range(func(entry *SandboxPhoneNumber) bool {
		if ctx.Err() != nil {
			return false
		}
		if entry.CreationTimestamp.Before(cutoff) {
			expired = append(expired, entry.PhoneNumber)
		}

		return true
	})

	for _, phone := range expired {
		b.smsSandbox.Delete(phone)
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

	b.registry.ResetAll()
	b.topicTags = make(map[string]*svcTags.Tags)
	b.topicMessageArchive = make(map[string][]*ArchivedMessage)
	b.optedOutPhoneNumbers = make(map[string]bool)
	b.smsAttributes = make(map[string]string)
	b.smsDeliveries = nil
	b.emailDeliveries = nil
	b.applicationDeliveries = nil
}
