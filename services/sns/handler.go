package sns

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const ()

const ()

const (
	snsVersion       = "Version=2010-03-31"
	snsContentType   = "application/x-www-form-urlencoded"
	snsMatchPriority = 80
	unknownOperation = "Unknown"
)

// Handler is the Echo HTTP handler for SNS operations.
type snsActionFn func(c *echo.Context) error

// fifoDedupTTL is the deduplication window for FIFO topic messages per AWS specification.
const fifoDedupTTL = 5 * time.Minute

// fifoDedupMaxEntries caps the in-memory deduplication map to bound memory growth
// for high-cardinality dedup IDs. When exceeded, all expired entries are evicted
// before the new one is inserted, regardless of the opportunistic sweep cadence.
const fifoDedupMaxEntries = 100_000

// fifoDeduplication tracks message deduplication IDs with a TTL for FIFO topics.
type fifoDeduplication struct {
	entries map[string]time.Time // dedupKey → expiry
	mu      sync.Mutex
}

// fifoDedupSweepInterval is the cadence at which the background goroutine
// evicts expired entries from the deduplication map. This supplements the
// opportunistic eviction inside isDuplicate/record and ensures that a
// long-idle FIFO topic does not retain stale entries indefinitely.
const fifoDedupSweepInterval = time.Minute

func newFifoDeduplication() *fifoDeduplication {
	return &fifoDeduplication{entries: make(map[string]time.Time)}
}

// startPeriodicSweep launches a background goroutine that evicts expired
// entries at fifoDedupSweepInterval. The goroutine stops when ctx is cancelled.
func (d *fifoDeduplication) startPeriodicSweep(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(fifoDedupSweepInterval)
		defer ticker.Stop()

		for {
			select {
			case now := <-ticker.C:
				d.mu.Lock()
				d.sweepExpiredLocked(now)
				d.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// check returns true if the dedup ID has already been seen within the TTL window,
// and records it otherwise.
// isDuplicate returns true if dedupID was already seen within the TTL window.
// It does NOT record the ID — call record() after a successful publish.
func (d *fifoDeduplication) isDuplicate(topicArn, dedupID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	// Evict expired entries opportunistically.
	d.sweepExpiredLocked(now)

	key := topicArn + "/" + dedupID
	exp, found := d.entries[key]

	return found && now.Before(exp)
}

// record marks dedupID as seen for the given topic ARN.
func (d *fifoDeduplication) record(topicArn, dedupID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	// Bound memory: when at the cap, force a sweep before insertion so a workload
	// with many short-lived dedup IDs cannot grow without bound.
	if len(d.entries) >= fifoDedupMaxEntries {
		d.sweepExpiredLocked(now)

		// If a burst of unique-but-still-fresh dedup IDs filled the map, fall
		// back to evicting the entry with the earliest expiration so insertions
		// remain bounded. AWS only guarantees a 5-minute window, so dropping
		// the soonest-to-expire entry is acceptable.
		if len(d.entries) >= fifoDedupMaxEntries {
			d.evictEarliestLocked()
		}
	}

	d.entries[topicArn+"/"+dedupID] = now.Add(fifoDedupTTL)
}

// sweepExpiredLocked removes all expired entries. Caller must hold d.mu.
func (d *fifoDeduplication) sweepExpiredLocked(now time.Time) {
	for k, exp := range d.entries {
		if now.After(exp) {
			delete(d.entries, k)
		}
	}
}

// evictEarliestLocked drops the single entry with the earliest expiration
// from the map. Caller must hold d.mu.
func (d *fifoDeduplication) evictEarliestLocked() {
	var (
		oldestKey string
		oldestExp time.Time
		first     = true
	)

	for k, exp := range d.entries {
		if first || exp.Before(oldestExp) {
			oldestKey = k
			oldestExp = exp
			first = false
		}
	}

	if !first {
		delete(d.entries, oldestKey)
	}
}

type Handler struct {
	actions map[string]snsActionFn
	Backend StorageBackend
	dedup   *fifoDeduplication
	// DefaultRegion is the fallback region used when region cannot be extracted from the request.
	DefaultRegion string
}

// NewHandler creates a new SNS Handler with the given backend and logger.
func NewHandler(backend StorageBackend) *Handler {
	dedup := newFifoDeduplication()

	// Start the periodic FIFO dedup sweep using the backend's lifecycle context
	// when available so the goroutine is properly cleaned up on shutdown.
	sweepCtx := context.Background()
	if b, ok := backend.(*InMemoryBackend); ok {
		sweepCtx = b.svcCtx
	}
	dedup.startPeriodicSweep(sweepCtx)

	h := &Handler{Backend: backend, dedup: dedup}
	h.actions = h.buildActions()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SNS"
}

// Purge implements service.Purgeable by deleting resources older than cutoff.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Purge(ctx, cutoff)
	}
}

// GetSupportedOperations returns the list of supported SNS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddPermission",
		"CheckIfPhoneNumberIsOptedOut",
		"ConfirmSubscription",
		"CreatePlatformApplication",
		"CreatePlatformEndpoint",
		"CreateSMSSandboxPhoneNumber",
		"CreateTopic",
		"DeleteEndpoint",
		"DeletePlatformApplication",
		"DeleteSMSSandboxPhoneNumber",
		"DeleteTopic",
		"GetDataProtectionPolicy",
		"GetEndpointAttributes",
		"GetPlatformApplicationAttributes",
		"GetSMSAttributes",
		"GetSMSSandboxAccountStatus",
		"GetSubscriptionAttributes",
		"GetTopicAttributes",
		"ListEndpointsByPlatformApplication",
		"ListOriginationNumbers",
		"ListPhoneNumbersOptedOut",
		"ListPlatformApplications",
		"ListSMSSandboxPhoneNumbers",
		"ListSubscriptions",
		"ListSubscriptionsByTopic",
		"ListTagsForResource",
		"ListTopics",
		"OptInPhoneNumber",
		"Publish",
		"PublishBatch",
		"PutDataProtectionPolicy",
		"RemovePermission",
		"SetEndpointAttributes",
		"SetPlatformApplicationAttributes",
		"SetSMSAttributes",
		"SetSubscriptionAttributes",
		"SetTopicAttributes",
		"Subscribe",
		"TagResource",
		"Unsubscribe",
		"UntagResource",
		"VerifySMSSandboxPhoneNumber",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "sns" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// deliveryWaiter is an optional interface implemented by backends that track
// in-flight HTTP/HTTPS delivery goroutines. Handlers check for this interface
// during graceful shutdown to drain pending deliveries.
type deliveryWaiter interface{ WaitDeliveries() }

// ChaosRegions returns all regions this SNS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// Shutdown implements service.Shutdowner. It waits for all in-flight HTTP/HTTPS
// delivery goroutines to finish. If ctx expires before all goroutines complete,
// Shutdown returns immediately so that process shutdown is not blocked.
func (h *Handler) Shutdown(ctx context.Context) {
	b, ok := h.Backend.(deliveryWaiter)
	if !ok {
		return
	}

	done := make(chan struct{})

	go func() {
		b.WaitDeliveries()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Ensure Handler implements service.Shutdowner at compile time.
var _ service.Shutdowner = (*Handler)(nil)

// RouteMatcher returns a function that matches SNS requests by Content-Type and body version.
// It also matches GET requests for the SNS signing certificate PEM file so that
// HTTP/HTTPS subscribers can verify notification signatures without contacting AWS.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		// Serve the signing cert PEM for signature verification.
		if c.Request().Method == http.MethodGet &&
			strings.HasSuffix(c.Request().URL.Path, "SimpleNotificationService.pem") {
			return true
		}

		ct := c.Request().Header.Get("Content-Type")
		if !strings.Contains(ct, snsContentType) {
			return false
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return false
		}

		return strings.Contains(string(body), snsVersion)
	}
}

// MatchPriority returns the routing priority for the SNS handler.
func (h *Handler) MatchPriority() int {
	return snsMatchPriority
}

// ExtractOperation extracts the SNS action from the request form.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return unknownOperation
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownOperation
	}

	action := vals.Get("Action")
	if action == "" {
		return unknownOperation
	}

	return action
}

// ExtractResource extracts the primary resource (TopicArn or Name) from the request form.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	if arn := vals.Get("TopicArn"); arn != "" {
		return arn
	}

	return vals.Get("Name")
}

// Handler returns the Echo HandlerFunc for SNS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Serve the signing certificate PEM for HTTP/HTTPS notification verification.
		if c.Request().Method == http.MethodGet &&
			strings.HasSuffix(c.Request().URL.Path, "SimpleNotificationService.pem") {
			return h.handleSigningCert(c)
		}

		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if err := c.Request().ParseForm(); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameter", err.Error())
		}

		action := c.Request().FormValue("Action")
		log.DebugContext(ctx, "SNS request", "action", action)

		return h.dispatch(c, action)
	}
}

// handleSigningCert serves the PEM-encoded self-signed certificate used to
// sign SNS HTTP/HTTPS notification envelopes. Subscribers can download this
// certificate to verify notification signatures without contacting AWS.
func (h *Handler) handleSigningCert(c *echo.Context) error {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return c.String(http.StatusNotFound, "signing cert not available")
	}

	certPEM := b.SigningCertPEM()
	c.Response().Header().Set("Content-Type", "application/x-pem-file")

	return c.String(http.StatusOK, string(certPEM))
}

// dispatch routes the action to the appropriate handler method.
func (h *Handler) dispatch(c *echo.Context, action string) error {
	fn, ok := h.actions[action]
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "InvalidAction",
			fmt.Sprintf("Action %s is not valid for this endpoint", action))
	}

	return fn(c)
}

// buildActions constructs the action dispatch table.
func (h *Handler) buildActions() map[string]snsActionFn {
	return map[string]snsActionFn{
		"AddPermission":                      h.handleAddPermission,
		"CheckIfPhoneNumberIsOptedOut":       h.handleCheckIfPhoneNumberIsOptedOut,
		"ConfirmSubscription":                h.handleConfirmSubscription,
		"CreatePlatformApplication":          h.handleCreatePlatformApplication,
		"CreatePlatformEndpoint":             h.handleCreatePlatformEndpoint,
		"CreateSMSSandboxPhoneNumber":        h.handleCreateSMSSandboxPhoneNumber,
		"CreateTopic":                        h.handleCreateTopic,
		"DeleteEndpoint":                     h.handleDeleteEndpoint,
		"DeletePlatformApplication":          h.handleDeletePlatformApplication,
		"DeleteSMSSandboxPhoneNumber":        h.handleDeleteSMSSandboxPhoneNumber,
		"DeleteTopic":                        h.handleDeleteTopic,
		"GetDataProtectionPolicy":            h.handleGetDataProtectionPolicy,
		"GetEndpointAttributes":              h.handleGetEndpointAttributes,
		"GetPlatformApplicationAttributes":   h.handleGetPlatformApplicationAttributes,
		"GetSMSAttributes":                   h.handleGetSMSAttributes,
		"GetSMSSandboxAccountStatus":         h.handleGetSMSSandboxAccountStatus,
		"GetSubscriptionAttributes":          h.handleGetSubscriptionAttributes,
		"GetTopicAttributes":                 h.handleGetTopicAttributes,
		"ListEndpointsByPlatformApplication": h.handleListEndpointsByPlatformApplication,
		"ListOriginationNumbers":             h.handleListOriginationNumbers,
		"ListPhoneNumbersOptedOut":           h.handleListPhoneNumbersOptedOut,
		"ListPlatformApplications":           h.handleListPlatformApplications,
		"ListSMSSandboxPhoneNumbers":         h.handleListSMSSandboxPhoneNumbers,
		"ListSubscriptions":                  h.handleListSubscriptions,
		"ListSubscriptionsByTopic":           h.handleListSubscriptionsByTopic,
		"ListTagsForResource":                h.handleListTagsForResource,
		"ListTopics":                         h.handleListTopics,
		"OptInPhoneNumber":                   h.handleOptInPhoneNumber,
		"Publish":                            h.handlePublish,
		"PublishBatch":                       h.handlePublishBatch,
		"PutDataProtectionPolicy":            h.handlePutDataProtectionPolicy,
		"RemovePermission":                   h.handleRemovePermission,
		"SetEndpointAttributes":              h.handleSetEndpointAttributes,
		"SetPlatformApplicationAttributes":   h.handleSetPlatformApplicationAttributes,
		"SetSMSAttributes":                   h.handleSetSMSAttributes,
		"SetSubscriptionAttributes":          h.handleSetSubscriptionAttributes,
		"SetTopicAttributes":                 h.handleSetTopicAttributes,
		"Subscribe":                          h.handleSubscribe,
		"TagResource":                        h.handleTagResource,
		"Unsubscribe":                        h.handleUnsubscribe,
		"UntagResource":                      h.handleUntagResource,
		"VerifySMSSandboxPhoneNumber":        h.handleVerifySMSSandboxPhoneNumber,
	}
}

func (h *Handler) handleCreateTopic(c *echo.Context) error {
	name := c.Request().FormValue("Name")
	if name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "Name is required")
	}

	attrs := extractFormAttributes(c)

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)
	topic, err := h.Backend.CreateTopicInRegion(name, region, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	// Apply Tags.member.N.Key/Value pairs supplied at topic creation time.
	tags := parseSNSTagsFromForm(c)
	if len(tags) > 0 {
		h.Backend.SetTopicTags(topic.TopicArn, svcTags.FromMap("sns."+topic.TopicArn+".tags", tags))
	}

	return h.writeXML(c, CreateTopicResponse{
		CreateTopicResult: CreateTopicResult{TopicArn: topic.TopicArn},
		ResponseMetadata:  ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleDeleteTopic(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	if err := h.Backend.DeleteTopic(topicArn); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, DeleteTopicResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListTopics(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")

	topics, token, err := h.Backend.ListTopics(nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLTopic, len(topics))
	for i, t := range topics {
		members[i] = XMLTopic{TopicArn: t.TopicArn}
	}

	return h.writeXML(c, ListTopicsResponse{
		ListTopicsResult: ListTopicsResult{Topics: members, NextToken: token},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetTopicAttributes(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	attrs, err := h.Backend.GetTopicAttributes(topicArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	entries := attrsToEntries(attrs)

	return h.writeXML(c, GetTopicAttributesResponse{
		GetTopicAttributesResult: GetTopicAttributesResult{Attributes: entries},
		ResponseMetadata:         ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleSetTopicAttributes(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	attrName := c.Request().FormValue("AttributeName")
	attrValue := c.Request().FormValue("AttributeValue")

	if topicArn == "" || attrName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn and AttributeName are required")
	}

	if err := h.Backend.SetTopicAttributes(topicArn, attrName, attrValue); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetTopicAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleSubscribe(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	protocol := c.Request().FormValue("Protocol")
	endpoint := c.Request().FormValue("Endpoint")

	if topicArn == "" || protocol == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn and Protocol are required")
	}

	validProtocols := map[string]bool{
		protocolEmail: true, protocolEmailJSON: true, "http": true, "https": true,
		"sqs": true, "lambda": true, "sms": true, "application": true, "firehose": true,
	}
	if !validProtocols[protocol] {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter",
			fmt.Sprintf("Invalid parameter: Protocol Reason: %s is not a valid protocol", protocol))
	}

	filterPolicy := extractFilterPolicy(c.Request().Form)

	sub, err := h.Backend.Subscribe(topicArn, protocol, endpoint, filterPolicy)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	// Apply subscription attributes passed at subscribe time (e.g. RawMessageDelivery, RedrivePolicy).
	attrs := extractFormAttributes(c)
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	for k, v := range attrs {
		if k == attrFilterPolicy {
			continue // already handled by Subscribe
		}

		if setErr := h.Backend.SetSubscriptionAttributes(sub.SubscriptionArn, k, v); setErr != nil {
			log.WarnContext(ctx, "failed to set subscription attribute", "attr", k, "error", setErr)
		}
	}

	// AWS: when ReturnSubscriptionArn is true, always return the real ARN
	// even for pending http/https subscriptions.
	returnArn := strings.EqualFold(c.Request().FormValue("ReturnSubscriptionArn"), "true")
	subArn := sub.SubscriptionArn
	if sub.PendingConfirmation && !returnArn {
		subArn = pendingConfirmationARN
	}

	return h.writeXML(c, SubscribeResponse{
		SubscribeResult:  SubscribeResult{SubscriptionArn: subArn},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleUnsubscribe(c *echo.Context) error {
	subscriptionArn := c.Request().FormValue("SubscriptionArn")
	if subscriptionArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required")
	}

	if err := h.Backend.Unsubscribe(subscriptionArn); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, UnsubscribeResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleConfirmSubscription(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	token := c.Request().FormValue("Token")

	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	if token == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "Token is required")
	}

	sub, err := h.Backend.ConfirmSubscription(topicArn, token)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ConfirmSubscriptionResponse{
		ConfirmSubscriptionResult: ConfirmSubscriptionResult{SubscriptionArn: sub.SubscriptionArn},
		ResponseMetadata:          ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListSubscriptions(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")

	subs, token, err := h.Backend.ListSubscriptions(nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ListSubscriptionsResponse{
		ListSubscriptionsResult: ListSubscriptionsResult{
			Subscriptions: toXMLSubscriptions(subs),
			NextToken:     token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListSubscriptionsByTopic(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	nextToken := c.Request().FormValue("NextToken")

	subs, token, err := h.Backend.ListSubscriptionsByTopic(topicArn, nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ListSubscriptionsByTopicResponse{
		ListSubscriptionsByTopicResult: ListSubscriptionsByTopicResult{
			Subscriptions: toXMLSubscriptions(subs),
			NextToken:     token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handlePublish(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	targetArn := c.Request().FormValue("TargetArn")
	phoneNumber := c.Request().FormValue("PhoneNumber")
	message := c.Request().FormValue("Message")
	subject := c.Request().FormValue("Subject")
	messageStructure := c.Request().FormValue("MessageStructure")

	// Exactly one of TopicArn, TargetArn, or PhoneNumber must be specified.
	if message == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "Message is required")
	}

	if topicArn == "" && targetArn == "" && phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter",
			"TopicArn, TargetArn, or PhoneNumber is required")
	}

	attrs := extractMessageAttributes(c.Request().Form)

	var messageID string
	var err error

	switch {
	case topicArn != "":
		// FIFO topics require MessageGroupId. Apply dedup (explicit or content-based).
		if strings.HasSuffix(topicArn, ".fifo") {
			return h.publishFIFOTopic(c, topicArn, message, subject, messageStructure, attrs)
		}

		messageID, err = h.Backend.Publish(topicArn, message, subject, messageStructure, attrs)
		if err != nil {
			return h.handleBackendError(c, err)
		}
	case targetArn != "":
		// TargetArn addresses a platform endpoint. In the mock, generate a message ID.
		messageID, err = h.Backend.PublishToTargetArn(targetArn, message, subject, attrs)
		if err != nil {
			return h.handleBackendError(c, err)
		}
	default:
		// PhoneNumber direct SMS publish — generate a message ID in the mock.
		messageID, err = h.Backend.PublishSMS(phoneNumber, message)
		if err != nil {
			return h.handleBackendError(c, err)
		}
	}

	return h.writeXML(c, PublishResponse{
		PublishResult:    PublishResult{MessageID: messageID},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// publishFIFOTopic handles a Publish call to a .fifo topic: it enforces
// MessageGroupId, resolves the effective deduplication ID via the
// ContentBasedDeduplication rules, drops the publish on a duplicate, and
// records the dedup ID after a successful underlying Publish.
func (h *Handler) publishFIFOTopic(
	c *echo.Context,
	topicArn, message, subject, messageStructure string,
	attrs map[string]MessageAttribute,
) error {
	if c.Request().FormValue("MessageGroupId") == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter",
			"MessageGroupId is required for FIFO topics")
	}

	explicitDedupID := c.Request().FormValue("MessageDeduplicationId")

	effectiveDedupID, dedupErr := h.resolveFIFODedupID(topicArn, explicitDedupID, message)
	if dedupErr != nil {
		return h.handleBackendError(c, dedupErr)
	}

	if effectiveDedupID != "" && h.dedup.isDuplicate(topicArn, effectiveDedupID) {
		// Duplicate within the 5-minute window: AWS still returns success with a
		// synthesized message ID and does not actually re-publish the message.
		return h.writeXML(c, PublishResponse{
			PublishResult:    PublishResult{MessageID: uuid.New().String()},
			ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
		})
	}

	messageID, err := h.Backend.Publish(topicArn, message, subject, messageStructure, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	if effectiveDedupID != "" {
		h.dedup.record(topicArn, effectiveDedupID)
	}

	return h.writeXML(c, PublishResponse{
		PublishResult:    PublishResult{MessageID: messageID},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// resolveFIFODedupID enforces ContentBasedDeduplication semantics for a FIFO topic
// publish and returns the effective deduplication ID to use:
//
//   - When CBD is enabled: explicit MessageDeduplicationId is forbidden, and the
//     SHA-256 hex digest of the message body is used as the implicit dedup ID.
//   - When CBD is disabled (the default): explicit MessageDeduplicationId is required.
//
// Caller must already have validated that topicArn names a FIFO topic.
func (h *Handler) resolveFIFODedupID(topicArn, explicitDedupID, message string) (string, error) {
	attrs, err := h.Backend.GetTopicAttributes(topicArn)
	if err != nil {
		return "", err
	}

	cbdEnabled := strings.EqualFold(attrs["ContentBasedDeduplication"], "true")

	if cbdEnabled {
		if explicitDedupID != "" {
			return "", fmt.Errorf(
				"%w: MessageDeduplicationId must not be set when ContentBasedDeduplication is enabled",
				ErrInvalidParameter,
			)
		}

		sum := sha256.Sum256([]byte(message))

		return hex.EncodeToString(sum[:]), nil
	}

	if explicitDedupID == "" {
		return "", fmt.Errorf(
			"%w: MessageDeduplicationId is required for FIFO topics without ContentBasedDeduplication",
			ErrInvalidParameter,
		)
	}

	return explicitDedupID, nil
}

func (h *Handler) handlePublishBatch(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	entries := extractBatchEntries(c.Request().Form)

	if len(entries) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PublishBatchRequestEntries is required")
	}

	if len(entries) > maxPublishBatchEntries {
		msg := fmt.Sprintf(
			"The batch request contains more entries than permissible. Maximum is %d.",
			maxPublishBatchEntries,
		)

		return h.writeError(c, http.StatusBadRequest, "TooManyEntriesInBatchRequest", msg)
	}

	// Validate each entry ID format (non-empty, max 80 chars, alphanumeric/-/_).
	for _, entry := range entries {
		if !isValidBatchEntryID(entry.id) {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidBatchEntryId",
				fmt.Sprintf(
					"Id '%s' is invalid: must be 1-%d chars, alphanumeric, hyphen, or underscore",
					entry.id,
					maxBatchEntryIDLen,
				),
			)
		}
	}

	// Validate batch entry IDs are unique within this request.
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if seen[entry.id] {
			return h.writeError(c, http.StatusBadRequest, "BatchEntryIdsNotDistinct",
				fmt.Sprintf("Id '%s' appears more than once in the batch request", entry.id))
		}

		seen[entry.id] = true
	}

	// AWS rejects PublishBatch when the combined message bodies exceed the
	// 256 KiB per-request limit (BatchRequestTooLong).
	totalBytes := 0
	for _, entry := range entries {
		totalBytes += len(entry.message)
	}

	if totalBytes > maxMessageSizeBytes {
		return h.writeError(c, http.StatusBadRequest, "BatchRequestTooLong",
			fmt.Sprintf("Batch requests cannot be longer than %d bytes; got %d.",
				maxMessageSizeBytes, totalBytes))
	}

	successful := make([]XMLPublishBatchSuccessEntry, 0, len(entries))
	failed := make([]XMLPublishBatchFailEntry, 0, len(entries))

	isFIFO := strings.HasSuffix(topicArn, ".fifo")

	for _, entry := range entries {
		ok, fail := h.processBatchEntry(topicArn, entry, isFIFO)
		if fail != nil {
			failed = append(failed, *fail)

			continue
		}

		successful = append(successful, *ok)
	}

	return h.writeXML(c, PublishBatchResponse{
		PublishBatchResult: PublishBatchResult{Successful: successful, Failed: failed},
		ResponseMetadata:   ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// processBatchEntry executes a single PublishBatch entry: it validates FIFO
// requirements, applies deduplication, performs the underlying Publish, and
// returns either a success or failure entry. Exactly one of the returned
// pointers is non-nil.
func (h *Handler) processBatchEntry(
	topicArn string, entry batchEntry, isFIFO bool,
) (*XMLPublishBatchSuccessEntry, *XMLPublishBatchFailEntry) {
	if isFIFO && entry.messageGroupID == "" {
		return nil, &XMLPublishBatchFailEntry{
			ID:          entry.id,
			Code:        "InvalidParameter",
			Message:     "MessageGroupId is required for FIFO topics",
			SenderFault: true,
		}
	}

	var effectiveDedupID string

	if isFIFO {
		id, dup, fail := h.batchEntryFIFODedup(topicArn, entry)
		if fail != nil {
			return nil, fail
		}

		if dup != nil {
			return dup, nil
		}

		effectiveDedupID = id
	}

	msgID, err := h.Backend.Publish(topicArn, entry.message, entry.subject, entry.messageStructure, entry.attrs)
	if err != nil {
		return nil, &XMLPublishBatchFailEntry{
			ID:          entry.id,
			Code:        errorCode(err),
			Message:     err.Error(),
			SenderFault: true,
		}
	}

	if effectiveDedupID != "" {
		h.dedup.record(topicArn, effectiveDedupID)
	}

	return &XMLPublishBatchSuccessEntry{MessageID: msgID, ID: entry.id}, nil
}

// batchEntryFIFODedup resolves the effective FIFO deduplication ID for a single
// batch entry. It returns:
//   - (id, nil, nil): the entry should be published with `id` recorded after success.
//   - ("", dup, nil): the entry is a duplicate within the dedup window; AWS
//     returns a synthesized success entry without re-publishing.
//   - ("", nil, fail): the dedup configuration is invalid (e.g. CBD conflict).
func (h *Handler) batchEntryFIFODedup(
	topicArn string, entry batchEntry,
) (string, *XMLPublishBatchSuccessEntry, *XMLPublishBatchFailEntry) {
	id, dedupErr := h.resolveFIFODedupID(topicArn, entry.dedupID, entry.message)
	if dedupErr != nil {
		return "", nil, &XMLPublishBatchFailEntry{
			ID:          entry.id,
			Code:        errorCode(dedupErr),
			Message:     dedupErr.Error(),
			SenderFault: true,
		}
	}

	if id != "" && h.dedup.isDuplicate(topicArn, id) {
		return "", &XMLPublishBatchSuccessEntry{
			MessageID: uuid.New().String(),
			ID:        entry.id,
		}, nil
	}

	return id, nil, nil
}

func (h *Handler) handleGetSubscriptionAttributes(c *echo.Context) error {
	subscriptionArn := c.Request().FormValue("SubscriptionArn")
	if subscriptionArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required")
	}

	attrs, err := h.Backend.GetSubscriptionAttributes(subscriptionArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	entries := attrsToEntries(attrs)

	return h.writeXML(c, GetSubscriptionAttributesResponse{
		GetSubscriptionAttributesResult: GetSubscriptionAttributesResult{Attributes: entries},
		ResponseMetadata:                ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleSetSubscriptionAttributes(c *echo.Context) error {
	subscriptionArn := c.Request().FormValue("SubscriptionArn")
	attrName := c.Request().FormValue("AttributeName")
	attrValue := c.Request().FormValue("AttributeValue")

	if subscriptionArn == "" || attrName == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"SubscriptionArn and AttributeName are required",
		)
	}

	if err := h.Backend.SetSubscriptionAttributes(subscriptionArn, attrName, attrValue); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetSubscriptionAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	tags := h.Backend.GetTopicTags(resourceArn)
	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	return h.writeXML(c, snsListTagsResponse{
		Result:           snsListTagsResult{Tags: tagList},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// parseSNSTagsFromForm reads Tags.member.N.Key/Value pairs from the form.
func parseSNSTagsFromForm(c *echo.Context) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := c.Request().FormValue(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			return tags
		}
		tags[k] = c.Request().FormValue(fmt.Sprintf("Tags.member.%d.Value", i))
	}
}

// parseSNSTagKeysFromForm reads TagKeys.member.N values from the form.
func parseSNSTagKeysFromForm(c *echo.Context) []string {
	var keys []string
	for i := 1; ; i++ {
		k := c.Request().FormValue(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	kv := parseSNSTagsFromForm(c)
	h.Backend.SetTopicTags(resourceArn, svcTags.FromMap("sns."+resourceArn+".tags.input", kv))

	return h.writeXML(
		c,
		snsEmptyResponse{
			XMLName: xml.Name{Space: "https://sns.amazonaws.com/doc/2010-03-31/", Local: "TagResourceResponse"},
		},
	)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	keys := parseSNSTagKeysFromForm(c)
	h.Backend.RemoveTopicTags(resourceArn, keys)

	return h.writeXML(
		c,
		snsEmptyResponse{
			XMLName: xml.Name{Space: "https://sns.amazonaws.com/doc/2010-03-31/", Local: "UntagResourceResponse"},
		},
	)
}

func (h *Handler) handleCreatePlatformApplication(c *echo.Context) error {
	name := c.Request().FormValue("Name")
	platform := c.Request().FormValue("Platform")

	if name == "" || platform == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "Name and Platform are required")
	}

	attrs := extractFormAttributes(c)

	app, err := h.Backend.CreatePlatformApplication(name, platform, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CreatePlatformApplicationResponse{
		CreatePlatformApplicationResult: CreatePlatformApplicationResult{
			PlatformApplicationArn: app.PlatformApplicationArn,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetPlatformApplicationAttributes(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	if platformApplicationArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PlatformApplicationArn is required")
	}

	attrs, err := h.Backend.GetPlatformApplicationAttributes(platformApplicationArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetPlatformApplicationAttributesResponse{
		GetPlatformApplicationAttributesResult: GetPlatformApplicationAttributesResult{
			Attributes: attrsToEntries(attrs),
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleSetPlatformApplicationAttributes(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	if platformApplicationArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PlatformApplicationArn is required")
	}

	attrs := extractFormAttributes(c)

	if err := h.Backend.SetPlatformApplicationAttributes(platformApplicationArn, attrs); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetPlatformApplicationAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListPlatformApplications(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")

	apps, token, err := h.Backend.ListPlatformApplications(nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLPlatformApplication, len(apps))
	for i, a := range apps {
		members[i] = XMLPlatformApplication{
			PlatformApplicationArn: a.PlatformApplicationArn,
			Attributes:             attrsToEntries(a.Attributes),
		}
	}

	return h.writeXML(c, ListPlatformApplicationsResponse{
		ListPlatformApplicationsResult: ListPlatformApplicationsResult{
			PlatformApplications: members,
			NextToken:            token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleDeletePlatformApplication(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	if platformApplicationArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PlatformApplicationArn is required")
	}

	if err := h.Backend.DeletePlatformApplication(platformApplicationArn); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, DeletePlatformApplicationResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleCreatePlatformEndpoint(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	token := c.Request().FormValue("Token")

	if platformApplicationArn == "" || token == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"PlatformApplicationArn and Token are required",
		)
	}

	attrs := extractFormAttributes(c)

	// CustomUserData is sent as a top-level form field by the AWS SDK, not under Attributes.entry.*.
	if customData := c.Request().FormValue("CustomUserData"); customData != "" {
		attrs["CustomUserData"] = customData
	}

	ep, err := h.Backend.CreatePlatformEndpoint(platformApplicationArn, token, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CreatePlatformEndpointResponse{
		CreatePlatformEndpointResult: CreatePlatformEndpointResult{EndpointArn: ep.EndpointArn},
		ResponseMetadata:             ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetEndpointAttributes(c *echo.Context) error {
	endpointArn := c.Request().FormValue("EndpointArn")
	if endpointArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "EndpointArn is required")
	}

	attrs, err := h.Backend.GetEndpointAttributes(endpointArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetEndpointAttributesResponse{
		GetEndpointAttributesResult: GetEndpointAttributesResult{Attributes: attrsToEntries(attrs)},
		ResponseMetadata:            ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleSetEndpointAttributes(c *echo.Context) error {
	endpointArn := c.Request().FormValue("EndpointArn")
	if endpointArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "EndpointArn is required")
	}

	attrs := extractFormAttributes(c)

	if err := h.Backend.SetEndpointAttributes(endpointArn, attrs); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetEndpointAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListEndpointsByPlatformApplication(c *echo.Context) error {
	platformApplicationArn := c.Request().FormValue("PlatformApplicationArn")
	if platformApplicationArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PlatformApplicationArn is required")
	}

	nextToken := c.Request().FormValue("NextToken")

	eps, token, err := h.Backend.ListEndpointsByPlatformApplication(platformApplicationArn, nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLPlatformEndpoint, len(eps))
	for i, ep := range eps {
		members[i] = XMLPlatformEndpoint{
			EndpointArn: ep.EndpointArn,
			Attributes:  attrsToEntries(ep.Attributes),
		}
	}

	return h.writeXML(c, ListEndpointsByPlatformApplicationResponse{
		ListEndpointsByPlatformApplicationResult: ListEndpointsByPlatformApplicationResult{
			Endpoints: members,
			NextToken: token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleDeleteEndpoint(c *echo.Context) error {
	endpointArn := c.Request().FormValue("EndpointArn")
	if endpointArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "EndpointArn is required")
	}

	if err := h.Backend.DeleteEndpoint(endpointArn); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, DeleteEndpointResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// parseMemberList reads Name.member.N values from the form and returns the slice.
func parseMemberList(c *echo.Context, prefix string) []string {
	var items []string

	for i := 1; ; i++ {
		v := c.Request().FormValue(fmt.Sprintf("%s.member.%d", prefix, i))
		if v == "" {
			return items
		}

		items = append(items, v)
	}
}

func (h *Handler) handleAddPermission(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	label := c.Request().FormValue("Label")

	if topicArn == "" || label == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn and Label are required")
	}

	accounts := parseMemberList(c, "AWSAccountId")
	actions := parseMemberList(c, "ActionName")

	if err := h.Backend.AddPermission(topicArn, label, accounts, actions); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, AddPermissionResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleCheckIfPhoneNumberIsOptedOut(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("phoneNumber")
	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "phoneNumber is required")
	}

	optedOut, err := h.Backend.CheckIfPhoneNumberIsOptedOut(phoneNumber)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CheckIfPhoneNumberIsOptedOutResponse{
		CheckIfPhoneNumberIsOptedOutResult: CheckIfPhoneNumberIsOptedOutResult{IsOptedOut: optedOut},
		ResponseMetadata:                   ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleCreateSMSSandboxPhoneNumber(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("PhoneNumber")
	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PhoneNumber is required")
	}

	languageCode := c.Request().FormValue("LanguageCode")

	if err := h.Backend.CreateSMSSandboxPhoneNumber(phoneNumber, languageCode); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CreateSMSSandboxPhoneNumberResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleDeleteSMSSandboxPhoneNumber(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("PhoneNumber")
	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PhoneNumber is required")
	}

	if err := h.Backend.DeleteSMSSandboxPhoneNumber(phoneNumber); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, DeleteSMSSandboxPhoneNumberResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetDataProtectionPolicy(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	if resourceArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "ResourceArn is required")
	}

	policy, err := h.Backend.GetDataProtectionPolicy(resourceArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetDataProtectionPolicyResponse{
		GetDataProtectionPolicyResult: GetDataProtectionPolicyResult{DataProtectionPolicy: policy},
		ResponseMetadata:              ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetSMSAttributes(c *echo.Context) error {
	names := parseMemberList(c, "attributes")

	attrs, err := h.Backend.GetSMSAttributes(names)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetSMSAttributesResponse{
		GetSMSAttributesResult: GetSMSAttributesResult{Attributes: attrsToEntries(attrs)},
		ResponseMetadata:       ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetSMSSandboxAccountStatus(c *echo.Context) error {
	inSandbox, err := h.Backend.GetSMSSandboxAccountStatus()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetSMSSandboxAccountStatusResponse{
		GetSMSSandboxAccountStatusResult: GetSMSSandboxAccountStatusResult{IsInSandbox: inSandbox},
		ResponseMetadata:                 ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListOriginationNumbers(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")

	nums, token, err := h.Backend.ListOriginationNumbers(nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ListOriginationNumbersResponse{
		ListOriginationNumbersResult: ListOriginationNumbersResult{
			PhoneNumbers: nums,
			NextToken:    token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListPhoneNumbersOptedOut(c *echo.Context) error {
	nextToken := c.Request().FormValue("nextToken")
	maxResults := parseIntParam(c, "maxResults", 0)

	nums, token, err := h.Backend.ListPhoneNumbersOptedOut(nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ListPhoneNumbersOptedOutResponse{
		ListPhoneNumbersOptedOutResult: ListPhoneNumbersOptedOutResult{
			PhoneNumbers: nums,
			NextToken:    token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListSMSSandboxPhoneNumbers(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")
	maxResults := parseIntParam(c, "MaxResults", 0)

	nums, token, err := h.Backend.ListSMSSandboxPhoneNumbers(nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLSandboxPhoneNumber, len(nums))
	for i, n := range nums {
		members[i] = XMLSandboxPhoneNumber{
			PhoneNumber:  n.PhoneNumber,
			LanguageCode: n.LanguageCode,
			Status:       n.Status,
		}
	}

	return h.writeXML(c, ListSMSSandboxPhoneNumbersResponse{
		ListSMSSandboxPhoneNumbersResult: ListSMSSandboxPhoneNumbersResult{
			PhoneNumbers: members,
			NextToken:    token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleVerifySMSSandboxPhoneNumber(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("PhoneNumber")
	otp := c.Request().FormValue("OneTimePassword")

	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PhoneNumber is required")
	}

	if err := h.Backend.VerifySMSSandboxPhoneNumber(phoneNumber, otp); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, VerifySMSSandboxPhoneNumberResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleOptInPhoneNumber(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("phoneNumber")
	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "phoneNumber is required")
	}

	if err := h.Backend.OptInPhoneNumber(phoneNumber); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, OptInPhoneNumberResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleRemovePermission(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	label := c.Request().FormValue("Label")

	if topicArn == "" || label == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn and Label are required")
	}

	if err := h.Backend.RemovePermission(topicArn, label); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, RemovePermissionResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// parseSetSMSAttributesForm reads Attributes.entry.N.key/value pairs for SetSMSAttributes.
func parseSetSMSAttributesForm(c *echo.Context) map[string]string {
	attrs := make(map[string]string)

	for i := 1; ; i++ {
		key := c.Request().FormValue(fmt.Sprintf("attributes.entry.%d.key", i))
		if key == "" {
			return attrs
		}

		attrs[key] = c.Request().FormValue(fmt.Sprintf("attributes.entry.%d.value", i))
	}
}

func (h *Handler) handleSetSMSAttributes(c *echo.Context) error {
	attrs := parseSetSMSAttributesForm(c)

	if err := h.Backend.SetSMSAttributes(attrs); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetSMSAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handlePutDataProtectionPolicy(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	policy := c.Request().FormValue("DataProtectionPolicy")

	if resourceArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "ResourceArn is required")
	}

	if err := h.Backend.PutDataProtectionPolicy(resourceArn, policy); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, PutDataProtectionPolicyResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// parseIntParam reads a query or form integer parameter by name; returns defaultVal on missing or parse error.
func parseIntParam(c *echo.Context, name string, defaultVal int) int {
	s := c.Request().FormValue(name)
	if s == "" {
		return defaultVal
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}

	return n
}

// writeXML marshals v to XML and writes an HTTP 200 OK response.
func (h *Handler) writeXML(c *echo.Context, v any) error {
	httputils.WriteXML(c.Request().Context(), c.Response(), http.StatusOK, v)

	return nil
}

// writeError writes an XML error response.
func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	errResp := ErrorResponse{
		Error:     Error{Type: "Sender", Code: code, Message: message},
		RequestID: uuid.New().String(),
	}

	httputils.WriteXML(c.Request().Context(), c.Response(), status, errResp)

	return nil
}

// handleBackendError maps a backend error to an XML error response.
func (h *Handler) handleBackendError(c *echo.Context, err error) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	code := errorCode(err)
	status := http.StatusBadRequest

	switch {
	case errors.Is(err, ErrTopicNotFound), errors.Is(err, ErrSubscriptionNotFound),
		errors.Is(err, ErrPlatformApplicationNotFound), errors.Is(err, ErrEndpointNotFound),
		errors.Is(err, ErrPhoneNumberNotFound):
		log.WarnContext(ctx, "SNS resource not found", "error", err)
	case errors.Is(err, ErrTopicAlreadyExists), errors.Is(err, ErrPlatformApplicationAlreadyExists),
		errors.Is(err, ErrSandboxPhoneAlreadyExists):
		log.WarnContext(ctx, "SNS resource already exists", "error", err)
	case errors.Is(err, ErrInvalidParameter):
		log.WarnContext(ctx, "SNS invalid parameter", "error", err)
	case errors.Is(err, ErrPermissionLabelExists), errors.Is(err, ErrPermissionLabelNotFound):
		log.WarnContext(ctx, "SNS permission label error", "error", err)
	default:
		status = http.StatusInternalServerError
		log.ErrorContext(ctx, "SNS internal error", "error", err)
	}

	return h.writeError(c, status, code, err.Error())
}

// errorCode returns the SNS error code string for the given error.
func errorCode(err error) string {
	switch {
	case errors.Is(err, ErrTopicNotFound), errors.Is(err, ErrSubscriptionNotFound),
		errors.Is(err, ErrPlatformApplicationNotFound), errors.Is(err, ErrEndpointNotFound),
		errors.Is(err, ErrPhoneNumberNotFound):
		return "NotFound"
	case errors.Is(err, ErrTopicAlreadyExists):
		return "TopicAlreadyExists"
	case errors.Is(err, ErrPlatformApplicationAlreadyExists):
		return "PlatformApplicationAlreadyExists"
	case errors.Is(err, ErrSandboxPhoneAlreadyExists):
		return "AlreadyExists"
	case errors.Is(err, ErrInvalidParameter), errors.Is(err, ErrSandboxPhoneNotVerified):
		return "InvalidParameter"
	case errors.Is(err, ErrPermissionLabelExists), errors.Is(err, ErrPermissionLabelNotFound):
		return "AuthorizationError"
	default:
		return "InternalError"
	}
}

// attrsToEntries converts a string map to sorted XMLAttributeEntry slice.
func attrsToEntries(attrs map[string]string) []XMLAttributeEntry {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	entries := make([]XMLAttributeEntry, len(keys))
	for i, k := range keys {
		entries[i] = XMLAttributeEntry{Key: k, Value: attrs[k]}
	}

	return entries
}

// toXMLSubscriptions converts Subscription slice to XMLSubscription slice.
// Pending (unconfirmed) subscriptions use pendingConfirmationARN ("pending
// confirmation") as their ARN, matching the AWS SNS ListSubscriptions /
// ListSubscriptionsByTopic behaviour.
func toXMLSubscriptions(subs []Subscription) []XMLSubscription {
	result := make([]XMLSubscription, len(subs))
	for i, s := range subs {
		subArn := s.SubscriptionArn
		if s.PendingConfirmation {
			subArn = pendingConfirmationARN
		}

		result[i] = XMLSubscription{
			TopicArn:        s.TopicArn,
			Protocol:        s.Protocol,
			SubscriptionArn: subArn,
			Owner:           s.Owner,
			Endpoint:        s.Endpoint,
		}
	}

	return result
}

// extractFormAttributes reads Attributes.entry.N.key/value pairs from the form.
func extractFormAttributes(c *echo.Context) map[string]string {
	attrs := make(map[string]string)

	for i := 1; ; i++ {
		key := c.Request().FormValue(fmt.Sprintf("Attributes.entry.%d.key", i))
		if key == "" {
			return attrs
		}

		val := c.Request().FormValue(fmt.Sprintf("Attributes.entry.%d.value", i))
		attrs[key] = val
	}
}

// extractFilterPolicy reads the FilterPolicy attribute from form Attributes entries.
func extractFilterPolicy(form url.Values) string {
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Attributes.entry.%d.key", i))
		if key == "" {
			return ""
		}

		if key == attrFilterPolicy {
			return form.Get(fmt.Sprintf("Attributes.entry.%d.value", i))
		}
	}
}

// extractMessageAttributes reads MessageAttributes.entry.N.Name/Value pairs from the form.
func extractMessageAttributes(form url.Values) map[string]MessageAttribute {
	return extractMessageAttributesWithPrefix(form, "MessageAttributes.")
}

// extractMessageAttributesWithPrefix reads MessageAttributes from form values using the
// given prefix (e.g. "MessageAttributes." or "PublishBatchRequestEntries.member.1.").
func extractMessageAttributesWithPrefix(form url.Values, prefix string) map[string]MessageAttribute {
	attrs := make(map[string]MessageAttribute)

	for i := 1; ; i++ {
		name := form.Get(fmt.Sprintf("%sentry.%d.Name", prefix, i))
		if name == "" {
			return attrs
		}

		attrs[name] = MessageAttribute{
			DataType:    form.Get(fmt.Sprintf("%sentry.%d.Value.DataType", prefix, i)),
			StringValue: form.Get(fmt.Sprintf("%sentry.%d.Value.StringValue", prefix, i)),
		}
	}
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

// extractBatchEntries reads PublishBatchRequestEntries.member.N entries from the form.
func extractBatchEntries(form url.Values) []batchEntry {
	entries := make([]batchEntry, 0)

	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i))
		if id == "" {
			return entries
		}

		// Extract per-entry MessageAttributes.
		prefix := fmt.Sprintf("PublishBatchRequestEntries.member.%d.", i)
		attrs := extractMessageAttributesWithPrefix(form, prefix)

		entries = append(entries, batchEntry{
			id:               id,
			message:          form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i)),
			subject:          form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Subject", i)),
			attrs:            attrs,
			messageGroupID:   form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.MessageGroupId", i)),
			messageStructure: form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.MessageStructure", i)),
			dedupID:          form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.MessageDeduplicationId", i)),
		})
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}

	// Clear FIFO deduplication cache.
	h.dedup.mu.Lock()
	h.dedup.entries = make(map[string]time.Time)
	h.dedup.mu.Unlock()
}
