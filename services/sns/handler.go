package sns

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	snsVersion       = "Version=2010-03-31"
	snsContentType   = "application/x-www-form-urlencoded"
	snsMatchPriority = 80
	unknownOperation = "Unknown"
	opSubscribe      = "Subscribe"
)

type Handler struct {
	actions     map[string]snsActionFn
	Backend     StorageBackend
	dedup       *fifoDeduplication
	fifoSeqNums sync.Map // topicArn → *atomic.Int64; FIFO message sequence counters
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
		opSubscribe,
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
			// Body unreadable (e.g. oversized): fall back to the User-Agent
			// marker every aws-sdk-go-v2 sns client sets (api_client.go's
			// AddSDKAgentKeyValue -- "api/sns"). That still identifies this
			// as ours, so claim it and let Handler() produce the typed
			// error instead of masking the read failure as a 404.
			return service.MatchesUserAgentMarker(c.Request().Header, "api/sns")
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
		opSubscribe:                          h.handleSubscribe,
		"TagResource":                        h.handleTagResource,
		"Unsubscribe":                        h.handleUnsubscribe,
		"UntagResource":                      h.handleUntagResource,
		"VerifySMSSandboxPhoneNumber":        h.handleVerifySMSSandboxPhoneNumber,
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}

	// Clear FIFO deduplication cache.
	func() {
		h.dedup.mu.Lock()
		defer h.dedup.mu.Unlock()

		h.dedup.entries = make(map[string]time.Time)
	}()

	// Clear FIFO sequence number counters.
	h.fifoSeqNums.Range(func(k, _ any) bool {
		h.fifoSeqNums.Delete(k)

		return true
	})
}
