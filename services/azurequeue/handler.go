package azurequeue

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// azureQueueVersion is the x-ms-version value echoed on every response. It
// is a plausible, well-formed Azure Storage REST API version string --
// picked so azure-sdk-for-go's queue client parses it without erroring, not
// because gopherstack implements that exact API version's full feature set.
// Mirrors services/azureblob's azureBlobVersion constant.
const azureQueueVersion = "2021-08-06"

// Operation name constants used for metrics (ExtractOperation) and
// GetSupportedOperations.
const (
	opListQueues     = "ListQueues"
	opCreateQueue    = "CreateQueue"
	opDeleteQueue    = "DeleteQueue"
	opPutMessage     = "PutMessage"
	opGetMessages    = "GetMessages"
	opPeekMessages   = "PeekMessages"
	opDeleteMessage  = "DeleteMessage"
	opUpdateMessage  = "UpdateMessage"
	opClearMessages  = "ClearMessages"
	unknownOperation = "Unknown"
)

// messagesSegment is the fixed path segment ("messages") under a queue that
// every message-scoped operation is nested beneath.
const messagesSegment = "messages"

// Handler is the Echo HTTP handler for Azure Queue Storage operations.
type Handler struct {
	Backend StorageBackend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	janitor *Janitor
	// Endpoint is e.g. "http://127.0.0.1:10001" -- used to build
	// ServiceEndpoint in List Queues responses.
	Endpoint string
	// Port is the TCP port StartWorker binds. Set from Settings at Init time
	// (see provider.go); defaults to DefaultPort. Like services/azureblob,
	// this is a single fixed, protocol-conventional port -- there is no
	// fallback pool, so StartWorker fails fast if it's unavailable rather
	// than silently binding a different port.
	Port int
}

// NewHandler creates a new Azure Queue Handler. Port defaults to
// DefaultPort; callers (typically provider.go) override it from Settings.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		Port:    DefaultPort,
		srvMu:   lockmetrics.New("azurequeue.server"),
	}
}

// WithJanitor attaches a background TTL-expiry janitor to the handler,
// mirroring services/xray's WithJanitor. If Backend is not an
// *InMemoryBackend the call is a no-op (a fake StorageBackend in tests has
// no sweepable state).
func (h *Handler) WithJanitor(interval time.Duration) *Handler {
	concrete, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h
	}

	h.janitor = NewJanitor(concrete, interval)

	return h
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name returns the service name.
func (h *Handler) Name() string { return "AzureQueue" }

// GetSupportedOperations returns the list of supported Azure Queue operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opListQueues,
		opCreateQueue,
		opDeleteQueue,
		opPutMessage,
		opGetMessages,
		opPeekMessages,
		opDeleteMessage,
		opUpdateMessage,
		opClearMessages,
	}
}

// RouteMatcher exists only to satisfy service.Registerable's interface
// contract: like services/azureblob, AzureQueue deliberately never matches
// on the shared AWS single-port Router. It runs on its own dedicated
// listener started by StartWorker (see provider.go for the full rationale).
// AzureQueue's Provider IS registered in cli.go's getMostRecentServiceProviders
// like every other service -- startBackgroundWorkers calls StartWorker via
// the service.BackgroundWorker interface regardless of routing, which is how
// the dedicated listener comes up. Only RouteMatcher itself is inert, kept
// so *Handler satisfies service.Registerable.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority returns the routing priority for the AzureQueue handler.
// Irrelevant in practice since RouteMatcher never matches; 0 (lowest) is
// the safe default.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation extracts the Azure Queue operation name from the request,
// for metrics labeling.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return operationFor(c.Request())
}

// ExtractResource extracts the queue/message resource identifier from the
// request path, for metrics labeling.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, queue, sub := splitPath(c.Request().URL.Path)
	if sub != "" {
		return queue + "/" + sub
	}

	return queue
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Handler returns the Echo handler function for Azure Queue operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()

		h.setCommonHeaders(c)
		h.checkAuth(r)

		account, queue, sub := splitPath(r.URL.Path)
		if account == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidUri",
				"The requested URI does not represent any resource on the server.")
		}

		switch {
		case queue == "":
			return h.handleAccountLevel(c)
		case sub == "":
			return h.handleQueueLevel(c, queue)
		case sub == messagesSegment:
			return h.handleMessagesLevel(c, queue)
		case strings.HasPrefix(sub, messagesSegment+"/"):
			return h.handleMessageLevel(c, queue, strings.TrimPrefix(sub, messagesSegment+"/"))
		default:
			return h.writeError(c, http.StatusBadRequest, "InvalidUri",
				"The requested URI does not represent any resource on the server.")
		}
	}
}

// checkAuth is intentionally permissive, mirroring services/azureblob's
// checkAuth exactly: it neither requires nor cryptographically verifies the
// Authorization header, matching this repo's permissive-by-default auth
// philosophy (see services/s3/sigv4.go). Any structurally-present
// "SharedKey ..." header, or its absence, is accepted.
func (h *Handler) checkAuth(r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return // anonymous; accepted by design at this milestone
	}

	if _, ok := azureauth.ParseAuthorizationHeader(authHeader); !ok {
		// Structurally malformed; still accepted at this milestone, but
		// logged so the gap is visible rather than silently swallowed.
		logger.Load(r.Context()).DebugContext(r.Context(), "azurequeue: malformed Authorization header accepted")
	}
}

// setCommonHeaders sets the headers real Azure SDKs expect on every
// response, success or error.
func (h *Handler) setCommonHeaders(c *echo.Context) {
	hdr := c.Response().Header()
	hdr.Set("X-Ms-Version", azureQueueVersion)
	hdr.Set("X-Ms-Request-Id", newRequestID())
	hdr.Set("Date", time.Now().UTC().Format(http.TimeFormat))
}

// newRequestID generates a plausible request-id (UUID-shaped, not
// cryptographically meaningful) for the x-ms-request-id header.
func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "00000000-0000-0000-0000-000000000000"
	}

	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// maxPathSegments bounds splitPath's strings.SplitN call: account, queue,
// and everything else (the "messages" or "messages/<id>" suffix) as sub.
const maxPathSegments = 3

// subSegmentIndex is the index (and, as a length check, the minimum part
// count) at which a sub-resource segment is present in splitPath's parts
// slice.
const subSegmentIndex = 2

// splitPath splits an Azure Queue REST path
// ("/<account>/<queue>[/messages[/<id>]]") into its three components.
func splitPath(p string) (string, string, string) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", "", ""
	}

	parts := strings.SplitN(p, "/", maxPathSegments)
	account := parts[0]

	var queue, sub string

	if len(parts) > 1 {
		queue = parts[1]
	}

	if len(parts) > subSegmentIndex {
		sub = parts[subSegmentIndex]
	}

	return account, queue, sub
}

// Query-parameter names and values shared by operationFor and the actual
// request handlers.
const (
	queryComp              = "comp"
	compList               = "list"
	queryNumOfMessages     = "numofmessages"
	queryVisibilityTimeout = "visibilitytimeout"
	queryMessageTTL        = "messagettl"
	queryPeekOnly          = "peekonly"
	queryPopReceipt        = "popreceipt"
)

// operationFor determines the Azure Queue operation name for a request, for
// metrics labeling. Mirrors the dispatch logic in
// handleAccountLevel/handleQueueLevel/handleMessagesLevel/handleMessageLevel
// without side effects.
func operationFor(r *http.Request) string {
	_, queue, sub := splitPath(r.URL.Path)

	switch {
	case strings.HasPrefix(sub, messagesSegment+"/"):
		return messageOperationFor(r.Method)
	case sub == messagesSegment:
		return messagesOperationFor(r)
	case queue != "":
		return queueOperationFor(r.Method)
	default:
		return accountOperationFor(r)
	}
}

// accountOperationFor covers the one account-level operation, List Queues
// (GET /<account>?comp=list).
func accountOperationFor(r *http.Request) string {
	if r.Method == http.MethodGet && r.URL.Query().Get(queryComp) == compList {
		return opListQueues
	}

	return unknownOperation
}

// queueOperationFor covers the two queue-scoped operations: Create Queue and
// Delete Queue.
func queueOperationFor(method string) string {
	switch method {
	case http.MethodPut:
		return opCreateQueue
	case http.MethodDelete:
		return opDeleteQueue
	default:
		return unknownOperation
	}
}

// messagesOperationFor covers the three /messages-scoped operations: Put,
// Get/Peek, and Clear.
func messagesOperationFor(r *http.Request) string {
	switch r.Method {
	case http.MethodPost:
		return opPutMessage
	case http.MethodGet:
		if r.URL.Query().Get(queryPeekOnly) == "true" {
			return opPeekMessages
		}

		return opGetMessages
	case http.MethodDelete:
		return opClearMessages
	default:
		return unknownOperation
	}
}

// messageOperationFor covers the two /messages/<id>-scoped operations:
// Delete Message and Update Message.
func messageOperationFor(method string) string {
	switch method {
	case http.MethodDelete:
		return opDeleteMessage
	case http.MethodPut:
		return opUpdateMessage
	default:
		return unknownOperation
	}
}

// serviceEndpoint returns the ServiceEndpoint attribute value for List
// Queues responses.
func (h *Handler) serviceEndpoint() string {
	if h.Endpoint != "" {
		return h.Endpoint
	}

	return fmt.Sprintf("http://127.0.0.1:%d", h.Port)
}

// handleAccountLevel serves GET /<account>?comp=list (List Queues).
func (h *Handler) handleAccountLevel(c *echo.Context) error {
	r := c.Request()
	if r.Method != http.MethodGet || c.QueryParam(queryComp) != compList {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"A query parameter is not supported for this operation.")
	}

	queues := h.Backend.ListQueues()
	result := enumerationResults{
		ServiceEndpoint: h.serviceEndpoint(),
		Queues:          &queueList{Queue: make([]queueEntry, 0, len(queues))},
	}

	for _, qi := range queues {
		result.Queues.Queue = append(result.Queues.Queue, queueEntry{Name: qi.Name})
	}

	return h.writeXML(c, http.StatusOK, result)
}

// handleQueueLevel serves the two queue-scoped operations: Create Queue and
// Delete Queue.
func (h *Handler) handleQueueLevel(c *echo.Context, queue string) error {
	switch c.Request().Method {
	case http.MethodPut:
		return h.createQueue(c, queue)
	case http.MethodDelete:
		return h.deleteQueue(c, queue)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "UnsupportedHttpVerb",
			"The resource doesn't support the specified HTTP verb.")
	}
}

func (h *Handler) createQueue(c *echo.Context, queue string) error {
	created, err := h.Backend.CreateQueue(queue)
	if err != nil {
		if errors.Is(err, ErrQueueAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "QueueAlreadyExists",
				"The specified queue already exists and has different metadata.")
		}

		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	if created {
		return c.NoContent(http.StatusCreated)
	}

	// Pre-existing queue with identical (empty) metadata: Azure treats this
	// as an idempotent success, not a conflict. See store.go's CreateQueue.
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) deleteQueue(c *echo.Context, queue string) error {
	if err := h.Backend.DeleteQueue(queue); err != nil {
		return h.writeError(c, http.StatusNotFound, "QueueNotFound",
			"The specified queue does not exist.")
	}

	return c.NoContent(http.StatusNoContent)
}

// StartWorker binds the dedicated Queue listener, starts serving on it, and
// -- if WithJanitor was called -- starts the background TTL-expiry sweep.
// See provider.go's Provider doc comment for why AzureQueue needs its own
// listener instead of registering into the shared AWS Router, and
// services/azureblob's StartWorker for the synchronous-bind rationale this
// mirrors exactly.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azurequeue: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureQueue", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: azureQueueReadHeaderTimeout,
		ReadTimeout:       azureQueueReadTimeout,
		IdleTimeout:       azureQueueIdleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "azurequeue", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "azurequeue: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "azurequeue: listener stopped", "error", serveErr)
		}
	}()

	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Timeouts for the dedicated Queue http.Server. See services/azureblob's
// identical constants for the ReadTimeout/IdleTimeout Slowloris rationale.
const (
	azureQueueReadHeaderTimeout = 10 * time.Second
	azureQueueReadTimeout       = 60 * time.Second
	azureQueueIdleTimeout       = 120 * time.Second
)

// Shutdown stops the dedicated Queue listener. A graceful Shutdown error
// (e.g. its context expiring before active connections finish) is logged
// and followed by Close, which forcibly closes the listener and any
// remaining idle/active connections; any Close error is logged too rather
// than leaving the listener to leak silently.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv := h.srv
	h.srv = nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	log := logger.Load(ctx)

	if err := srv.Shutdown(ctx); err != nil {
		log.ErrorContext(ctx, "azurequeue: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "azurequeue: forced close also failed", "error", closeErr)
		}
	}
}

// writeXML marshals v and writes it as the response body. v is marshaled
// without an XML header/prolog: echo's XMLBlob prepends xml.Header itself,
// so marshaling one here would duplicate it (see services/azureblob's
// identical writeXML for the same trap, originally hit in services/sqs).
func (h *Handler) writeXML(c *echo.Context, status int, v any) error {
	body, err := xml.Marshal(v)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to marshal response.")
	}

	return c.XMLBlob(status, body)
}

// writeError writes a standard Azure Storage REST error body, mirroring
// services/azureblob's writeError, plus the x-ms-error-code header real
// Azure Storage sets on every error response.
func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	c.Response().Header().Set("X-Ms-Error-Code", code)

	return h.writeXML(c, status, azureError{Code: code, Message: message})
}

// writeQueueNotFoundError maps a StorageBackend not-found error to the
// corresponding Azure error code/status.
func (h *Handler) writeQueueNotFoundError(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "QueueNotFound", "The specified queue does not exist.")
}
