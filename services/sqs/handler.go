package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Handler is the Echo HTTP handler for SQS operations.
type Handler struct {
	Backend       StorageBackend
	janitor       *Janitor
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}
	Endpoint      string
	DefaultRegion string
	janitorMu     sync.Mutex
}

// NewHandler creates a new SQS Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// WithJanitor attaches a background janitor to the handler. It stops the
// backend's auto-started internal janitor so the two do not run concurrently
// and race on the shared lock.
func (h *Handler) WithJanitor(interval ...time.Duration) *Handler {
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		memBackend.stopInternalJanitor()

		var d time.Duration
		if len(interval) > 0 {
			d = interval[0]
		}
		h.janitor = NewJanitor(memBackend, d)
	}

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		h.janitorMu.Lock()
		if h.janitorDone != nil {
			h.janitorMu.Unlock()

			return nil
		}

		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		h.janitorCancel = cancel
		h.janitorDone = done
		h.janitorMu.Unlock()

		go func() {
			defer close(done)
			h.janitor.Run(runCtx)
		}()
	}

	return nil
}

// Shutdown stops the janitor worker and waits for it to exit.
func (h *Handler) Shutdown(ctx context.Context) {
	h.janitorMu.Lock()
	cancel := h.janitorCancel
	done := h.janitorDone
	h.janitorCancel = nil
	h.janitorDone = nil
	h.janitorMu.Unlock()

	if cancel == nil || done == nil {
		return
	}

	cancel()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

var _ service.BackgroundWorker = (*Handler)(nil)
var _ service.Shutdowner = (*Handler)(nil)

// Name returns the service name.
func (h *Handler) Name() string {
	return "SQS"
}

// Purge implements service.Purgeable by delegating to the backend structure if supported.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Purge(ctx, cutoff)
	}
}

// GetSupportedOperations returns the list of supported SQS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opAddPermission,
		opCancelMessageMoveTask,
		opChangeMessageVisibility,
		opChangeMessageVisibilityBatch,
		opCreateQueue,
		opDeleteMessage,
		opDeleteMessageBatch,
		opDeleteQueue,
		opGetQueueAttributes,
		opGetQueueURL,
		opListDeadLetterSourceQueues,
		opListMessageMoveTasks,
		opListQueueTags,
		opListQueues,
		opPurgeQueue,
		opReceiveMessage,
		opRemovePermission,
		opSendMessage,
		opSendMessageBatch,
		opSetQueueAttributes,
		opStartMessageMoveTask,
		opTagQueue,
		opUntagQueue,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "sqs" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this SQS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches incoming SQS requests.
// It accepts two request styles:
//  1. JSON protocol: POST with X-Amz-Target: AmazonSQS.<Action>
//  2. Query protocol: POST with Content-Type: application/x-www-form-urlencoded
//     and a recognised Action= body parameter
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()

		// JSON protocol: X-Amz-Target header approach
		if strings.HasPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.") {
			path := r.URL.Path

			return path == "/" || strings.HasPrefix(path, "/000000000000/")
		}

		// Query protocol: form-encoded POST with a known Action
		if !isQueryProtocol(r) {
			return false
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return isKnownSQSAction(vals.Get("Action"))
	}
}

// sqsMatchPriority is lower than header-based matchers (e.g. SSM at 100) but higher
// than path-based matchers (e.g. Dashboard at 50).
const sqsMatchPriority = 75

// unknownOperation is the default operation name returned when the action cannot be determined.
const unknownOperation = "Unknown"

// SQS operation name constants shared between JSON and Query protocol dispatch.
const (
	opAddPermission                = "AddPermission"
	opCancelMessageMoveTask        = "CancelMessageMoveTask"
	opChangeMessageVisibility      = "ChangeMessageVisibility"
	opChangeMessageVisibilityBatch = "ChangeMessageVisibilityBatch"
	opCreateQueue                  = "CreateQueue"
	opDeleteMessage                = "DeleteMessage"
	opDeleteMessageBatch           = "DeleteMessageBatch"
	opDeleteQueue                  = "DeleteQueue"
	opGetQueueAttributes           = "GetQueueAttributes"
	opGetQueueURL                  = "GetQueueUrl"
	opListDeadLetterSourceQueues   = "ListDeadLetterSourceQueues"
	opListMessageMoveTasks         = "ListMessageMoveTasks"
	opListQueueTags                = "ListQueueTags"
	opListQueues                   = "ListQueues"
	opPurgeQueue                   = "PurgeQueue"
	opReceiveMessage               = "ReceiveMessage"
	opRemovePermission             = "RemovePermission"
	opSendMessage                  = "SendMessage"
	opSendMessageBatch             = "SendMessageBatch"
	opSetQueueAttributes           = "SetQueueAttributes"
	opStartMessageMoveTask         = "StartMessageMoveTask"
	opTagQueue                     = "TagQueue"
	opUntagQueue                   = "UntagQueue"
)

// MatchPriority returns the routing priority for the SQS handler.
func (h *Handler) MatchPriority() int {
	return sqsMatchPriority
}

// ExtractOperation extracts the SQS action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "AmazonSQS.")

	if action == "" || action == target {
		return unknownOperation
	}

	return action
}

type extractQueueURLInput struct {
	QueueURL string `json:"QueueUrl"`
}

// ExtractResource extracts the queue name from the JSON request body's QueueUrl field.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractQueueURLInput

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return queueNameFromURL(req.QueueURL)
}

// Handler returns the Echo handler function for SQS operations.
// It supports both the JSON protocol (X-Amz-Target) and the legacy Query
// (form-encoded) protocol.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		if isQueryProtocol(c.Request()) {
			return h.handleQueryProtocol(c)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"SQS", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.sqsRoute(ctx, c.Request(), action, body)
			},
			h.handleError,
		)
	}
}

type sqsDispatchFn func(ctx context.Context, r *http.Request, body []byte) (any, error)

func (h *Handler) sqsDispatchTable() map[string]sqsDispatchFn {
	return map[string]sqsDispatchFn{
		opCreateQueue:                  h.handleCreateQueue,
		opDeleteQueue:                  h.handleDeleteQueue,
		opListQueues:                   h.handleListQueues,
		opGetQueueURL:                  h.handleGetQueueURL,
		opGetQueueAttributes:           h.handleGetQueueAttributes,
		opSetQueueAttributes:           h.handleSetQueueAttributes,
		opSendMessage:                  h.handleSendMessage,
		opReceiveMessage:               h.handleReceiveMessage,
		opDeleteMessage:                h.handleDeleteMessage,
		opChangeMessageVisibility:      h.handleChangeMessageVisibility,
		opSendMessageBatch:             h.handleSendMessageBatch,
		opDeleteMessageBatch:           h.handleDeleteMessageBatch,
		opChangeMessageVisibilityBatch: h.handleChangeMessageVisibilityBatch,
		opPurgeQueue:                   h.handlePurgeQueue,
		opTagQueue:                     h.handleTagQueue,
		opUntagQueue:                   h.handleUntagQueue,
		opListQueueTags:                h.handleListQueueTags,
		opListDeadLetterSourceQueues:   h.handleListDeadLetterSourceQueues,
		opAddPermission:                h.handleAddPermission,
		opRemovePermission:             h.handleRemovePermission,
		opStartMessageMoveTask:         h.handleStartMessageMoveTask,
		opCancelMessageMoveTask:        h.handleCancelMessageMoveTask,
		opListMessageMoveTasks:         h.handleListMessageMoveTasks,
	}
}

// sqsRoute dispatches an SQS action to the appropriate handler method.
func (h *Handler) sqsRoute(ctx context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	fn, ok := h.sqsDispatchTable()[action]
	if !ok {
		return nil, ErrUnknownAction
	}

	result, err := fn(ctx, r, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// handleError writes an SQS error response using the standard error details mapping.
func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	errType, message, status := errorDetails(err)

	return c.JSON(status, jsonSQSError{Type: errType, Message: message})
}

// --- JSON request types ---

type jsonCreateQueueReq struct {
	Attributes map[string]string `json:"Attributes"`
	Tags       *tags.Tags        `json:"tags"`
	QueueName  string            `json:"QueueName"`
}

type jsonGetQueueURLReq struct {
	QueueName string `json:"QueueName"`
}

type jsonListQueuesReq struct {
	QueueNamePrefix string `json:"QueueNamePrefix"`
	NextToken       string `json:"NextToken"`
	MaxResults      int    `json:"MaxResults"`
}

type jsonQueueURLReq struct {
	QueueURL string `json:"QueueUrl"`
}

type jsonGetQueueAttributesReq struct {
	QueueURL       string   `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames"`
}

type jsonSetQueueAttributesReq struct {
	Attributes map[string]string `json:"Attributes"`
	QueueURL   string            `json:"QueueUrl"`
}

type jsonMsgAttr struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue"`
	BinaryValue []byte `json:"BinaryValue"`
}

type jsonSendMessageReq struct {
	MessageAttributes       map[string]jsonMsgAttr `json:"MessageAttributes"`
	MessageSystemAttributes map[string]jsonMsgAttr `json:"MessageSystemAttributes"`
	QueueURL                string                 `json:"QueueUrl"`
	MessageBody             string                 `json:"MessageBody"`
	MessageGroupID          string                 `json:"MessageGroupId"`
	MessageDeduplicationID  string                 `json:"MessageDeduplicationId"`
	DelaySeconds            int                    `json:"DelaySeconds"`
}

type jsonReceiveMessageReq struct {
	VisibilityTimeout           *int     `json:"VisibilityTimeout"`
	QueueURL                    string   `json:"QueueUrl"`
	ReceiveRequestAttemptID     string   `json:"ReceiveRequestAttemptId"`
	AttributeNames              []string `json:"AttributeNames"`
	MessageAttributeNames       []string `json:"MessageAttributeNames"`
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames"`
	MaxNumberOfMessages         int      `json:"MaxNumberOfMessages"`
	WaitTimeSeconds             int      `json:"WaitTimeSeconds"`
}

type jsonDeleteMessageReq struct {
	QueueURL      string `json:"QueueUrl"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

type jsonChangeVisibilityReq struct {
	QueueURL          string `json:"QueueUrl"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

type jsonSendBatchEntry struct {
	MessageAttributes       map[string]jsonMsgAttr `json:"MessageAttributes"`
	MessageSystemAttributes map[string]jsonMsgAttr `json:"MessageSystemAttributes"`
	ID                      string                 `json:"Id"`
	MessageBody             string                 `json:"MessageBody"`
	MessageGroupID          string                 `json:"MessageGroupId"`
	MessageDeduplicationID  string                 `json:"MessageDeduplicationId"`
	DelaySeconds            int                    `json:"DelaySeconds"`
}

type jsonSendMessageBatchReq struct {
	QueueURL string               `json:"QueueUrl"`
	Entries  []jsonSendBatchEntry `json:"Entries"`
}

type jsonDeleteBatchEntry struct {
	ID            string `json:"Id"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

type jsonDeleteMessageBatchReq struct {
	QueueURL string                 `json:"QueueUrl"`
	Entries  []jsonDeleteBatchEntry `json:"Entries"`
}

type jsonChangeBatchEntry struct {
	ID                string `json:"Id"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

type jsonChangeVisibilityBatchReq struct {
	QueueURL string                 `json:"QueueUrl"`
	Entries  []jsonChangeBatchEntry `json:"Entries"`
}

type jsonTagQueueReq struct {
	Tags     *tags.Tags `json:"Tags"`
	QueueURL string     `json:"QueueUrl"`
}

type jsonUntagQueueReq struct {
	QueueURL string   `json:"QueueUrl"`
	TagKeys  []string `json:"TagKeys"`
}

// --- JSON response types ---

type jsonQueueURLResp struct {
	QueueURL string `json:"QueueUrl"`
}

type jsonListQueuesResp struct {
	NextToken string   `json:"NextToken,omitempty"`
	QueueURLs []string `json:"QueueUrls"`
}

type jsonSendMessageResp struct {
	MessageID              string `json:"MessageId"`
	MD5OfMessageBody       string `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes string `json:"MD5OfMessageAttributes,omitempty"`
	SequenceNumber         string `json:"SequenceNumber"`
}

type jsonReceivedMessage struct {
	Attributes             map[string]string      `json:"Attributes"`
	MessageAttributes      map[string]jsonMsgAttr `json:"MessageAttributes"`
	MessageID              string                 `json:"MessageId"`
	ReceiptHandle          string                 `json:"ReceiptHandle"`
	MD5OfBody              string                 `json:"MD5OfBody"`
	MD5OfMessageAttributes string                 `json:"MD5OfMessageAttributes,omitempty"`
	Body                   string                 `json:"Body"`
}

type jsonReceiveMessageResp struct {
	Messages []jsonReceivedMessage `json:"Messages"`
}

type jsonGetQueueAttributesResp struct {
	Attributes map[string]string `json:"Attributes"`
}

type jsonBatchSuccess struct {
	ID                     string `json:"Id"`
	MessageID              string `json:"MessageId,omitempty"`
	MD5OfMessageBody       string `json:"MD5OfMessageBody,omitempty"`
	MD5OfMessageAttributes string `json:"MD5OfMessageAttributes,omitempty"`
	SequenceNumber         string `json:"SequenceNumber,omitempty"`
}

type jsonBatchFailure struct {
	ID          string `json:"Id"`
	Code        string `json:"Code"`
	Message     string `json:"Message"`
	SenderFault bool   `json:"SenderFault"`
}

type jsonBatchResult struct {
	Successful []jsonBatchSuccess `json:"Successful"`
	Failed     []jsonBatchFailure `json:"Failed"`
}

type jsonListQueueTagsResp struct {
	Tags *tags.Tags `json:"Tags"`
}

type jsonListDeadLetterSourceQueuesReq struct {
	QueueURL   string `json:"QueueUrl"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type jsonListDeadLetterSourceQueuesResp struct {
	NextToken string   `json:"NextToken,omitempty"`
	QueueURLs []string `json:"queueUrls"`
}

type jsonSQSError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// --- handler methods ---

func (h *Handler) handleCreateQueue(
	ctx context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonCreateQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	endpoint := h.Endpoint
	if endpoint == "" {
		endpoint = r.Host
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	var initialTags map[string]string
	if req.Tags != nil {
		initialTags = req.Tags.Clone()
	}

	out, err := h.Backend.CreateQueue(&CreateQueueInput{
		QueueName:  req.QueueName,
		Attributes: req.Attributes,
		Tags:       initialTags,
		Endpoint:   endpoint,
		Scheme:     scheme,
		Region:     region,
	})
	if err != nil {
		if !errors.Is(err, ErrQueueAlreadyExists) {
			logger.Load(ctx).WarnContext(ctx, "CreateQueue failed", "error", err)
		}

		return nil, err
	}

	return jsonQueueURLResp{QueueURL: out.QueueURL}, nil
}

func (h *Handler) handleDeleteQueue(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	if err := h.Backend.DeleteQueue(&DeleteQueueInput{QueueURL: req.QueueURL, Region: region}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListQueues(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonListQueuesReq
	// ListQueues body may be empty; ignore unmarshal errors
	_ = json.Unmarshal(body, &req)

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	out, err := h.Backend.ListQueues(&ListQueuesInput{
		QueueNamePrefix: req.QueueNamePrefix,
		NextToken:       req.NextToken,
		MaxResults:      req.MaxResults,
		Region:          region,
	})
	if err != nil {
		return nil, err
	}

	queueURLs := out.QueueURLs
	if queueURLs == nil {
		queueURLs = []string{}
	}

	return jsonListQueuesResp{QueueURLs: queueURLs, NextToken: out.NextToken}, nil
}

func (h *Handler) handleGetQueueURL(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	out, err := h.Backend.GetQueueURL(&GetQueueURLInput{QueueName: req.QueueName, Region: region})
	if err != nil {
		return nil, err
	}

	return jsonQueueURLResp{QueueURL: out.QueueURL}, nil
}

func (h *Handler) handleGetQueueAttributes(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetQueueAttributesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.GetQueueAttributes(&GetQueueAttributesInput{
		QueueURL:       req.QueueURL,
		Region:         httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		AttributeNames: req.AttributeNames,
	})
	if err != nil {
		return nil, err
	}

	attrs := out.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}

	return jsonGetQueueAttributesResp{Attributes: attrs}, nil
}

func (h *Handler) handleSetQueueAttributes(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonSetQueueAttributesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.SetQueueAttributes(&SetQueueAttributesInput{
		QueueURL:   req.QueueURL,
		Region:     httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Attributes: req.Attributes,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleSendMessage(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonSendMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.SendMessage(&SendMessageInput{
		QueueURL:                req.QueueURL,
		Region:                  httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		MessageBody:             req.MessageBody,
		MessageGroupID:          req.MessageGroupID,
		MessageDeduplicationID:  req.MessageDeduplicationID,
		DelaySeconds:            req.DelaySeconds,
		MessageAttributes:       toMessageAttributeValues(req.MessageAttributes),
		MessageSystemAttributes: toMessageAttributeValues(req.MessageSystemAttributes),
	})
	if err != nil {
		return nil, err
	}

	return jsonSendMessageResp{
		MessageID:              out.MessageID,
		MD5OfMessageBody:       out.MD5OfBody,
		MD5OfMessageAttributes: out.MD5OfMessageAttributes,
		SequenceNumber:         out.SequenceNumber,
	}, nil
}

func (h *Handler) handleReceiveMessage(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonReceiveMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	vt := noVisibilitySet
	if req.VisibilityTimeout != nil {
		vt = *req.VisibilityTimeout
	}

	// Merge MessageSystemAttributeNames into AttributeNames for unified system attribute filtering.
	// AWS treats them identically in the mock; this allows callers using the new parameter
	// style to receive system attributes like ApproximateFirstReceiveTimestamp.
	effectiveAttrNames := req.AttributeNames
	if len(req.MessageSystemAttributeNames) > 0 {
		seen := make(map[string]bool, len(effectiveAttrNames))
		for _, n := range effectiveAttrNames {
			seen[n] = true
		}
		for _, n := range req.MessageSystemAttributeNames {
			if !seen[n] {
				effectiveAttrNames = append(effectiveAttrNames, n)
			}
		}
	}

	out, err := h.Backend.ReceiveMessage(&ReceiveMessageInput{
		QueueURL:                req.QueueURL,
		Region:                  httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		MaxNumberOfMessages:     req.MaxNumberOfMessages,
		VisibilityTimeout:       vt,
		WaitTimeSeconds:         req.WaitTimeSeconds,
		AttributeNames:          effectiveAttrNames,
		MessageAttributeNames:   req.MessageAttributeNames,
		ReceiveRequestAttemptID: req.ReceiveRequestAttemptID,
	})
	if err != nil {
		return nil, err
	}

	msgs := make([]jsonReceivedMessage, 0, len(out.Messages))
	for _, msg := range out.Messages {
		attrs := msg.Attributes
		if attrs == nil {
			attrs = map[string]string{}
		}

		msgs = append(msgs, jsonReceivedMessage{
			MessageID:              msg.MessageID,
			ReceiptHandle:          msg.ReceiptHandle,
			MD5OfBody:              msg.MD5OfBody,
			MD5OfMessageAttributes: msg.MD5OfMessageAttributes,
			Body:                   msg.Body,
			Attributes:             filterSystemAttrs(attrs, effectiveAttrNames),
			MessageAttributes:      filterJSONMsgAttrs(msg.MessageAttributes, req.MessageAttributeNames),
		})
	}

	return jsonReceiveMessageResp{Messages: msgs}, nil
}

func (h *Handler) handleDeleteMessage(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonDeleteMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.DeleteMessage(&DeleteMessageInput{
		QueueURL:      req.QueueURL,
		Region:        httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		ReceiptHandle: req.ReceiptHandle,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleChangeMessageVisibility(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonChangeVisibilityReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
		QueueURL:          req.QueueURL,
		Region:            httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		ReceiptHandle:     req.ReceiptHandle,
		VisibilityTimeout: req.VisibilityTimeout,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleSendMessageBatch(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonSendMessageBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	entries := make([]SendMessageBatchEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, SendMessageBatchEntry{
			ID:                      e.ID,
			MessageBody:             e.MessageBody,
			MessageGroupID:          e.MessageGroupID,
			MessageDeduplicationID:  e.MessageDeduplicationID,
			DelaySeconds:            e.DelaySeconds,
			MessageAttributes:       toMessageAttributeValues(e.MessageAttributes),
			MessageSystemAttributes: toMessageAttributeValues(e.MessageSystemAttributes),
		})
	}

	out, err := h.Backend.SendMessageBatch(&SendMessageBatchInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Entries:  entries,
	})
	if err != nil {
		return nil, err
	}

	result := jsonBatchResult{
		Successful: make([]jsonBatchSuccess, 0, len(out.Successful)),
		Failed:     make([]jsonBatchFailure, 0, len(out.Failed)),
	}

	for _, s := range out.Successful {
		result.Successful = append(result.Successful, jsonBatchSuccess{
			ID:                     s.ID,
			MessageID:              s.MessageID,
			MD5OfMessageBody:       s.MD5OfBody,
			MD5OfMessageAttributes: s.MD5OfMessageAttributes,
			SequenceNumber:         s.SequenceNumber,
		})
	}

	for _, f := range out.Failed {
		result.Failed = append(result.Failed, jsonBatchFailure(f))
	}

	return result, nil
}

//nolint:dupl // JSON batch request/response flow intentionally mirrors sibling batch handlers.
func (h *Handler) handleDeleteMessageBatch(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonDeleteMessageBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	entries := make([]DeleteMessageBatchEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, DeleteMessageBatchEntry(e))
	}

	out, err := h.Backend.DeleteMessageBatch(&DeleteMessageBatchInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Entries:  entries,
	})
	if err != nil {
		return nil, err
	}

	result := jsonBatchResult{
		Successful: make([]jsonBatchSuccess, 0, len(out.Successful)),
		Failed:     make([]jsonBatchFailure, 0, len(out.Failed)),
	}

	for _, s := range out.Successful {
		result.Successful = append(result.Successful, jsonBatchSuccess{ID: s.ID})
	}

	for _, f := range out.Failed {
		result.Failed = append(result.Failed, jsonBatchFailure(f))
	}

	return result, nil
}

//nolint:dupl // JSON batch request/response flow intentionally mirrors sibling batch handlers.
func (h *Handler) handleChangeMessageVisibilityBatch(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonChangeVisibilityBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	entries := make([]ChangeMessageVisibilityBatchRequestEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, ChangeMessageVisibilityBatchRequestEntry(e))
	}

	out, err := h.Backend.ChangeMessageVisibilityBatch(&ChangeMessageVisibilityBatchInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Entries:  entries,
	})
	if err != nil {
		return nil, err
	}

	result := jsonBatchResult{
		Successful: make([]jsonBatchSuccess, 0, len(out.Successful)),
		Failed:     make([]jsonBatchFailure, 0, len(out.Failed)),
	}

	for _, s := range out.Successful {
		result.Successful = append(result.Successful, jsonBatchSuccess{ID: s.ID})
	}

	for _, f := range out.Failed {
		result.Failed = append(result.Failed, jsonBatchFailure(f))
	}

	return result, nil
}

func (h *Handler) handlePurgeQueue(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.PurgeQueue(&PurgeQueueInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleTagQueue(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonTagQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.TagQueue(&TagQueueInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Tags:     req.Tags,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleUntagQueue(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonUntagQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.UntagQueue(&UntagQueueInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		TagKeys:  req.TagKeys,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListQueueTags(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.ListQueueTags(&ListQueueTagsInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
	})
	if err != nil {
		return nil, err
	}

	return jsonListQueueTagsResp{Tags: out.Tags}, nil
}

func (h *Handler) handleListDeadLetterSourceQueues(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonListDeadLetterSourceQueuesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.ListDeadLetterSourceQueues(&ListDeadLetterSourceQueuesInput{
		QueueURL:   req.QueueURL,
		Region:     httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		NextToken:  req.NextToken,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	queueURLs := out.QueueURLs
	if queueURLs == nil {
		queueURLs = []string{}
	}

	return jsonListDeadLetterSourceQueuesResp{QueueURLs: queueURLs, NextToken: out.NextToken}, nil
}

const errTypeInvalidParameterValue = "com.amazonaws.sqs#InvalidParameterValue"

// invalidParameterValueMessage returns the AWS error message for parameter-validation
// sentinel errors, or ("", false) if the error is not a parameter error.
func invalidParameterValueMessage(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrInvalidWaitTime):
		return "Value for parameter WaitTimeSeconds is invalid. Reason: Must be between 0 and 20, if provided.", true
	case errors.Is(err, ErrInvalidVisibilityTimeout):
		return "Value for parameter VisibilityTimeout is invalid. Reason: Must be between 0 and 43200, if provided.", true
	case errors.Is(err, ErrMissingMessageGroupID):
		return "The request must contain the parameter MessageGroupId.", true
	case errors.Is(err, ErrMissingDeduplicationID):
		const dedupMsg = "The queue should either have ContentBasedDeduplication enabled" +
			" or MessageDeduplicationId provided explicitly."

		return dedupMsg, true
	default:
		return "", false
	}
}

// errorEntry maps a sentinel error to its SQS error type, message, and HTTP status code.
type errorEntry struct {
	errType string
	message string
	status  int
}

// errorDetails maps an error to its SQS JSON error type, message, and HTTP status.
func errorDetails(err error) (string, string, int) {
	if msg, ok := invalidParameterValueMessage(err); ok {
		return errTypeInvalidParameterValue, msg, http.StatusBadRequest
	}

	if e, ok := sqsErrorDetails(err); ok {
		return e.errType, e.message, e.status
	}

	return "com.amazonaws.sqs#InternalError",
		"An internal error occurred.",
		http.StatusInternalServerError
}

// sqsErrorDetails looks up an error in the well-known SQS error table.
// Extracted to keep errorDetails itself under the funlen limit.
// The table is split across two helpers to stay within funlen.
func sqsErrorDetails(err error) (errorEntry, bool) {
	if e, ok := sqsCoreErrorDetails(err); ok {
		return e, true
	}

	return sqsPermMoveErrorDetails(err)
}

// sqsCoreErrorDetails handles the core queue/message sentinel errors.
func sqsCoreErrorDetails(err error) (errorEntry, bool) {
	type errRow struct {
		sentinel error
		entry    errorEntry
	}

	const badReq = http.StatusBadRequest

	rows := [...]errRow{
		{
			ErrQueueNotFound,
			errorEntry{"com.amazonaws.sqs#QueueDoesNotExist", "The specified queue does not exist.", badReq},
		},
		{
			ErrQueueAlreadyExists,
			errorEntry{"com.amazonaws.sqs#QueueNameExists", "A queue with this name already exists.", badReq},
		},
		{
			ErrReceiptHandleInvalid,
			errorEntry{
				"com.amazonaws.sqs#ReceiptHandleIsInvalid",
				"The receipt handle is not valid.",
				badReq,
			},
		},
		{ErrMessageNotInflight, errorEntry{
			"com.amazonaws.sqs#MessageNotInflight",
			"The message referred to by the receipt handle is not in-flight.",
			badReq,
		}},
		{
			ErrTooManyEntriesInBatch,
			errorEntry{
				"com.amazonaws.sqs#TooManyEntriesInBatchRequest",
				"Too many entries in batch request.",
				badReq,
			},
		},
		{
			ErrBatchEntryIDsNotDistinct,
			errorEntry{
				"com.amazonaws.sqs#BatchEntryIdsNotDistinct",
				"Two or more batch entries in the request have the same Id.",
				badReq,
			},
		},
		{
			ErrInvalidBatchEntry,
			errorEntry{"com.amazonaws.sqs#EmptyBatchRequest", "The batch request is empty.", badReq},
		},
		{
			ErrInvalidAttribute,
			errorEntry{"com.amazonaws.sqs#InvalidAttributeValue", "Invalid attribute value.", badReq},
		},
		{
			ErrMessageTooLarge,
			errorEntry{
				"com.amazonaws.sqs#InvalidMessageContents",
				"The message exceeds the maximum message size.",
				badReq,
			},
		},
		{
			ErrUnknownAction,
			errorEntry{
				"com.amazonaws.sqs#InvalidAction",
				"The action or operation requested is invalid.",
				badReq,
			},
		},
	}

	for _, row := range rows {
		if errors.Is(err, row.sentinel) {
			return row.entry, true
		}
	}

	return errorEntry{}, false
}

// sqsPermMoveErrorDetails handles permission, move-task, and validation sentinel errors.
func sqsPermMoveErrorDetails(err error) (errorEntry, bool) {
	if e, ok := sqsPermErrorDetails(err); ok {
		return e, true
	}

	return sqsValidationErrorDetails(err)
}

// sqsPermErrorDetails covers permission and move-task sentinel errors.
func sqsPermErrorDetails(err error) (errorEntry, bool) {
	type errRow struct {
		sentinel error
		entry    errorEntry
	}

	const badReq = http.StatusBadRequest
	const conflict = "com.amazonaws.sqs#ResourceInConflict"
	const ipv = errTypeInvalidParameterValue

	rows := [...]errRow{
		{ErrTaskHandleInvalid, errorEntry{ipv, "The task handle provided is not valid.", badReq}},
		{ErrInvalidPermissionLabel, errorEntry{
			ipv,
			"The value for the required parameter 'Label' is not valid. Reason: label must not be empty.",
			badReq,
		}},
		{ErrInvalidPermissionActions, errorEntry{
			ipv,
			"The value for 'Actions' is not valid. Reason: Actions must not be empty.",
			badReq,
		}},
		{ErrInvalidPermissionAccountIDs, errorEntry{
			ipv,
			"The value for 'AWSAccountIds' is not valid. Reason: AWSAccountIds must not be empty.",
			badReq,
		}},
		{ErrInvalidSourceArn, errorEntry{
			ipv,
			"The value for 'SourceArn' is not valid. Reason: SourceArn must not be empty.",
			badReq,
		}},
		{ErrInvalidMaxMessagesPerSecond, errorEntry{
			ipv,
			"The value for 'MaxNumberOfMessagesPerSecond' is not valid. Reason: must be >= 0.",
			badReq,
		}},
		{ErrMoveTaskAlreadyRunning, errorEntry{
			conflict,
			"A message move task already exists for the specified source queue.",
			badReq,
		}},
		{ErrMoveTaskNotRunning, errorEntry{
			conflict,
			"A message move task with the specified task handle is not running.",
			badReq,
		}},
	}

	for _, row := range rows {
		if errors.Is(err, row.sentinel) {
			return row.entry, true
		}
	}

	return errorEntry{}, false
}

// sqsValidationErrorDetails covers queue name, message, and limit sentinel errors.
func sqsValidationErrorDetails(err error) (errorEntry, bool) {
	type errRow struct {
		sentinel error
		entry    errorEntry
	}

	const badReq = http.StatusBadRequest
	const ipv = errTypeInvalidParameterValue

	rows := [...]errRow{
		{ErrInvalidQueueName, errorEntry{
			ipv,
			"The name of a queue can only include alphanumeric characters, hyphens, or underscores. " +
				"Queue name must be between 1 and 80 characters.",
			badReq,
		}},
		{ErrInvalidMessageBody, errorEntry{
			ipv,
			"The request includes a parameter that is not valid for this queue type.",
			badReq,
		}},
		{ErrInvalidMaxMessages, errorEntry{
			ipv,
			"Value for parameter MaxNumberOfMessages is invalid. Reason: must be between 1 and 10, if provided.",
			badReq,
		}},
		{ErrPurgeQueueInProgress, errorEntry{
			"com.amazonaws.sqs#PurgeQueueInProgress",
			"Only one PurgeQueue operation on SomeQueue is allowed every 60 seconds.",
			badReq,
		}},
		{ErrOverLimit, errorEntry{
			"OverLimit",
			"The specified action violates a service quota.",
			http.StatusForbidden,
		}},
		{ErrInvalidMessageAttributeValue, errorEntry{
			ipv,
			"Message attribute value is invalid. Check that the DataType and the associated value are correct.",
			badReq,
		}},
		{ErrInvalidAttributeName, errorEntry{
			"com.amazonaws.sqs#InvalidAttributeName",
			"Unknown Attribute FifoQueue.",
			badReq,
		}},
		{ErrFIFODelayNotSupported, errorEntry{
			ipv,
			"The request include parameter that is not valid for this queue type." +
				" DelaySeconds is not supported for FIFO queues.",
			badReq,
		}},
		{ErrBatchRequestTooLong, errorEntry{
			"com.amazonaws.sqs#BatchRequestTooLong",
			"The length of all the messages put together is more than the limit.",
			badReq,
		}},
	}

	for _, row := range rows {
		if errors.Is(err, row.sentinel) {
			return row.entry, true
		}
	}

	return errorEntry{}, false
}

// queueNameFromURL extracts the queue name from a full queue URL.
func queueNameFromURL(queueURL string) string {
	parts := strings.Split(queueURL, "/")
	if len(parts) == 0 {
		return ""
	}

	return parts[len(parts)-1]
}

// toMessageAttributeValues converts JSON message attributes to internal representation.
func toMessageAttributeValues(attrs map[string]jsonMsgAttr) map[string]MessageAttributeValue {
	if len(attrs) == 0 {
		return nil
	}

	result := make(map[string]MessageAttributeValue, len(attrs))

	for k, v := range attrs {
		result[k] = MessageAttributeValue(v)
	}

	return result
}

// toJSONMsgAttrs converts internal message attributes to JSON representation.
func toJSONMsgAttrs(attrs map[string]MessageAttributeValue) map[string]jsonMsgAttr {
	result := make(map[string]jsonMsgAttr, len(attrs))

	for k, v := range attrs {
		result[k] = jsonMsgAttr(v)
	}

	return result
}

func filterJSONMsgAttrs(attrs map[string]MessageAttributeValue, requested []string) map[string]jsonMsgAttr {
	if len(attrs) == 0 || len(requested) == 0 {
		return map[string]jsonMsgAttr{}
	}

	// AWS SDKs may send either "All" or ".*" to request all message attributes.
	// Both are treated as wildcards that return every attribute, matching the
	// behaviour of the real SQS service.
	if containsStr(requested, attrAll) || containsStr(requested, ".*") {
		return toJSONMsgAttrs(attrs)
	}

	exact := make(map[string]struct{}, len(requested))
	prefixes := make([]string, 0, len(requested))
	for _, name := range requested {
		if before, ok := strings.CutSuffix(name, ".*"); ok {
			prefixes = append(prefixes, before)

			continue
		}

		exact[name] = struct{}{}
	}

	result := make(map[string]jsonMsgAttr)
	for name, value := range attrs {
		if _, ok := exact[name]; ok {
			result[name] = jsonMsgAttr(value)

			continue
		}

		for _, prefix := range prefixes {
			if strings.HasPrefix(name, prefix) {
				result[name] = jsonMsgAttr(value)

				break
			}
		}
	}

	return result
}

// filterSystemAttrs returns a copy of attrs filtered to only the names requested by
// the AttributeNames parameter of a ReceiveMessage call. When requested is empty or
// contains the "All" sentinel the full map is returned unchanged.
func filterSystemAttrs(attrs map[string]string, requested []string) map[string]string {
	if len(requested) == 0 {
		return attrs
	}

	if containsStr(requested, attrAll) {
		return attrs
	}

	result := make(map[string]string, len(requested))

	for _, name := range requested {
		if v, ok := attrs[name]; ok {
			result[name] = v
		}
	}

	return result
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}

// --- JSON request/response types for new operations ---

type jsonAddPermissionReq struct {
	QueueURL      string   `json:"QueueUrl"`
	Label         string   `json:"Label"`
	Actions       []string `json:"Actions"`
	AWSAccountIDs []string `json:"AWSAccountIds"`
}

type jsonRemovePermissionReq struct {
	QueueURL string `json:"QueueUrl"`
	Label    string `json:"Label"`
}

type jsonStartMessageMoveTaskReq struct {
	SourceArn                    string `json:"SourceArn"`
	DestinationArn               string `json:"DestinationArn"`
	MaxNumberOfMessagesPerSecond int32  `json:"MaxNumberOfMessagesPerSecond"`
}

type jsonStartMessageMoveTaskResp struct {
	TaskHandle string `json:"TaskHandle"`
}

type jsonCancelMessageMoveTaskReq struct {
	TaskHandle string `json:"TaskHandle"`
}

type jsonCancelMessageMoveTaskResp struct {
	ApproximateNumberOfMessagesMoved int64 `json:"ApproximateNumberOfMessagesMoved"`
}

type jsonListMessageMoveTasksReq struct {
	SourceArn  string `json:"SourceArn"`
	MaxResults int32  `json:"MaxResults"`
}

type jsonMessageMoveTask struct {
	ApproximateNumberOfMessagesToMove *int64  `json:"ApproximateNumberOfMessagesToMove,omitempty"`
	MaxNumberOfMessagesPerSecond      *int32  `json:"MaxNumberOfMessagesPerSecond,omitempty"`
	FailureReason                     *string `json:"FailureReason,omitempty"`
	TaskHandle                        string  `json:"TaskHandle,omitempty"`
	SourceArn                         string  `json:"SourceArn"`
	DestinationArn                    string  `json:"DestinationArn,omitempty"`
	Status                            string  `json:"Status"`
	// Always present — matches AWS SDK ListMessageMoveTasksResultEntry.
	ApproximateNumberOfMessagesMoved int64 `json:"ApproximateNumberOfMessagesMoved"`
	StartedTimestamp                 int64 `json:"StartedTimestamp"`
}

type jsonListMessageMoveTasksResp struct {
	Results []jsonMessageMoveTask `json:"Results"`
}

// --- handler methods for new operations ---

func (h *Handler) handleAddPermission(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonAddPermissionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.AddPermission(&AddPermissionInput{
		QueueURL:      req.QueueURL,
		Region:        httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Label:         req.Label,
		AWSAccountIDs: req.AWSAccountIDs,
		Actions:       req.Actions,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleRemovePermission(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonRemovePermissionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.RemovePermission(&RemovePermissionInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Label:    req.Label,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleStartMessageMoveTask(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonStartMessageMoveTaskReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.StartMessageMoveTask(&StartMessageMoveTaskInput{
		SourceArn:                    req.SourceArn,
		DestinationArn:               req.DestinationArn,
		MaxNumberOfMessagesPerSecond: req.MaxNumberOfMessagesPerSecond,
	})
	if err != nil {
		return nil, err
	}

	return jsonStartMessageMoveTaskResp{TaskHandle: out.TaskHandle}, nil
}

func (h *Handler) handleCancelMessageMoveTask(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonCancelMessageMoveTaskReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.CancelMessageMoveTask(&CancelMessageMoveTaskInput{
		TaskHandle: req.TaskHandle,
	})
	if err != nil {
		return nil, err
	}

	return jsonCancelMessageMoveTaskResp{
		ApproximateNumberOfMessagesMoved: out.ApproximateNumberOfMessagesMoved,
	}, nil
}

func (h *Handler) handleListMessageMoveTasks(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListMessageMoveTasksReq
	// Body may be empty; ignore unmarshal errors.
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListMessageMoveTasks(&ListMessageMoveTasksInput{
		SourceArn:  req.SourceArn,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	results := make([]jsonMessageMoveTask, 0, len(out.Results))
	for _, t := range out.Results {
		results = append(results, jsonMessageMoveTask{
			TaskHandle:                        t.TaskHandle,
			SourceArn:                         t.SourceArn,
			DestinationArn:                    t.DestinationArn,
			Status:                            string(t.Status),
			StartedTimestamp:                  t.StartedTimestamp,
			ApproximateNumberOfMessagesMoved:  t.ApproximateNumberOfMessagesMoved,
			ApproximateNumberOfMessagesToMove: t.ApproximateNumberOfMessagesToMove,
			MaxNumberOfMessagesPerSecond:      t.MaxNumberOfMessagesPerSecond,
			FailureReason:                     t.FailureReason,
		})
	}

	return jsonListMessageMoveTasksResp{Results: results}, nil
}
