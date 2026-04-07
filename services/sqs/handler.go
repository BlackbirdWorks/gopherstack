package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Handler is the Echo HTTP handler for SQS operations.
type Handler struct {
	Backend  StorageBackend
	Endpoint string
	// DefaultRegion is the fallback region used when region cannot be extracted from the request.
	DefaultRegion string
}

// NewHandler creates a new SQS Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

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
		"CreateQueue",
		"DeleteQueue",
		"ListQueues",
		"GetQueueUrl",
		"GetQueueAttributes",
		"SetQueueAttributes",
		"SendMessage",
		"ReceiveMessage",
		"DeleteMessage",
		"ChangeMessageVisibility",
		"SendMessageBatch",
		"DeleteMessageBatch",
		"ChangeMessageVisibilityBatch",
		"PurgeQueue",
		"TagQueue",
		"UntagQueue",
		"ListQueueTags",
		"ListDeadLetterSourceQueues",
		"AddPermission",
		"RemovePermission",
		"StartMessageMoveTask",
		"CancelMessageMoveTask",
		"ListMessageMoveTasks",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "sqs" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this SQS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches incoming SQS requests.
// It matches POST requests whose X-Amz-Target header starts with "AmazonSQS." and whose
// path is "/" or starts with "/000000000000/" (to avoid capturing Dashboard form POSTs).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if !strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), "AmazonSQS.") {
			return false
		}

		path := c.Request().URL.Path

		return path == "/" || strings.HasPrefix(path, "/000000000000/")
	}
}

// sqsMatchPriority is lower than header-based matchers (e.g. SSM at 100) but higher
// than path-based matchers (e.g. Dashboard at 50).
const sqsMatchPriority = 75

// unknownOperation is the default operation name returned when the action cannot be determined.
const unknownOperation = "Unknown"

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
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
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
		"CreateQueue":                  h.handleCreateQueue,
		"DeleteQueue":                  h.handleDeleteQueue,
		"ListQueues":                   h.handleListQueues,
		"GetQueueUrl":                  h.handleGetQueueURL,
		"GetQueueAttributes":           h.handleGetQueueAttributes,
		"SetQueueAttributes":           h.handleSetQueueAttributes,
		"SendMessage":                  h.handleSendMessage,
		"ReceiveMessage":               h.handleReceiveMessage,
		"DeleteMessage":                h.handleDeleteMessage,
		"ChangeMessageVisibility":      h.handleChangeMessageVisibility,
		"SendMessageBatch":             h.handleSendMessageBatch,
		"DeleteMessageBatch":           h.handleDeleteMessageBatch,
		"ChangeMessageVisibilityBatch": h.handleChangeMessageVisibilityBatch,
		"PurgeQueue":                   h.handlePurgeQueue,
		"TagQueue":                     h.handleTagQueue,
		"UntagQueue":                   h.handleUntagQueue,
		"ListQueueTags":                h.handleListQueueTags,
		"ListDeadLetterSourceQueues":   h.handleListDeadLetterSourceQueues,
		"AddPermission":                h.handleAddPermission,
		"RemovePermission":             h.handleRemovePermission,
		"StartMessageMoveTask":         h.handleStartMessageMoveTask,
		"CancelMessageMoveTask":        h.handleCancelMessageMoveTask,
		"ListMessageMoveTasks":         h.handleListMessageMoveTasks,
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
	MessageAttributes      map[string]jsonMsgAttr `json:"MessageAttributes"`
	QueueURL               string                 `json:"QueueUrl"`
	MessageBody            string                 `json:"MessageBody"`
	MessageGroupID         string                 `json:"MessageGroupId"`
	MessageDeduplicationID string                 `json:"MessageDeduplicationId"`
	DelaySeconds           int                    `json:"DelaySeconds"`
}

type jsonReceiveMessageReq struct {
	VisibilityTimeout     *int     `json:"VisibilityTimeout"`
	QueueURL              string   `json:"QueueUrl"`
	AttributeNames        []string `json:"AttributeNames"`
	MessageAttributeNames []string `json:"MessageAttributeNames"`
	MaxNumberOfMessages   int      `json:"MaxNumberOfMessages"`
	WaitTimeSeconds       int      `json:"WaitTimeSeconds"`
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
	MessageAttributes      map[string]jsonMsgAttr `json:"MessageAttributes"`
	ID                     string                 `json:"Id"`
	MessageBody            string                 `json:"MessageBody"`
	MessageGroupID         string                 `json:"MessageGroupId"`
	MessageDeduplicationID string                 `json:"MessageDeduplicationId"`
	DelaySeconds           int                    `json:"DelaySeconds"`
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

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	out, err := h.Backend.CreateQueue(&CreateQueueInput{
		QueueName:  req.QueueName,
		Attributes: req.Attributes,
		Endpoint:   endpoint,
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
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.DeleteQueue(&DeleteQueueInput{QueueURL: req.QueueURL}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListQueues(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListQueuesReq
	// ListQueues body may be empty; ignore unmarshal errors
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListQueues(&ListQueuesInput{
		QueueNamePrefix: req.QueueNamePrefix,
		NextToken:       req.NextToken,
		MaxResults:      req.MaxResults,
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
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.GetQueueURL(&GetQueueURLInput{QueueName: req.QueueName})
	if err != nil {
		return nil, err
	}

	return jsonQueueURLResp{QueueURL: out.QueueURL}, nil
}

func (h *Handler) handleGetQueueAttributes(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetQueueAttributesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.GetQueueAttributes(&GetQueueAttributesInput{
		QueueURL:       req.QueueURL,
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
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonSetQueueAttributesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.SetQueueAttributes(&SetQueueAttributesInput{
		QueueURL:   req.QueueURL,
		Attributes: req.Attributes,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleSendMessage(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonSendMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.SendMessage(&SendMessageInput{
		QueueURL:               req.QueueURL,
		MessageBody:            req.MessageBody,
		MessageGroupID:         req.MessageGroupID,
		MessageDeduplicationID: req.MessageDeduplicationID,
		DelaySeconds:           req.DelaySeconds,
		MessageAttributes:      toMessageAttributeValues(req.MessageAttributes),
	})
	if err != nil {
		return nil, err
	}

	return jsonSendMessageResp{
		MessageID:              out.MessageID,
		MD5OfMessageBody:       out.MD5OfBody,
		MD5OfMessageAttributes: out.MD5OfMessageAttributes,
		SequenceNumber:         "",
	}, nil
}

func (h *Handler) handleReceiveMessage(
	_ context.Context,
	_ *http.Request,
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

	out, err := h.Backend.ReceiveMessage(&ReceiveMessageInput{
		QueueURL:            req.QueueURL,
		MaxNumberOfMessages: req.MaxNumberOfMessages,
		VisibilityTimeout:   vt,
		WaitTimeSeconds:     req.WaitTimeSeconds,
		AttributeNames:      req.AttributeNames,
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
			Attributes:             attrs,
			MessageAttributes:      toJSONMsgAttrs(msg.MessageAttributes),
		})
	}

	return jsonReceiveMessageResp{Messages: msgs}, nil
}

func (h *Handler) handleDeleteMessage(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDeleteMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.DeleteMessage(&DeleteMessageInput{
		QueueURL:      req.QueueURL,
		ReceiptHandle: req.ReceiptHandle,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleChangeMessageVisibility(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonChangeVisibilityReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
		QueueURL:          req.QueueURL,
		ReceiptHandle:     req.ReceiptHandle,
		VisibilityTimeout: req.VisibilityTimeout,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleSendMessageBatch(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonSendMessageBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	entries := make([]SendMessageBatchEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, SendMessageBatchEntry{
			ID:                     e.ID,
			MessageBody:            e.MessageBody,
			MessageGroupID:         e.MessageGroupID,
			MessageDeduplicationID: e.MessageDeduplicationID,
			DelaySeconds:           e.DelaySeconds,
			MessageAttributes:      toMessageAttributeValues(e.MessageAttributes),
		})
	}

	out, err := h.Backend.SendMessageBatch(&SendMessageBatchInput{
		QueueURL: req.QueueURL,
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
		})
	}

	for _, f := range out.Failed {
		//nolint:staticcheck // struct tags differ; type conversion not possible
		result.Failed = append(result.Failed, jsonBatchFailure{
			ID:          f.ID,
			Code:        f.Code,
			Message:     f.Message,
			SenderFault: f.SenderFault,
		})
	}

	return result, nil
}

func (h *Handler) handleDeleteMessageBatch(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDeleteMessageBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	entries := make([]DeleteMessageBatchEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		//nolint:staticcheck // struct tags differ; type conversion not possible
		entries = append(entries, DeleteMessageBatchEntry{
			ID:            e.ID,
			ReceiptHandle: e.ReceiptHandle,
		})
	}

	out, err := h.Backend.DeleteMessageBatch(&DeleteMessageBatchInput{
		QueueURL: req.QueueURL,
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
		//nolint:staticcheck // struct tags differ; type conversion not possible
		result.Failed = append(result.Failed, jsonBatchFailure{
			ID:          f.ID,
			Code:        f.Code,
			Message:     f.Message,
			SenderFault: f.SenderFault,
		})
	}

	return result, nil
}

func (h *Handler) handleChangeMessageVisibilityBatch(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonChangeVisibilityBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	entries := make([]ChangeMessageVisibilityBatchRequestEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		//nolint:staticcheck // struct tags differ; type conversion not possible
		entries = append(entries, ChangeMessageVisibilityBatchRequestEntry{
			ID:                e.ID,
			ReceiptHandle:     e.ReceiptHandle,
			VisibilityTimeout: e.VisibilityTimeout,
		})
	}

	out, err := h.Backend.ChangeMessageVisibilityBatch(&ChangeMessageVisibilityBatchInput{
		QueueURL: req.QueueURL,
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
		//nolint:staticcheck // struct tags differ; type conversion not possible
		result.Failed = append(result.Failed, jsonBatchFailure{
			ID:          f.ID,
			Code:        f.Code,
			Message:     f.Message,
			SenderFault: f.SenderFault,
		})
	}

	return result, nil
}

func (h *Handler) handlePurgeQueue(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.PurgeQueue(&PurgeQueueInput{QueueURL: req.QueueURL}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleTagQueue(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonTagQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.TagQueue(&TagQueueInput{
		QueueURL: req.QueueURL,
		Tags:     req.Tags,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleUntagQueue(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUntagQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.UntagQueue(&UntagQueueInput{
		QueueURL: req.QueueURL,
		TagKeys:  req.TagKeys,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListQueueTags(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.ListQueueTags(&ListQueueTagsInput{QueueURL: req.QueueURL})
	if err != nil {
		return nil, err
	}

	return jsonListQueueTagsResp{Tags: out.Tags}, nil
}

func (h *Handler) handleListDeadLetterSourceQueues(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListDeadLetterSourceQueuesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.ListDeadLetterSourceQueues(&ListDeadLetterSourceQueuesInput{
		QueueURL:   req.QueueURL,
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
		//nolint:staticcheck // struct tags differ; type conversion not possible
		result[k] = MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}

	return result
}

// toJSONMsgAttrs converts internal message attributes to JSON representation.
func toJSONMsgAttrs(attrs map[string]MessageAttributeValue) map[string]jsonMsgAttr {
	result := make(map[string]jsonMsgAttr, len(attrs))

	for k, v := range attrs {
		result[k] = jsonMsgAttr{ //nolint:staticcheck // types have same fields but different struct tags
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
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
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonAddPermissionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.AddPermission(&AddPermissionInput{
		QueueURL:      req.QueueURL,
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
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonRemovePermissionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.RemovePermission(&RemovePermissionInput{
		QueueURL: req.QueueURL,
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
