package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Handler is the Echo HTTP handler for SQS operations.
type Handler struct {
	Backend  StorageBackend
	Logger   *slog.Logger
	Endpoint string
	// DefaultRegion is the fallback region used when region cannot be extracted from the request.
	DefaultRegion string
}

// NewHandler creates a new SQS Handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SQS"
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
	}
}

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
	body, err := httputil.ReadBody(c.Request())
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
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read SQS request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), "AmazonSQS.")
		requestID := uuid.New().String()

		if action == "" {
			h.writeError(c.Response(), ErrUnknownAction, requestID)

			return nil
		}

		log.DebugContext(ctx, "SQS request", "action", action)
		h.dispatch(ctx, c.Response(), c.Request(), body, action, requestID)

		return nil
	}
}

type sqsDispatchFn func(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte, requestID string)

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
	}
}

// dispatch routes the action to the appropriate handler method.
func (h *Handler) dispatch(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	action, requestID string,
) {
	fn, ok := h.sqsDispatchTable()[action]
	if !ok {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	fn(ctx, w, r, body, requestID)
}

// --- JSON request types ---

type jsonCreateQueueReq struct {
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"tags"`
	QueueName  string            `json:"QueueName"`
}

type jsonGetQueueURLReq struct {
	QueueName string `json:"QueueName"`
}

type jsonListQueuesReq struct {
	QueueNamePrefix string `json:"QueueNamePrefix"`
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
	Tags     map[string]string `json:"Tags"`
	QueueURL string            `json:"QueueUrl"`
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
	Tags map[string]string `json:"Tags"`
}

type jsonSQSError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// --- handler methods ---

func (h *Handler) handleCreateQueue(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonCreateQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	endpoint := h.Endpoint
	if endpoint == "" {
		endpoint = r.Host
	}

	region := httputil.ExtractRegionFromRequest(r, h.DefaultRegion)

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

		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonQueueURLResp{QueueURL: out.QueueURL})
}

func (h *Handler) handleDeleteQueue(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	if err := h.Backend.DeleteQueue(&DeleteQueueInput{QueueURL: req.QueueURL}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleListQueues(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonListQueuesReq
	// ListQueues body may be empty; ignore unmarshal errors
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListQueues(&ListQueuesInput{
		QueueNamePrefix: req.QueueNamePrefix,
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	queueURLs := out.QueueURLs
	if queueURLs == nil {
		queueURLs = []string{}
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonListQueuesResp{QueueURLs: queueURLs})
}

func (h *Handler) handleGetQueueURL(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonGetQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	out, err := h.Backend.GetQueueURL(&GetQueueURLInput{QueueName: req.QueueName})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonQueueURLResp{QueueURL: out.QueueURL})
}

func (h *Handler) handleGetQueueAttributes(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonGetQueueAttributesReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	out, err := h.Backend.GetQueueAttributes(&GetQueueAttributesInput{
		QueueURL:       req.QueueURL,
		AttributeNames: req.AttributeNames,
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	attrs := out.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonGetQueueAttributesResp{Attributes: attrs})
}

func (h *Handler) handleSetQueueAttributes(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonSetQueueAttributesReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	if err := h.Backend.SetQueueAttributes(&SetQueueAttributesInput{
		QueueURL:   req.QueueURL,
		Attributes: req.Attributes,
	}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleSendMessage(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonSendMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
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
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonSendMessageResp{
		MessageID:              out.MessageID,
		MD5OfMessageBody:       out.MD5OfBody,
		MD5OfMessageAttributes: out.MD5OfMessageAttributes,
		SequenceNumber:         "",
	})
}

func (h *Handler) handleReceiveMessage(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonReceiveMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
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
		h.writeError(w, err, requestID)

		return
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

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonReceiveMessageResp{Messages: msgs})
}

func (h *Handler) handleDeleteMessage(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonDeleteMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	if err := h.Backend.DeleteMessage(&DeleteMessageInput{
		QueueURL:      req.QueueURL,
		ReceiptHandle: req.ReceiptHandle,
	}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleChangeMessageVisibility(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonChangeVisibilityReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	if err := h.Backend.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
		QueueURL:          req.QueueURL,
		ReceiptHandle:     req.ReceiptHandle,
		VisibilityTimeout: req.VisibilityTimeout,
	}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleSendMessageBatch(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonSendMessageBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
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
		h.writeError(w, err, requestID)

		return
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

	httputil.WriteJSON(h.Logger, w, http.StatusOK, result)
}

func (h *Handler) handleDeleteMessageBatch(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonDeleteMessageBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
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
		h.writeError(w, err, requestID)

		return
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

	httputil.WriteJSON(h.Logger, w, http.StatusOK, result)
}

func (h *Handler) handleChangeMessageVisibilityBatch(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonChangeVisibilityBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
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
		h.writeError(w, err, requestID)

		return
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

	httputil.WriteJSON(h.Logger, w, http.StatusOK, result)
}

func (h *Handler) handlePurgeQueue(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	if err := h.Backend.PurgeQueue(&PurgeQueueInput{QueueURL: req.QueueURL}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleTagQueue(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonTagQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	if err := h.Backend.TagQueue(&TagQueueInput{
		QueueURL: req.QueueURL,
		Tags:     req.Tags,
	}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagQueue(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonUntagQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	if err := h.Backend.UntagQueue(&UntagQueueInput{
		QueueURL: req.QueueURL,
		TagKeys:  req.TagKeys,
	}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleListQueueTags(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	out, err := h.Backend.ListQueueTags(&ListQueueTagsInput{QueueURL: req.QueueURL})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	tags := out.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonListQueueTagsResp{Tags: tags})
}

// writeError writes a JSON SQS error response.
func (h *Handler) writeError(w http.ResponseWriter, err error, _ string) {
	errType, message, status := errorDetails(err)
	httputil.WriteJSON(h.Logger, w, status, jsonSQSError{
		Type:    errType,
		Message: message,
	})
}

// errorDetails maps an error to its SQS JSON error type, message, and HTTP status.
func errorDetails(err error) (string, string, int) {
	switch {
	case errors.Is(err, ErrQueueNotFound):
		// Use the legacy error code that real AWS SQS returns; the AWS SDK v2
		// maps "AWS.SimpleQueueService.NonExistentQueue" → *types.QueueDoesNotExist.
		return "AWS.SimpleQueueService.NonExistentQueue",
			"The specified queue does not exist.",
			http.StatusBadRequest
	case errors.Is(err, ErrQueueAlreadyExists):
		return "com.amazonaws.sqs#QueueNameExists",
			"A queue with this name already exists.",
			http.StatusBadRequest
	case errors.Is(err, ErrReceiptHandleInvalid):
		return "com.amazonaws.sqs#ReceiptHandleIsInvalid",
			"The receipt handle is not valid.",
			http.StatusBadRequest
	case errors.Is(err, ErrTooManyEntriesInBatch):
		return "com.amazonaws.sqs#TooManyEntriesInBatchRequest",
			"Too many entries in batch request.",
			http.StatusBadRequest
	case errors.Is(err, ErrInvalidBatchEntry):
		return "com.amazonaws.sqs#EmptyBatchRequest",
			"The batch request is empty.",
			http.StatusBadRequest
	case errors.Is(err, ErrInvalidAttribute):
		return "com.amazonaws.sqs#InvalidAttributeValue",
			"Invalid attribute value.",
			http.StatusBadRequest
	case errors.Is(err, ErrUnknownAction):
		return "com.amazonaws.sqs#InvalidAction",
			"The action or operation requested is invalid.",
			http.StatusBadRequest
	default:
		return "com.amazonaws.sqs#InternalError",
			"An internal error occurred.",
			http.StatusInternalServerError
	}
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
