package sqs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
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
		"PurgeQueue",
	}
}

// RouteMatcher returns a function that matches incoming requests for SQS.
// It matches POST requests with form-encoded bodies sent to the SQS path namespace.
// AWS SQS SDK requests always target "/" (for queue-level ops like CreateQueue/ListQueues)
// or "/000000000000/<queue-name>" (for queue operations). This prevents intercepting
// Dashboard HTMX form submissions that also use application/x-www-form-urlencoded.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		ct := c.Request().Header.Get("Content-Type")
		if !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
			return false
		}

		path := c.Request().URL.Path

		return path == "/" || strings.HasPrefix(path, "/000000000000/")
	}
}

// sqsMatchPriority is lower than header-based matchers (e.g. SSM at 100) but higher
// than path-based matchers (e.g. Dashboard at 50), so content-type matching runs second.
const sqsMatchPriority = 75

// unknownOperation is the default operation name returned when the action cannot be determined.
const unknownOperation = "Unknown"

// MatchPriority returns the routing priority for the SQS handler.
func (h *Handler) MatchPriority() int {
	return sqsMatchPriority
}

// ExtractOperation extracts the SQS Action from the request form body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return unknownOperation
	}

	form, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownOperation
	}

	if action := form.Get("Action"); action != "" {
		return action
	}

	return unknownOperation
}

// ExtractResource extracts the queue name from the request form body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	form, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	return queueNameFromURL(form.Get("QueueUrl"))
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

		form, err := url.ParseQuery(string(body))
		if err != nil {
			return c.String(http.StatusBadRequest, "invalid request body")
		}

		action := form.Get("Action")
		requestID := uuid.New().String()

		if action == "" {
			h.writeError(c.Response(), ErrUnknownAction, requestID)

			return nil
		}

		log.DebugContext(ctx, "SQS request", "action", action)
		h.dispatch(ctx, c.Response(), c.Request(), form, action, requestID)

		return nil
	}
}

// dispatch routes the action to the appropriate handler method.
func (h *Handler) dispatch(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	form url.Values,
	action, requestID string,
) {
	switch action {
	case "CreateQueue":
		h.handleCreateQueue(ctx, w, r, form, requestID)
	case "DeleteQueue":
		h.handleDeleteQueue(ctx, w, r, form, requestID)
	case "ListQueues":
		h.handleListQueues(ctx, w, r, form, requestID)
	case "GetQueueUrl":
		h.handleGetQueueURL(ctx, w, r, form, requestID)
	case "GetQueueAttributes":
		h.handleGetQueueAttributes(ctx, w, r, form, requestID)
	case "SetQueueAttributes":
		h.handleSetQueueAttributes(ctx, w, r, form, requestID)
	case "SendMessage":
		h.handleSendMessage(ctx, w, r, form, requestID)
	case "ReceiveMessage":
		h.handleReceiveMessage(ctx, w, r, form, requestID)
	case "DeleteMessage":
		h.handleDeleteMessage(ctx, w, r, form, requestID)
	case "ChangeMessageVisibility":
		h.handleChangeMessageVisibility(ctx, w, r, form, requestID)
	case "SendMessageBatch":
		h.handleSendMessageBatch(ctx, w, r, form, requestID)
	case "DeleteMessageBatch":
		h.handleDeleteMessageBatch(ctx, w, r, form, requestID)
	case "PurgeQueue":
		h.handlePurgeQueue(ctx, w, r, form, requestID)
	default:
		h.writeError(w, ErrUnknownAction, requestID)
	}
}

func (h *Handler) handleCreateQueue(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	form url.Values,
	requestID string,
) {
	endpoint := h.Endpoint
	if endpoint == "" {
		endpoint = r.Host
	}

	out, err := h.Backend.CreateQueue(&CreateQueueInput{
		QueueName:  form.Get("QueueName"),
		Attributes: parseKeyValuePairs(form, "Attribute"),
		Endpoint:   endpoint,
	})
	if err != nil {
		if !errors.Is(err, ErrQueueAlreadyExists) {
			logger.Load(ctx).WarnContext(ctx, "CreateQueue failed", "error", err)
		}
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, CreateQueueResponse{
		Xmlns:             sqsNamespace,
		CreateQueueResult: CreateQueueResult{QueueURL: out.QueueURL},
		ResponseMetadata:  XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleDeleteQueue(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	if err := h.Backend.DeleteQueue(&DeleteQueueInput{QueueURL: form.Get("QueueUrl")}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, DeleteQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleListQueues(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	out, err := h.Backend.ListQueues(&ListQueuesInput{
		QueueNamePrefix: form.Get("QueueNamePrefix"),
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, ListQueuesResponse{
		Xmlns:            sqsNamespace,
		ListQueuesResult: ListQueuesResult{QueueURLs: out.QueueURLs},
		ResponseMetadata: XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleGetQueueURL(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	out, err := h.Backend.GetQueueURL(&GetQueueURLInput{QueueName: form.Get("QueueName")})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, GetQueueURLResponse{
		Xmlns:             sqsNamespace,
		GetQueueURLResult: GetQueueURLResult{QueueURL: out.QueueURL},
		ResponseMetadata:  XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleGetQueueAttributes(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	out, err := h.Backend.GetQueueAttributes(&GetQueueAttributesInput{
		QueueURL:       form.Get("QueueUrl"),
		AttributeNames: parseIndexedStrings(form, "AttributeName"),
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	var xmlAttrs []XMLAttribute
	for k, v := range out.Attributes {
		xmlAttrs = append(xmlAttrs, XMLAttribute{Name: k, Value: v})
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, GetQueueAttributesResponse{
		Xmlns:                    sqsNamespace,
		GetQueueAttributesResult: GetQueueAttributesResult{Attributes: xmlAttrs},
		ResponseMetadata:         XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleSetQueueAttributes(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	if err := h.Backend.SetQueueAttributes(&SetQueueAttributesInput{
		QueueURL:   form.Get("QueueUrl"),
		Attributes: parseKeyValuePairs(form, "Attribute"),
	}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, SetQueueAttributesResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleSendMessage(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	delay, _ := strconv.Atoi(form.Get("DelaySeconds"))

	out, err := h.Backend.SendMessage(&SendMessageInput{
		QueueURL:               form.Get("QueueUrl"),
		MessageBody:            form.Get("MessageBody"),
		MessageGroupID:         form.Get("MessageGroupId"),
		MessageDeduplicationID: form.Get("MessageDeduplicationId"),
		DelaySeconds:           delay,
		MessageAttributes:      parseMessageAttributes(form),
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, SendMessageResponse{
		Xmlns:             sqsNamespace,
		SendMessageResult: SendMessageResult{MD5OfMessageBody: out.MD5OfBody, MessageID: out.MessageID},
		ResponseMetadata:  XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleReceiveMessage(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	maxMsgs, _ := strconv.Atoi(form.Get("MaxNumberOfMessages"))
	waitSecs, _ := strconv.Atoi(form.Get("WaitTimeSeconds"))

	vt := noVisibilitySet
	if vtStr := form.Get("VisibilityTimeout"); vtStr != "" {
		if v, err := strconv.Atoi(vtStr); err == nil {
			vt = v
		}
	}

	out, err := h.Backend.ReceiveMessage(&ReceiveMessageInput{
		QueueURL:            form.Get("QueueUrl"),
		MaxNumberOfMessages: maxMsgs,
		VisibilityTimeout:   vt,
		WaitTimeSeconds:     waitSecs,
		AttributeNames:      parseIndexedStrings(form, "AttributeName"),
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	xmlMsgs := make([]XMLMessage, 0, len(out.Messages))
	for _, msg := range out.Messages {
		xmlMsgs = append(xmlMsgs, toXMLMessage(msg))
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, ReceiveMessageResponse{
		Xmlns:                sqsNamespace,
		ReceiveMessageResult: ReceiveMessageResult{Messages: xmlMsgs},
		ResponseMetadata:     XMLResponseMetadata{RequestID: requestID},
	})
}

// toXMLMessage converts an internal Message to its XML representation.
func toXMLMessage(msg *Message) XMLMessage {
	attrs := make([]XMLAttribute, 0, len(msg.Attributes))

	for k, v := range msg.Attributes {
		attrs = append(attrs, XMLAttribute{Name: k, Value: v})
	}

	return XMLMessage{
		MessageID:     msg.MessageID,
		ReceiptHandle: msg.ReceiptHandle,
		MD5OfBody:     msg.MD5OfBody,
		Body:          msg.Body,
		Attributes:    attrs,
	}
}

func (h *Handler) handleDeleteMessage(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	if err := h.Backend.DeleteMessage(&DeleteMessageInput{
		QueueURL:      form.Get("QueueUrl"),
		ReceiptHandle: form.Get("ReceiptHandle"),
	}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, DeleteMessageResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleChangeMessageVisibility(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	vt, _ := strconv.Atoi(form.Get("VisibilityTimeout"))

	if err := h.Backend.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
		QueueURL:          form.Get("QueueUrl"),
		ReceiptHandle:     form.Get("ReceiptHandle"),
		VisibilityTimeout: vt,
	}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, ChangeMessageVisibilityResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: requestID},
	})
}

func (h *Handler) handleSendMessageBatch(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	out, err := h.Backend.SendMessageBatch(&SendMessageBatchInput{
		QueueURL: form.Get("QueueUrl"),
		Entries:  parseSendBatchEntries(form),
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	result := buildXMLSendBatchResult(out)

	httputil.WriteXML(h.Logger, w, http.StatusOK, SendMessageBatchResponse{
		Xmlns:                  sqsNamespace,
		SendMessageBatchResult: result,
		ResponseMetadata:       XMLResponseMetadata{RequestID: requestID},
	})
}

func buildXMLSendBatchResult(out *SendMessageBatchOutput) XMLSendMessageBatchResult {
	result := XMLSendMessageBatchResult{}

	for _, s := range out.Successful {
		result.Successful = append(result.Successful, XMLSendMessageBatchResultEntry{
			ID:               s.ID,
			MessageID:        s.MessageID,
			MD5OfMessageBody: s.MD5OfBody,
		})
	}

	for _, f := range out.Failed {
		result.Failed = append(result.Failed, XMLSendMessageBatchFailedEntry(f))
	}

	return result
}

func (h *Handler) handleDeleteMessageBatch(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	out, err := h.Backend.DeleteMessageBatch(&DeleteMessageBatchInput{
		QueueURL: form.Get("QueueUrl"),
		Entries:  parseDeleteBatchEntries(form),
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	result := buildXMLDeleteBatchResult(out)

	httputil.WriteXML(h.Logger, w, http.StatusOK, DeleteMessageBatchResponse{
		Xmlns:                    sqsNamespace,
		DeleteMessageBatchResult: result,
		ResponseMetadata:         XMLResponseMetadata{RequestID: requestID},
	})
}

func buildXMLDeleteBatchResult(out *DeleteMessageBatchOutput) XMLDeleteMessageBatchResult {
	result := XMLDeleteMessageBatchResult{}

	for _, s := range out.Successful {
		result.Successful = append(result.Successful, XMLDeleteMessageBatchResultEntry(s))
	}

	for _, f := range out.Failed {
		result.Failed = append(result.Failed, XMLDeleteMessageBatchFailedEntry(f))
	}

	return result
}

func (h *Handler) handlePurgeQueue(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	form url.Values,
	requestID string,
) {
	if err := h.Backend.PurgeQueue(&PurgeQueueInput{QueueURL: form.Get("QueueUrl")}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteXML(h.Logger, w, http.StatusOK, PurgeQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: requestID},
	})
}

// writeError writes an SQS XML error response.
func (h *Handler) writeError(w http.ResponseWriter, err error, requestID string) {
	code, message, status := errorDetails(err)
	httputil.WriteXML(h.Logger, w, status, XMLErrorResponse{
		Xmlns: sqsNamespace,
		Error: XMLError{
			Type:    errTypeSender,
			Code:    code,
			Message: message,
			Detail:  XMLErrorDetail{},
		},
		RequestID: requestID,
	})
}

// errorDetails maps an error to its SQS error code, message, and HTTP status.
func errorDetails(err error) (string, string, int) {
	switch {
	case errors.Is(err, ErrQueueNotFound):
		return "AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.", http.StatusBadRequest
	case errors.Is(err, ErrQueueAlreadyExists):
		return "QueueAlreadyExists", "A queue with this name already exists.", http.StatusBadRequest
	case errors.Is(err, ErrReceiptHandleInvalid):
		return "ReceiptHandleIsInvalid", "The receipt handle is not valid.", http.StatusBadRequest
	case errors.Is(err, ErrTooManyEntriesInBatch):
		return "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
			"Too many entries in batch request.", http.StatusBadRequest
	case errors.Is(err, ErrInvalidBatchEntry):
		return "AWS.SimpleQueueService.EmptyBatchRequest", "The batch request is empty.", http.StatusBadRequest
	case errors.Is(err, ErrInvalidAttribute):
		return "InvalidAttributeValue", "Invalid attribute value.", http.StatusBadRequest
	case errors.Is(err, ErrUnknownAction):
		return "InvalidAction", "The action or operation requested is invalid.", http.StatusBadRequest
	default:
		return "InternalError", "An internal error occurred.", http.StatusInternalServerError
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

// parseKeyValuePairs parses indexed Attribute.N.Name / Attribute.N.Value form pairs.
func parseKeyValuePairs(form url.Values, prefix string) map[string]string {
	result := make(map[string]string)

	for i := 1; i <= maxParseIterations; i++ {
		name := form.Get(fmt.Sprintf("%s.%d.Name", prefix, i))
		if name == "" {
			break
		}

		result[name] = form.Get(fmt.Sprintf("%s.%d.Value", prefix, i))
	}

	return result
}

// parseIndexedStrings parses AttributeName.N or MessageAttributeName.N lists.
func parseIndexedStrings(form url.Values, prefix string) []string {
	var result []string

	for i := 1; i <= maxParseIterations; i++ {
		val := form.Get(fmt.Sprintf("%s.%d", prefix, i))
		if val == "" {
			break
		}

		result = append(result, val)
	}

	return result
}

// parseMessageAttributes parses MessageAttribute.N.* parameters from a form.
func parseMessageAttributes(form url.Values) map[string]MessageAttributeValue {
	result := make(map[string]MessageAttributeValue)

	for i := 1; i <= maxParseIterations; i++ {
		name := form.Get(fmt.Sprintf("MessageAttribute.%d.Name", i))
		if name == "" {
			break
		}

		result[name] = MessageAttributeValue{
			DataType:    form.Get(fmt.Sprintf("MessageAttribute.%d.Value.DataType", i)),
			StringValue: form.Get(fmt.Sprintf("MessageAttribute.%d.Value.StringValue", i)),
		}
	}

	return result
}

// parseSendBatchEntries parses SendMessageBatchRequestEntry.N.* parameters.
func parseSendBatchEntries(form url.Values) []SendMessageBatchEntry {
	var entries []SendMessageBatchEntry

	for i := 1; i <= maxBatchSize; i++ {
		id := form.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.Id", i))
		if id == "" {
			break
		}

		delay, _ := strconv.Atoi(form.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.DelaySeconds", i)))

		entries = append(entries, SendMessageBatchEntry{
			ID:                     id,
			MessageBody:            form.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageBody", i)),
			MessageGroupID:         form.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageGroupId", i)),
			MessageDeduplicationID: form.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageDeduplicationId", i)),
			DelaySeconds:           delay,
		})
	}

	return entries
}

// parseDeleteBatchEntries parses DeleteMessageBatchRequestEntry.N.* parameters.
func parseDeleteBatchEntries(form url.Values) []DeleteMessageBatchEntry {
	var entries []DeleteMessageBatchEntry

	for i := 1; i <= maxBatchSize; i++ {
		id := form.Get(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.Id", i))
		if id == "" {
			break
		}

		entries = append(entries, DeleteMessageBatchEntry{
			ID:            id,
			ReceiptHandle: form.Get(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", i)),
		})
	}

	return entries
}
