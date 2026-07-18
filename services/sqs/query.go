package sqs

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// queryRequestID is the fixed request ID returned in Query protocol responses.
const queryRequestID = "gopherstack"

// isQueryProtocol reports whether the request uses the SQS Query (form-encoded) protocol.
func isQueryProtocol(r *http.Request) bool {
	return r.Method == http.MethodPost &&
		strings.Contains(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded")
}

// isKnownSQSAction reports whether action is a recognised SQS operation name.
func isKnownSQSAction(action string) bool {
	switch action {
	case opCreateQueue, opDeleteQueue, opListQueues, opGetQueueURL,
		opGetQueueAttributes, opSetQueueAttributes,
		opSendMessage, opReceiveMessage, opDeleteMessage,
		opChangeMessageVisibility, opSendMessageBatch, opDeleteMessageBatch,
		opChangeMessageVisibilityBatch, opPurgeQueue,
		opTagQueue, opUntagQueue, opListQueueTags,
		opListDeadLetterSourceQueues, opAddPermission, opRemovePermission,
		opStartMessageMoveTask, opCancelMessageMoveTask, opListMessageMoveTasks:
		return true
	}

	return false
}

// handleQueryProtocol handles an SQS Query (form-encoded) protocol request and
// writes an XML response.
func (h *Handler) handleQueryProtocol(c *echo.Context) error {
	r := c.Request()

	body, err := httputils.ReadBody(r)
	if err != nil {
		return writeQueryError(c, "InternalError", "Failed to read request body.", http.StatusInternalServerError)
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return writeQueryError(c, "InvalidAction", "Failed to parse request.", http.StatusBadRequest)
	}

	action := vals.Get("Action")
	if !isKnownSQSAction(action) {
		return writeQueryError(
			c, "InvalidAction",
			"The action "+action+" is not valid for this endpoint.",
			http.StatusBadRequest,
		)
	}

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	xmlResp, httpStatus, qErr := h.dispatchQueryAction(action, vals, r, region)
	if qErr != nil {
		return c.XMLBlob(qErr.status, qErr.xml)
	}

	return c.XMLBlob(httpStatus, xmlResp)
}

type queryError struct {
	xml    []byte
	status int
}

func writeQueryError(c *echo.Context, code, message string, status int) error {
	resp := XMLErrorResponse{
		Xmlns:     sqsNamespace,
		RequestID: queryRequestID,
		Error: XMLError{
			Type:    "Sender",
			Code:    code,
			Message: message,
		},
	}

	b, _ := xml.Marshal(resp)

	// c.XMLBlob already writes the XML declaration ("<?xml version=... ?>")
	// before the blob; prepending xml.Header here as well produced a
	// malformed document with two XML prologs (see marshalXML for the full
	// explanation of this bug class).
	return c.XMLBlob(status, b)
}

// buildQueryError builds a queryError from a backend error.
// The Query (XML) protocol uses legacy error codes (e.g. AWS.SimpleQueueService.NonExistentQueue)
// rather than the JSON-API codes (e.g. com.amazonaws.sqs#QueueDoesNotExist).
func buildQueryError(err error) *queryError {
	code, message, status := queryErrorDetails(err)

	resp := XMLErrorResponse{
		Xmlns:     sqsNamespace,
		RequestID: queryRequestID,
		Error: XMLError{
			Type:    "Sender",
			Code:    code,
			Message: message,
		},
	}

	// Not xml.Header-prefixed here: the caller (handleQueryProtocol) always
	// delivers queryError.xml via c.XMLBlob, which itself writes the XML
	// declaration. See marshalXML's doc comment for why double-prepending
	// produced a malformed (two-prolog) XML document.
	b, _ := xml.Marshal(resp)

	return &queryError{xml: b, status: status}
}

// queryErrorDetails returns the Query-protocol error code, message, and HTTP status for err.
// The Query API uses legacy-style codes unlike the JSON API.
func queryErrorDetails(err error) (string, string, int) {
	if errors.Is(err, ErrQueueNotFound) {
		return "AWS.SimpleQueueService.NonExistentQueue",
			"The specified queue does not exist for this wsdl version.",
			http.StatusBadRequest
	}

	// Fall through to shared JSON-API details for all other errors.
	return errorDetails(err)
}

// marshalXML marshals v to XML bytes. It intentionally does NOT prepend the
// XML declaration ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"): every
// caller in this file hands the result to echo's c.XMLBlob, which already
// writes that declaration itself before the blob (see
// echo/v5.Context.XMLBlob). Prepending it here too — as this function
// previously did — produced a response body with TWO XML prologs, which is
// not well-formed XML (a second "<?xml ...?>" processing instruction is only
// legal at byte offset 0 of a document) and could be rejected outright by
// strict XML parsers in non-Go AWS SDKs even though Go's own encoding/xml
// tolerates it.
func marshalXML(v any) ([]byte, error) {
	return xml.Marshal(v)
}

// queueURLEndpoint returns the endpoint to use for queue URL construction.
func (h *Handler) queueURLEndpoint(r *http.Request) string {
	if h.Endpoint != "" {
		return h.Endpoint
	}

	return r.Host
}

// parseQueryAttrMap parses numbered Attribute.N.Name / Attribute.N.Value pairs.
func parseQueryAttrMap(vals url.Values) map[string]string {
	attrs := make(map[string]string)

	for i := 1; i <= maxParseIterations; i++ {
		name := vals.Get(fmt.Sprintf("Attribute.%d.Name", i))
		if name == "" {
			break
		}

		attrs[name] = vals.Get(fmt.Sprintf("Attribute.%d.Value", i))
	}

	return attrs
}

// parseQueryTagMap parses numbered Tag.N.Key / Tag.N.Value pairs.
func parseQueryTagMap(vals url.Values) map[string]string {
	tagMap := make(map[string]string)

	for i := 1; i <= maxParseIterations; i++ {
		key := vals.Get(fmt.Sprintf("Tag.%d.Key", i))
		if key == "" {
			break
		}

		tagMap[key] = vals.Get(fmt.Sprintf("Tag.%d.Value", i))
	}

	return tagMap
}

// parseQueryList parses a numbered list like AttributeName.1, AttributeName.2, ...
func parseQueryList(vals url.Values, prefix string) []string {
	var result []string

	for i := 1; i <= maxParseIterations; i++ {
		v := vals.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			break
		}

		result = append(result, v)
	}

	return result
}

// queryActionFn is the dispatch signature for a single Query protocol action
// handler. Every action handler is adapted to this common shape (even those
// that ignore r) so all actions can live in one flat lookup table instead of
// a multi-branch switch.
type queryActionFn func(h *Handler, vals url.Values, r *http.Request, region string) ([]byte, int, *queryError)

// queryActionTable maps every supported Query protocol action name to its handler.
func queryActionTable() map[string]queryActionFn {
	return map[string]queryActionFn{
		opCreateQueue: func(h *Handler, vals url.Values, r *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryCreateQueue(vals, r, region)
		},
		opDeleteQueue: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryDeleteQueue(vals, region)
		},
		opListQueues: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryListQueues(vals, region)
		},
		opGetQueueURL: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryGetQueueURL(vals, region)
		},
		opGetQueueAttributes: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryGetQueueAttributes(vals, region)
		},
		opSetQueueAttributes: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.querySetQueueAttributes(vals, region)
		},
		opSendMessage: func(h *Handler, vals url.Values, r *http.Request, region string) ([]byte, int, *queryError) {
			return h.querySendMessage(vals, r, region)
		},
		opReceiveMessage: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryReceiveMessage(vals, region)
		},
		opDeleteMessage: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryDeleteMessage(vals, region)
		},
		opChangeMessageVisibility: func(
			h *Handler, vals url.Values, _ *http.Request, region string,
		) ([]byte, int, *queryError) {
			return h.queryChangeMessageVisibility(vals, region)
		},
		opPurgeQueue: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryPurgeQueue(vals, region)
		},
		opTagQueue: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryTagQueue(vals, region)
		},
		opUntagQueue: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryUntagQueue(vals, region)
		},
		opListQueueTags: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryListQueueTags(vals, region)
		},
		opSendMessageBatch: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.querySendMessageBatch(vals, region)
		},
		opDeleteMessageBatch: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryDeleteMessageBatch(vals, region)
		},
		opChangeMessageVisibilityBatch: func(
			h *Handler, vals url.Values, _ *http.Request, region string,
		) ([]byte, int, *queryError) {
			return h.queryChangeMessageVisibilityBatch(vals, region)
		},
		opAddPermission: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryAddPermission(vals, region)
		},
		opRemovePermission: func(h *Handler, vals url.Values, _ *http.Request, region string) ([]byte, int, *queryError) {
			return h.queryRemovePermission(vals, region)
		},
		opListDeadLetterSourceQueues: func(
			h *Handler, vals url.Values, _ *http.Request, region string,
		) ([]byte, int, *queryError) {
			return h.queryListDeadLetterSourceQueues(vals, region)
		},
		opStartMessageMoveTask: func(h *Handler, vals url.Values, _ *http.Request, _ string) ([]byte, int, *queryError) {
			return h.queryStartMessageMoveTask(vals)
		},
		opCancelMessageMoveTask: func(h *Handler, vals url.Values, _ *http.Request, _ string) ([]byte, int, *queryError) {
			return h.queryCancelMessageMoveTask(vals)
		},
		opListMessageMoveTasks: func(h *Handler, vals url.Values, _ *http.Request, _ string) ([]byte, int, *queryError) {
			return h.queryListMessageMoveTasks(vals)
		},
	}
}

// dispatchQueryAction dispatches the Query protocol action and returns the XML
// response bytes, HTTP status, and an optional query error.
func (h *Handler) dispatchQueryAction(
	action string,
	vals url.Values,
	r *http.Request,
	region string,
) ([]byte, int, *queryError) {
	fn, ok := queryActionTable()[action]
	if !ok {
		return nil, 0, buildQueryError(ErrUnknownAction)
	}

	return fn(h, vals, r, region)
}

// atoiInt32 parses a decimal string into an int32, returning 0 for empty or
// out-of-range input. Using ParseInt with bitSize 32 keeps the conversion within
// int32 bounds (no architecture-dependent overflow).
func atoiInt32(s string) int32 {
	if s == "" {
		return 0
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}
