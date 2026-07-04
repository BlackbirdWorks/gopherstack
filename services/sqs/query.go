package sqs

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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

	return c.XMLBlob(status, append([]byte(xml.Header), b...))
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

	b, _ := xml.Marshal(resp)
	xmlBytes := append([]byte(xml.Header), b...)

	return &queryError{xml: xmlBytes, status: status}
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

// marshalXML marshals v to XML bytes with the XML header prepended.
func marshalXML(v any) ([]byte, error) {
	b, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), b...), nil
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

// parseQueryMsgAttr parses MessageAttribute.N.Name / MessageAttribute.N.Value.DataType etc.
// Binary values are base64-encoded in the Query protocol.
func parseQueryMsgAttr(vals url.Values) map[string]MessageAttributeValue {
	attrs := make(map[string]MessageAttributeValue)

	for i := 1; i <= maxParseIterations; i++ {
		name := vals.Get(fmt.Sprintf("MessageAttribute.%d.Name", i))
		if name == "" {
			break
		}

		attr := MessageAttributeValue{
			DataType:    vals.Get(fmt.Sprintf("MessageAttribute.%d.Value.DataType", i)),
			StringValue: vals.Get(fmt.Sprintf("MessageAttribute.%d.Value.StringValue", i)),
		}

		if b64 := vals.Get(fmt.Sprintf("MessageAttribute.%d.Value.BinaryValue", i)); b64 != "" {
			decoded, decErr := decodeMsgAttrBinary(b64)
			if decErr == nil {
				attr.BinaryValue = decoded
			}
		}

		attrs[name] = attr
	}

	if len(attrs) == 0 {
		return nil
	}

	return attrs
}

// decodeMsgAttrBinary base64-decodes a binary message attribute value as sent
// in the SQS Query protocol.
func decodeMsgAttrBinary(encoded string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(encoded)
}

// parseQueryBatchMsgAttrs parses per-entry message attributes for SendMessageBatch.
// AWS Query protocol encodes them as:
//
//	SendMessageBatchRequestEntry.{entryIdx}.MessageAttribute.{j}.Name
//	SendMessageBatchRequestEntry.{entryIdx}.MessageAttribute.{j}.Value.DataType
//	SendMessageBatchRequestEntry.{entryIdx}.MessageAttribute.{j}.Value.StringValue
//	SendMessageBatchRequestEntry.{entryIdx}.MessageAttribute.{j}.Value.BinaryValue
func parseQueryBatchMsgAttrs(vals url.Values, entryIdx int) map[string]MessageAttributeValue {
	attrs := make(map[string]MessageAttributeValue)
	prefix := fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageAttribute", entryIdx)

	for j := 1; j <= maxParseIterations; j++ {
		name := vals.Get(fmt.Sprintf("%s.%d.Name", prefix, j))
		if name == "" {
			break
		}

		attr := MessageAttributeValue{
			DataType:    vals.Get(fmt.Sprintf("%s.%d.Value.DataType", prefix, j)),
			StringValue: vals.Get(fmt.Sprintf("%s.%d.Value.StringValue", prefix, j)),
		}

		if b64 := vals.Get(fmt.Sprintf("%s.%d.Value.BinaryValue", prefix, j)); b64 != "" {
			decoded, decErr := decodeMsgAttrBinary(b64)
			if decErr == nil {
				attr.BinaryValue = decoded
			}
		}

		attrs[name] = attr
	}

	if len(attrs) == 0 {
		return nil
	}

	return attrs
}

// parseQuerySendBatchEntries parses SendMessageBatch entries from Query params.
func parseQuerySendBatchEntries(vals url.Values) []SendMessageBatchEntry {
	var entries []SendMessageBatchEntry

	for i := 1; i <= maxParseIterations; i++ {
		id := vals.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.Id", i))
		if id == "" {
			break
		}

		delay, _ := strconv.Atoi(vals.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.DelaySeconds", i)))
		entries = append(entries, SendMessageBatchEntry{
			ID:                     id,
			MessageBody:            vals.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageBody", i)),
			DelaySeconds:           delay,
			MessageGroupID:         vals.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageGroupId", i)),
			MessageDeduplicationID: vals.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageDeduplicationId", i)),
			MessageAttributes:      parseQueryBatchMsgAttrs(vals, i),
		})
	}

	return entries
}

// parseQueryDeleteBatchEntries parses DeleteMessageBatch entries from Query params.
func parseQueryDeleteBatchEntries(vals url.Values) []DeleteMessageBatchEntry {
	var entries []DeleteMessageBatchEntry

	for i := 1; i <= maxParseIterations; i++ {
		id := vals.Get(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.Id", i))
		if id == "" {
			break
		}

		entries = append(entries, DeleteMessageBatchEntry{
			ID:            id,
			ReceiptHandle: vals.Get(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", i)),
		})
	}

	return entries
}

// parseQueryChangeBatchEntries parses ChangeMessageVisibilityBatch entries from Query params.
func parseQueryChangeBatchEntries(vals url.Values) []ChangeMessageVisibilityBatchRequestEntry {
	var entries []ChangeMessageVisibilityBatchRequestEntry

	for i := 1; i <= maxParseIterations; i++ {
		id := vals.Get(fmt.Sprintf("ChangeMessageVisibilityBatchRequestEntry.%d.Id", i))
		if id == "" {
			break
		}

		vt, _ := strconv.Atoi(vals.Get(fmt.Sprintf("ChangeMessageVisibilityBatchRequestEntry.%d.VisibilityTimeout", i)))
		entries = append(entries, ChangeMessageVisibilityBatchRequestEntry{
			ID:                id,
			ReceiptHandle:     vals.Get(fmt.Sprintf("ChangeMessageVisibilityBatchRequestEntry.%d.ReceiptHandle", i)),
			VisibilityTimeout: vt,
		})
	}

	return entries
}

// dispatchQueryAction dispatches the Query protocol action and returns the XML
// response bytes, HTTP status, and an optional query error. Queue and message
// management actions are handled here; batch and permission actions are
// delegated to dispatchQueryBatchAction.
//
//nolint:cyclop // action dispatcher; complexity is inherent to query routing
func (h *Handler) dispatchQueryAction(
	action string,
	vals url.Values,
	r *http.Request,
	region string,
) ([]byte, int, *queryError) {
	switch action {
	case opCreateQueue:
		return h.queryCreateQueue(vals, r, region)
	case opDeleteQueue:
		return h.queryDeleteQueue(vals, region)
	case opListQueues:
		return h.queryListQueues(vals, region)
	case opGetQueueURL:
		return h.queryGetQueueURL(vals, region)
	case opGetQueueAttributes:
		return h.queryGetQueueAttributes(vals, region)
	case opSetQueueAttributes:
		return h.querySetQueueAttributes(vals, region)
	case opSendMessage:
		return h.querySendMessage(vals, r, region)
	case opReceiveMessage:
		return h.queryReceiveMessage(vals, region)
	case opDeleteMessage:
		return h.queryDeleteMessage(vals, region)
	case opChangeMessageVisibility:
		return h.queryChangeMessageVisibility(vals, region)
	case opPurgeQueue:
		return h.queryPurgeQueue(vals, region)
	case opTagQueue:
		return h.queryTagQueue(vals, region)
	case opUntagQueue:
		return h.queryUntagQueue(vals, region)
	case opListQueueTags:
		return h.queryListQueueTags(vals, region)
	default:
		return h.dispatchQueryBatchAction(action, vals, region)
	}
}

// dispatchQueryBatchAction handles batch and permission Query protocol actions.
func (h *Handler) dispatchQueryBatchAction(
	action string,
	vals url.Values,
	region string,
) ([]byte, int, *queryError) {
	switch action {
	case opSendMessageBatch:
		return h.querySendMessageBatch(vals, region)
	case opDeleteMessageBatch:
		return h.queryDeleteMessageBatch(vals, region)
	case opChangeMessageVisibilityBatch:
		return h.queryChangeMessageVisibilityBatch(vals, region)
	case opAddPermission:
		return h.queryAddPermission(vals, region)
	case opRemovePermission:
		return h.queryRemovePermission(vals, region)
	case opListDeadLetterSourceQueues:
		return h.queryListDeadLetterSourceQueues(vals, region)
	case opStartMessageMoveTask:
		return h.queryStartMessageMoveTask(vals)
	case opCancelMessageMoveTask:
		return h.queryCancelMessageMoveTask(vals)
	case opListMessageMoveTasks:
		return h.queryListMessageMoveTasks(vals)
	default:
		return nil, 0, buildQueryError(ErrUnknownAction)
	}
}

func (h *Handler) queryCreateQueue(vals url.Values, r *http.Request, region string) ([]byte, int, *queryError) {
	attrs := parseQueryAttrMap(vals)
	tagMap := parseQueryTagMap(vals)

	var initialTags map[string]string
	if len(tagMap) > 0 {
		initialTags = tagMap
	}

	out, err := h.Backend.CreateQueue(&CreateQueueInput{
		QueueName:  vals.Get("QueueName"),
		Attributes: attrs,
		Tags:       initialTags,
		Endpoint:   h.queueURLEndpoint(r),
		Region:     region,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := CreateQueueResponse{
		Xmlns:             sqsNamespace,
		CreateQueueResult: CreateQueueResult{QueueURL: out.QueueURL},
		ResponseMetadata:  XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryDeleteQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	if err := h.Backend.DeleteQueue(&DeleteQueueInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := DeleteQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryListQueues(vals url.Values, region string) ([]byte, int, *queryError) {
	maxResults, _ := strconv.Atoi(vals.Get("MaxResults"))

	out, err := h.Backend.ListQueues(&ListQueuesInput{
		QueueNamePrefix: vals.Get("QueueNamePrefix"),
		NextToken:       vals.Get("NextToken"),
		Region:          region,
		MaxResults:      maxResults,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := ListQueuesResponse{
		Xmlns: sqsNamespace,
		ListQueuesResult: ListQueuesResult{
			QueueURLs: out.QueueURLs,
			NextToken: out.NextToken,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryGetQueueURL(vals url.Values, region string) ([]byte, int, *queryError) {
	out, err := h.Backend.GetQueueURL(&GetQueueURLInput{
		QueueName: vals.Get("QueueName"),
		Region:    region,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := GetQueueURLResponse{
		Xmlns:             sqsNamespace,
		GetQueueURLResult: GetQueueURLResult{QueueURL: out.QueueURL},
		ResponseMetadata:  XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryGetQueueAttributes(vals url.Values, region string) ([]byte, int, *queryError) {
	attrNames := parseQueryList(vals, "AttributeName")

	out, err := h.Backend.GetQueueAttributes(&GetQueueAttributesInput{
		QueueURL:       vals.Get("QueueUrl"),
		Region:         region,
		AttributeNames: attrNames,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	xmlAttrs := make([]XMLAttribute, 0, len(out.Attributes))
	for k, v := range out.Attributes {
		xmlAttrs = append(xmlAttrs, XMLAttribute{Name: k, Value: v})
	}

	resp := GetQueueAttributesResponse{
		Xmlns: sqsNamespace,
		GetQueueAttributesResult: GetQueueAttributesResult{
			Attributes: xmlAttrs,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) querySetQueueAttributes(vals url.Values, region string) ([]byte, int, *queryError) {
	attrs := parseQueryAttrMap(vals)

	if err := h.Backend.SetQueueAttributes(&SetQueueAttributesInput{
		QueueURL:   vals.Get("QueueUrl"),
		Region:     region,
		Attributes: attrs,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := SetQueueAttributesResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) querySendMessage(vals url.Values, r *http.Request, region string) ([]byte, int, *queryError) {
	delay, _ := strconv.Atoi(vals.Get("DelaySeconds"))
	msgAttrs := parseQueryMsgAttr(vals)

	out, err := h.Backend.SendMessage(&SendMessageInput{
		QueueURL:               vals.Get("QueueUrl"),
		Region:                 region,
		MessageBody:            vals.Get("MessageBody"),
		DelaySeconds:           delay,
		MessageGroupID:         vals.Get("MessageGroupId"),
		MessageDeduplicationID: vals.Get("MessageDeduplicationId"),
		MessageAttributes:      msgAttrs,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	_ = r // region already extracted above

	resp := SendMessageResponse{
		Xmlns: sqsNamespace,
		SendMessageResult: SendMessageResult{
			MD5OfMessageBody:       out.MD5OfBody,
			MD5OfMessageAttributes: out.MD5OfMessageAttributes,
			MessageID:              out.MessageID,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryReceiveMessage(vals url.Values, region string) ([]byte, int, *queryError) {
	maxMsgs, _ := strconv.Atoi(vals.Get("MaxNumberOfMessages"))
	waitSecs, _ := strconv.Atoi(vals.Get("WaitTimeSeconds"))
	vt := noVisibilitySet

	if vtStr := vals.Get("VisibilityTimeout"); vtStr != "" {
		if n, err := strconv.Atoi(vtStr); err == nil {
			vt = n
		}
	}

	attrNames := parseQueryList(vals, "AttributeName")
	msgAttrNames := parseQueryList(vals, "MessageAttributeName")

	out, err := h.Backend.ReceiveMessage(&ReceiveMessageInput{
		QueueURL:                vals.Get("QueueUrl"),
		Region:                  region,
		MaxNumberOfMessages:     maxMsgs,
		VisibilityTimeout:       vt,
		WaitTimeSeconds:         waitSecs,
		AttributeNames:          attrNames,
		MessageAttributeNames:   msgAttrNames,
		ReceiveRequestAttemptID: vals.Get("ReceiveRequestAttemptID"),
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	xmlMsgs := make([]XMLMessage, 0, len(out.Messages))
	for _, msg := range out.Messages {
		xmlAttrs := make([]XMLAttribute, 0, len(msg.Attributes))
		for k, v := range filterSystemAttrs(msg.Attributes, attrNames) {
			xmlAttrs = append(xmlAttrs, XMLAttribute{Name: k, Value: v})
		}

		sort.Slice(xmlAttrs, func(i, j int) bool { return xmlAttrs[i].Name < xmlAttrs[j].Name })

		// Serialize user-defined message attributes into XML, filtering by
		// the consumer's MessageAttributeNames request parameter.
		filtered := filterMsgAttrs(msg.MessageAttributes, msgAttrNames)
		xmlMsgAttrs := make([]XMLMessageAttribute, 0, len(filtered))
		for name, val := range filtered {
			// Binary values must be base64-encoded in the XML wire format because
			// encoding/xml does not automatically encode []byte (unlike encoding/json).
			binaryVal := ""
			if len(val.BinaryValue) > 0 {
				binaryVal = base64.StdEncoding.EncodeToString(val.BinaryValue)
			}

			xmlMsgAttrs = append(xmlMsgAttrs, XMLMessageAttribute{
				Name: name,
				Value: XMLMessageAttributeValue{
					DataType:    val.DataType,
					StringValue: val.StringValue,
					BinaryValue: binaryVal,
				},
			})
		}

		sort.Slice(xmlMsgAttrs, func(i, j int) bool { return xmlMsgAttrs[i].Name < xmlMsgAttrs[j].Name })

		// Compute MD5 over the returned attribute subset, matching JSON protocol behaviour.
		var md5MsgAttrs string
		switch {
		case len(filtered) == 0:
			// no attributes requested or none present
		case len(filtered) == len(msg.MessageAttributes):
			md5MsgAttrs = msg.MD5OfMessageAttributes
		default:
			md5MsgAttrs = computeMD5OfMessageAttributes(filtered)
		}

		xmlMsgs = append(xmlMsgs, XMLMessage{
			MessageID:              msg.MessageID,
			ReceiptHandle:          msg.ReceiptHandle,
			MD5OfBody:              msg.MD5OfBody,
			MD5OfMessageAttributes: md5MsgAttrs,
			Body:                   msg.Body,
			Attributes:             xmlAttrs,
			MessageAttributes:      xmlMsgAttrs,
		})
	}

	resp := ReceiveMessageResponse{
		Xmlns: sqsNamespace,
		ReceiveMessageResult: ReceiveMessageResult{
			Messages: xmlMsgs,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryDeleteMessage(vals url.Values, region string) ([]byte, int, *queryError) {
	if err := h.Backend.DeleteMessage(&DeleteMessageInput{
		QueueURL:      vals.Get("QueueUrl"),
		Region:        region,
		ReceiptHandle: vals.Get("ReceiptHandle"),
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := DeleteMessageResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryChangeMessageVisibility(vals url.Values, region string) ([]byte, int, *queryError) {
	vt, _ := strconv.Atoi(vals.Get("VisibilityTimeout"))

	if err := h.Backend.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
		QueueURL:          vals.Get("QueueUrl"),
		Region:            region,
		ReceiptHandle:     vals.Get("ReceiptHandle"),
		VisibilityTimeout: vt,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := ChangeMessageVisibilityResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) querySendMessageBatch(vals url.Values, region string) ([]byte, int, *queryError) {
	entries := parseQuerySendBatchEntries(vals)

	out, err := h.Backend.SendMessageBatch(&SendMessageBatchInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		Entries:  entries,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	successful := make([]XMLSendMessageBatchResultEntry, 0, len(out.Successful))
	for _, s := range out.Successful {
		successful = append(successful, XMLSendMessageBatchResultEntry{
			ID:                     s.ID,
			MessageID:              s.MessageID,
			MD5OfMessageBody:       s.MD5OfBody,
			MD5OfMessageAttributes: s.MD5OfMessageAttributes,
		})
	}

	failed := make([]XMLSendMessageBatchFailedEntry, 0, len(out.Failed))
	for _, f := range out.Failed {
		failed = append(failed, XMLSendMessageBatchFailedEntry(f))
	}

	resp := SendMessageBatchResponse{
		Xmlns: sqsNamespace,
		SendMessageBatchResult: XMLSendMessageBatchResult{
			Successful: successful,
			Failed:     failed,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryDeleteMessageBatch(vals url.Values, region string) ([]byte, int, *queryError) {
	entries := parseQueryDeleteBatchEntries(vals)

	out, err := h.Backend.DeleteMessageBatch(&DeleteMessageBatchInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		Entries:  entries,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	successful := make([]XMLDeleteMessageBatchResultEntry, 0, len(out.Successful))
	for _, s := range out.Successful {
		successful = append(successful, XMLDeleteMessageBatchResultEntry(s))
	}

	failed := make([]XMLDeleteMessageBatchFailedEntry, 0, len(out.Failed))
	for _, f := range out.Failed {
		failed = append(failed, XMLDeleteMessageBatchFailedEntry(f))
	}

	resp := DeleteMessageBatchResponse{
		Xmlns: sqsNamespace,
		DeleteMessageBatchResult: XMLDeleteMessageBatchResult{
			Successful: successful,
			Failed:     failed,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryChangeMessageVisibilityBatch(vals url.Values, region string) ([]byte, int, *queryError) {
	entries := parseQueryChangeBatchEntries(vals)

	out, err := h.Backend.ChangeMessageVisibilityBatch(&ChangeMessageVisibilityBatchInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		Entries:  entries,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := ChangeMessageVisibilityBatchResponse{
		Xmlns: sqsNamespace,
		Result: ChangeMessageVisibilityBatchResult{
			Successful: out.Successful,
			Failed:     out.Failed,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryPurgeQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	if err := h.Backend.PurgeQueue(&PurgeQueueInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := PurgeQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryAddPermission(vals url.Values, region string) ([]byte, int, *queryError) {
	actions := parseQueryList(vals, "ActionName")
	accountIDs := parseQueryList(vals, "AWSAccountId")

	if err := h.Backend.AddPermission(&AddPermissionInput{
		QueueURL:      vals.Get("QueueUrl"),
		Region:        region,
		Label:         vals.Get("Label"),
		Actions:       actions,
		AWSAccountIDs: accountIDs,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	type addPermResp struct {
		XMLName          xml.Name            `xml:"AddPermissionResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
	}

	resp := addPermResp{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

// parseQueryTagMembers parses Query-protocol Tags.member.N.Key / Tags.member.N.Value
// pairs (the encoding used by the SQS TagQueue Query API). Returns nil if no tags
// are present so callers can leave the backend Tags input nil.
func parseQueryTagMembers(vals url.Values) map[string]string {
	tagMap := make(map[string]string)

	for i := 1; i <= maxParseIterations; i++ {
		key := vals.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if key == "" {
			break
		}

		tagMap[key] = vals.Get(fmt.Sprintf("Tags.member.%d.Value", i))
	}

	if len(tagMap) == 0 {
		return nil
	}

	return tagMap
}

func (h *Handler) queryTagQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	tagMap := parseQueryTagMembers(vals)

	var tagInput *tags.Tags
	if len(tagMap) > 0 {
		tagInput = tags.FromMap("sqs.query.tagqueue", tagMap)
		defer tagInput.Close()
	}

	if err := h.Backend.TagQueue(&TagQueueInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		Tags:     tagInput,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := TagQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryUntagQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	tagKeys := parseQueryList(vals, "TagKeys.member")

	if err := h.Backend.UntagQueue(&UntagQueueInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		TagKeys:  tagKeys,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := UntagQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryListQueueTags(vals url.Values, region string) ([]byte, int, *queryError) {
	out, err := h.Backend.ListQueueTags(&ListQueueTagsInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	var entries []TagEntry
	if out.Tags != nil {
		out.Tags.Range(func(k, v string) bool {
			entries = append(entries, TagEntry{Key: k, Value: v})

			return true
		})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	resp := ListQueueTagsResponse{
		Xmlns:            sqsNamespace,
		Result:           ListQueueTagsResult{Tags: entries},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryListDeadLetterSourceQueues(vals url.Values, region string) ([]byte, int, *queryError) {
	maxResults, _ := strconv.Atoi(vals.Get("MaxResults"))

	out, err := h.Backend.ListDeadLetterSourceQueues(&ListDeadLetterSourceQueuesInput{
		QueueURL:   vals.Get("QueueUrl"),
		Region:     region,
		NextToken:  vals.Get("NextToken"),
		MaxResults: maxResults,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	type result struct {
		NextToken string   `xml:"NextToken,omitempty"`
		QueueURLs []string `xml:"queueUrls"`
	}

	type response struct {
		XMLName          xml.Name            `xml:"ListDeadLetterSourceQueuesResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
		Result           result              `xml:"ListDeadLetterSourceQueuesResult"`
	}

	resp := response{
		Xmlns: sqsNamespace,
		Result: result{
			QueueURLs: out.QueueURLs,
			NextToken: out.NextToken,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
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

func (h *Handler) queryStartMessageMoveTask(vals url.Values) ([]byte, int, *queryError) {
	maxPerSec := atoiInt32(vals.Get("MaxNumberOfMessagesPerSecond"))

	out, err := h.Backend.StartMessageMoveTask(&StartMessageMoveTaskInput{
		SourceArn:                    vals.Get("SourceArn"),
		DestinationArn:               vals.Get("DestinationArn"),
		MaxNumberOfMessagesPerSecond: maxPerSec,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	type result struct {
		TaskHandle string `xml:"TaskHandle"`
	}

	type response struct {
		XMLName          xml.Name            `xml:"StartMessageMoveTaskResponse"`
		Result           result              `xml:"StartMessageMoveTaskResult"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
	}

	resp := response{
		Xmlns:            sqsNamespace,
		Result:           result{TaskHandle: out.TaskHandle},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryCancelMessageMoveTask(vals url.Values) ([]byte, int, *queryError) {
	out, err := h.Backend.CancelMessageMoveTask(&CancelMessageMoveTaskInput{
		TaskHandle: vals.Get("TaskHandle"),
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	type result struct {
		ApproximateNumberOfMessagesMoved int64 `xml:"ApproximateNumberOfMessagesMoved"`
	}

	type response struct {
		XMLName          xml.Name            `xml:"CancelMessageMoveTaskResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
		Result           result              `xml:"CancelMessageMoveTaskResult"`
	}

	resp := response{
		Xmlns:            sqsNamespace,
		Result:           result{ApproximateNumberOfMessagesMoved: out.ApproximateNumberOfMessagesMoved},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryListMessageMoveTasks(vals url.Values) ([]byte, int, *queryError) {
	maxResults := atoiInt32(vals.Get("MaxResults"))

	out, err := h.Backend.ListMessageMoveTasks(&ListMessageMoveTasksInput{
		SourceArn:  vals.Get("SourceArn"),
		MaxResults: maxResults,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	type taskEntry struct {
		ApproximateNumberOfMessagesToMove *int64 `xml:"ApproximateNumberOfMessagesToMove,omitempty"`
		MaxNumberOfMessagesPerSecond      *int32 `xml:"MaxNumberOfMessagesPerSecond,omitempty"`
		FailureReason                     string `xml:"FailureReason,omitempty"`
		TaskHandle                        string `xml:"TaskHandle"`
		SourceArn                         string `xml:"SourceArn"`
		DestinationArn                    string `xml:"DestinationArn"`
		Status                            string `xml:"Status"`
		ApproximateNumberOfMessagesMoved  int64  `xml:"ApproximateNumberOfMessagesMoved"`
		StartedTimestamp                  int64  `xml:"StartedTimestamp"`
	}

	type result struct {
		Results []taskEntry `xml:"Results"`
	}

	type response struct {
		XMLName          xml.Name            `xml:"ListMessageMoveTasksResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
		Result           result              `xml:"ListMessageMoveTasksResult"`
	}

	entries := make([]taskEntry, 0, len(out.Results))
	for _, t := range out.Results {
		var failure string
		if t.FailureReason != nil {
			failure = *t.FailureReason
		}

		entries = append(entries, taskEntry{
			TaskHandle:                        t.TaskHandle,
			SourceArn:                         t.SourceArn,
			DestinationArn:                    t.DestinationArn,
			Status:                            string(t.Status),
			StartedTimestamp:                  t.StartedTimestamp,
			ApproximateNumberOfMessagesMoved:  t.ApproximateNumberOfMessagesMoved,
			ApproximateNumberOfMessagesToMove: t.ApproximateNumberOfMessagesToMove,
			MaxNumberOfMessagesPerSecond:      t.MaxNumberOfMessagesPerSecond,
			FailureReason:                     failure,
		})
	}

	resp := response{
		Xmlns:            sqsNamespace,
		Result:           result{Results: entries},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryRemovePermission(vals url.Values, region string) ([]byte, int, *queryError) {
	if err := h.Backend.RemovePermission(&RemovePermissionInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		Label:    vals.Get("Label"),
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	type removePermResp struct {
		XMLName          xml.Name            `xml:"RemovePermissionResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
	}

	resp := removePermResp{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}
