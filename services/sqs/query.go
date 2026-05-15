package sqs

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// isQueryProtocol reports whether the request uses the SQS Query (form-encoded) protocol.
func isQueryProtocol(r *http.Request) bool {
	return r.Method == http.MethodPost &&
		strings.Contains(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded")
}

// isKnownSQSAction reports whether action is a recognised SQS operation name.
func isKnownSQSAction(action string) bool {
	switch action {
	case "CreateQueue", "DeleteQueue", "ListQueues", "GetQueueUrl",
		"GetQueueAttributes", "SetQueueAttributes",
		"SendMessage", "ReceiveMessage", "DeleteMessage",
		"ChangeMessageVisibility", "SendMessageBatch", "DeleteMessageBatch",
		"ChangeMessageVisibilityBatch", "PurgeQueue",
		"TagQueue", "UntagQueue", "ListQueueTags",
		"ListDeadLetterSourceQueues", "AddPermission", "RemovePermission",
		"StartMessageMoveTask", "CancelMessageMoveTask", "ListMessageMoveTasks":
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
		return writeQueryError(c, "InvalidAction", "The action "+action+" is not valid for this endpoint.", http.StatusBadRequest)
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
		RequestID: "gopherstack",
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
func buildQueryError(err error) *queryError {
	errType, message, status := errorDetails(err)

	resp := XMLErrorResponse{
		Xmlns:     sqsNamespace,
		RequestID: "gopherstack",
		Error: XMLError{
			Type:    "Sender",
			Code:    errType,
			Message: message,
		},
	}

	b, _ := xml.Marshal(resp)
	xmlBytes := append([]byte(xml.Header), b...)

	return &queryError{xml: xmlBytes, status: status}
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
// response bytes, HTTP status, and an optional query error.
func (h *Handler) dispatchQueryAction(
	action string,
	vals url.Values,
	r *http.Request,
	region string,
) ([]byte, int, *queryError) {
	switch action {
	case "CreateQueue":
		return h.queryCreateQueue(vals, r, region)
	case "DeleteQueue":
		return h.queryDeleteQueue(vals, region)
	case "ListQueues":
		return h.queryListQueues(vals, region)
	case "GetQueueUrl":
		return h.queryGetQueueURL(vals, region)
	case "GetQueueAttributes":
		return h.queryGetQueueAttributes(vals, region)
	case "SetQueueAttributes":
		return h.querySetQueueAttributes(vals, region)
	case "SendMessage":
		return h.querySendMessage(vals, r, region)
	case "ReceiveMessage":
		return h.queryReceiveMessage(vals, region)
	case "DeleteMessage":
		return h.queryDeleteMessage(vals, region)
	case "ChangeMessageVisibility":
		return h.queryChangeMessageVisibility(vals, region)
	case "SendMessageBatch":
		return h.querySendMessageBatch(vals, region)
	case "DeleteMessageBatch":
		return h.queryDeleteMessageBatch(vals, region)
	case "ChangeMessageVisibilityBatch":
		return h.queryChangeMessageVisibilityBatch(vals, region)
	case "PurgeQueue":
		return h.queryPurgeQueue(vals, region)
	case "AddPermission":
		return h.queryAddPermission(vals, region)
	case "RemovePermission":
		return h.queryRemovePermission(vals, region)
	default:
		qErr := buildQueryError(ErrUnknownAction)

		return nil, 0, qErr
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
		ResponseMetadata:  XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata:  XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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

		xmlMsgs = append(xmlMsgs, XMLMessage{
			MessageID:     msg.MessageID,
			ReceiptHandle: msg.ReceiptHandle,
			MD5OfBody:     msg.MD5OfBody,
			Body:          msg.Body,
			Attributes:    xmlAttrs,
		})
	}

	resp := ReceiveMessageResponse{
		Xmlns: sqsNamespace,
		ReceiveMessageResult: ReceiveMessageResult{
			Messages: xmlMsgs,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		failed = append(failed, XMLSendMessageBatchFailedEntry{
			ID:          f.ID,
			Code:        f.Code,
			Message:     f.Message,
			SenderFault: f.SenderFault,
		})
	}

	resp := SendMessageBatchResponse{
		Xmlns: sqsNamespace,
		SendMessageBatchResult: XMLSendMessageBatchResult{
			Successful: successful,
			Failed:     failed,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		successful = append(successful, XMLDeleteMessageBatchResultEntry{ID: s.ID})
	}

	failed := make([]XMLDeleteMessageBatchFailedEntry, 0, len(out.Failed))
	for _, f := range out.Failed {
		failed = append(failed, XMLDeleteMessageBatchFailedEntry{
			ID:          f.ID,
			Code:        f.Code,
			Message:     f.Message,
			SenderFault: f.SenderFault,
		})
	}

	resp := DeleteMessageBatchResponse{
		Xmlns: sqsNamespace,
		DeleteMessageBatchResult: XMLDeleteMessageBatchResult{
			Successful: successful,
			Failed:     failed,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
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
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

// tagQueueQueryInput wraps a parsed Tag map for TagQueue via Query protocol.
type tagQueueQueryInput struct {
	queueURL string
	tags     map[string]string
}

// parseQueryTagQueueInput extracts the QueueUrl and Tag.N.Key/Value pairs from
// a TagQueue Query request.
func parseQueryTagQueueInput(vals url.Values) tagQueueQueryInput {
	return tagQueueQueryInput{
		queueURL: vals.Get("QueueUrl"),
		tags:     parseQueryTagMap(vals),
	}
}

// queryTagQueue handles the TagQueue action in Query protocol.
func (h *Handler) queryTagQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	in := parseQueryTagQueueInput(vals)

	if err := h.Backend.TagQueue(&TagQueueInput{
		QueueURL: in.queueURL,
		Region:   region,
		Tags:     tags.FromMap("", in.tags),
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := TagQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: "gopherstack"},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}
