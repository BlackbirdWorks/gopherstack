package sqs

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

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

type jsonSendMessageResp struct {
	MessageID                    string `json:"MessageId"`
	MD5OfMessageBody             string `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes       string `json:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes string `json:"MD5OfMessageSystemAttributes,omitempty"`
	SequenceNumber               string `json:"SequenceNumber,omitempty"`
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

type jsonBatchSuccess struct {
	ID                           string `json:"Id"`
	MessageID                    string `json:"MessageId,omitempty"`
	MD5OfMessageBody             string `json:"MD5OfMessageBody,omitempty"`
	MD5OfMessageAttributes       string `json:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes string `json:"MD5OfMessageSystemAttributes,omitempty"`
	SequenceNumber               string `json:"SequenceNumber,omitempty"`
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
		MessageID:                    out.MessageID,
		MD5OfMessageBody:             out.MD5OfBody,
		MD5OfMessageAttributes:       out.MD5OfMessageAttributes,
		MD5OfMessageSystemAttributes: out.MD5OfMessageSystemAttributes,
		SequenceNumber:               out.SequenceNumber,
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

	vt := NoVisibilityTimeout
	if req.VisibilityTimeout != nil {
		v := *req.VisibilityTimeout
		if v < 0 || v > maxVisibilityTimeoutSeconds {
			return nil, ErrInvalidVisibilityTimeout
		}
		vt = v
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

		// AWS computes MD5OfMessageAttributes over only the attributes actually
		// returned to the consumer. When the caller requests a subset via
		// MessageAttributeNames, the digest must cover that subset so SDK-side
		// checksum verification passes (it would otherwise fail against the
		// send-time digest computed over the full attribute set).
		returnedAttrs := filterMsgAttrs(msg.MessageAttributes, req.MessageAttributeNames)

		// When the full attribute set is returned (filterMsgAttrs returns all
		// attrs), reuse the MD5 that was computed at send time to avoid the
		// O(k log k) sort on every receive for attribute-heavy messages.
		// If the returned count is smaller, the subset must be re-hashed.
		var md5Attrs string
		switch {
		case len(returnedAttrs) == 0:
			// no attributes requested or message has none
		case len(returnedAttrs) == len(msg.MessageAttributes):
			md5Attrs = msg.MD5OfMessageAttributes
		default:
			md5Attrs = computeMD5OfSubset(msg, returnedAttrs)
		}

		msgs = append(msgs, jsonReceivedMessage{
			MessageID:              msg.MessageID,
			ReceiptHandle:          msg.ReceiptHandle,
			MD5OfBody:              msg.MD5OfBody,
			MD5OfMessageAttributes: md5Attrs,
			Body:                   msg.Body,
			Attributes:             filterSystemAttrs(attrs, effectiveAttrNames),
			MessageAttributes:      toJSONMsgAttrs(returnedAttrs),
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
			ID:                           s.ID,
			MessageID:                    s.MessageID,
			MD5OfMessageBody:             s.MD5OfBody,
			MD5OfMessageAttributes:       s.MD5OfMessageAttributes,
			MD5OfMessageSystemAttributes: s.MD5OfMessageSystemAttributes,
			SequenceNumber:               s.SequenceNumber,
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

// filterMsgAttrs returns the subset of message attributes the consumer asked
// for via the ReceiveMessage MessageAttributeNames parameter. AWS supports
// exact names, the "All"/".*" wildcards, and "<prefix>.*" prefix wildcards.
// The result is the internal MessageAttributeValue representation so callers
// can recompute MD5OfMessageAttributes over exactly the returned subset.
func filterMsgAttrs(
	attrs map[string]MessageAttributeValue, requested []string,
) map[string]MessageAttributeValue {
	if len(attrs) == 0 || len(requested) == 0 {
		return nil
	}

	// AWS SDKs may send either "All" or ".*" to request all message attributes.
	// Both are treated as wildcards that return every attribute, matching the
	// behaviour of the real SQS service.
	if containsStr(requested, attrAll) || containsStr(requested, ".*") {
		return attrs
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

	result := make(map[string]MessageAttributeValue)
	for name, value := range attrs {
		if _, ok := exact[name]; ok {
			result[name] = value

			continue
		}

		for _, prefix := range prefixes {
			if strings.HasPrefix(name, prefix) {
				result[name] = value

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
