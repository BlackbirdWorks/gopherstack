package sqs

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
)

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
	vt := NoVisibilityTimeout

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
