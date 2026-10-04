package sqs

import (
	"encoding/base64"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

var queueURLRegionRE = regexp.MustCompile(`sqs\.([a-z0-9-]+)\.`)

// PeekMessages returns copies of a queue's messages without touching visibility or receive counts.
func (b *InMemoryBackend) PeekMessages(region, queueName string, showInvisible, showDelayed bool) ([]*Message, error) {
	b.mu.RLock("PeekMessages")
	q, ok := b.lookupQueueByName(region, queueName)
	b.mu.RUnlock()

	if !ok {
		return nil, ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	now := b.now()
	out := make([]*Message, 0, len(q.messages)+len(q.inFlightMessages))

	for _, m := range q.messages {
		if now.Before(m.VisibleAt) && !showDelayed {
			continue
		}

		out = append(out, b.peekCopy(m))
	}

	for _, inf := range q.inFlightMessages {
		if now.Before(inf.VisibleAt) && !showInvisible {
			continue
		}

		out = append(out, b.peekCopy(inf.Msg))
	}

	return out, nil
}

func (b *InMemoryBackend) peekCopy(m *Message) *Message {
	c := *m
	c.Attributes = maps.Clone(m.Attributes)

	if c.Attributes == nil {
		c.Attributes = make(map[string]string)
	}

	c.Attributes[attrSentTimestamp] = strconv.FormatInt(m.SentTimestamp, 10)
	c.Attributes[attrApproxReceiveCount] = strconv.Itoa(m.ApproximateReceiveCount)

	if _, ok := c.Attributes[attrSenderID]; !ok {
		c.Attributes[attrSenderID] = b.accountID
	}

	return &c
}

type inspectJSONMessage struct {
	Attributes             map[string]string               `json:"Attributes,omitempty"`
	MessageAttributes      map[string]inspectJSONAttrValue `json:"MessageAttributes,omitempty"`
	MessageID              string                          `json:"MessageId"`
	ReceiptHandle          string                          `json:"ReceiptHandle,omitempty"`
	MD5OfBody              string                          `json:"MD5OfBody"`
	MD5OfMessageAttributes string                          `json:"MD5OfMessageAttributes,omitempty"`
	Body                   string                          `json:"Body"`
}

type inspectJSONAttrValue struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue,omitempty"`
	BinaryValue string `json:"BinaryValue,omitempty"`
}

// ServeInspectMessages serves LocalStack's /_aws/sqs/messages inspection endpoint.
func (h *Handler) ServeInspectMessages(c *echo.Context) error {
	r := c.Request()
	_ = r.ParseForm()

	region, name := c.Param("region"), c.Param("queue")

	if name == "" {
		queueURL := r.Form.Get("QueueUrl")
		if queueURL == "" {
			return writeQueryError(
				c,
				"MissingParameter",
				"The request must contain the parameter QueueUrl.",
				http.StatusBadRequest,
			)
		}

		name = queueNameFromInput(queueURL)
		if m := queueURLRegionRE.FindStringSubmatch(queueURL); m != nil {
			region = m[1]
		}
	}

	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return writeQueryError(
			c,
			"InternalFailure",
			"inspection unsupported by backend",
			http.StatusInternalServerError,
		)
	}

	msgs, err := b.PeekMessages(region, name, isTrue(r.Form.Get("ShowInvisible")), isTrue(r.Form.Get("ShowDelayed")))
	if err != nil {
		qe := buildQueryError(err)

		return c.XMLBlob(qe.status, qe.xml)
	}

	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		return c.JSON(http.StatusOK, inspectJSONBody(msgs))
	}

	return inspectXML(c, msgs)
}

func isTrue(v string) bool {
	v = strings.ToLower(v)

	return v == "true" || v == "1"
}

func inspectJSONBody(msgs []*Message) map[string]any {
	if len(msgs) == 0 {
		return map[string]any{}
	}

	out := make([]inspectJSONMessage, 0, len(msgs))

	for _, m := range msgs {
		jm := inspectJSONMessage{
			MessageID: m.MessageID, ReceiptHandle: m.ReceiptHandle, MD5OfBody: m.MD5OfBody,
			MD5OfMessageAttributes: m.MD5OfMessageAttributes, Body: m.Body, Attributes: m.Attributes,
		}

		if len(m.MessageAttributes) > 0 {
			jm.MessageAttributes = make(map[string]inspectJSONAttrValue, len(m.MessageAttributes))
			for k, v := range m.MessageAttributes {
				av := inspectJSONAttrValue{DataType: v.DataType, StringValue: v.StringValue}
				if len(v.BinaryValue) > 0 {
					av.BinaryValue = base64.StdEncoding.EncodeToString(v.BinaryValue)
				}

				jm.MessageAttributes[k] = av
			}
		}

		out = append(out, jm)
	}

	return map[string]any{"Messages": out}
}

func inspectXML(c *echo.Context, msgs []*Message) error {
	xmlMsgs := make([]XMLMessage, 0, len(msgs))

	for _, m := range msgs {
		attrs := make([]XMLAttribute, 0, len(m.Attributes))
		for k, v := range m.Attributes {
			attrs = append(attrs, XMLAttribute{Name: k, Value: v})
		}

		sort.Slice(attrs, func(i, j int) bool { return attrs[i].Name < attrs[j].Name })

		mattrs := make([]XMLMessageAttribute, 0, len(m.MessageAttributes))
		for k, v := range m.MessageAttributes {
			bin := ""
			if len(v.BinaryValue) > 0 {
				bin = base64.StdEncoding.EncodeToString(v.BinaryValue)
			}

			mattrs = append(mattrs, XMLMessageAttribute{
				Name:  k,
				Value: XMLMessageAttributeValue{DataType: v.DataType, StringValue: v.StringValue, BinaryValue: bin},
			})
		}

		sort.Slice(mattrs, func(i, j int) bool { return mattrs[i].Name < mattrs[j].Name })

		xmlMsgs = append(xmlMsgs, XMLMessage{
			MessageID:              m.MessageID,
			ReceiptHandle:          m.ReceiptHandle,
			MD5OfBody:              m.MD5OfBody,
			MD5OfMessageAttributes: m.MD5OfMessageAttributes,
			Body:                   m.Body,
			Attributes:             attrs,
			MessageAttributes:      mattrs,
		})
	}

	body, err := marshalXML(ReceiveMessageResponse{
		Xmlns:                sqsNamespace,
		ReceiveMessageResult: ReceiveMessageResult{Messages: xmlMsgs},
		ResponseMetadata:     XMLResponseMetadata{RequestID: queryRequestID},
	})
	if err != nil {
		return err
	}

	return c.XMLBlob(http.StatusOK, body)
}
