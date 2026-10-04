package lambda

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"
	"unicode/utf8"
)

// Event shapes follow https://docs.aws.amazon.com/lambda/latest/dg/with-mq.html.
const (
	mqEventSourceActiveMQ = "aws:mq"
	mqEventSourceRabbitMQ = "aws:rmq"
	mqDefaultVHost        = "/"
	mqQueuePrefix         = "/queue/"
	mqJavaTimeLayout      = "Jan 2, 2006, 3:04:05 PM"
	mqJMSBytesMessage     = "jms/bytes-message"
	mqJMSTextMessage      = "jms/text-message"
)

// mqMessage is one broker message handed to the ESM batcher.
type mqMessage struct {
	Timestamp       time.Time
	DeliveredAt     time.Time
	raw             any
	Headers         map[string]any
	Properties      map[string]string
	Queue           string
	MessageID       string
	MessageType     string
	CorrelationID   string
	ReplyTo         string
	Type            string
	Expiration      string
	ContentType     string
	ContentEncoding string
	UserID          string
	AppID           string
	ClusterID       string
	Body            []byte
	DeliveryMode    int
	Priority        int
	Redelivered     bool
}

type mqDestination struct {
	PhysicalName string `json:"physicalName"`
}

type activeMQEventMessage struct {
	ReplyTo       *string           `json:"replyTo"`
	Type          *string           `json:"type"`
	CorrelationID *string           `json:"correlationId"`
	Properties    map[string]string `json:"properties"`
	MessageID     string            `json:"messageID"`
	MessageType   string            `json:"messageType"`
	Expiration    string            `json:"expiration"`
	Data          string            `json:"data"`
	Destination   mqDestination     `json:"destination"`
	DeliveryMode  int               `json:"deliveryMode"`
	Priority      int               `json:"priority"`
	Timestamp     int64             `json:"timestamp"`
	BrokerInTime  int64             `json:"brokerInTime"`
	BrokerOutTime int64             `json:"brokerOutTime"`
	Redelivered   bool              `json:"redelivered"`
}

type activeMQEvent struct {
	EventSource    string                 `json:"eventSource"`
	EventSourceARN string                 `json:"eventSourceArn"`
	Messages       []activeMQEventMessage `json:"messages"`
}

type rabbitMQBasicProperties struct {
	ContentType     *string        `json:"contentType"`
	ContentEncoding *string        `json:"contentEncoding"`
	CorrelationID   *string        `json:"correlationId"`
	ReplyTo         *string        `json:"replyTo"`
	Expiration      *string        `json:"expiration"`
	MessageID       *string        `json:"messageId"`
	Timestamp       *string        `json:"timestamp"`
	Type            *string        `json:"type"`
	UserID          *string        `json:"userId"`
	AppID           *string        `json:"appId"`
	ClusterID       *string        `json:"clusterId"`
	Headers         map[string]any `json:"headers"`
	DeliveryMode    int            `json:"deliveryMode"`
	Priority        int            `json:"priority"`
	BodySize        int            `json:"bodySize"`
}

type rabbitMQEventMessage struct {
	Data            string                  `json:"data"`
	BasicProperties rabbitMQBasicProperties `json:"basicProperties"`
	Redelivered     bool                    `json:"redelivered"`
}

type rabbitMQEvent struct {
	RMQMessagesByQueue map[string][]rabbitMQEventMessage `json:"rmqMessagesByQueue"`
	EventSource        string                            `json:"eventSource"`
	EventSourceARN     string                            `json:"eventSourceArn"`
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

// buildMQEventPayload shapes msgs into the documented aws:mq or aws:rmq event.
func buildMQEventPayload(eventSource, eventSourceARN, vhost string, msgs []mqMessage) ([]byte, error) {
	if eventSource == mqEventSourceRabbitMQ {
		return json.Marshal(buildRabbitMQEvent(eventSourceARN, vhost, msgs))
	}

	return json.Marshal(buildActiveMQEvent(eventSourceARN, msgs))
}

func buildActiveMQEvent(eventSourceARN string, msgs []mqMessage) activeMQEvent {
	ev := activeMQEvent{
		EventSource:    mqEventSourceActiveMQ,
		EventSourceARN: eventSourceARN,
		Messages:       make([]activeMQEventMessage, 0, len(msgs)),
	}

	for _, m := range msgs {
		props := m.Properties
		if props == nil {
			props = map[string]string{}
		}

		ev.Messages = append(ev.Messages, activeMQEventMessage{
			MessageID:     m.MessageID,
			MessageType:   m.MessageType,
			DeliveryMode:  m.DeliveryMode,
			ReplyTo:       strPtr(m.ReplyTo),
			Type:          strPtr(m.Type),
			Expiration:    m.Expiration,
			Priority:      m.Priority,
			CorrelationID: strPtr(m.CorrelationID),
			Redelivered:   m.Redelivered,
			Destination:   mqDestination{PhysicalName: m.Queue},
			Data:          base64.StdEncoding.EncodeToString(m.Body),
			Timestamp:     m.Timestamp.UnixMilli(),
			BrokerInTime:  m.Timestamp.UnixMilli(),
			BrokerOutTime: m.DeliveredAt.UnixMilli(),
			Properties:    props,
		})
	}

	return ev
}

func buildRabbitMQEvent(eventSourceARN, vhost string, msgs []mqMessage) rabbitMQEvent {
	ev := rabbitMQEvent{
		EventSource:        mqEventSourceRabbitMQ,
		EventSourceARN:     eventSourceARN,
		RMQMessagesByQueue: make(map[string][]rabbitMQEventMessage),
	}

	for _, m := range msgs {
		var ts *string

		if !m.Timestamp.IsZero() {
			s := m.Timestamp.UTC().Format(mqJavaTimeLayout)
			ts = &s
		}

		key := m.Queue + "::" + vhost
		ev.RMQMessagesByQueue[key] = append(ev.RMQMessagesByQueue[key], rabbitMQEventMessage{
			Redelivered: m.Redelivered,
			Data:        base64.StdEncoding.EncodeToString(m.Body),
			BasicProperties: rabbitMQBasicProperties{
				ContentType:     strPtr(m.ContentType),
				ContentEncoding: strPtr(m.ContentEncoding),
				Headers:         rabbitMQHeaders(m.Headers),
				DeliveryMode:    m.DeliveryMode,
				Priority:        m.Priority,
				CorrelationID:   strPtr(m.CorrelationID),
				ReplyTo:         strPtr(m.ReplyTo),
				Expiration:      strPtr(m.Expiration),
				MessageID:       strPtr(m.MessageID),
				Timestamp:       ts,
				Type:            strPtr(m.Type),
				UserID:          strPtr(m.UserID),
				AppID:           strPtr(m.AppID),
				ClusterID:       strPtr(m.ClusterID),
				BodySize:        len(m.Body),
			},
		})
	}

	return ev
}

// rabbitMQHeaders renders string and byte header values as {"bytes":[...]} like the documented event.
func rabbitMQHeaders(h map[string]any) map[string]any {
	out := make(map[string]any, len(h))

	for k, v := range h {
		switch t := v.(type) {
		case string:
			out[k] = map[string][]int{"bytes": bytesAsInts([]byte(t))}
		case []byte:
			out[k] = map[string][]int{"bytes": bytesAsInts(t)}
		default:
			out[k] = v
		}
	}

	return out
}

func bytesAsInts(b []byte) []int {
	out := make([]int, len(b))
	for i, c := range b {
		out[i] = int(c)
	}

	return out
}

// mqFilterView is the shape FilterCriteria is matched against; "data" is the decoded body.
func mqFilterView(m mqMessage) map[string]any {
	view := map[string]any{"messageID": m.MessageID, "redelivered": m.Redelivered}

	if !utf8.Valid(m.Body) {
		return view
	}

	if obj, ok := parseJSONObjectBytes(m.Body); ok {
		view["data"] = obj
	} else {
		view["data"] = string(m.Body)
	}

	return view
}

func splitMQByFilter(fc *FilterCriteria, msgs []mqMessage) ([]mqMessage, []mqMessage) {
	if fc == nil || len(fc.Filters) == 0 {
		return msgs, nil
	}

	var matched, dropped []mqMessage

	for _, m := range msgs {
		if eventFilterMatches(fc, mqFilterView(m)) {
			matched = append(matched, m)
		} else {
			dropped = append(dropped, m)
		}
	}

	return matched, dropped
}

func mqMessageSize(m mqMessage) int {
	return len(m.Body)*kafkaBase64Num/kafkaBase64Den + kafkaRecordOverheadBytes
}

func mqBatchBytes(msgs []mqMessage) int {
	n := 0
	for _, m := range msgs {
		n += mqMessageSize(m)
	}

	return n
}

// splitMQByPayload chunks msgs so each chunk's encoded size stays under limit.
func splitMQByPayload(msgs []mqMessage, limit int) [][]mqMessage {
	var (
		chunks [][]mqMessage
		size   int
	)

	cur := make([]mqMessage, 0, len(msgs))

	for _, m := range msgs {
		sz := mqMessageSize(m)
		if len(cur) > 0 && size+sz > limit {
			chunks = append(chunks, cur)
			cur, size = make([]mqMessage, 0, len(msgs)), 0
		}

		cur = append(cur, m)
		size += sz
	}

	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}

	return chunks
}

// stompMessageType maps STOMP framing to the JMS type: content-length marks a BytesMessage.
func stompMessageType(hasContentLength bool) string {
	if hasContentLength {
		return mqJMSBytesMessage
	}

	return mqJMSTextMessage
}

func atoiOr(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}

	return def
}
