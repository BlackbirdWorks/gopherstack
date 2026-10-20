package azureservicebus

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

const (
	annSequence      = "x-opt-sequence-number"
	annEnqueued      = "x-opt-enqueued-time"
	annLockedUntil   = "x-opt-locked-until"
	annScheduled     = "x-opt-scheduled-enqueue-time"
	annMessageState  = "x-opt-message-state"
	annLockToken     = "x-opt-lock-token" //nolint:gosec // AMQP annotation name, not a credential
	appDeadLetterWhy = "DeadLetterReason"
	appDeadLetterMsg = "DeadLetterErrorDescription"

	messageStateDeferred  int32 = 1
	messageStateScheduled int32 = 2
)

func parseUUID(s string) (amqp1.UUID, bool) {
	u, err := uuid.Parse(s)

	return amqp1.UUID(u), err == nil
}

func uuidText(u amqp1.UUID) string { return uuid.UUID(u).String() }

// toAMQP renders a stored message as an AMQP message.
func toAMQP(info MessageInfo, now time.Time) *amqp1.Message {
	m := &amqp1.Message{
		Data: [][]byte{info.Body},
		Header: &amqp1.Header{
			Durable:       true,
			DeliveryCount: uint32(min(max(info.DeliveryCount-1, 0), math.MaxInt32)),
			TTL:           max(info.ExpiresAt.Sub(info.EnqueuedTime), 0),
		},
		Properties: &amqp1.Properties{
			MessageID: info.MessageID, Subject: info.Label, ContentType: info.ContentType,
			ReplyTo: info.ReplyTo, To: info.To, GroupID: info.SessionID, ReplyToGroupID: info.ReplyToSessionID,
		},
		Annotations: amqp1.Map{
			amqp1.Symbol(annSequence): info.SequenceNumber,
			amqp1.Symbol(annEnqueued): info.EnqueuedTime.UTC(),
		},
		ApplicationProperties: appProps(info),
	}

	if info.CorrelationID != "" {
		m.Properties.CorrelationID = info.CorrelationID
	}

	if !info.LockedUntil.IsZero() {
		m.Annotations[amqp1.Symbol(annLockedUntil)] = info.LockedUntil.UTC()
	}

	switch {
	case info.Deferred:
		m.Annotations[amqp1.Symbol(annMessageState)] = messageStateDeferred
	case info.ScheduledFor.After(now):
		m.Annotations[amqp1.Symbol(annMessageState)] = messageStateScheduled
		m.Annotations[amqp1.Symbol(annScheduled)] = info.ScheduledFor.UTC()
	}

	return m
}

func appProps(info MessageInfo) amqp1.Map {
	props := amqp1.Map{}

	if info.AppProps != nil {
		for k, v := range info.AppProps {
			if _, err := amqp1.Marshal(nil, v); err != nil {
				v = fmt.Sprint(v)
			}

			props[k] = v
		}
	} else {
		for k, v := range info.CustomHeaders {
			props[k] = v
		}
	}

	if info.DeadLetterReason != "" {
		props[appDeadLetterWhy] = info.DeadLetterReason
		props[appDeadLetterMsg] = info.DeadLetterErrorDescription
	}

	if len(props) == 0 {
		return nil
	}

	return props
}

// fromAMQP converts an inbound AMQP message into a backend message.
func fromAMQP(m *amqp1.Message) NewMessage {
	nm := NewMessage{Body: bodyBytes(m)}

	if p := m.Properties; p != nil {
		nm.MessageID = amqp1.IDString(p.MessageID)
		nm.CorrelationID = amqp1.IDString(p.CorrelationID)
		nm.Label, nm.ContentType, nm.ReplyTo, nm.To = p.Subject, p.ContentType, p.ReplyTo, p.To
		nm.SessionID, nm.ReplyToSessionID = p.GroupID, p.ReplyToGroupID
	}

	if m.Header != nil {
		nm.TimeToLive = m.Header.TTL
	}

	if t, ok := m.Annotations[amqp1.Symbol(annScheduled)].(time.Time); ok {
		nm.ScheduledFor = t
	}

	if len(m.ApplicationProperties) > 0 {
		nm.AppProps = make(map[string]any, len(m.ApplicationProperties))
		nm.CustomHeaders = make(map[string]string, len(m.ApplicationProperties))

		for k, v := range m.ApplicationProperties {
			key := fmt.Sprint(k)
			nm.AppProps[key] = v
			nm.CustomHeaders[key] = fmt.Sprint(v)
		}
	}

	return nm
}

func bodyBytes(m *amqp1.Message) []byte {
	switch {
	case len(m.Data) == 1:
		return m.Data[0]
	case len(m.Data) > 1:
		return []byte(strings.Join(dataStrings(m.Data), ""))
	case m.HasValue:
		switch v := m.Value.(type) {
		case string:
			return []byte(v)
		case []byte:
			return v
		}

		b, _ := amqp1.Marshal(nil, m.Value)

		return b
	}

	return nil
}

func dataStrings(d [][]byte) []string {
	out := make([]string, len(d))
	for i, b := range d {
		out[i] = string(b)
	}

	return out
}

// splitBatch expands a batch-format envelope into its inner messages.
func splitBatch(outer *amqp1.Message) ([]*amqp1.Message, error) {
	out := make([]*amqp1.Message, 0, len(outer.Data))

	for _, d := range outer.Data {
		inner, err := amqp1.DecodeMessage(d)
		if err != nil {
			return nil, err
		}

		out = append(out, inner)
	}

	return out, nil
}

// guidTag returns u in .NET Guid byte order, which is how Service Bus (and the
// SDK, see its lockTokenFromMessageTag) encodes a lock token in the delivery tag.
func guidTag(u amqp1.UUID) []byte {
	t := u
	t[0], t[3] = t[3], t[0]
	t[1], t[2] = t[2], t[1]
	t[4], t[5] = t[5], t[4]
	t[6], t[7] = t[7], t[6]

	return t[:]
}
