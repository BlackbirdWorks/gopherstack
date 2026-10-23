//nolint:mnd,gosec // AMQP field indexes and wire widths are spec constants; lengths are bounded by frame size
package amqp1

import (
	"fmt"
	"time"
)

// Message section descriptors.
const (
	secHeader       uint64 = 0x70
	secDeliveryAnn  uint64 = 0x71
	secMessageAnn   uint64 = 0x72
	secProperties   uint64 = 0x73
	secAppProps     uint64 = 0x74
	secData         uint64 = 0x75
	secSequence     uint64 = 0x76
	secValue        uint64 = 0x77
	secFooter       uint64 = 0x78
	maxMessageDepth        = 4096
)

// BatchMessageFormat is the Service Bus message-format of a transfer that
// carries several encoded messages as data sections.
const BatchMessageFormat uint32 = 0x80013700

// Header is the AMQP message header section.
type Header struct {
	TTL           time.Duration
	DeliveryCount uint32
	Priority      uint8
	Durable       bool
	FirstAcquirer bool
}

// Properties is the AMQP message properties section. Message and correlation
// IDs may be strings, uuids, ulongs or binary on the wire.
type Properties struct {
	AbsoluteExpiryTime time.Time
	CreationTime       time.Time
	MessageID          any
	CorrelationID      any
	Subject            string
	To                 string
	ReplyTo            string
	ContentType        string
	ContentEncoding    string
	GroupID            string
	ReplyToGroupID     string
	UserID             []byte
	GroupSequence      uint32
}

// Message is a decoded AMQP message. Value holds an amqp-value body; Data
// holds data sections.
type Message struct {
	Header                *Header
	Properties            *Properties
	DeliveryAnnotations   Map
	Annotations           Map
	ApplicationProperties Map
	Footer                Map
	Value                 any
	Data                  [][]byte
	Sequence              []List
	HasValue              bool
}

// DecodeMessage parses concatenated message sections.
func DecodeMessage(b []byte) (*Message, error) {
	m := &Message{}

	for sections := 0; len(b) > 0; sections++ {
		if sections > maxMessageDepth {
			return nil, ErrTooDeep
		}

		v, rest, err := Unmarshal(b)
		if err != nil {
			return nil, err
		}

		b = rest

		d, ok := v.(Described)
		if !ok {
			return nil, fmt.Errorf("%w: message section is not described", ErrBadValue)
		}

		if serr := m.addSection(d); serr != nil {
			return nil, serr
		}
	}

	return m, nil
}

func (m *Message) addSection(d Described) error {
	code, _ := d.Descriptor.(uint64)

	switch code {
	case secHeader:
		m.Header = parseHeader(d.Value)
	case secDeliveryAnn:
		m.DeliveryAnnotations, _ = d.Value.(Map)
	case secMessageAnn:
		m.Annotations, _ = d.Value.(Map)
	case secProperties:
		m.Properties = parseProperties(d.Value)
	case secAppProps:
		m.ApplicationProperties, _ = d.Value.(Map)
	case secFooter:
		m.Footer, _ = d.Value.(Map)
	case secData:
		b, ok := d.Value.([]byte)
		if !ok {
			return fmt.Errorf("%w: data section is not binary", ErrBadValue)
		}

		m.Data = append(m.Data, b)
	case secSequence:
		l, _ := d.Value.(List)
		m.Sequence = append(m.Sequence, l)
	case secValue:
		m.Value, m.HasValue = d.Value, true
	default:
		return fmt.Errorf("%w: message section 0x%x", ErrUnknownType, code)
	}

	return nil
}

func parseHeader(v any) *Header {
	l, _ := v.(List)
	f := fields(l)
	h := &Header{
		Durable: f.boolean(0), Priority: uint8(min(f.u64(1), 255)), FirstAcquirer: f.boolean(3),
		DeliveryCount: f.u32(4),
	}

	if ms, ok := toUint(f.at(2)); ok {
		h.TTL = time.Duration(ms) * time.Millisecond
	}

	return h
}

func timeAt(f fields, i int) time.Time {
	t, _ := f.at(i).(time.Time)

	return t
}

func parseProperties(v any) *Properties {
	l, _ := v.(List)
	f := fields(l)

	return &Properties{
		MessageID: f.at(0), UserID: f.bytes(1), To: f.str(2), Subject: f.str(3), ReplyTo: f.str(4),
		CorrelationID: f.at(5), ContentType: f.str(6), ContentEncoding: f.str(7),
		AbsoluteExpiryTime: timeAt(f, 8), CreationTime: timeAt(f, 9), GroupID: f.str(10),
		GroupSequence: f.u32(11), ReplyToGroupID: f.str(12),
	}
}

// Encode serializes the message sections.
func (m *Message) Encode() ([]byte, error) {
	var (
		b   []byte
		err error
	)

	for _, s := range m.sections() {
		if b, err = Marshal(b, Described{Descriptor: s.code, Value: s.value}); err != nil {
			return nil, err
		}
	}

	return b, nil
}

type section struct {
	value any
	code  uint64
}

func (m *Message) sections() []section {
	var out []section

	if m.Header != nil {
		out = append(out, section{code: secHeader, value: m.Header.toList()})
	}

	if len(m.DeliveryAnnotations) > 0 {
		out = append(out, section{code: secDeliveryAnn, value: m.DeliveryAnnotations})
	}

	if len(m.Annotations) > 0 {
		out = append(out, section{code: secMessageAnn, value: m.Annotations})
	}

	if m.Properties != nil {
		out = append(out, section{code: secProperties, value: m.Properties.toList()})
	}

	if len(m.ApplicationProperties) > 0 {
		out = append(out, section{code: secAppProps, value: m.ApplicationProperties})
	}

	for _, d := range m.Data {
		out = append(out, section{code: secData, value: d})
	}

	for _, s := range m.Sequence {
		out = append(out, section{code: secSequence, value: s})
	}

	if m.HasValue {
		out = append(out, section{code: secValue, value: m.Value})
	}

	if len(m.Footer) > 0 {
		out = append(out, section{code: secFooter, value: m.Footer})
	}

	return out
}

func (h *Header) toList() List {
	var ttl any
	if h.TTL > 0 {
		ttl = uint32(h.TTL / time.Millisecond)
	}

	return List{h.Durable, h.Priority, ttl, h.FirstAcquirer, h.DeliveryCount}
}

func zeroTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}

	return t
}

func (p *Properties) toList() List {
	var user any
	if p.UserID != nil {
		user = p.UserID
	}

	return List{
		p.MessageID, user, nullIfEmpty(p.To), nullIfEmpty(p.Subject), nullIfEmpty(p.ReplyTo), p.CorrelationID,
		symbolOrNil(p.ContentType), symbolOrNil(p.ContentEncoding), zeroTime(p.AbsoluteExpiryTime),
		zeroTime(p.CreationTime), nullIfEmpty(p.GroupID), p.GroupSequence, nullIfEmpty(p.ReplyToGroupID),
	}
}

func symbolOrNil(s string) any {
	if s == "" {
		return nil
	}

	return Symbol(s)
}

// IDString renders a message-id or correlation-id as a string.
func IDString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case UUID:
		return uuidString(x)
	case []byte:
		return string(x)
	}

	return fmt.Sprint(v)
}

func uuidString(u UUID) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}
