//nolint:mnd // AMQP field indexes and wire widths are spec constants
package amqp1

import (
	"fmt"
	"math"
)

// Performative descriptor codes.
const (
	descOpen        uint64 = 0x10
	descBegin       uint64 = 0x11
	descAttach      uint64 = 0x12
	descFlow        uint64 = 0x13
	descTransfer    uint64 = 0x14
	descDisposition uint64 = 0x15
	descDetach      uint64 = 0x16
	descEnd         uint64 = 0x17
	descClose       uint64 = 0x18
	descError       uint64 = 0x1d
	descReceived    uint64 = 0x23
	descAccepted    uint64 = 0x24
	descRejected    uint64 = 0x25
	descReleased    uint64 = 0x26
	descModified    uint64 = 0x27
	descSource      uint64 = 0x28
	descTarget      uint64 = 0x29
	descSASLMechs   uint64 = 0x40
	descSASLInit    uint64 = 0x41
	descSASLOutcome uint64 = 0x44
)

// Performative is a decoded or to-be-encoded AMQP/SASL performative.
type Performative interface {
	descriptor() uint64
	toList() List
}

// EncodePerformative returns the wire form of p.
func EncodePerformative(p Performative) ([]byte, error) {
	l := p.toList()
	for len(l) > 0 && l[len(l)-1] == nil {
		l = l[:len(l)-1]
	}

	return Marshal(nil, Described{Descriptor: p.descriptor(), Value: l})
}

// DecodePerformative parses a frame body into a performative and the
// remaining bytes (a transfer's payload).
func DecodePerformative(body []byte) (Performative, []byte, error) {
	v, rest, err := Unmarshal(body)
	if err != nil {
		return nil, nil, err
	}

	d, ok := v.(Described)
	if !ok {
		return nil, nil, fmt.Errorf("%w: performative is not described", ErrBadValue)
	}

	code, ok := d.Descriptor.(uint64)
	if !ok {
		return nil, nil, fmt.Errorf("%w: non-numeric descriptor", ErrBadValue)
	}

	l, _ := d.Value.(List)

	p, err := performativeFor(code, fields(l))
	if err != nil {
		return nil, nil, err
	}

	if t, isTransfer := p.(*Transfer); isTransfer {
		t.Payload = rest
		rest = nil
	}

	return p, rest, nil
}

func performativeFor(code uint64, f fields) (Performative, error) {
	switch code {
	case descOpen:
		return parseOpen(f), nil
	case descBegin:
		return parseBegin(f), nil
	case descAttach:
		return parseAttach(f), nil
	case descFlow:
		return parseFlow(f), nil
	case descTransfer:
		return parseTransfer(f), nil
	case descDisposition:
		return parseDisposition(f), nil
	case descDetach:
		return &Detach{Handle: f.u32(0), Closed: f.boolean(1), Error: f.errorAt(2)}, nil
	case descEnd:
		return &End{Error: f.errorAt(0)}, nil
	case descClose:
		return &Close{Error: f.errorAt(0)}, nil
	case descSASLMechs:
		return &SASLMechanisms{Mechanisms: f.symbols(0)}, nil
	case descSASLOutcome:
		return &SASLOutcome{Code: uint8(min(f.u64(0), math.MaxUint8))}, nil
	case descSASLInit:
		return &SASLInit{Mechanism: f.symbol(0), InitialResponse: f.bytes(1), Hostname: f.str(2)}, nil
	}

	return nil, fmt.Errorf("%w: performative 0x%x", ErrUnknownType, code)
}

// fields is a decoded performative field list with tolerant accessors.
type fields List

func (f fields) at(i int) any {
	if i < len(f) {
		return f[i]
	}

	return nil
}

func toUint(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint8:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint64:
		return x, true
	}

	return 0, false
}

func (f fields) u64(i int) uint64 {
	n, _ := toUint(f.at(i))

	return n
}

func (f fields) u32(i int) uint32 {
	return uint32(min(f.u64(i), math.MaxUint32))
}

func (f fields) optU32(i int) *uint32 {
	n, ok := toUint(f.at(i))
	if !ok {
		return nil
	}

	v := uint32(min(n, math.MaxUint32))

	return &v
}

func (f fields) boolean(i int) bool {
	b, _ := f.at(i).(bool)

	return b
}

func (f fields) str(i int) string {
	switch x := f.at(i).(type) {
	case string:
		return x
	case Symbol:
		return string(x)
	}

	return ""
}

func (f fields) symbol(i int) Symbol { return Symbol(f.str(i)) }

func (f fields) bytes(i int) []byte {
	b, _ := f.at(i).([]byte)

	return b
}

func (f fields) mapAt(i int) Map {
	m, _ := f.at(i).(Map)

	return m
}

func (f fields) symbols(i int) []Symbol {
	switch x := f.at(i).(type) {
	case []Symbol:
		return x
	case Symbol:
		return []Symbol{x}
	}

	return nil
}

func (f fields) errorAt(i int) *Error {
	d, ok := f.at(i).(Described)
	if !ok {
		return nil
	}

	l, _ := d.Value.(List)
	ef := fields(l)

	return &Error{Condition: ef.symbol(0), Description: ef.str(1), Info: ef.mapAt(2)}
}

// Error is an AMQP error.
type Error struct {
	Info        Map
	Condition   Symbol
	Description string
}

// Error implements error.
func (e *Error) Error() string { return string(e.Condition) + ": " + e.Description }

func (e *Error) value() any {
	if e == nil {
		return nil
	}

	var info any
	if e.Info != nil {
		info = e.Info
	}

	return Described{Descriptor: descError, Value: List{e.Condition, nullIfEmpty(e.Description), info}}
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}

	return s
}

// Open is the connection open performative.
type Open struct {
	Properties          Map
	ContainerID         string
	Hostname            string
	OfferedCapabilities []Symbol
	MaxFrameSize        uint32
	IdleTimeout         uint32
	ChannelMax          uint16
}

func (*Open) descriptor() uint64 { return descOpen }

func (o *Open) toList() List {
	var props, offered any
	if o.Properties != nil {
		props = o.Properties
	}

	if len(o.OfferedCapabilities) > 0 {
		offered = o.OfferedCapabilities
	}

	var idle any
	if o.IdleTimeout > 0 {
		idle = o.IdleTimeout
	}

	return List{o.ContainerID, nullIfEmpty(o.Hostname), o.MaxFrameSize, o.ChannelMax, idle, nil, offered, nil, props}
}

func parseOpen(f fields) *Open {
	o := &Open{
		ContainerID:         f.str(0),
		Hostname:            f.str(1),
		MaxFrameSize:        f.u32(2),
		ChannelMax:          uint16(min(f.u64(3), math.MaxUint16)),
		IdleTimeout:         f.u32(4),
		OfferedCapabilities: f.symbols(6),
		Properties:          f.mapAt(7),
	}

	if f.at(2) == nil {
		o.MaxFrameSize = math.MaxUint32
	}

	if f.at(3) == nil {
		o.ChannelMax = math.MaxUint16
	}

	return o
}

// Begin starts a session.
type Begin struct {
	RemoteChannel  *uint16
	NextOutgoingID uint32
	IncomingWindow uint32
	OutgoingWindow uint32
	HandleMax      uint32
}

func (*Begin) descriptor() uint64 { return descBegin }

func (b *Begin) toList() List {
	var rc any
	if b.RemoteChannel != nil {
		rc = *b.RemoteChannel
	}

	return List{rc, b.NextOutgoingID, b.IncomingWindow, b.OutgoingWindow, b.HandleMax}
}

func parseBegin(f fields) *Begin {
	b := &Begin{
		NextOutgoingID: f.u32(1),
		IncomingWindow: f.u32(2),
		OutgoingWindow: f.u32(3),
		HandleMax:      f.u32(4),
	}

	if n, ok := toUint(f.at(0)); ok {
		rc := uint16(min(n, math.MaxUint16))
		b.RemoteChannel = &rc
	}

	if f.at(4) == nil {
		b.HandleMax = math.MaxUint32
	}

	return b
}

// Link settle modes.
const (
	SenderSettleUnsettled uint8 = 0
	SenderSettleSettled   uint8 = 1
	SenderSettleMixed     uint8 = 2
	ReceiverSettleFirst   uint8 = 0
	ReceiverSettleSecond  uint8 = 1
)

// Source is a link source terminus.
type Source struct {
	Filter       Map
	Address      string
	Capabilities []Symbol
	Timeout      uint32
	Durable      uint32
	Dynamic      bool
}

func (s *Source) value() any {
	if s == nil {
		return nil
	}

	var filter, caps any
	if s.Filter != nil {
		filter = s.Filter
	}

	if len(s.Capabilities) > 0 {
		caps = s.Capabilities
	}

	l := List{nullIfEmpty(s.Address), s.Durable, nil, s.Timeout, s.Dynamic, nil, nil, filter, nil, nil, caps}

	return Described{Descriptor: descSource, Value: l}
}

func parseSource(v any) *Source {
	d, ok := v.(Described)
	if !ok {
		return nil
	}

	l, _ := d.Value.(List)
	f := fields(l)

	return &Source{
		Address: f.str(0), Durable: f.u32(1), Timeout: f.u32(3), Dynamic: f.boolean(4),
		Filter: f.mapAt(7), Capabilities: f.symbols(10),
	}
}

// Target is a link target terminus.
type Target struct {
	Address      string
	Capabilities []Symbol
	Timeout      uint32
	Durable      uint32
	Dynamic      bool
}

func (t *Target) value() any {
	if t == nil {
		return nil
	}

	var caps any
	if len(t.Capabilities) > 0 {
		caps = t.Capabilities
	}

	l := List{nullIfEmpty(t.Address), t.Durable, nil, t.Timeout, t.Dynamic, nil, caps}

	return Described{Descriptor: descTarget, Value: l}
}

func parseTarget(v any) *Target {
	d, ok := v.(Described)
	if !ok {
		return nil
	}

	l, _ := d.Value.(List)
	f := fields(l)

	return &Target{
		Address: f.str(0), Durable: f.u32(1), Timeout: f.u32(3), Dynamic: f.boolean(4), Capabilities: f.symbols(6),
	}
}

// Attach opens a link. Role is true when the sender of this frame is the
// receiving end of the link.
type Attach struct {
	Source               *Source
	Target               *Target
	Properties           Map
	Name                 string
	OfferedCapabilities  []Symbol
	DesiredCapabilities  []Symbol
	MaxMessageSize       uint64
	Handle               uint32
	InitialDeliveryCount uint32
	SndSettleMode        uint8
	RcvSettleMode        uint8
	Role                 bool
}

func (*Attach) descriptor() uint64 { return descAttach }

func (a *Attach) toList() List {
	var props, offered any
	if a.Properties != nil {
		props = a.Properties
	}

	if len(a.OfferedCapabilities) > 0 {
		offered = a.OfferedCapabilities
	}

	var maxSize any
	if a.MaxMessageSize > 0 {
		maxSize = a.MaxMessageSize
	}

	return List{
		a.Name, a.Handle, a.Role, a.SndSettleMode, a.RcvSettleMode, a.Source.value(), a.Target.value(),
		nil, nil, a.InitialDeliveryCount, maxSize, offered, nil, props,
	}
}

func parseAttach(f fields) *Attach {
	snd := SenderSettleMixed
	if n, ok := toUint(f.at(3)); ok {
		snd = uint8(min(n, math.MaxUint8))
	}

	return &Attach{
		Name: f.str(0), Handle: f.u32(1), Role: f.boolean(2), SndSettleMode: snd,
		RcvSettleMode: uint8(min(f.u64(4), math.MaxUint8)), Source: parseSource(f.at(5)),
		Target: parseTarget(f.at(6)), InitialDeliveryCount: f.u32(9), MaxMessageSize: f.u64(10),
		OfferedCapabilities: f.symbols(11), DesiredCapabilities: f.symbols(12), Properties: f.mapAt(13),
	}
}

// Flow updates session windows and link credit.
type Flow struct {
	NextIncomingID *uint32
	Handle         *uint32
	DeliveryCount  *uint32
	LinkCredit     *uint32
	IncomingWindow uint32
	NextOutgoingID uint32
	OutgoingWindow uint32
	Drain          bool
	Echo           bool
}

func (*Flow) descriptor() uint64 { return descFlow }

func optVal(p *uint32) any {
	if p == nil {
		return nil
	}

	return *p
}

func (fl *Flow) toList() List {
	return List{
		optVal(fl.NextIncomingID), fl.IncomingWindow, fl.NextOutgoingID, fl.OutgoingWindow,
		optVal(fl.Handle), optVal(fl.DeliveryCount), optVal(fl.LinkCredit), nil, fl.Drain, fl.Echo,
	}
}

func parseFlow(f fields) *Flow {
	return &Flow{
		NextIncomingID: f.optU32(0), IncomingWindow: f.u32(1), NextOutgoingID: f.u32(2), OutgoingWindow: f.u32(3),
		Handle: f.optU32(4), DeliveryCount: f.optU32(5), LinkCredit: f.optU32(6), Drain: f.boolean(8),
		Echo: f.boolean(9),
	}
}

// Delivery state kinds.
const (
	StateReceived uint64 = descReceived
	StateAccepted uint64 = descAccepted
	StateRejected uint64 = descRejected
	StateReleased uint64 = descReleased
	StateModified uint64 = descModified
)

// DeliveryState is a terminal or intermediate delivery outcome.
type DeliveryState struct {
	Error             *Error
	Kind              uint64
	DeliveryFailed    bool
	UndeliverableHere bool
}

func (s *DeliveryState) value() any {
	if s == nil || s.Kind == 0 {
		return nil
	}

	switch s.Kind {
	case StateRejected:
		return Described{Descriptor: s.Kind, Value: List{s.Error.value()}}
	case StateModified:
		return Described{Descriptor: s.Kind, Value: List{s.DeliveryFailed, s.UndeliverableHere}}
	}

	return Described{Descriptor: s.Kind, Value: List{}}
}

func parseState(v any) *DeliveryState {
	d, ok := v.(Described)
	if !ok {
		return nil
	}

	kind, _ := d.Descriptor.(uint64)
	l, _ := d.Value.(List)
	f := fields(l)
	st := &DeliveryState{Kind: kind}

	switch kind {
	case StateRejected:
		st.Error = f.errorAt(0)
	case StateModified:
		st.DeliveryFailed, st.UndeliverableHere = f.boolean(0), f.boolean(1)
	}

	return st
}

// Transfer carries (part of) a message.
type Transfer struct {
	DeliveryID    *uint32
	MessageFormat *uint32
	State         *DeliveryState
	DeliveryTag   []byte
	Payload       []byte
	Handle        uint32
	Settled       bool
	More          bool
	Aborted       bool
}

func (*Transfer) descriptor() uint64 { return descTransfer }

func (t *Transfer) toList() List {
	return List{
		t.Handle, optVal(t.DeliveryID), nullBytes(t.DeliveryTag), optVal(t.MessageFormat), t.Settled, t.More,
		nil, t.State.value(), nil, t.Aborted,
	}
}

func nullBytes(b []byte) any {
	if b == nil {
		return nil
	}

	return b
}

func parseTransfer(f fields) *Transfer {
	return &Transfer{
		Handle: f.u32(0), DeliveryID: f.optU32(1), DeliveryTag: f.bytes(2), MessageFormat: f.optU32(3),
		Settled: f.boolean(4), More: f.boolean(5), State: parseState(f.at(7)), Aborted: f.boolean(9),
	}
}

// Disposition settles or updates deliveries. Role is true when sent by the
// receiving end.
type Disposition struct {
	Last    *uint32
	State   *DeliveryState
	First   uint32
	Role    bool
	Settled bool
}

func (*Disposition) descriptor() uint64 { return descDisposition }

func (d *Disposition) toList() List {
	return List{d.Role, d.First, optVal(d.Last), d.Settled, d.State.value()}
}

func parseDisposition(f fields) *Disposition {
	return &Disposition{
		Role: f.boolean(0), First: f.u32(1), Last: f.optU32(2), Settled: f.boolean(3), State: parseState(f.at(4)),
	}
}

// Detach closes or detaches a link.
type Detach struct {
	Error  *Error
	Handle uint32
	Closed bool
}

func (*Detach) descriptor() uint64 { return descDetach }
func (d *Detach) toList() List     { return List{d.Handle, d.Closed, d.Error.value()} }

// End ends a session.
type End struct{ Error *Error }

func (*End) descriptor() uint64 { return descEnd }
func (e *End) toList() List     { return List{e.Error.value()} }

// Close closes the connection.
type Close struct{ Error *Error }

func (*Close) descriptor() uint64 { return descClose }
func (c *Close) toList() List     { return List{c.Error.value()} }

// SASLMechanisms advertises SASL mechanisms.
type SASLMechanisms struct{ Mechanisms []Symbol }

func (*SASLMechanisms) descriptor() uint64 { return descSASLMechs }
func (m *SASLMechanisms) toList() List     { return List{m.Mechanisms} }

// SASLInit selects a mechanism.
type SASLInit struct {
	Mechanism       Symbol
	Hostname        string
	InitialResponse []byte
}

func (*SASLInit) descriptor() uint64 { return descSASLInit }

func (i *SASLInit) toList() List {
	return List{i.Mechanism, nullBytes(i.InitialResponse), nullIfEmpty(i.Hostname)}
}

// SASL outcome codes.
const (
	SASLOK   uint8 = 0
	SASLAuth uint8 = 1
)

// SASLOutcome reports authentication success or failure.
type SASLOutcome struct{ Code uint8 }

func (*SASLOutcome) descriptor() uint64 { return descSASLOutcome }
func (o *SASLOutcome) toList() List     { return List{o.Code} }
