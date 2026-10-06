package iot

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// mqttV5 is the MQTT protocol version number that carries DISCONNECT reason codes.
const mqttV5 = 5

// basicIngestPrefix starts every Basic Ingest topic: $aws/rules/<ruleName>/<topic>.
const basicIngestPrefix = "$aws/rules/"

// ErrBrokerNotStarted is returned when a publish is attempted before the broker is started.
var ErrBrokerNotStarted = errors.New("mqtt broker not started")

// Broker wraps a mochi-mqtt server to provide the IoT MQTT endpoint.
type Broker struct {
	// server is accessed atomically to avoid data races between Start and Publish.
	server    atomic.Pointer[mqtt.Server]
	ready     chan struct{}
	backend   *InMemoryBackend
	others    func() []*InMemoryBackend
	boundPort atomic.Int64
	readyOnce sync.Once
	port      int
}

// NewBroker creates a new Broker using the given backend and port.
func NewBroker(backend *InMemoryBackend, port int) *Broker {
	return &Broker{
		backend: backend,
		port:    port,
		ready:   make(chan struct{}),
	}
}

// Port returns the TCP port the broker is listening on; valid once Ready is closed.
func (b *Broker) Port() int { return int(b.boundPort.Load()) }

// Ready is closed once Start has bound its listener and begun serving.
func (b *Broker) Ready() <-chan struct{} { return b.ready }

// Start initialises the MQTT server, registers the rule hook, and begins listening.
// It blocks until ctx is cancelled.
func (b *Broker) Start(ctx context.Context) error {
	log := logger.Load(ctx)

	s := mqtt.New(&mqtt.Options{
		Logger:       log,
		InlineClient: true,
	})

	if err := s.AddHook(new(auth.AllowHook), nil); err != nil {
		return fmt.Errorf("iot broker: add auth hook: %w", err)
	}

	hook := &ruleHook{
		broker:  b,
		backend: b.backend,
		others:  b.others,
		ctx:     ctx,
	}

	if err := s.AddHook(hook, nil); err != nil {
		return fmt.Errorf("iot broker: add rule hook: %w", err)
	}

	tcp := listeners.NewTCP(listeners.Config{
		ID:      "tcp1",
		Address: fmt.Sprintf(":%d", b.port),
	})

	if err := s.AddListener(tcp); err != nil {
		return fmt.Errorf("iot broker: add listener: %w", err)
	}

	// Store the server atomically before Serve() so Publish() can access it concurrently.
	b.server.Store(s)

	// mochi's Serve starts its listeners and event loop in goroutines and returns at once.
	if err := s.Serve(); err != nil {
		return fmt.Errorf("iot broker: serve: %w", err)
	}

	if _, portStr, err := net.SplitHostPort(tcp.Address()); err == nil {
		if p, convErr := strconv.Atoi(portStr); convErr == nil {
			b.boundPort.Store(int64(p))
		}
	}

	b.readyOnce.Do(func() { close(b.ready) })

	<-ctx.Done()

	return s.Close()
}

// Run implements worker.Runner, adapting Start's blocking-with-error shape to
// the fire-and-forget Runner contract used by worker.SingleRun so
// Handler.Shutdown can deterministically drain the broker goroutine.
func (b *Broker) Run(ctx context.Context) {
	log := logger.Load(ctx)
	if err := b.Start(ctx); err != nil {
		log.ErrorContext(ctx, "IoT MQTT broker stopped", "error", err)
	}
}

// Publish delivers a message directly to the broker (used by the IoT Data Plane).
func (b *Broker) Publish(topic string, payload []byte, retain bool, qos byte) error {
	s := b.server.Load()
	if s == nil {
		return ErrBrokerNotStarted
	}

	return s.Publish(topic, payload, retain, qos)
}

// PublishWithProperties implements iotdataplane.MQTTPublisher. It behaves
// like Publish but also attaches props to the injected packet as real MQTT5
// packet properties. mochi-mqtt only encodes packet properties for a
// receiving client whose own negotiated protocol version is 5
// (packets.Packet.PublishEncode gates on pk.ProtocolVersion == 5, which
// Client.WritePacket sets from the *receiving* client's own
// cl.Properties.ProtocolVersion right before encoding -- see
// packets/packets.go and clients.go in
// github.com/mochi-mqtt/server/v2@v2.7.9); an MQTT 3.1.1 subscriber observes
// the same message Publish alone would have delivered, with properties
// silently absent, matching AWS's own documented behavior ("For MQTT 3.1.1
// clients, user properties are silently dropped",
// SendDirectMessageInput.UserProperties doc).
func (b *Broker) PublishWithProperties(
	topic string, payload []byte, retain bool, qos byte, props iotdataplane.MQTT5Properties,
) error {
	s := b.server.Load()
	if s == nil {
		return ErrBrokerNotStarted
	}

	cl, ok := s.Clients.Get(mqtt.InlineClientId)
	if !ok {
		return ErrBrokerNotStarted
	}

	return s.InjectPacket(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Qos:    qos,
			Retain: retain,
		},
		TopicName:  topic,
		Payload:    payload,
		PacketID:   uint16(qos),
		Properties: mqtt5PropertiesToPacket(props),
	})
}

// mqtt5PropertiesToPacket converts an iotdataplane.MQTT5Properties into the
// equivalent mochi-mqtt packets.Properties. A zero-value field is left unset
// on the resulting packets.Properties, matching "not supplied".
func mqtt5PropertiesToPacket(props iotdataplane.MQTT5Properties) packets.Properties {
	pp := packets.Properties{
		ContentType:     props.ContentType,
		ResponseTopic:   props.ResponseTopic,
		CorrelationData: props.CorrelationData,
	}

	if props.MessageExpiry > 0 {
		// PublishInput.MessageExpiry is int64, the wire property is uint32; clamp defensively.
		if props.MessageExpiry <= math.MaxUint32 {
			pp.MessageExpiryInterval = uint32(props.MessageExpiry)
		} else {
			pp.MessageExpiryInterval = math.MaxUint32
		}
	}

	switch props.PayloadFormatIndicator {
	case "UTF8_DATA":
		pp.PayloadFormat = 1
		pp.PayloadFormatFlag = true
	case "UNSPECIFIED_BYTES":
		pp.PayloadFormatFlag = true
	}

	if len(props.UserProperties) > 0 {
		pp.User = make([]packets.UserProperty, 0, len(props.UserProperties))
		for _, up := range props.UserProperties {
			pp.User = append(pp.User, packets.UserProperty{Key: up.Key, Val: up.Value})
		}
	}

	return pp
}

// ClientSubscriptions implements iotdataplane.MQTTPublisher. It reads the
// live per-client subscription state mochi-mqtt tracks in
// Client.State.Subscriptions -- the only place gopherstack has real MQTT
// subscription data, since no other service parses SUBSCRIBE packets. Returns
// (nil, false) when the broker hasn't started, or has no live client with the
// given ID; a connected client with zero subscriptions returns a non-nil
// empty map and true.
func (b *Broker) ClientSubscriptions(clientID string) (map[string]byte, bool) {
	s := b.server.Load()
	if s == nil {
		return nil, false
	}

	cl, ok := s.Clients.Get(clientID)
	if !ok {
		return nil, false
	}

	all := cl.State.Subscriptions.GetAll()
	subs := make(map[string]byte, len(all))

	for filter, sub := range all {
		subs[filter] = sub.Qos
	}

	return subs, true
}

// SendToClient implements iotdataplane.MQTTPublisher. It writes a PUBLISH
// packet straight to clientID's live connection via Client.WritePacket,
// bypassing topic subscription matching entirely -- mirroring real AWS
// SendDirectMessage's documented "the receiving client does not need to
// subscribe to the topic" semantics. Returns ok=false (no error) when the
// broker hasn't started or has no live client with that ID: nothing was sent,
// and the caller decides how to degrade.
func (b *Broker) SendToClient(clientID, topic string, payload []byte, qos byte) (bool, error) {
	return b.SendToClientWithProperties(
		clientID,
		topic,
		payload,
		qos,
		iotdataplane.MQTT5Properties{},
	)
}

// ClientSession implements iotdataplane.MQTTPublisher from the live client's
// CONNECT-time properties and socket addresses.
func (b *Broker) ClientSession(clientID string) (iotdataplane.SessionInfo, bool) {
	s := b.server.Load()
	if s == nil {
		return iotdataplane.SessionInfo{}, false
	}

	cl, ok := s.Clients.Get(clientID)
	if !ok || cl.Closed() {
		return iotdataplane.SessionInfo{}, false
	}

	info := iotdataplane.SessionInfo{
		Clean:         cl.Properties.Clean,
		KeepAlive:     cl.State.Keepalive,
		RemoteAddr:    cl.Net.Remote,
		SessionExpiry: cl.Properties.Props.SessionExpiryInterval,
		ExpiryKnown:   cl.Properties.Props.SessionExpiryIntervalFlag,
	}

	if cl.Net.Conn != nil && cl.Net.Conn.LocalAddr() != nil {
		info.LocalAddr = cl.Net.Conn.LocalAddr().String()
	}

	return info, true
}

// DisconnectClient implements iotdataplane.MQTTPublisher; cleanSession drops stored session
// state and preventWill clears the Last Will so mochi-mqtt does not publish it.
func (b *Broker) DisconnectClient(clientID string, cleanSession, preventWill bool) (bool, error) {
	s := b.server.Load()
	if s == nil {
		return false, ErrBrokerNotStarted
	}

	cl, ok := s.Clients.Get(clientID)
	if !ok || cl.Closed() {
		return false, nil
	}

	if preventWill {
		atomic.StoreUint32(&cl.Properties.Will.Flag, 0)
	}

	if cleanSession {
		cl.Properties.Clean = true
		cl.Properties.Props.SessionExpiryInterval = 0
	}

	if cl.Properties.ProtocolVersion >= mqttV5 {
		if err := s.DisconnectClient(cl, packets.ErrAdministrativeAction); err != nil &&
			!errors.Is(err, packets.ErrAdministrativeAction) {
			return false, fmt.Errorf("iot broker: disconnect client %s: %w", clientID, err)
		}

		return true, nil
	}

	cl.Stop(packets.ErrAdministrativeAction)

	return true, nil
}

// SendToClientWithProperties implements iotdataplane.MQTTPublisher. It
// behaves like SendToClient but also attaches props as real MQTT5 packet
// properties -- see PublishWithProperties for the protocol-version encoding
// caveat, which applies here identically.
func (b *Broker) SendToClientWithProperties(
	clientID, topic string, payload []byte, qos byte, props iotdataplane.MQTT5Properties,
) (bool, error) {
	s := b.server.Load()
	if s == nil {
		return false, ErrBrokerNotStarted
	}

	cl, ok := s.Clients.Get(clientID)
	if !ok {
		return false, nil
	}

	err := cl.WritePacket(packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish,
			Qos:  qos,
		},
		TopicName:  topic,
		Payload:    payload,
		PacketID:   uint16(qos), // matches Server.Publish's own inline packet construction.
		Properties: mqtt5PropertiesToPacket(props),
	})
	if err != nil {
		return false, fmt.Errorf("iot broker: send to client %s: %w", clientID, err)
	}

	return true, nil
}

// ruleHook is a mochi-mqtt hook that evaluates IoT rules on every published message.
type ruleHook struct {
	mqtt.HookBase

	broker  *Broker
	backend *InMemoryBackend
	others  func() []*InMemoryBackend
	ctx     context.Context //nolint:containedctx // required to propagate broker lifecycle context into hook callbacks
}

// ID returns the hook identifier.
func (h *ruleHook) ID() string { return "iot-rule-hook" }

// Provides reports which hook events this hook handles.
func (h *ruleHook) Provides(b byte) bool {
	return b == mqtt.OnPublish
}

// OnPublish is called for every MQTT message published to the broker.
func (h *ruleHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	dispatcher := h.backend.GetDispatcher()
	log := logger.Load(h.ctx)
	hops := takeHops(&pk)
	received := time.Now()
	ruleName, ingestTopic, ingest := basicIngestTopic(pk.TopicName)

	known := false

	for _, rule := range h.allRules() {
		if ingest && rule.RuleName != ruleName {
			continue
		}

		known = true

		region, account := ruleRegionAccount(rule.ARN)
		msg := &ruleMessage{
			received: received, topic: pk.TopicName, clientID: publisherID(pk.Origin),
			region: region, account: account, payload: pk.Payload, original: pk.Payload, hops: hops,
			hook: h, props: packetProps(pk), sourceIP: sourceIPOf(cl), traceID: traceIDOf(pk),
		}

		if ingest {
			msg.topic, msg.ingest = ingestTopic, true
		}

		if !rule.fire(msg) {
			if msg.fatal != nil {
				log.Warn("iot rule sql function failed", "rule", rule.RuleName, "reason", msg.fatal.Error())
			}

			continue
		}

		log.Info("iot rule matched", "rule", rule.RuleName)

		h.dispatchActions(rule, dispatcher, msg)
	}

	if ingest {
		if !known {
			log.Warn("iot basic ingest names no rule")
		}

		pk.FixedHeader.Retain = false

		return pk, packets.CodeSuccessIgnore
	}

	return pk, nil
}

// basicIngestTopic splits $aws/rules/<rule>/<topic> into the rule name and the topic the rule sees.
func basicIngestTopic(topic string) (string, string, bool) {
	rest, ok := strings.CutPrefix(topic, basicIngestPrefix)
	if !ok {
		return "", "", false
	}

	name, remainder, _ := strings.Cut(rest, "/")
	if name == "" {
		return "", "", false
	}

	return name, remainder, true
}

func packetProps(pk packets.Packet) *mqttProps {
	if pk.Origin == mqtt.InlineClientId {
		return nil
	}

	p := &mqttProps{
		contentType: pk.Properties.ContentType, responseTopic: pk.Properties.ResponseTopic,
		correlation: pk.Properties.CorrelationData, utf8: pk.Properties.PayloadFormat == 1,
	}

	for _, u := range pk.Properties.User {
		p.user = append(p.user, userProp{key: u.Key, val: u.Val})
	}

	return p
}

// sourceIPOf is the publishing client's remote address without its port.
func sourceIPOf(cl *mqtt.Client) string {
	if cl == nil || cl.Net.Remote == "" {
		return ""
	}

	host, _, err := net.SplitHostPort(cl.Net.Remote)
	if err != nil {
		return ""
	}

	return host
}

// traceIDOf mints a trace id for messages a device published over MQTT.
func traceIDOf(pk packets.Packet) string {
	if pk.Origin == mqtt.InlineClientId {
		return ""
	}

	return uuid.NewString()
}

func publisherID(origin string) string {
	if origin == mqtt.InlineClientId {
		return ""
	}

	return origin
}

// allRules returns the home region's rules plus every regional sibling's.
func (h *ruleHook) allRules() []*TopicRule {
	rules := h.backend.GetRules()

	if h.others != nil {
		for _, ob := range h.others() {
			rules = append(rules, ob.GetRules()...)
		}
	}

	return rules
}
