package iot

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ErrBrokerNotStarted is returned when a publish is attempted before the broker is started.
var ErrBrokerNotStarted = errors.New("mqtt broker not started")

// Broker wraps a mochi-mqtt server to provide the IoT MQTT endpoint.
type Broker struct {
	// server is accessed atomically to avoid data races between Start and Publish.
	server  atomic.Pointer[mqtt.Server]
	backend *InMemoryBackend
	port    int
}

// NewBroker creates a new Broker using the given backend and port.
func NewBroker(backend *InMemoryBackend, port int) *Broker {
	return &Broker{
		backend: backend,
		port:    port,
	}
}

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
		backend: b.backend,
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

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-done:
			// Serve() returned; goroutine exits cleanly.
		}
	}()

	if err := s.Serve(); err != nil {
		return fmt.Errorf("iot broker: serve: %w", err)
	}

	return nil
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
		TopicName: topic,
		Payload:   payload,
		PacketID:  uint16(qos), // matches Server.Publish's own inline packet construction.
	})
	if err != nil {
		return false, fmt.Errorf("iot broker: send to client %s: %w", clientID, err)
	}

	return true, nil
}

// ruleHook is a mochi-mqtt hook that evaluates IoT rules on every published message.
type ruleHook struct {
	mqtt.HookBase

	backend *InMemoryBackend
	ctx     context.Context //nolint:containedctx // required to propagate broker lifecycle context into hook callbacks
}

// ID returns the hook identifier.
func (h *ruleHook) ID() string { return "iot-rule-hook" }

// Provides reports which hook events this hook handles.
func (h *ruleHook) Provides(b byte) bool {
	return b == mqtt.OnPublish
}

// OnPublish is called for every MQTT message published to the broker.
func (h *ruleHook) OnPublish(_ *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	dispatcher := h.backend.GetDispatcher()
	log := logger.Load(h.ctx)

	for _, rule := range h.backend.GetRules() {
		if !EvaluateRule(rule, pk.TopicName, pk.Payload) {
			continue
		}

		log.Info("iot rule matched", "rule", rule.RuleName, "topic", pk.TopicName)
		h.dispatchActions(rule, dispatcher, pk.Payload)
	}

	return pk, nil
}

func (h *ruleHook) dispatchActions(rule *TopicRule, dispatcher RuleDispatcher, payload []byte) {
	if dispatcher == nil {
		return
	}

	log := logger.Load(h.ctx)

	for _, action := range rule.Actions {
		if action.SQS != nil {
			if err := dispatcher.SendToSQS(action.SQS.QueueURL, string(payload)); err != nil {
				log.Error("iot sqs action failed", "rule", rule.RuleName, "error", err)
			}
		}

		if action.Lambda != nil {
			if err := dispatcher.InvokeLambda(h.ctx, action.Lambda.FunctionARN, payload); err != nil {
				log.Error("iot lambda action failed", "rule", rule.RuleName, "error", err)
			}
		}
	}
}
