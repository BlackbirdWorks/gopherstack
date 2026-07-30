package iotdataplane

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// RegisterConnection adds a client connection to the backend.
// Returns ErrConnectionExists if the clientID is already registered.
// ClientIDs beginning with '$' are rejected per AWS rules.
func (b *InMemoryBackend) RegisterConnection(clientID, sourceIP string) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	if clientID == "" {
		return fmt.Errorf("%w: clientId is required", ErrValidation)
	}

	b.mu.Lock("RegisterConnection")
	defer b.mu.Unlock()

	if b.connections.Has(clientID) {
		return fmt.Errorf("%w: %s", ErrConnectionExists, clientID)
	}

	b.connections.Put(&connectionEntry{
		clientID:    clientID,
		connectedAt: time.Now(),
		sourceIP:    sourceIP,
	})

	return nil
}

// DeleteConnection disconnects a tracked MQTT client connection.
// Returns ErrConnectionNotFound if clientID has no tracked connection (real AWS
// models ResourceNotFoundException for this op -- see ErrConnectionNotFound).
// ClientIDs beginning with '$' are rejected per AWS rules.
func (b *InMemoryBackend) DeleteConnection(clientID string) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if !b.connections.Has(clientID) {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, clientID)
	}

	b.connections.Delete(clientID)

	return nil
}

// ListConnections returns all registered connections sorted by ConnectedAt ascending.
func (b *InMemoryBackend) ListConnections() []*Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	all := b.connections.All()
	out := make([]*Connection, 0, len(all))

	for _, entry := range all {
		out = append(out, &Connection{
			ClientID:    entry.clientID,
			SourceIP:    entry.sourceIP,
			ConnectedAt: entry.connectedAt,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ConnectedAt.Before(out[j].ConnectedAt)
	})

	return out
}

// AddConnectionInternal seeds a connected client ID for testing purposes.
func (b *InMemoryBackend) AddConnectionInternal(clientID string) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections.Put(&connectionEntry{clientID: clientID, connectedAt: time.Now()})
}

// GetConnection returns connection information for clientID. Returns
// ErrConnectionNotFound (real AWS models ResourceNotFoundException for
// GetConnection; confirmed via deserializers.go's
// awsRestjson1_deserializeOpErrorGetConnection case list) when clientID has
// no tracked connection.
//
// gopherstack's only concept of "connected" is the connections table
// populated by the gopherstack-only RegisterConnection admin extension (see
// admin-only-extensions family in PARITY.md) -- there is no real MQTT broker
// session tracking backing this. Fields the real GetConnectionOutput carries
// but this backend has no genuine data for (cleanSession, disconnectReason,
// disconnectedSince, keepAliveDuration, sessionExpiry, sourcePort, targetIp,
// targetPort, thingName, vpcEndpointId) are deliberately left unset on the
// returned Connection/omitted from the JSON response rather than fabricated
// -- see handleGetConnection.
func (b *InMemoryBackend) GetConnection(clientID string) (*Connection, error) {
	if strings.HasPrefix(clientID, "$") {
		return nil, fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	entry, ok := b.connections.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrConnectionNotFound, clientID)
	}

	return &Connection{
		ClientID:    entry.clientID,
		SourceIP:    entry.sourceIP,
		ConnectedAt: entry.connectedAt,
	}, nil
}

// ListSubscriptions validates that clientID is a tracked connection, mirroring
// GetConnection/DeleteConnection's not-found semantics (real AWS models
// ResourceNotFoundException for ListSubscriptions too), then reports the
// client's live MQTT subscriptions read straight off the mochi-mqtt broker's
// per-client session state (MQTTPublisher.ClientSubscriptions, implemented in
// services/iot/broker.go off cl.State.Subscriptions). gopherstack's own
// connections table (populated only via the admin-only RegisterConnection
// extension) is a distinct, weaker notion of "connected" than a real broker
// session -- a tracked clientID whose broker session the broker doesn't
// currently know about (never actually connected over MQTT, or no broker
// wired at all) genuinely has no subscriptions to report, so an empty list is
// returned honestly rather than fabricated.
func (b *InMemoryBackend) ListSubscriptions(clientID string) ([]SubscriptionSummary, error) {
	if strings.HasPrefix(clientID, "$") {
		return nil, fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	b.mu.RLock("ListSubscriptions")
	tracked := b.connections.Has(clientID)
	broker := b.broker
	b.mu.RUnlock()

	if !tracked {
		return nil, fmt.Errorf("%w: %s", ErrConnectionNotFound, clientID)
	}

	if broker == nil {
		return []SubscriptionSummary{}, nil
	}

	subs, _ := broker.ClientSubscriptions(clientID)
	out := make([]SubscriptionSummary, 0, len(subs))

	for filter, qos := range subs {
		out = append(out, SubscriptionSummary{TopicFilter: filter, QoS: qos})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].TopicFilter < out[j].TopicFilter })

	return out, nil
}

// SendDirectMessage delivers payload to topic, after confirming clientID is
// a tracked connection (ErrConnectionNotFound otherwise, matching
// GetConnection/ListSubscriptions/DeleteConnection).
//
// Real AWS SendDirectMessage delivers to the target client specifically --
// "the receiving client does not need to subscribe to the topic". When the
// mochi-mqtt broker has a live session for clientID, this now delivers via
// MQTTPublisher.SendToClient (services/iot/broker.go, cl.WritePacket), which
// really does write straight to that one client's connection regardless of
// its subscriptions -- true per-client delivery, matching AWS. gopherstack's
// own connections table (populated only via the admin-only
// RegisterConnection extension) is a distinct, weaker notion of "connected"
// than a real broker session, though: a clientID tracked there but with no
// live broker session (e.g. registered but never actually connected over
// MQTT) falls back to broadcasting on topic via Publish, the same honest
// best-effort approximation used before this method could address individual
// clients -- any live subscriber of topic really does observe the message.
// This fallback is a documented, deliberate divergence from real per-client
// delivery -- see PARITY.md gaps.
func (b *InMemoryBackend) SendDirectMessage(clientID, topic string, payload []byte, qos int32) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	b.mu.RLock("SendDirectMessage")
	tracked := b.connections.Has(clientID)
	broker := b.broker
	b.mu.RUnlock()

	if !tracked {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, clientID)
	}

	if broker == nil {
		return ErrNoBroker
	}

	var qosByte byte
	if qos > 0 {
		qosByte = 1
	}

	delivered, err := broker.SendToClient(clientID, topic, payload, qosByte)
	if err != nil {
		return err
	}

	if delivered {
		return nil
	}

	return broker.Publish(topic, payload, false, qosByte)
}
