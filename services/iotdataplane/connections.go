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
// ResourceNotFoundException for ListSubscriptions too). gopherstack does not
// track live MQTT subscription state for any client -- the real subscription
// data lives inside the mochi-mqtt broker (services/iot/broker.go)'s
// per-client session state, which is reachable only through the
// MQTTPublisher interface boundary this package doesn't own (see PARITY.md
// gaps) -- so a tracked client always yields an honestly empty subscription
// list; this method exists purely so the HTTP layer can return 404 for a
// clientID gopherstack has never heard of, rather than a misleadingly
// successful empty response for literally any string.
func (b *InMemoryBackend) ListSubscriptions(clientID string) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	b.mu.RLock("ListSubscriptions")
	defer b.mu.RUnlock()

	if !b.connections.Has(clientID) {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, clientID)
	}

	return nil
}

// SendDirectMessage delivers payload to topic via the same broker-backed
// delivery path as Publish (see InMemoryBackend.Publish), after confirming
// clientID is a tracked connection (ErrConnectionNotFound otherwise, matching
// GetConnection/ListSubscriptions/DeleteConnection).
//
// Real AWS SendDirectMessage delivers to the target client specifically --
// "the receiving client does not need to subscribe to the topic" -- but
// gopherstack's MQTTPublisher interface (interfaces.go) only exposes
// topic-broadcast Publish(); there is no per-client-targeted send available
// without extending the broker (services/iot/broker.go), which is out of
// this package's scope. Broadcasting on topic is the closest honest
// approximation: any live subscriber of topic (including the target client,
// if it happens to be subscribed) really does observe the message the same
// way a Publish would deliver it, which is far better than writing to a
// dead-end store no caller could ever observe. This is a documented,
// deliberate divergence from real per-client delivery -- see PARITY.md gaps.
func (b *InMemoryBackend) SendDirectMessage(clientID, topic string, payload []byte, qos int32) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	b.mu.RLock("SendDirectMessage")
	connected := b.connections.Has(clientID)
	b.mu.RUnlock()

	if !connected {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, clientID)
	}

	return b.Publish(topic, payload, qos, false)
}
