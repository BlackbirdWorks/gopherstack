package iot

import (
	"encoding/json"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

const (
	presenceTopicPrefix      = eventsTopicPrefix + "presence/"
	subscriptionsTopicPrefix = eventsTopicPrefix + "subscriptions/"
	reasonDuplicateClientID  = "DUPLICATE_CLIENTID"
	mqttSubscribeFailure     = 0x80
)

// lifecycleSession is the identity a connection keeps for the life of its connect and disconnect events.
type lifecycleSession struct {
	id        string
	principal string
	version   int64
}

// publishEvent publishes a registry event; it is dropped while the broker is not serving.
func (b *Broker) publishEvent(topic string, payload []byte) {
	if ctx := b.runCtx.Load(); ctx == nil || (*ctx).Err() != nil {
		return
	}

	_ = b.Publish(topic, payload, false, 0)
}

// lifecycleEligible reports whether clientID may receive lifecycle events (not if it holds MQTT wildcards).
func lifecycleEligible(clientID string) bool {
	return clientID != "" && !strings.ContainsAny(clientID, "#+")
}

func (b *Broker) publishLifecycle(topic string, payload map[string]any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}

	b.publishEvent(topic, raw)
}

func (b *Broker) nextVersion(clientID string) int64 {
	v, _ := b.versions.LoadOrStore(clientID, new(atomic.Int64))
	counter, _ := v.(*atomic.Int64)

	return counter.Add(1) - 1
}

// principalIdentifier is the certificate ID for mutual-TLS connections and empty otherwise.
func (b *Broker) principalIdentifier(cl *mqtt.Client) string {
	if principal, _, _ := b.principalOf(cl); principal != "" {
		return principal[strings.LastIndex(principal, "/")+1:]
	}

	return ""
}

func (b *Broker) publishConnected(cl *mqtt.Client) {
	if !lifecycleEligible(cl.ID) {
		return
	}

	sess := &lifecycleSession{
		id: uuid.NewString(), principal: b.principalIdentifier(cl), version: b.nextVersion(cl.ID),
	}
	b.sessions.Store(cl, sess)

	b.publishLifecycle(presenceTopicPrefix+"connected/"+cl.ID, map[string]any{
		keyClientID:               cl.ID,
		keyTimestamp:              eventTimestamp(),
		keyEventType:              "connected",
		keySessionID:              sess.id,
		keyPrincipalID:            sess.principal,
		"ipAddress":               sourceIPOf(cl),
		jobsExecutionStateVersion: sess.version,
	})
}

func (b *Broker) session(cl *mqtt.Client) *lifecycleSession {
	if v, ok := b.sessions.Load(cl); ok {
		if sess, isSess := v.(*lifecycleSession); isSess {
			return sess
		}
	}

	return nil
}

func (b *Broker) publishDisconnected(cl *mqtt.Client, reason string) {
	v, loaded := b.sessions.LoadAndDelete(cl)
	if !loaded {
		return
	}

	sess, _ := v.(*lifecycleSession)
	if sess == nil {
		return
	}

	b.publishLifecycle(presenceTopicPrefix+"disconnected/"+cl.ID, map[string]any{
		keyClientID:                 cl.ID,
		keyTimestamp:                eventTimestamp(),
		keyEventType:                "disconnected",
		keySessionID:                sess.id,
		keyPrincipalID:              sess.principal,
		"clientInitiatedDisconnect": reason == "CLIENT_INITIATED_DISCONNECT",
		"disconnectReason":          reason,
		jobsExecutionStateVersion:   sess.version,
	})
}

func (b *Broker) publishConnectFailed(cl *mqtt.Client) {
	if !lifecycleEligible(cl.ID) {
		return
	}

	b.publishLifecycle(presenceTopicPrefix+"connect_failed/"+cl.ID, map[string]any{
		keyClientID:            cl.ID,
		keyTimestamp:           eventTimestamp(),
		keyEventType:           "connect_failed",
		"connectFailureReason": "AUTHORIZATION_FAILED",
		keyPrincipalID:         b.principalIdentifier(cl),
		keySessionID:           uuid.NewString(),
		"ipAddress":            sourceIPOf(cl),
	})
}

func (b *Broker) publishSubscription(cl *mqtt.Client, kind string, filters []packets.Subscription, codes []byte) {
	sess := b.session(cl)
	if sess == nil {
		return
	}

	topics := make([]string, 0, len(filters))

	for i, f := range filters {
		if kind == "subscribed" && i < len(codes) && codes[i] >= mqttSubscribeFailure {
			continue
		}

		topics = append(topics, f.Filter)
	}

	if len(topics) == 0 {
		return
	}

	b.publishLifecycle(subscriptionsTopicPrefix+kind+"/"+cl.ID, map[string]any{
		keyClientID:    cl.ID,
		keyTimestamp:   eventTimestamp(),
		keyEventType:   kind,
		keySessionID:   sess.id,
		keyPrincipalID: sess.principal,
		"topics":       topics,
	})
}

// OnSubscribed publishes the subscribed lifecycle event.
func (h *ruleHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	h.broker.publishSubscription(cl, "subscribed", pk.Filters, reasonCodes)
}

// OnUnsubscribed publishes the unsubscribed lifecycle event.
func (h *ruleHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	h.broker.publishSubscription(cl, "unsubscribed", pk.Filters, nil)
}
