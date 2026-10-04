package iot

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

const (
	hopsProperty   = "x-gopherstack-republish-hops"
	maxTopicBytes  = 256
	maxQoSRepublic = 1
)

type republishWire struct {
	Headers *struct {
		ContentType            string `json:"contentType"`
		CorrelationData        string `json:"correlationData"`
		MessageExpiry          string `json:"messageExpiry"`
		PayloadFormatIndicator string `json:"payloadFormatIndicator"`
		ResponseTopic          string `json:"responseTopic"`
		UserProperties         []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"userProperties"`
	} `json:"headers"`
	RoleARN string `json:"roleArn"`
	Topic   string `json:"topic"`
	Qos     int32  `json:"qos"`
}

func (h *ruleHook) runRepublish(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w republishWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.Topic); err != nil {
		return err
	}

	if err := validateRepublishTopic(w.Topic); err != nil {
		return err
	}

	if msg.hops >= maxRepublishHop {
		return fmt.Errorf("%w: republish loop detected", errTemplate)
	}

	topicARN := arn.Build("iot", msg.region, msg.account, "topic/"+w.Topic)
	if err := h.allow(w.RoleARN, topicARN, "iot:Publish"); err != nil {
		return err
	}

	props, err := republishProperties(msg, w)
	if err != nil {
		return err
	}

	if h.broker == nil {
		return ErrBrokerNotStarted
	}

	qos := byte(min(max(w.Qos, 0), maxQoSRepublic))

	return h.broker.republish(w.Topic, msg.payload, qos, props, msg.hops+1)
}

func validateRepublishTopic(topic string) error {
	switch {
	case topic == "" || len(topic) > maxTopicBytes:
		return fmt.Errorf("%w: republish topic must be 1 to %d bytes", errTemplate, maxTopicBytes)
	case strings.HasPrefix(topic, "$"):
		return fmt.Errorf("%w: republish to a reserved topic", errTemplate)
	case strings.ContainsAny(topic, "+#"):
		return fmt.Errorf("%w: republish topic cannot hold wildcards", errTemplate)
	default:
		return nil
	}
}

func republishProperties(msg *ruleMessage, w republishWire) (iotdataplane.MQTT5Properties, error) {
	var props iotdataplane.MQTT5Properties

	if w.Headers == nil {
		return props, nil
	}

	h := w.Headers
	if err := msg.expandAll(&h.ContentType, &h.CorrelationData, &h.MessageExpiry,
		&h.PayloadFormatIndicator, &h.ResponseTopic); err != nil {
		return props, err
	}

	props.ContentType = h.ContentType
	props.ResponseTopic = h.ResponseTopic
	props.PayloadFormatIndicator = h.PayloadFormatIndicator

	if h.CorrelationData != "" {
		data, err := base64.StdEncoding.DecodeString(h.CorrelationData)
		if err != nil {
			data = []byte(h.CorrelationData)
		}

		props.CorrelationData = data
	}

	if h.MessageExpiry != "" {
		secs, err := strconv.ParseInt(h.MessageExpiry, 10, 64)
		if err != nil {
			return props, fmt.Errorf("%w: messageExpiry is not a number", errTemplate)
		}

		props.MessageExpiry = secs
	}

	for _, up := range h.UserProperties {
		k, v := up.Key, up.Value
		if err := msg.expandAll(&k, &v); err != nil {
			return props, err
		}

		props.UserProperties = append(props.UserProperties, iotdataplane.MQTT5UserProperty{Key: k, Value: v})
	}

	return props, nil
}

// republish injects a rule-published message tagged with its hop count so loops end.
func (b *Broker) republish(topic string, payload []byte, qos byte, props iotdataplane.MQTT5Properties, hops int) error {
	s := b.server.Load()
	if s == nil {
		return ErrBrokerNotStarted
	}

	cl, ok := s.Clients.Get(mqtt.InlineClientId)
	if !ok {
		return ErrBrokerNotStarted
	}

	pp := mqtt5PropertiesToPacket(props)
	pp.User = append(pp.User, packets.UserProperty{Key: hopsProperty, Val: strconv.Itoa(hops)})

	return s.InjectPacket(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
		TopicName:   topic,
		Payload:     payload,
		PacketID:    uint16(qos),
		Properties:  pp,
	})
}

// takeHops removes the republish marker from pk and returns the hop count it carried.
func takeHops(pk *packets.Packet) int {
	idx := slices.IndexFunc(pk.Properties.User, func(u packets.UserProperty) bool { return u.Key == hopsProperty })
	if idx < 0 {
		return 0
	}

	hops, _ := strconv.Atoi(pk.Properties.User[idx].Val)
	pk.Properties.User = slices.Concat(pk.Properties.User[:idx], pk.Properties.User[idx+1:])

	return hops
}
