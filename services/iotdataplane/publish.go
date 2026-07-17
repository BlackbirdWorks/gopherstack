package iotdataplane

import (
	"fmt"
	"slices"
	"strings"
)

// validateTopic checks that a topic string conforms to MQTT publishing rules.
// Wildcards (# or +) are forbidden, empty levels are rejected, and each segment
// is validated for control characters. The reserved $aws/things/{name}/shadow/*
// prefix is blocked for external publishers to prevent spoofing shadow events.
func validateTopic(topic string) error {
	if len(topic) > maxTopicLength {
		return fmt.Errorf("%w: topic exceeds %d characters", ErrValidation, maxTopicLength)
	}

	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return fmt.Errorf("%w: topic must not contain wildcards (# or +)", ErrValidation)
	}

	segments := strings.Split(topic, "/")

	// Reject empty topic levels (consecutive slashes or leading/trailing slash).
	if slices.Contains(segments, "") {
		return fmt.Errorf("%w: topic must not contain empty levels", ErrValidation)
	}

	// Reject control characters (0x00–0x1F, 0x7F) in any segment.
	for _, seg := range segments {
		for _, ch := range seg {
			if ch < 0x20 || ch == 0x7F {
				return fmt.Errorf("%w: topic segment contains control character", ErrValidation)
			}
		}
	}

	// Gate $aws/things/{name}/shadow/* to internal callers; external publish would
	// spoof the shadow event topics reserved for the backend.
	if strings.HasPrefix(topic, "$aws/things/") && len(segments) >= 4 && segments[3] == "shadow" {
		return fmt.Errorf("%w: publishing to $aws/things/{name}/shadow/* is reserved for internal use", ErrValidation)
	}

	return nil
}

// Publish delivers a message to the given MQTT topic.
// If no broker is configured the call returns ErrNoBroker.
// The retain flag is forwarded to the broker so live subscribers receive RETAIN=1
// and the broker maintains retention canonically.
func (b *InMemoryBackend) Publish(topic string, payload []byte, qos int32, retain bool) error {
	b.mu.RLock("Publish")
	broker := b.broker
	b.mu.RUnlock()

	if broker == nil {
		return ErrNoBroker
	}

	// Clamp qos to valid MQTT range [0,1] before narrowing to byte.
	var qosByte byte
	if qos > 0 {
		qosByte = 1
	}

	return broker.Publish(topic, payload, retain, qosByte)
}
