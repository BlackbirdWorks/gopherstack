package iotdataplane

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// payloadFormatIndicatorUnspecifiedBytes and payloadFormatIndicatorUTF8Data
// are the only two values of the real SDK's PayloadFormatIndicator enum
// (aws-sdk-go-v2/service/iotdataplane/types.PayloadFormatIndicator).
const (
	payloadFormatIndicatorUnspecifiedBytes = "UNSPECIFIED_BYTES"
	payloadFormatIndicatorUTF8Data         = "UTF8_DATA"
)

// validatePayloadFormatIndicator checks that v (the X-Amz-Mqtt5-Payload-Format-Indicator
// header value) is empty or one of the real SDK's known enum values.
func validatePayloadFormatIndicator(v string) error {
	if v == "" || v == payloadFormatIndicatorUnspecifiedBytes ||
		v == payloadFormatIndicatorUTF8Data {
		return nil
	}

	return fmt.Errorf("%w: payloadFormatIndicator must be %s or %s",
		ErrValidation, payloadFormatIndicatorUnspecifiedBytes, payloadFormatIndicatorUTF8Data)
}

// validateResponseTopic checks that a responseTopic contains no wildcard
// characters, per AWS docs ("The topic must not contain wildcard characters").
// An empty responseTopic (not supplied) is always valid.
func validateResponseTopic(topic string) error {
	if topic == "" {
		return nil
	}

	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return fmt.Errorf("%w: responseTopic must not contain wildcards (# or +)", ErrValidation)
	}

	return nil
}

// parseMessageExpiry parses the messageExpiry query parameter (seconds).
// An empty string means "not supplied" (0, the SDK zero value meaning the
// message never expires). A negative or non-integer value is invalid.
func parseMessageExpiry(raw string) (int64, error) {
	if raw == "" {
		return 0, nil
	}

	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%w: messageExpiry must be a non-negative integer", ErrValidation)
	}

	return v, nil
}

// decodeUserProperties base64-decodes the X-Amz-Mqtt5-User-Properties header
// value. AWS requires callers to base64-encode this header ("If you don't use
// [the SDK] ... you must encode the JSON string to base64 format before adding
// it to the HTTP header"). Returns nil, nil when header is empty (not supplied).
func decodeUserProperties(header string) ([]byte, error) {
	if header == "" {
		// Absent header is a valid "not supplied" state, distinct from an error.
		return nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		return nil, fmt.Errorf("%w: userProperties header must be valid base64", ErrValidation)
	}

	return decoded, nil
}

// decodeCorrelationData base64-decodes the X-Amz-Mqtt5-Correlation-Data
// header value, mirroring decodeUserProperties: PublishInput.CorrelationData
// is documented as "The base64-encoded binary data used by the sender ...
// correlationData is an HTTP header value in the API" (api_op_Publish.go),
// so the header carries base64 text wrapping arbitrary binary MQTT5
// correlation data. Returns nil, nil when header is empty (not supplied).
func decodeCorrelationData(header string) ([]byte, error) {
	if header == "" {
		return nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		return nil, fmt.Errorf("%w: correlationData header must be valid base64", ErrValidation)
	}

	return decoded, nil
}

// parseUserProperties parses a decoded userProperties JSON blob (an array of
// single-key JSON objects, e.g. [{"deviceName":"alpha"},{"deviceCnt":"45"}],
// per PublishInput.UserProperties's doc comment) into individual key/value
// pairs for forwarding as real MQTT5 user properties. Returns nil, nil for
// an empty/absent blob.
func parseUserProperties(raw []byte) ([]MQTT5UserProperty, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var entries []map[string]string
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf(
			"%w: userProperties must be a JSON array of single-key objects",
			ErrValidation,
		)
	}

	out := make([]MQTT5UserProperty, 0, len(entries))

	for _, entry := range entries {
		if len(entry) != 1 {
			return nil, fmt.Errorf(
				"%w: each userProperties entry must have exactly one key",
				ErrValidation,
			)
		}

		for k, v := range entry {
			out = append(out, MQTT5UserProperty{Key: k, Value: v})
		}
	}

	return out, nil
}

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
		return fmt.Errorf(
			"%w: publishing to $aws/things/{name}/shadow/* is reserved for internal use",
			ErrValidation,
		)
	}

	return nil
}

// Publish delivers a message to the given MQTT topic, along with props (the
// optional MQTT5 packet properties PublishInput accepts) so live MQTT5
// subscribers receive them as real packet properties.
// If no broker is configured the call returns ErrNoBroker.
// The retain flag is forwarded to the broker so live subscribers receive RETAIN=1
// and the broker maintains retention canonically.
func (b *InMemoryBackend) Publish(
	topic string,
	payload []byte,
	qos int32,
	retain bool,
	props MQTT5Properties,
) error {
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

	return broker.PublishWithProperties(topic, payload, retain, qosByte, props)
}
