// Package iotdataplane provides the IoT Data Plane HTTP API for publishing
// messages directly to MQTT topics.
package iotdataplane

import "time"

// RetainedMessage holds the details of a retained MQTT message stored by IoT.
type RetainedMessage struct {
	Topic   string
	Payload []byte
	// UserProperties holds the raw (base64-decoded) bytes of the MQTT5 user
	// properties JSON array supplied on the Publish call that established
	// this retained value, or nil when none were set. Mirrors
	// GetRetainedMessageOutput.UserProperties in the real SDK.
	UserProperties   []byte
	Qos              int32
	LastModifiedTime int64 // epoch milliseconds
}

// Connection represents a registered MQTT client connection.
type Connection struct {
	ConnectedAt time.Time `json:"connectedAt"`
	ClientID    string    `json:"clientId"`
	SourceIP    string    `json:"sourceIp,omitempty"`
}
