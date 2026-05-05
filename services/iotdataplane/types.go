// Package iotdataplane provides the IoT Data Plane HTTP API for publishing
// messages directly to MQTT topics.
package iotdataplane

import "time"

// RetainedMessage holds the details of a retained MQTT message stored by IoT.
type RetainedMessage struct {
	Topic            string
	Payload          []byte
	Qos              int32
	LastModifiedTime int64 // epoch milliseconds
}

// Connection represents a registered MQTT client connection.
type Connection struct {
	ClientID    string    `json:"clientId"`
	SourceIP    string    `json:"sourceIp,omitempty"`
	ConnectedAt time.Time `json:"connectedAt"`
}
