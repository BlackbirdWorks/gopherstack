// Package iotdataplane provides the IoT Data Plane HTTP API for publishing
// messages directly to MQTT topics.
package iotdataplane

// RetainedMessage holds the details of a retained MQTT message stored by IoT.
type RetainedMessage struct {
	Topic            string
	Payload          []byte
	Qos              int32
	LastModifiedTime int64 // epoch milliseconds
}
