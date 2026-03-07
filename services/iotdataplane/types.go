// Package iotdataplane provides the IoT Data Plane HTTP API for publishing
// messages directly to MQTT topics.
package iotdataplane

// PublishInput is the request body for the Publish operation.
type PublishInput struct {
	Payload string `json:"payload"`
}
