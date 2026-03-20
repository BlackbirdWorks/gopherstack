package iotwireless

import "time"

// WirelessDevice represents a LoRaWAN or Sidewalk wireless device.
type WirelessDevice struct {
	CreatedAt       time.Time         `json:"createdAt"`
	Tags            map[string]string `json:"tags,omitempty"`
	Name            string            `json:"name"`
	ID              string            `json:"id"`
	ARN             string            `json:"arn"`
	Description     string            `json:"description,omitempty"`
	Type            string            `json:"type"`
	DestinationName string            `json:"destinationName,omitempty"`
}

// WirelessGateway represents a LoRaWAN gateway.
type WirelessGateway struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	ID          string            `json:"id"`
	ARN         string            `json:"arn"`
	Description string            `json:"description,omitempty"`
}

// ServiceProfile contains settings for a LoRaWAN service profile.
type ServiceProfile struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags,omitempty"`
	Name      string            `json:"name"`
	ID        string            `json:"id"`
	ARN       string            `json:"arn"`
}

// Destination routes messages from a device to AWS services.
type Destination struct {
	CreatedAt      time.Time         `json:"createdAt"`
	Tags           map[string]string `json:"tags,omitempty"`
	Name           string            `json:"name"`
	ARN            string            `json:"arn"`
	Expression     string            `json:"expression,omitempty"`
	ExpressionType string            `json:"expressionType,omitempty"`
	RoleArn        string            `json:"roleArn,omitempty"`
	Description    string            `json:"description,omitempty"`
}
