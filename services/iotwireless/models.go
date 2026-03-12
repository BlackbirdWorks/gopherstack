package iotwireless

import "time"

// WirelessDevice represents a LoRaWAN or Sidewalk wireless device.
type WirelessDevice struct {
	CreatedAt       time.Time
	Tags            map[string]string
	Name            string
	ID              string
	ARN             string
	Description     string
	Type            string
	DestinationName string
}

// WirelessGateway represents a LoRaWAN gateway.
type WirelessGateway struct {
	CreatedAt   time.Time
	Tags        map[string]string
	Name        string
	ID          string
	ARN         string
	Description string
}

// ServiceProfile contains settings for a LoRaWAN service profile.
type ServiceProfile struct {
	CreatedAt time.Time
	Tags      map[string]string
	Name      string
	ID        string
	ARN       string
}

// Destination routes messages from a device to AWS services.
type Destination struct {
	CreatedAt      time.Time
	Tags           map[string]string
	Name           string
	ARN            string
	Expression     string
	ExpressionType string
	RoleArn        string
	Description    string
}
