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

// DeviceProfile contains LoRaWAN device profile settings.
type DeviceProfile struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags,omitempty"`
	Name      string            `json:"name"`
	ID        string            `json:"id"`
	ARN       string            `json:"arn"`
}

// FuotaTask represents a Firmware Update Over the Air (FUOTA) task.
type FuotaTask struct {
	CreatedAt           time.Time         `json:"createdAt"`
	Tags                map[string]string `json:"tags,omitempty"`
	Name                string            `json:"name"`
	ID                  string            `json:"id"`
	ARN                 string            `json:"arn"`
	Description         string            `json:"description,omitempty"`
	FirmwareUpdateImage string            `json:"firmwareUpdateImage,omitempty"`
	FirmwareUpdateRole  string            `json:"firmwareUpdateRole,omitempty"`
}

// MulticastGroup represents an IoT Wireless multicast group.
type MulticastGroup struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	ID          string            `json:"id"`
	ARN         string            `json:"arn"`
	Description string            `json:"description,omitempty"`
	Status      string            `json:"status"`
}

// NetworkAnalyzerConfig represents an IoT Wireless network analyzer configuration.
type NetworkAnalyzerConfig struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Name             string            `json:"name"`
	ARN              string            `json:"arn"`
	Description      string            `json:"description,omitempty"`
	WirelessDevices  []string          `json:"wirelessDevices,omitempty"`
	WirelessGateways []string          `json:"wirelessGateways,omitempty"`
}
