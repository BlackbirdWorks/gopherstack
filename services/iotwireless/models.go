package iotwireless

import "time"

// WirelessDevice represents a LoRaWAN or Sidewalk wireless device.
type WirelessDevice struct {
	CreatedAt            time.Time         `json:"createdAt"`
	LastUplinkReceivedAt *time.Time        `json:"lastUplinkReceivedAt,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	Name                 string            `json:"name"`
	ID                   string            `json:"id"`
	ARN                  string            `json:"arn"`
	Description          string            `json:"description,omitempty"`
	Type                 string            `json:"type"`
	DestinationName      string            `json:"destinationName,omitempty"`
}

// WirelessGateway represents a LoRaWAN gateway.
type WirelessGateway struct {
	CreatedAt            time.Time         `json:"createdAt"`
	LastUplinkReceivedAt *time.Time        `json:"lastUplinkReceivedAt,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	Name                 string            `json:"name"`
	ID                   string            `json:"id"`
	ARN                  string            `json:"arn"`
	Description          string            `json:"description,omitempty"`
	ConnectionStatus     string            `json:"connectionStatus,omitempty"`
	FirmwareVersion      string            `json:"firmwareVersion,omitempty"`
	FirmwareModel        string            `json:"firmwareModel,omitempty"`
	FirmwareStation      string            `json:"firmwareStation,omitempty"`
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

// WirelessDeviceImportTask represents an IoT Wireless device bulk-import task.
type WirelessDeviceImportTask struct {
	CreatedAt                      time.Time `json:"createdAt"`
	ID                             string    `json:"id"`
	ARN                            string    `json:"arn"`
	DestinationName                string    `json:"destinationName"`
	Status                         string    `json:"status"`
	StatusReason                   string    `json:"statusReason,omitempty"`
	InitializedImportedDeviceCount int64     `json:"initializedImportedDeviceCount"`
	PendingImportedDeviceCount     int64     `json:"pendingImportedDeviceCount"`
	OnboardedImportedDeviceCount   int64     `json:"onboardedImportedDeviceCount"`
	FailedImportedDeviceCount      int64     `json:"failedImportedDeviceCount"`
}

// SingleWirelessDeviceImportTask represents an IoT Wireless single-device import task.
type SingleWirelessDeviceImportTask struct {
	CreatedAt        time.Time `json:"createdAt"`
	ARN              string    `json:"arn"`
	WirelessDeviceID string    `json:"wirelessDeviceId"`
	DestinationName  string    `json:"destinationName"`
	Status           string    `json:"status"`
}

// PositionConfigEntry represents a stored position configuration for a
// wireless device or gateway resource, as set via PutPositionConfiguration.
type PositionConfigEntry struct {
	Solvers            map[string]any `json:"solvers,omitempty"`
	ResourceIdentifier string         `json:"resourceIdentifier"`
	ResourceType       string         `json:"resourceType"`
	Destination        string         `json:"destination,omitempty"`
}

// EventConfigDoc is an event-notification configuration document keyed by
// AWS's five event-type fields. Each value is stored and echoed back exactly
// as submitted by the client (real mutate/read state, not fabricated).
type EventConfigDoc struct {
	ConnectionStatus        map[string]any `json:"ConnectionStatus,omitempty"`
	DeviceRegistrationState map[string]any `json:"DeviceRegistrationState,omitempty"`
	Join                    map[string]any `json:"Join,omitempty"`
	MessageDeliveryStatus   map[string]any `json:"MessageDeliveryStatus,omitempty"`
	Proximity               map[string]any `json:"Proximity,omitempty"`
}

// ResourceEventConfigEntry represents a stored per-resource event
// configuration, as set via UpdateResourceEventConfiguration.
type ResourceEventConfigEntry struct {
	Config         EventConfigDoc `json:"config"`
	Identifier     string         `json:"identifier"`
	IdentifierType string         `json:"identifierType"`
	PartnerType    string         `json:"partnerType,omitempty"`
}
