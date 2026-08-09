package iotwireless

import "time"

// WirelessDevice represents a LoRaWAN or Sidewalk wireless device.
type WirelessDevice struct {
	CreatedAt            time.Time         `json:"createdAt"`
	LastUplinkReceivedAt *time.Time        `json:"lastUplinkReceivedAt,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	// LoRaWAN uses the types.LoRaWANDevice shape shared by
	// CreateWirelessDevice's request and GetWirelessDevice's response.
	// Sidewalk is stored as the richer types.SidewalkDevice (get) shape;
	// CreateWirelessDevice's narrower SidewalkCreateWirelessDevice request is
	// converted into it via sidewalkDeviceFromCreate (lorawan_types.go).
	LoRaWAN         *LoRaWANDevice  `json:"loRaWAN,omitempty"`
	Sidewalk        *SidewalkDevice `json:"sidewalk,omitempty"`
	Positioning     string          `json:"positioning,omitempty"`
	Name            string          `json:"name"`
	ID              string          `json:"id"`
	ARN             string          `json:"arn"`
	Description     string          `json:"description,omitempty"`
	Type            string          `json:"type"`
	DestinationName string          `json:"destinationName,omitempty"`
	// AccountID and Region are not part of the AWS wire shape for this
	// resource; they exist purely so store.Table's keyFn (store_setup.go) can
	// derive the account+region-scoped composite key this backend requires.
	// Tagged json:"-" so they never leak into an API response; Snapshot/
	// Restore round-trips them via the deviceSnapshot DTO instead (see
	// persistence.go).
	AccountID string `json:"-"`
	Region    string `json:"-"`
}

// WirelessGateway represents a LoRaWAN gateway.
type WirelessGateway struct {
	CreatedAt            time.Time         `json:"createdAt"`
	LastUplinkReceivedAt *time.Time        `json:"lastUplinkReceivedAt,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	// LoRaWAN uses the types.LoRaWANGateway shape shared by
	// CreateWirelessGateway/GetWirelessGateway.
	LoRaWAN          *LoRaWANGateway `json:"loRaWAN,omitempty"`
	Name             string          `json:"name"`
	ID               string          `json:"id"`
	ARN              string          `json:"arn"`
	Description      string          `json:"description,omitempty"`
	ConnectionStatus string          `json:"connectionStatus,omitempty"`
	FirmwareVersion  string          `json:"firmwareVersion,omitempty"`
	FirmwareModel    string          `json:"firmwareModel,omitempty"`
	FirmwareStation  string          `json:"firmwareStation,omitempty"`
	// AccountID and Region exist purely for store.Table's keyFn; see the
	// identical comment on WirelessDevice above.
	AccountID string `json:"-"`
	Region    string `json:"-"`
}

// ServiceProfile contains settings for a LoRaWAN service profile.
type ServiceProfile struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags,omitempty"`
	// LoRaWAN holds the create-shape LoRaWANServiceProfile; GetServiceProfile
	// converts it to the wider LoRaWANGetServiceProfileInfo via
	// loRaWANGetServiceProfileInfoFrom (lorawan_types.go).
	LoRaWAN *LoRaWANServiceProfile `json:"loRaWAN,omitempty"`
	Name    string                 `json:"name"`
	ID      string                 `json:"id"`
	ARN     string                 `json:"arn"`
	// AccountID and Region exist purely for store.Table's keyFn; see the
	// identical comment on WirelessDevice above.
	AccountID string `json:"-"`
	Region    string `json:"-"`
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
	// AccountID and Region exist purely for store.Table's keyFn; see the
	// identical comment on WirelessDevice above.
	AccountID string `json:"-"`
	Region    string `json:"-"`
}

// DeviceProfile contains LoRaWAN device profile settings.
type DeviceProfile struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags,omitempty"`
	// LoRaWAN uses the types.LoRaWANDeviceProfile shape shared verbatim by
	// Create and Get. Sidewalk is stored as the richer SidewalkGetDeviceProfile
	// (get) shape; CreateDeviceProfile's SidewalkCreateDeviceProfile request
	// (no fields of its own) is converted via sidewalkGetDeviceProfileFromCreate.
	LoRaWAN  *LoRaWANDeviceProfile     `json:"loRaWAN,omitempty"`
	Sidewalk *SidewalkGetDeviceProfile `json:"sidewalk,omitempty"`
	Name     string                    `json:"name"`
	ID       string                    `json:"id"`
	ARN      string                    `json:"arn"`
	// AccountID and Region exist purely for store.Table's keyFn; see the
	// identical comment on WirelessDevice above.
	AccountID string `json:"-"`
	Region    string `json:"-"`
}

// FuotaTask represents a Firmware Update Over the Air (FUOTA) task.
type FuotaTask struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags,omitempty"`
	// LoRaWAN holds the create/update-shape LoRaWANFuotaTask; GetFuotaTask
	// converts it to LoRaWANFuotaTaskGetInfo via loRaWANFuotaTaskGetInfoFrom
	// (lorawan_types.go).
	LoRaWAN             *LoRaWANFuotaTask `json:"loRaWAN,omitempty"`
	Name                string            `json:"name"`
	ID                  string            `json:"id"`
	ARN                 string            `json:"arn"`
	Description         string            `json:"description,omitempty"`
	FirmwareUpdateImage string            `json:"firmwareUpdateImage,omitempty"`
	FirmwareUpdateRole  string            `json:"firmwareUpdateRole,omitempty"`
	// Descriptor is the base64-encoded firmware metadata blob.
	Descriptor string `json:"descriptor,omitempty"`
	// Status tracks the FUOTA task lifecycle (Pending, FuotaSession_Waiting,
	// In_FuotaSession, FuotaDone, Delete_Waiting), matching
	// types.FuotaTaskStatus. Set to "Pending" at creation and advanced by
	// StartFuotaTask; previously this was fabricated by overwriting
	// FirmwareUpdateRole, corrupting that field -- see StartFuotaTask.
	Status string `json:"status,omitempty"`
	// AccountID and Region exist purely for store.Table's keyFn; see the
	// identical comment on WirelessDevice above.
	AccountID          string `json:"-"`
	Region             string `json:"-"`
	FragmentIntervalMS int32  `json:"fragmentIntervalMs,omitempty"`
	FragmentSizeBytes  int32  `json:"fragmentSizeBytes,omitempty"`
	RedundancyPercent  int32  `json:"redundancyPercent,omitempty"`
}

// MulticastGroup represents an IoT Wireless multicast group.
type MulticastGroup struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags,omitempty"`
	// LoRaWAN holds the create/update-shape LoRaWANMulticast; GetMulticastGroup
	// converts it to LoRaWANMulticastGet via loRaWANMulticastGetFrom
	// (lorawan_types.go), adding the real device-association count.
	LoRaWAN     *LoRaWANMulticast `json:"loRaWAN,omitempty"`
	Name        string            `json:"name"`
	ID          string            `json:"id"`
	ARN         string            `json:"arn"`
	Description string            `json:"description,omitempty"`
	Status      string            `json:"status"`
	// AccountID and Region exist purely for store.Table's keyFn; see the
	// identical comment on WirelessDevice above.
	AccountID string `json:"-"`
	Region    string `json:"-"`
}

// NetworkAnalyzerConfig represents an IoT Wireless network analyzer configuration.
type NetworkAnalyzerConfig struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Name             string            `json:"name"`
	ARN              string            `json:"arn"`
	Description      string            `json:"description,omitempty"`
	AccountID        string            `json:"-"`
	Region           string            `json:"-"`
	TraceContent     *TraceContent     `json:"traceContent,omitempty"`
	WirelessDevices  []string          `json:"wirelessDevices,omitempty"`
	WirelessGateways []string          `json:"wirelessGateways,omitempty"`
	MulticastGroups  []string          `json:"multicastGroups,omitempty"`
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
