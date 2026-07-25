package iotwireless

import "time"

// StorageBackend is the interface for the IoT Wireless backend.
type StorageBackend interface {
	Reset()

	CreateWirelessDevice(
		accountID, region, name, devType, destinationName, description, positioning string,
		loRaWAN, sidewalk map[string]any,
		tags map[string]string,
	) (*WirelessDevice, error)
	GetWirelessDevice(accountID, region, id string) (*WirelessDevice, error)
	ListWirelessDevices(accountID, region string) []*WirelessDevice
	DeleteWirelessDevice(accountID, region, id string) error

	CreateWirelessGateway(
		accountID, region, name, description string,
		loRaWAN map[string]any,
		tags map[string]string,
	) (*WirelessGateway, error)
	GetWirelessGateway(accountID, region, id string) (*WirelessGateway, error)
	ListWirelessGateways(accountID, region string) []*WirelessGateway
	DeleteWirelessGateway(accountID, region, id string) error

	CreateServiceProfile(
		accountID, region, name string,
		loRaWAN map[string]any,
		tags map[string]string,
	) (*ServiceProfile, error)
	GetServiceProfile(accountID, region, id string) (*ServiceProfile, error)
	ListServiceProfiles(accountID, region string) []*ServiceProfile
	DeleteServiceProfile(accountID, region, id string) error

	CreateDestination(
		accountID, region, name, expression, expressionType, roleArn, description string,
		tags map[string]string,
	) (*Destination, error)
	GetDestination(accountID, region, name string) (*Destination, error)
	ListDestinations(accountID, region string) []*Destination
	DeleteDestination(accountID, region, name string) error

	CreateDeviceProfile(
		accountID, region, name string,
		loRaWAN, sidewalk map[string]any,
		tags map[string]string,
	) (*DeviceProfile, error)
	GetDeviceProfile(accountID, region, id string) (*DeviceProfile, error)
	ListDeviceProfiles(accountID, region string) []*DeviceProfile
	DeleteDeviceProfile(accountID, region, id string) error

	CreateFuotaTask(
		accountID, region, name, description, firmwareUpdateImage, firmwareUpdateRole, descriptor string,
		fragmentIntervalMS, fragmentSizeBytes, redundancyPercent int32,
		loRaWAN map[string]any,
		tags map[string]string,
	) (*FuotaTask, error)
	GetFuotaTask(accountID, region, id string) (*FuotaTask, error)
	ListFuotaTasks(accountID, region string) []*FuotaTask
	DeleteFuotaTask(accountID, region, id string) error
	UpdateFuotaTask(accountID, region, id, name, description string) error

	UpdateWirelessGateway(accountID, region, id, name, description string, loRaWANUpdates map[string]any) error

	UpdateDestination(accountID, region, name, expression, expressionType, roleArn, description string) error

	CreateMulticastGroup(
		accountID, region, name, description string,
		loRaWAN map[string]any,
		tags map[string]string,
	) (*MulticastGroup, error)
	GetMulticastGroup(accountID, region, id string) (*MulticastGroup, error)
	ListMulticastGroups(accountID, region string) []*MulticastGroup
	DeleteMulticastGroup(accountID, region, id string) error
	UpdateMulticastGroup(accountID, region, id, name, description string) error

	CreateNetworkAnalyzerConfig(
		accountID, region, name, description string,
		wirelessDevices, wirelessGateways, multicastGroups []string,
		traceContent map[string]any,
		tags map[string]string,
	) (*NetworkAnalyzerConfig, error)
	GetNetworkAnalyzerConfig(accountID, region, name string) (*NetworkAnalyzerConfig, error)
	ListNetworkAnalyzerConfigs(accountID, region string) []*NetworkAnalyzerConfig
	DeleteNetworkAnalyzerConfig(accountID, region, name string) error
	UpdateNetworkAnalyzerConfig(
		accountID, region, name, description string,
		wirelessDevices, wirelessGateways []string,
		traceContent map[string]any,
	) error

	AssociateAwsAccountWithPartnerAccount(
		accountID, region, partnerAccountID string,
		tags map[string]string,
	) (string, error)
	AssociateMulticastGroupWithFuotaTask(fuotaTaskID, multicastGroupID string) error
	AssociateWirelessDeviceWithFuotaTask(fuotaTaskID, wirelessDeviceID string) error
	AssociateWirelessDeviceWithMulticastGroup(multicastGroupID, wirelessDeviceID string) error
	AssociateWirelessDeviceWithThing(accountID, region, wirelessDeviceID, thingArn string) error
	AssociateWirelessGatewayWithCertificate(accountID, region, gatewayID, iotCertificateID string) (string, error)
	AssociateWirelessGatewayWithThing(accountID, region, gatewayID, thingArn string) error
	CancelMulticastGroupSession(multicastGroupID string) error
	ListMulticastGroupDeviceIDs(multicastGroupID string) []string
	ListFuotaTaskDeviceIDs(fuotaTaskID string) []string
	StartBulkAssociateWirelessDeviceWithMulticastGroup(accountID, region, multicastGroupID string) error
	StartBulkDisassociateWirelessDeviceFromMulticastGroup(multicastGroupID string) error

	// GetWirelessDeviceThingArn returns the ARN of the IoT Thing associated
	// with a wireless device via AssociateWirelessDeviceWithThing, or "" if
	// none is associated. Used by GetWirelessDevice to surface ThingArn/
	// ThingName, matching real AWS's response shape.
	GetWirelessDeviceThingArn(wirelessDeviceID string) string
	// GetWirelessGatewayThingArn returns the ARN of the IoT Thing associated
	// with a wireless gateway via AssociateWirelessGatewayWithThing, or "" if
	// none is associated. Used by GetWirelessGateway to surface ThingArn/
	// ThingName, matching real AWS's response shape.
	GetWirelessGatewayThingArn(gatewayID string) string

	// Extended operations implemented across the various <family>.go files.
	StartFuotaTask(accountID, region, id string) error
	DisassociateWirelessDeviceFromFuotaTask(fuotaTaskID, wirelessDeviceID string) error
	ListMulticastGroupsByFuotaTask(accountID, region, fuotaTaskID string) []*MulticastGroup
	DisassociateMulticastGroupFromFuotaTask(fuotaTaskID, multicastGroupID string) error
	DisassociateWirelessDeviceFromMulticastGroup(multicastGroupID, wirelessDeviceID string) error
	StartMulticastGroupSession(multicastGroupID string) error
	GetMulticastGroupSession(multicastGroupID string) (time.Time, error)

	DisassociateWirelessGatewayFromCertificate(accountID, region, gatewayID string) error
	DisassociateWirelessGatewayFromThing(accountID, region, gatewayID string) error
	GetWirelessGatewayCertificate(accountID, region, gatewayID string) (string, error)

	UpdateWirelessDevice(
		accountID, region, id, name, description, destinationName, positioning string,
		loRaWAN, sidewalk map[string]any,
	) error
	DisassociateWirelessDeviceFromThing(accountID, region, wirelessDeviceID string) error

	GetPartnerAccount(partnerAccountID string) (string, error)
	ListPartnerAccounts() map[string]string
	DisassociateAwsAccountFromPartnerAccount(partnerAccountID string) error

	GetLogLevelsByResourceTypes() LogLevelsConfig
	UpdateLogLevelsByResourceTypes(cfg LogLevelsConfig) error
	ResetAllResourceLogLevels() error
	GetResourceLogLevel(resourceID string) string
	PutResourceLogLevel(resourceID, logLevel string) error
	ResetResourceLogLevel(resourceID string) error

	CreateWirelessGatewayTask(gatewayID, taskDefID string) (*GatewayTask, error)
	GetWirelessGatewayTask(gatewayID string) (*GatewayTask, error)
	DeleteWirelessGatewayTask(gatewayID string) error
	CreateWirelessGatewayTaskDefinition(
		accountID, region, name string,
		autoCreateTasks bool,
		update map[string]any,
	) (*GatewayTaskDefinition, error)
	GetWirelessGatewayTaskDefinition(id string) (*GatewayTaskDefinition, error)
	ListWirelessGatewayTaskDefinitions() []*GatewayTaskDefinition
	DeleteWirelessGatewayTaskDefinition(id string) error

	GetPosition(resourceID string) map[string]any
	UpdatePosition(resourceID string, position map[string]any) error

	PutPositionConfiguration(resourceID, resourceType, destination string, solvers map[string]any) error
	GetPositionConfiguration(resourceID string) (*PositionConfigEntry, bool)
	ListPositionConfigurations(resourceType string) []*PositionConfigEntry

	GetEventConfigurationByResourceTypes() *EventConfigDoc
	UpdateEventConfigurationByResourceTypes(doc *EventConfigDoc)
	ListEventConfigurations(resourceType string) []*ResourceEventConfigEntry
	GetResourceEventConfiguration(identifier string) (*ResourceEventConfigEntry, bool)
	UpdateResourceEventConfiguration(identifier, identifierType, partnerType string, doc *EventConfigDoc)

	GetMetricConfigurationStatus() string
	UpdateMetricConfigurationStatus(status string) error

	ListQueuedMessages(wirelessDeviceID string) []QueuedMessage
	DeleteQueuedMessages(wirelessDeviceID string) error
	EnqueueMessage(wirelessDeviceID string, msg QueuedMessage)

	StartWirelessDeviceImportTask(accountID, region, destinationName string) (*WirelessDeviceImportTask, error)
	StartSingleWirelessDeviceImportTask(
		accountID, region, destinationName string,
	) (*SingleWirelessDeviceImportTask, error)
	GetWirelessDeviceImportTask(id string) (*WirelessDeviceImportTask, error)
	DeleteWirelessDeviceImportTask(id string) error
	UpdateWirelessDeviceImportTask(id, destinationName string) error
	ListWirelessDeviceImportTasks() []*WirelessDeviceImportTask

	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, tagKeys []string) error
	ListTagsForResource(arn string) (map[string]string, error)
}

// Compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
