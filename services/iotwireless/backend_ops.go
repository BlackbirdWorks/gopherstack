package iotwireless

import (
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// Sentinel errors for new backend operations.
var (
	// ErrPartnerAccountNotFound is returned when a partner account does not exist.
	ErrPartnerAccountNotFound = errors.New("ResourceNotFoundException: Partner account not found")
	// ErrImportTaskNotFound is returned when a wireless device import task does not exist.
	ErrImportTaskNotFound = errors.New("ResourceNotFoundException: Wireless device import task not found")
	// ErrGatewayTaskNotFound is returned when a wireless gateway task does not exist.
	ErrGatewayTaskNotFound = errors.New(
		"ResourceNotFoundException: Wireless gateway task not found",
	)
	// ErrGatewayTaskDefNotFound is returned when a wireless gateway task definition does not exist.
	ErrGatewayTaskDefNotFound = errors.New(
		"ResourceNotFoundException: Wireless gateway task definition not found",
	)
)

// GatewayTask represents a wireless gateway task.
type GatewayTask struct {
	WirelessGatewayID string
	TaskDefID         string
	Status            string
}

// GatewayTaskDefinition represents a wireless gateway task definition.
type GatewayTaskDefinition struct {
	ID              string
	ARN             string
	Name            string
	AutoCreateTasks bool
}

// --- FuotaTask extended operations ---

// StartFuotaTask sets the FUOTA task status to FUOTA_SESSION_STARTED.
func (b *InMemoryBackend) StartFuotaTask(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	ft, ok := b.fuotaTasks[key]
	if !ok {
		return ErrFuotaTaskNotFound
	}

	ft.FirmwareUpdateRole = "FUOTA_SESSION_STARTED" // reuse field to track status

	return nil
}

// DisassociateWirelessDeviceFromFuotaTask removes the association of a wireless device from a FUOTA task.
func (b *InMemoryBackend) DisassociateWirelessDeviceFromFuotaTask(fuotaTaskID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.fuotaTaskDevices, fuotaTaskID)

	return nil
}

// ListMulticastGroupsByFuotaTask returns multicast groups linked to a FUOTA task.
func (b *InMemoryBackend) ListMulticastGroupsByFuotaTask(
	accountID, region, fuotaTaskID string,
) []*MulticastGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	mgID, ok := b.fuotaTaskMulticast[fuotaTaskID]
	if !ok {
		return []*MulticastGroup{}
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: mgID}

	mg, ok := b.multicastGroups[key]
	if !ok {
		return []*MulticastGroup{}
	}

	return []*MulticastGroup{copyMulticastGroup(mg)}
}

// DisassociateMulticastGroupFromFuotaTask removes the association of a multicast group from a FUOTA task.
func (b *InMemoryBackend) DisassociateMulticastGroupFromFuotaTask(fuotaTaskID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.fuotaTaskMulticast, fuotaTaskID)

	return nil
}

// DisassociateWirelessDeviceFromMulticastGroup removes a device from a multicast group.
func (b *InMemoryBackend) DisassociateWirelessDeviceFromMulticastGroup(
	multicastGroupID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.multicastGroupDevices, multicastGroupID)

	return nil
}

// StartMulticastGroupSession marks a multicast group session as active.
func (b *InMemoryBackend) StartMulticastGroupSession(multicastGroupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.multicastGroupSessions[multicastGroupID] = true

	return nil
}

// --- WirelessGateway extended operations ---

// DisassociateWirelessGatewayFromCertificate clears the certificate association for a gateway.
func (b *InMemoryBackend) DisassociateWirelessGatewayFromCertificate(
	accountID, region, gatewayID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: gatewayID}
	if _, ok := b.gateways[key]; !ok {
		return ErrGatewayNotFound
	}

	delete(b.wirelessGatewayCerts, gatewayID)

	return nil
}

// DisassociateWirelessGatewayFromThing clears the thing association for a gateway.
func (b *InMemoryBackend) DisassociateWirelessGatewayFromThing(
	accountID, region, gatewayID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: gatewayID}
	if _, ok := b.gateways[key]; !ok {
		return ErrGatewayNotFound
	}

	delete(b.wirelessGatewayThings, gatewayID)

	return nil
}

// GetWirelessGatewayCertificate returns the certificate ID associated with a gateway.
func (b *InMemoryBackend) GetWirelessGatewayCertificate(
	accountID, region, gatewayID string,
) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: gatewayID}
	if _, ok := b.gateways[key]; !ok {
		return "", ErrGatewayNotFound
	}

	certID, ok := b.wirelessGatewayCerts[gatewayID]
	if !ok {
		return "", ErrGatewayNotFound
	}

	return certID, nil
}

// --- WirelessDevice extended operations ---

// UpdateWirelessDevice updates mutable fields on an existing wireless device.
func (b *InMemoryBackend) UpdateWirelessDevice(
	accountID, region, id, name, description, destinationName string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	d, ok := b.devices[key]
	if !ok {
		return ErrDeviceNotFound
	}

	if name != "" {
		d.Name = name
	}

	d.Description = description

	if destinationName != "" {
		d.DestinationName = destinationName
	}

	return nil
}

// DisassociateWirelessDeviceFromThing clears the thing association for a wireless device.
func (b *InMemoryBackend) DisassociateWirelessDeviceFromThing(
	accountID, region, wirelessDeviceID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: wirelessDeviceID}
	if _, ok := b.devices[key]; !ok {
		return ErrDeviceNotFound
	}

	delete(b.wirelessDeviceThings, wirelessDeviceID)

	return nil
}

// --- Partner account operations ---

// GetPartnerAccount returns the ARN for a partner account.
func (b *InMemoryBackend) GetPartnerAccount(partnerAccountID string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	arn, ok := b.partnerAccounts[partnerAccountID]
	if !ok {
		return "", ErrPartnerAccountNotFound
	}

	return arn, nil
}

// ListPartnerAccounts returns all partner account ARNs.
func (b *InMemoryBackend) ListPartnerAccounts() map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make(map[string]string, len(b.partnerAccounts))
	maps.Copy(result, b.partnerAccounts)

	return result
}

// DisassociateAwsAccountFromPartnerAccount removes a partner account association.
func (b *InMemoryBackend) DisassociateAwsAccountFromPartnerAccount(partnerAccountID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.partnerAccounts[partnerAccountID]; !ok {
		return ErrPartnerAccountNotFound
	}

	delete(b.partnerAccounts, partnerAccountID)

	return nil
}

// --- Log level operations ---

// GetLogLevelsByResourceTypes returns the default log level settings.
func (b *InMemoryBackend) GetLogLevelsByResourceTypes() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if level, ok := b.logLevels["default"]; ok {
		return level
	}

	return "INFO"
}

// UpdateLogLevelsByResourceTypes updates the default log level.
func (b *InMemoryBackend) UpdateLogLevelsByResourceTypes(defaultLogLevel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.logLevels["default"] = defaultLogLevel

	return nil
}

// ResetAllResourceLogLevels clears all resource-level log level overrides.
func (b *InMemoryBackend) ResetAllResourceLogLevels() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resourceLogLevels = make(map[string]string)

	return nil
}

// GetResourceLogLevel returns the log level for a specific resource.
func (b *InMemoryBackend) GetResourceLogLevel(resourceID string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if level, ok := b.resourceLogLevels[resourceID]; ok {
		return level
	}

	return "INFO"
}

// PutResourceLogLevel sets the log level for a specific resource.
func (b *InMemoryBackend) PutResourceLogLevel(resourceID, logLevel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resourceLogLevels[resourceID] = logLevel

	return nil
}

// ResetResourceLogLevel clears the log level override for a specific resource.
func (b *InMemoryBackend) ResetResourceLogLevel(resourceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.resourceLogLevels, resourceID)

	return nil
}

// --- Gateway task operations ---

// CreateWirelessGatewayTask creates a task for a wireless gateway.
func (b *InMemoryBackend) CreateWirelessGatewayTask(
	gatewayID, taskDefID string,
) (*GatewayTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	task := &GatewayTask{
		WirelessGatewayID: gatewayID,
		TaskDefID:         taskDefID,
		Status:            "PENDING",
	}

	b.gatewayTasks[gatewayID] = task

	return task, nil
}

// GetWirelessGatewayTask returns the task for a wireless gateway.
func (b *InMemoryBackend) GetWirelessGatewayTask(gatewayID string) (*GatewayTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	task, ok := b.gatewayTasks[gatewayID]
	if !ok {
		return nil, ErrGatewayTaskNotFound
	}

	cp := *task

	return &cp, nil
}

// DeleteWirelessGatewayTask removes the task for a wireless gateway.
func (b *InMemoryBackend) DeleteWirelessGatewayTask(gatewayID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.gatewayTasks[gatewayID]; !ok {
		return ErrGatewayTaskNotFound
	}

	delete(b.gatewayTasks, gatewayID)

	return nil
}

// CreateWirelessGatewayTaskDefinition creates a new gateway task definition.
func (b *InMemoryBackend) CreateWirelessGatewayTaskDefinition(
	accountID, region, name string,
	autoCreateTasks bool,
) (*GatewayTaskDefinition, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := wirelessGatewayTaskDefARN(accountID, region, id)

	def := &GatewayTaskDefinition{
		ID:              id,
		ARN:             arn,
		Name:            name,
		AutoCreateTasks: autoCreateTasks,
	}

	b.gatewayTaskDefs[id] = def

	return def, nil
}

// GetWirelessGatewayTaskDefinition returns a gateway task definition by ID.
func (b *InMemoryBackend) GetWirelessGatewayTaskDefinition(
	id string,
) (*GatewayTaskDefinition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	def, ok := b.gatewayTaskDefs[id]
	if !ok {
		return nil, ErrGatewayTaskDefNotFound
	}

	cp := *def

	return &cp, nil
}

// ListWirelessGatewayTaskDefinitions returns all gateway task definitions.
func (b *InMemoryBackend) ListWirelessGatewayTaskDefinitions() []*GatewayTaskDefinition {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*GatewayTaskDefinition, 0, len(b.gatewayTaskDefs))

	for _, def := range b.gatewayTaskDefs {
		cp := *def
		result = append(result, &cp)
	}

	return result
}

// DeleteWirelessGatewayTaskDefinition removes a gateway task definition.
func (b *InMemoryBackend) DeleteWirelessGatewayTaskDefinition(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.gatewayTaskDefs[id]; !ok {
		return ErrGatewayTaskDefNotFound
	}

	delete(b.gatewayTaskDefs, id)

	return nil
}

// wirelessGatewayTaskDefARN generates an ARN for a wireless gateway task definition.
func wirelessGatewayTaskDefARN(accountID, region, id string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("WirelessGatewayTaskDefinition/%s", id))
}

// --- Position operations ---

// GetPosition returns the position data for a resource.
func (b *InMemoryBackend) GetPosition(resourceID string) map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if pos, ok := b.positions[resourceID]; ok {
		result := make(map[string]any, len(pos))
		maps.Copy(result, pos)

		return result
	}

	return map[string]any{}
}

// UpdatePosition updates the position data for a resource.
func (b *InMemoryBackend) UpdatePosition(resourceID string, position map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	pos := make(map[string]any, len(position))
	maps.Copy(pos, position)
	b.positions[resourceID] = pos

	return nil
}

// --- Queued messages operations ---

// ListQueuedMessages returns queued messages for a wireless device.
func (b *InMemoryBackend) ListQueuedMessages(wirelessDeviceID string) []QueuedMessage {
	b.mu.RLock()
	defer b.mu.RUnlock()

	msgs, ok := b.queuedMessages[wirelessDeviceID]
	if !ok {
		return []QueuedMessage{}
	}

	result := make([]QueuedMessage, len(msgs))
	copy(result, msgs)

	return result
}

// DeleteQueuedMessages clears the message queue for a wireless device.
func (b *InMemoryBackend) DeleteQueuedMessages(wirelessDeviceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.queuedMessages, wirelessDeviceID)

	return nil
}

// QueuedMessage represents a downlink message queued for a wireless device.
type QueuedMessage struct {
	MessageID     string
	PayloadBase64 string
}

// wirelessDeviceImportTaskARN generates an ARN for a wireless device import task.
func wirelessDeviceImportTaskARN(region, accountID, id string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("ImportTask/%s", id))
}

// singleWirelessDeviceImportTaskARN generates an ARN for a single wireless device import task.
func singleWirelessDeviceImportTaskARN(region, accountID, id string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("ImportTask/%s", id))
}

// copyImportTask returns a shallow copy of a WirelessDeviceImportTask.
func copyImportTask(t *WirelessDeviceImportTask) *WirelessDeviceImportTask {
	cp := *t

	return &cp
}

// copySingleImportTask returns a shallow copy of a SingleWirelessDeviceImportTask.
func copySingleImportTask(t *SingleWirelessDeviceImportTask) *SingleWirelessDeviceImportTask {
	cp := *t

	return &cp
}

// --- Wireless Device Import Task operations ---

// StartWirelessDeviceImportTask creates a bulk wireless device import task.
func (b *InMemoryBackend) StartWirelessDeviceImportTask(
	accountID, region, destinationName string,
) (*WirelessDeviceImportTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := wirelessDeviceImportTaskARN(region, accountID, id)

	task := &WirelessDeviceImportTask{
		ID:              id,
		ARN:             arn,
		DestinationName: destinationName,
		Status:          "Initialized",
		CreatedAt:       time.Now(),
	}

	b.importTasks[id] = task

	return copyImportTask(task), nil
}

// StartSingleWirelessDeviceImportTask creates a single wireless device import task.
func (b *InMemoryBackend) StartSingleWirelessDeviceImportTask(
	accountID, region, destinationName string,
) (*SingleWirelessDeviceImportTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := singleWirelessDeviceImportTaskARN(region, accountID, id)
	wirelessDeviceID := uuid.NewString()

	task := &SingleWirelessDeviceImportTask{
		ARN:              arn,
		WirelessDeviceID: wirelessDeviceID,
		DestinationName:  destinationName,
		Status:           "Initialized",
		CreatedAt:        time.Now(),
	}

	b.singleImportTasks[arn] = task

	return copySingleImportTask(task), nil
}

// GetWirelessDeviceImportTask returns a wireless device import task by ID.
func (b *InMemoryBackend) GetWirelessDeviceImportTask(id string) (*WirelessDeviceImportTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	task, ok := b.importTasks[id]
	if !ok {
		return nil, ErrImportTaskNotFound
	}

	return copyImportTask(task), nil
}

// DeleteWirelessDeviceImportTask removes a wireless device import task.
func (b *InMemoryBackend) DeleteWirelessDeviceImportTask(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.importTasks[id]; !ok {
		return ErrImportTaskNotFound
	}

	delete(b.importTasks, id)

	return nil
}

// UpdateWirelessDeviceImportTask updates the destination name of a wireless device import task.
func (b *InMemoryBackend) UpdateWirelessDeviceImportTask(id, destinationName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	task, ok := b.importTasks[id]
	if !ok {
		return ErrImportTaskNotFound
	}

	if destinationName != "" {
		task.DestinationName = destinationName
	}

	return nil
}

// ListWirelessDeviceImportTasks returns all wireless device import tasks.
func (b *InMemoryBackend) ListWirelessDeviceImportTasks() []*WirelessDeviceImportTask {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*WirelessDeviceImportTask, 0, len(b.importTasks))

	for _, task := range b.importTasks {
		result = append(result, copyImportTask(task))
	}

	return result
}

// --- Metric configuration operations ---

func (b *InMemoryBackend) GetMetricConfiguration() map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make(map[string]any, len(b.metricConfig))
	maps.Copy(result, b.metricConfig)

	return result
}

func (b *InMemoryBackend) UpdateMetricConfiguration(config map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metricConfig = make(map[string]any, len(config))
	maps.Copy(b.metricConfig, config)

	return nil
}

// --- Event configuration by resource type ---

func (b *InMemoryBackend) GetEventConfigByResourceTypes() map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make(map[string]any, len(b.eventConfigByType))
	maps.Copy(result, b.eventConfigByType)

	return result
}

func (b *InMemoryBackend) UpdateEventConfigByResourceTypes(config map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.eventConfigByType = make(map[string]any, len(config))
	maps.Copy(b.eventConfigByType, config)

	return nil
}

// --- Per-resource event configuration ---

func (b *InMemoryBackend) ListResourceEventConfigs() []map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]map[string]any, 0, len(b.resourceEventConfigs))

	for _, cfg := range b.resourceEventConfigs {
		cp := make(map[string]any, len(cfg))
		maps.Copy(cp, cfg)
		result = append(result, cp)
	}

	return result
}

func (b *InMemoryBackend) GetResourceEventConfig(resourceID string) map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cfg, ok := b.resourceEventConfigs[resourceID]
	if !ok {
		return map[string]any{}
	}

	result := make(map[string]any, len(cfg))
	maps.Copy(result, cfg)

	return result
}

func (b *InMemoryBackend) UpdateResourceEventConfig(resourceID string, config map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := make(map[string]any, len(config))
	maps.Copy(cp, config)
	b.resourceEventConfigs[resourceID] = cp

	return nil
}

// --- Position configuration ---

func (b *InMemoryBackend) GetPositionConfig(resourceID string) map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cfg, ok := b.positionConfigs[resourceID]
	if !ok {
		return map[string]any{}
	}

	result := make(map[string]any, len(cfg))
	maps.Copy(result, cfg)

	return result
}

func (b *InMemoryBackend) PutPositionConfig(resourceID string, config map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := make(map[string]any, len(config))
	maps.Copy(cp, config)
	b.positionConfigs[resourceID] = cp

	return nil
}

func (b *InMemoryBackend) ListPositionConfigs() []map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]map[string]any, 0, len(b.positionConfigs))

	for id, cfg := range b.positionConfigs {
		cp := make(map[string]any, len(cfg)+1)
		maps.Copy(cp, cfg)
		cp["ResourceIdentifier"] = id
		result = append(result, cp)
	}

	return result
}

// --- Downlink message queue ---

func (b *InMemoryBackend) EnqueueDownlinkMessage(wirelessDeviceID, messageID, payloadBase64 string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queuedMessages[wirelessDeviceID] = append(
		b.queuedMessages[wirelessDeviceID],
		QueuedMessage{
			MessageID:     messageID,
			PayloadBase64: payloadBase64,
		},
	)

	return nil
}
