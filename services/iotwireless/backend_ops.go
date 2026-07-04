package iotwireless

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
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
	// ErrMulticastGroupSessionNotFound is returned when a multicast group has no active session.
	ErrMulticastGroupSessionNotFound = errors.New(
		"ResourceNotFoundException: Multicast group session not found",
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

// StartMulticastGroupSession marks a multicast group session as active,
// recording its start time so GetMulticastGroupSession can report it back.
func (b *InMemoryBackend) StartMulticastGroupSession(multicastGroupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.multicastGroupSessions[multicastGroupID] = true
	b.multicastGroupSessionStart[multicastGroupID] = time.Now().UTC()

	return nil
}

// GetMulticastGroupSession returns the start time of a multicast group's active
// session. Returns ErrMulticastGroupSessionNotFound if no session has been
// started (or it has since been cancelled), matching real AWS's
// ResourceNotFoundException for a group with no active session.
func (b *InMemoryBackend) GetMulticastGroupSession(multicastGroupID string) (time.Time, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.multicastGroupSessions[multicastGroupID] {
		return time.Time{}, ErrMulticastGroupSessionNotFound
	}

	return b.multicastGroupSessionStart[multicastGroupID], nil
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
	name string,
	autoCreateTasks bool,
) (*GatewayTaskDefinition, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := wirelessGatewayTaskDefARN(id)

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
func wirelessGatewayTaskDefARN(id string) string {
	return "arn:aws:iotwireless:us-east-1:000000000000:WirelessGatewayTaskDefinition/" + id
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

// EnqueueMessage appends a downlink message to a wireless device's message
// queue, so that a subsequent ListQueuedMessages reflects messages sent via
// SendDataToWirelessDevice.
func (b *InMemoryBackend) EnqueueMessage(wirelessDeviceID string, msg QueuedMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queuedMessages[wirelessDeviceID] = append(b.queuedMessages[wirelessDeviceID], msg)
}

// QueuedMessage represents a downlink message queued for a wireless device.
type QueuedMessage struct {
	ReceivedAt    time.Time
	MessageID     string
	PayloadBase64 string
	TransmitMode  int32
}

// wirelessDeviceImportTaskARN generates an ARN for a wireless device import task.
func wirelessDeviceImportTaskARN(region, accountID, id string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:ImportTask/%s", region, accountID, id)
}

// singleWirelessDeviceImportTaskARN generates an ARN for a single wireless device import task.
func singleWirelessDeviceImportTaskARN(region, accountID, id string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:ImportTask/%s", region, accountID, id)
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

// --- Position configuration operations ---

// annotateSemtechGnssSolver returns a copy of solvers with the constant
// Provider/Type fields ("Semtech"/"GNSS") injected into the SemtechGnss
// sub-object, matching AWS's behaviour of always reporting these values since
// Semtech GNSS is currently the only supported position solver.
func annotateSemtechGnssSolver(solvers map[string]any) map[string]any {
	if solvers == nil {
		return nil
	}

	out := make(map[string]any, len(solvers))
	maps.Copy(out, solvers)

	if sg, ok := out["SemtechGnss"].(map[string]any); ok {
		const injectedSolverFields = 2 // Provider + Type

		sgCopy := make(map[string]any, len(sg)+injectedSolverFields)
		maps.Copy(sgCopy, sg)
		sgCopy["Provider"] = "Semtech"
		sgCopy["Type"] = "GNSS"
		out["SemtechGnss"] = sgCopy
	}

	return out
}

// PutPositionConfiguration stores the position solver configuration for a
// resource.
func (b *InMemoryBackend) PutPositionConfiguration(
	resourceID, resourceType, destination string, solvers map[string]any,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.positionConfigs[resourceID] = &PositionConfigEntry{
		ResourceIdentifier: resourceID,
		ResourceType:       resourceType,
		Destination:        destination,
		Solvers:            annotateSemtechGnssSolver(solvers),
	}

	return nil
}

// GetPositionConfiguration returns the stored position configuration for a
// resource, if any.
func (b *InMemoryBackend) GetPositionConfiguration(resourceID string) (*PositionConfigEntry, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	e, ok := b.positionConfigs[resourceID]
	if !ok {
		return nil, false
	}

	cp := *e

	return &cp, true
}

// ListPositionConfigurations returns all stored position configurations,
// optionally filtered by resource type.
func (b *InMemoryBackend) ListPositionConfigurations(resourceType string) []*PositionConfigEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*PositionConfigEntry, 0, len(b.positionConfigs))

	for _, e := range b.positionConfigs {
		if resourceType != "" && e.ResourceType != resourceType {
			continue
		}

		cp := *e
		result = append(result, &cp)
	}

	slices.SortFunc(result, func(a, b *PositionConfigEntry) int {
		return cmp.Compare(a.ResourceIdentifier, b.ResourceIdentifier)
	})

	return result
}

// --- Event configuration operations ---

// GetEventConfigurationByResourceTypes returns the account-wide default event
// configuration. Returns an empty (zero-value) document if never configured.
func (b *InMemoryBackend) GetEventConfigurationByResourceTypes() *EventConfigDoc {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.eventConfigDefault == nil {
		return &EventConfigDoc{}
	}

	cp := *b.eventConfigDefault

	return &cp
}

// UpdateEventConfigurationByResourceTypes replaces the account-wide default
// event configuration.
func (b *InMemoryBackend) UpdateEventConfigurationByResourceTypes(doc *EventConfigDoc) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *doc
	b.eventConfigDefault = &cp
}

// GetResourceEventConfiguration returns the stored event configuration for a
// specific resource identifier, if any.
func (b *InMemoryBackend) GetResourceEventConfiguration(identifier string) (*ResourceEventConfigEntry, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	e, ok := b.resourceEventConfigs[identifier]
	if !ok {
		return nil, false
	}

	cp := *e

	return &cp, true
}

// UpdateResourceEventConfiguration stores the event configuration for a
// specific resource identifier.
func (b *InMemoryBackend) UpdateResourceEventConfiguration(
	identifier, identifierType, partnerType string, doc *EventConfigDoc,
) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resourceEventConfigs[identifier] = &ResourceEventConfigEntry{
		Identifier:     identifier,
		IdentifierType: identifierType,
		PartnerType:    partnerType,
		Config:         *doc,
	}
}

// ListEventConfigurations returns all stored per-resource event
// configurations, optionally filtered by resource type (matched as a prefix
// against the stored IdentifierType, e.g. "WirelessDevice" matches
// "WirelessDeviceId").
func (b *InMemoryBackend) ListEventConfigurations(resourceType string) []*ResourceEventConfigEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*ResourceEventConfigEntry, 0, len(b.resourceEventConfigs))

	for _, e := range b.resourceEventConfigs {
		if resourceType != "" && !strings.HasPrefix(e.IdentifierType, resourceType) {
			continue
		}

		cp := *e
		result = append(result, &cp)
	}

	slices.SortFunc(result, func(a, b *ResourceEventConfigEntry) int {
		return cmp.Compare(a.Identifier, b.Identifier)
	})

	return result
}

// --- Metric configuration operations ---

// GetMetricConfigurationStatus returns the account's summary metric
// aggregation status. Defaults to "Enabled" (AWS's documented default) when
// never explicitly configured.
func (b *InMemoryBackend) GetMetricConfigurationStatus() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.metricConfigStatus == "" {
		return "Enabled"
	}

	return b.metricConfigStatus
}

// UpdateMetricConfigurationStatus sets the account's summary metric
// aggregation status.
func (b *InMemoryBackend) UpdateMetricConfigurationStatus(status string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metricConfigStatus = status

	return nil
}
