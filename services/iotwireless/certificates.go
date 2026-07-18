package iotwireless

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func iotCertificateARN(accountID, region, certID string) string {
	return arn.Build("iot", region, accountID, fmt.Sprintf("cert/%s", certID))
}

// AssociateWirelessGatewayWithCertificate associates a wireless gateway with an IoT certificate
// and returns the certificate ARN. Returns ErrGatewayNotFound when the gateway does not exist.
func (b *InMemoryBackend) AssociateWirelessGatewayWithCertificate(
	accountID, region, gatewayID, iotCertificateID string,
) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.gateways.Has(compositeKey(accountID, region, gatewayID)) {
		return "", ErrGatewayNotFound
	}

	b.wirelessGatewayCerts[gatewayID] = iotCertificateID
	certARN := iotCertificateARN(accountID, region, iotCertificateID)

	return certARN, nil
}

// DisassociateWirelessGatewayFromCertificate clears the certificate association for a gateway.
func (b *InMemoryBackend) DisassociateWirelessGatewayFromCertificate(
	accountID, region, gatewayID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.gateways.Has(compositeKey(accountID, region, gatewayID)) {
		return ErrGatewayNotFound
	}

	delete(b.wirelessGatewayCerts, gatewayID)

	return nil
}

// GetWirelessGatewayCertificate returns the certificate ID associated with a gateway.
func (b *InMemoryBackend) GetWirelessGatewayCertificate(
	accountID, region, gatewayID string,
) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.gateways.Has(compositeKey(accountID, region, gatewayID)) {
		return "", ErrGatewayNotFound
	}

	certID, ok := b.wirelessGatewayCerts[gatewayID]
	if !ok {
		return "", ErrGatewayNotFound
	}

	return certID, nil
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

	b.importTasks.Put(task)

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

	b.singleImportTasks.Put(task)

	return copySingleImportTask(task), nil
}

// GetWirelessDeviceImportTask returns a wireless device import task by ID.
func (b *InMemoryBackend) GetWirelessDeviceImportTask(id string) (*WirelessDeviceImportTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	task, ok := b.importTasks.Get(id)
	if !ok {
		return nil, ErrImportTaskNotFound
	}

	return copyImportTask(task), nil
}

// DeleteWirelessDeviceImportTask removes a wireless device import task.
func (b *InMemoryBackend) DeleteWirelessDeviceImportTask(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.importTasks.Delete(id) {
		return ErrImportTaskNotFound
	}

	return nil
}

// UpdateWirelessDeviceImportTask updates the destination name of a wireless device import task.
func (b *InMemoryBackend) UpdateWirelessDeviceImportTask(id, destinationName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	task, ok := b.importTasks.Get(id)
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

	all := b.importTasks.All()
	result := make([]*WirelessDeviceImportTask, 0, len(all))

	for _, task := range all {
		result = append(result, copyImportTask(task))
	}

	return result
}
