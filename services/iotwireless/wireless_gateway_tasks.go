package iotwireless

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

	b.gatewayTasks.Put(task)

	return task, nil
}

// GetWirelessGatewayTask returns the task for a wireless gateway.
func (b *InMemoryBackend) GetWirelessGatewayTask(gatewayID string) (*GatewayTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	task, ok := b.gatewayTasks.Get(gatewayID)
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

	if !b.gatewayTasks.Delete(gatewayID) {
		return ErrGatewayTaskNotFound
	}

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

	b.gatewayTaskDefs.Put(def)

	return def, nil
}

// GetWirelessGatewayTaskDefinition returns a gateway task definition by ID.
func (b *InMemoryBackend) GetWirelessGatewayTaskDefinition(
	id string,
) (*GatewayTaskDefinition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	def, ok := b.gatewayTaskDefs.Get(id)
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

	all := b.gatewayTaskDefs.All()
	result := make([]*GatewayTaskDefinition, 0, len(all))

	for _, def := range all {
		cp := *def
		result = append(result, &cp)
	}

	return result
}

// DeleteWirelessGatewayTaskDefinition removes a gateway task definition.
func (b *InMemoryBackend) DeleteWirelessGatewayTaskDefinition(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.gatewayTaskDefs.Delete(id) {
		return ErrGatewayTaskDefNotFound
	}

	return nil
}

// wirelessGatewayTaskDefARN generates an ARN for a wireless gateway task definition.
func wirelessGatewayTaskDefARN(accountID, region, id string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("WirelessGatewayTaskDefinition/%s", id))
}
