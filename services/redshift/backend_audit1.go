package redshift

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ---- HSM Client Certificate ----

// CreateHsmClientCertificate creates a new HSM client certificate.
func (b *InMemoryBackend) CreateHsmClientCertificate(
	id string,
	tagMap map[string]string,
) (*HsmClientCertificate, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: HsmClientCertificateIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateHsmClientCertificate")
	defer b.mu.Unlock()

	if _, exists := b.hsmClientCerts[id]; exists {
		return nil, fmt.Errorf("%w: certificate %s already exists", ErrHsmClientCertAlreadyExists, id)
	}

	cert := &HsmClientCertificate{
		HsmClientCertificateIdentifier: id,
		HsmClientCertificatePublicKey:  "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA" + id + "\n-----END PUBLIC KEY-----",
		Tags:                           tagMap,
	}
	b.hsmClientCerts[id] = cert

	cp := *cert

	return &cp, nil
}

// DeleteHsmClientCertificate deletes the named HSM client certificate.
func (b *InMemoryBackend) DeleteHsmClientCertificate(id string) error {
	if id == "" {
		return fmt.Errorf("%w: HsmClientCertificateIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteHsmClientCertificate")
	defer b.mu.Unlock()

	if _, exists := b.hsmClientCerts[id]; !exists {
		return fmt.Errorf("%w: certificate %s not found", ErrHsmClientCertNotFound, id)
	}

	delete(b.hsmClientCerts, id)

	return nil
}

// DescribeHsmClientCertificates returns HSM client certificates, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeHsmClientCertificates(id string) ([]HsmClientCertificate, error) {
	b.mu.RLock("DescribeHsmClientCertificates")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.hsmClientCerts[id]
		if !exists {
			return nil, fmt.Errorf("%w: certificate %s not found", ErrHsmClientCertNotFound, id)
		}

		cp := *c

		return []HsmClientCertificate{cp}, nil
	}

	result := make([]HsmClientCertificate, 0, len(b.hsmClientCerts))

	for _, c := range b.hsmClientCerts {
		result = append(result, *c)
	}

	return result, nil
}

// ---- HSM Configuration ----

// CreateHsmConfiguration creates a new HSM configuration.
func (b *InMemoryBackend) CreateHsmConfiguration(
	id, description, hsmIpAddress, hsmPartitionName string,
	tagMap map[string]string,
) (*HsmConfiguration, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: HsmConfigurationIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateHsmConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.hsmConfigs[id]; exists {
		return nil, fmt.Errorf("%w: configuration %s already exists", ErrHsmConfigAlreadyExists, id)
	}

	cfg := &HsmConfiguration{
		HsmConfigurationIdentifier: id,
		Description:                description,
		HsmIpAddress:               hsmIpAddress,
		HsmPartitionName:           hsmPartitionName,
		Tags:                       tagMap,
	}
	b.hsmConfigs[id] = cfg

	cp := *cfg

	return &cp, nil
}

// DeleteHsmConfiguration deletes the named HSM configuration.
func (b *InMemoryBackend) DeleteHsmConfiguration(id string) error {
	if id == "" {
		return fmt.Errorf("%w: HsmConfigurationIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteHsmConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.hsmConfigs[id]; !exists {
		return fmt.Errorf("%w: configuration %s not found", ErrHsmConfigNotFound, id)
	}

	delete(b.hsmConfigs, id)

	return nil
}

// DescribeHsmConfigurations returns HSM configurations, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeHsmConfigurations(id string) ([]HsmConfiguration, error) {
	b.mu.RLock("DescribeHsmConfigurations")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.hsmConfigs[id]
		if !exists {
			return nil, fmt.Errorf("%w: configuration %s not found", ErrHsmConfigNotFound, id)
		}

		cp := *c

		return []HsmConfiguration{cp}, nil
	}

	result := make([]HsmConfiguration, 0, len(b.hsmConfigs))

	for _, c := range b.hsmConfigs {
		result = append(result, *c)
	}

	return result, nil
}

// ---- Scheduled Actions ----

// CreateScheduledAction creates a new Redshift scheduled action.
func (b *InMemoryBackend) CreateScheduledAction(
	name, schedule, iamRole, description, targetAction string,
) (*ScheduledAction, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ScheduledActionName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateScheduledAction")
	defer b.mu.Unlock()

	if _, exists := b.scheduledActions[name]; exists {
		return nil, fmt.Errorf("%w: scheduled action %s already exists", ErrScheduledActionAlreadyExists, name)
	}

	action := &ScheduledAction{
		ScheduledActionName:        name,
		Schedule:                   schedule,
		IamRole:                    iamRole,
		ScheduledActionDescription: description,
		State:                      "ACTIVE",
		TargetAction:               targetAction,
	}
	b.scheduledActions[name] = action

	cp := *action

	return &cp, nil
}

// DeleteScheduledAction deletes the named scheduled action.
func (b *InMemoryBackend) DeleteScheduledAction(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ScheduledActionName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteScheduledAction")
	defer b.mu.Unlock()

	if _, exists := b.scheduledActions[name]; !exists {
		return fmt.Errorf("%w: scheduled action %s not found", ErrScheduledActionNotFound, name)
	}

	delete(b.scheduledActions, name)

	return nil
}

// DescribeScheduledActions returns scheduled actions, optionally filtered by name.
func (b *InMemoryBackend) DescribeScheduledActions(name string) ([]ScheduledAction, error) {
	b.mu.RLock("DescribeScheduledActions")
	defer b.mu.RUnlock()

	if name != "" {
		a, exists := b.scheduledActions[name]
		if !exists {
			return nil, fmt.Errorf("%w: scheduled action %s not found", ErrScheduledActionNotFound, name)
		}

		cp := *a

		return []ScheduledAction{cp}, nil
	}

	result := make([]ScheduledAction, 0, len(b.scheduledActions))

	for _, a := range b.scheduledActions {
		result = append(result, *a)
	}

	return result, nil
}

// ModifyScheduledAction updates a scheduled action's schedule, IAM role, or description.
func (b *InMemoryBackend) ModifyScheduledAction(
	name, schedule, iamRole, description string,
) (*ScheduledAction, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ScheduledActionName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyScheduledAction")
	defer b.mu.Unlock()

	a, exists := b.scheduledActions[name]
	if !exists {
		return nil, fmt.Errorf("%w: scheduled action %s not found", ErrScheduledActionNotFound, name)
	}

	if schedule != "" {
		a.Schedule = schedule
	}
	if iamRole != "" {
		a.IamRole = iamRole
	}
	if description != "" {
		a.ScheduledActionDescription = description
	}

	cp := *a

	return &cp, nil
}

// ---- Table Restore (stateful) ----

// CreateTableRestoreStatus creates a table restore status entry.
func (b *InMemoryBackend) CreateTableRestoreStatus(
	clusterID, snapshotID, sourceDatabaseName, sourceTableName, targetDatabaseName, targetTableName string,
) (*TableRestoreStatus, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTableRestoreStatus")
	defer b.mu.Unlock()

	restoreID := uuid.New().String()
	tr := &TableRestoreStatus{
		TableRestoreRequestID: restoreID,
		ClusterIdentifier:     clusterID,
		Status:                "IN_PROGRESS",
		SourceDatabaseName:    sourceDatabaseName,
		SourceTableName:       sourceTableName,
		TargetDatabaseName:    targetDatabaseName,
		TargetTableName:       targetTableName,
		RequestTime:           time.Now().UTC(),
	}
	b.tableRestores[restoreID] = tr

	cp := *tr

	return &cp, nil
}
