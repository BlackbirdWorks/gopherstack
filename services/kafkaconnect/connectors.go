package kafkaconnect

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func connectorARN(region, accountID, name string) string {
	return arn.Build("kafkaconnect", region, accountID, fmt.Sprintf("connector/%s/%s", name, uuid.NewString()))
}

func connectorOperationARN(connectorArn string) string {
	return connectorArn + "/operation/" + uuid.NewString()
}

// CreateConnector creates a connector in CREATING; it settles to RUNNING after provisionDelay.
func (b *InMemoryBackend) CreateConnector(accountID, region string, spec ConnectorSpec) (*Connector, error) {
	if spec.Name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateConnector")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	if _, ok := b.connectorByName(spec.Name); ok {
		return nil, ErrConnectorNameInUse
	}

	if err := b.validateConnectorRefsLocked(spec); err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	tags := make(map[string]string, len(spec.Tags))
	maps.Copy(tags, spec.Tags)

	cfg := make(map[string]string, len(spec.ConnectorConfiguration))
	maps.Copy(cfg, spec.ConnectorConfiguration)

	c := &Connector{
		Name:                             spec.Name,
		ARN:                              connectorARN(region, accountID, spec.Name),
		Description:                      spec.Description,
		State:                            connectorStateCreating,
		PendingUntil:                     now.Add(provisionDelay),
		CurrentVersion:                   newVersion(),
		CreationTime:                     now,
		ConnectorConfiguration:           cfg,
		Capacity:                         spec.Capacity.clone(),
		ApacheKafkaCluster:               spec.ApacheKafkaCluster,
		KafkaClusterClientAuthentication: spec.KafkaClusterClientAuthentication,
		KafkaClusterEncryptionInTransit:  spec.KafkaClusterEncryptionInTransit,
		KafkaConnectVersion:              spec.KafkaConnectVersion,
		ServiceExecutionRoleArn:          spec.ServiceExecutionRoleArn,
		NetworkType:                      spec.NetworkType,
		Plugins:                          append([]PluginRef(nil), spec.Plugins...),
		WorkerConfiguration:              spec.WorkerConfiguration,
		WorkerLogDelivery:                spec.WorkerLogDelivery.clone(),
		Tags:                             tags,
	}

	b.connectors.Put(c)

	return c.clone(), nil
}

// DescribeConnector returns the current information about a connector.
func (b *InMemoryBackend) DescribeConnector(connectorArn string) (*Connector, error) {
	b.mu.Lock("DescribeConnector")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	c, ok := b.connectors.Get(connectorArn)
	if !ok {
		return nil, ErrConnectorNotFound
	}

	return c.clone(), nil
}

// ListConnectors returns connectors matching namePrefix, paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListConnectors(namePrefix, nextToken string, maxResults int) ([]*Connector, string, error) {
	b.mu.Lock("ListConnectors")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	all := b.connectors.All()

	matched := make([]*Connector, 0, len(all))

	for _, c := range all {
		if namePrefix != "" && !strings.HasPrefix(c.Name, namePrefix) {
			continue
		}

		matched = append(matched, c.clone())
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })

	p := page.New(matched, nextToken, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}

// UpdateConnector updates a connector's capacity or configuration under
// optimistic lock (currentVersion). Exactly one of update.Capacity or
// update.ConnectorConfiguration must be set. The connector goes UPDATING and
// the returned operation UPDATE_IN_PROGRESS until provisionDelay elapses.
func (b *InMemoryBackend) UpdateConnector(
	connectorArn, currentVersion string,
	update ConnectorUpdate,
) (*Connector, *ConnectorOperation, error) {
	if (update.Capacity == nil) == (update.ConnectorConfiguration == nil) {
		return nil, nil, ErrValidation
	}

	b.mu.Lock("UpdateConnector")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	c, ok := b.connectors.Get(connectorArn)
	if !ok {
		return nil, nil, ErrConnectorNotFound
	}

	if c.CurrentVersion != currentVersion {
		return nil, nil, ErrVersionMismatch
	}

	if c.State == deletingState {
		return nil, nil, ErrConnectorDeleting
	}

	now := time.Now().UTC()
	b.completeOperationsLocked(connectorArn, now)

	op := &ConnectorOperation{
		ARN:                          connectorOperationARN(connectorArn),
		ConnectorArn:                 connectorArn,
		State:                        connectorOperationStateInProgress,
		CreationTime:                 now,
		OriginConnectorConfiguration: maps.Clone(c.ConnectorConfiguration),
		TargetConnectorConfiguration: maps.Clone(c.ConnectorConfiguration),
	}

	if update.Capacity != nil {
		op.Type = connectorOperationTypeWorkerSetting
		op.Steps = []ConnectorOperationStep{
			{StepType: connectorOperationStepUpdateWorkerSetting, StepState: connectorOperationStepStateInProgress},
		}

		origin := c.Capacity.clone()
		op.OriginCapacity = &origin
		c.Capacity = update.Capacity.clone()
		target := c.Capacity.clone()
		op.TargetCapacity = &target
	} else {
		op.Type = connectorOperationTypeConfiguration
		op.Steps = []ConnectorOperationStep{
			{StepType: connectorOperationStepUpdateConfiguration, StepState: connectorOperationStepStateInProgress},
		}

		op.TargetConnectorConfiguration = maps.Clone(update.ConnectorConfiguration)
		c.ConnectorConfiguration = maps.Clone(update.ConnectorConfiguration)
	}

	c.State = connectorStateUpdating
	c.PendingUntil = now.Add(provisionDelay)
	c.CurrentVersion = newVersion()

	b.connectorOperations.Put(op)

	return c.clone(), op.clone(), nil
}

// DeleteConnector marks a connector DELETING under optimistic lock
// (currentVersion, when supplied); it is removed once deletionDelay elapses.
func (b *InMemoryBackend) DeleteConnector(connectorArn, currentVersion string) (*Connector, error) {
	b.mu.Lock("DeleteConnector")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	c, ok := b.connectors.Get(connectorArn)
	if !ok {
		return nil, ErrConnectorNotFound
	}

	if currentVersion != "" && c.CurrentVersion != currentVersion {
		return nil, ErrVersionMismatch
	}

	if c.State != deletingState {
		c.State = deletingState
		c.PendingUntil = time.Now().UTC().Add(deletionDelay)
	}

	return c.clone(), nil
}

// RestartConnector moves a connector to RESTARTING and records a
// RESTART_IN_PROGRESS operation that completes after provisionDelay.
func (b *InMemoryBackend) RestartConnector(connectorArn string, _ bool) (*Connector, *ConnectorOperation, error) {
	b.mu.Lock("RestartConnector")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	c, ok := b.connectors.Get(connectorArn)
	if !ok {
		return nil, nil, ErrConnectorNotFound
	}

	if c.State == deletingState {
		return nil, nil, ErrConnectorDeleting
	}

	now := time.Now().UTC()
	b.completeOperationsLocked(connectorArn, now)

	op := &ConnectorOperation{
		ARN:          connectorOperationARN(connectorArn),
		ConnectorArn: connectorArn,
		Type:         connectorOperationTypeRestart,
		State:        connectorOperationStateRestartInProgress,
		CreationTime: now,
	}

	c.State = connectorStateRestarting
	c.PendingUntil = now.Add(provisionDelay)

	b.connectorOperations.Put(op)

	return c.clone(), op.clone(), nil
}

// DescribeConnectorOperation returns the details of a single connector operation.
func (b *InMemoryBackend) DescribeConnectorOperation(operationArn string) (*ConnectorOperation, error) {
	b.mu.Lock("DescribeConnectorOperation")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	op, ok := b.connectorOperations.Get(operationArn)
	if !ok {
		return nil, ErrConnectorOperationNotFound
	}

	return op.clone(), nil
}

// ListConnectorOperations returns operations for a connector, paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListConnectorOperations(
	connectorArn, nextToken string,
	maxResults int,
) ([]*ConnectorOperation, string, error) {
	b.mu.Lock("ListConnectorOperations")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	all := b.connectorOperations.All()

	matched := make([]*ConnectorOperation, 0, len(all))

	for _, op := range all {
		if op.ConnectorArn != connectorArn {
			continue
		}

		matched = append(matched, op.clone())
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].CreationTime.Before(matched[j].CreationTime) })

	p := page.New(matched, nextToken, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}

func (b *InMemoryBackend) validateConnectorRefsLocked(spec ConnectorSpec) error {
	for _, ref := range spec.Plugins {
		if _, ok := b.customPlugins.Get(ref.CustomPluginArn); !ok {
			return ErrCustomPluginNotFound
		}
	}

	if spec.WorkerConfiguration != nil {
		if _, ok := b.workerConfigurations.Get(spec.WorkerConfiguration.Arn); !ok {
			return ErrWorkerConfigNotFound
		}
	}

	return nil
}
