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

// CreateConnector creates a connector. Connectors become RUNNING immediately
// -- see PARITY.md for the CREATING/UPDATING/DELETING transient states this
// backend deliberately does not model.
func (b *InMemoryBackend) CreateConnector(accountID, region string, spec ConnectorSpec) (*Connector, error) {
	if spec.Name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateConnector")
	defer b.mu.Unlock()

	if _, ok := b.connectorByName(spec.Name); ok {
		return nil, ErrConnectorNameInUse
	}

	tags := make(map[string]string, len(spec.Tags))
	maps.Copy(tags, spec.Tags)

	cfg := make(map[string]string, len(spec.ConnectorConfiguration))
	maps.Copy(cfg, spec.ConnectorConfiguration)

	c := &Connector{
		Name:                             spec.Name,
		ARN:                              connectorARN(region, accountID, spec.Name),
		Description:                      spec.Description,
		State:                            connectorStateRunning,
		CurrentVersion:                   newVersion(),
		CreationTime:                     time.Now().UTC(),
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
	b.mu.RLock("DescribeConnector")
	defer b.mu.RUnlock()

	c, ok := b.connectors.Get(connectorArn)
	if !ok {
		return nil, ErrConnectorNotFound
	}

	return c.clone(), nil
}

// ListConnectors returns connectors matching namePrefix, paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListConnectors(namePrefix, nextToken string, maxResults int) ([]*Connector, string, error) {
	b.mu.RLock("ListConnectors")
	defer b.mu.RUnlock()

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
// update.ConnectorConfiguration must be set. The returned ConnectorOperation
// completes immediately (UPDATE_COMPLETE) -- see PARITY.md.
func (b *InMemoryBackend) UpdateConnector(
	connectorArn, currentVersion string,
	update ConnectorUpdate,
) (*Connector, *ConnectorOperation, error) {
	if (update.Capacity == nil) == (update.ConnectorConfiguration == nil) {
		return nil, nil, ErrValidation
	}

	b.mu.Lock("UpdateConnector")
	defer b.mu.Unlock()

	c, ok := b.connectors.Get(connectorArn)
	if !ok {
		return nil, nil, ErrConnectorNotFound
	}

	if c.CurrentVersion != currentVersion {
		return nil, nil, ErrVersionMismatch
	}

	op := &ConnectorOperation{
		ARN:                          connectorOperationARN(connectorArn),
		ConnectorArn:                 connectorArn,
		State:                        connectorOperationStateComplete,
		CreationTime:                 time.Now().UTC(),
		OriginConnectorConfiguration: maps.Clone(c.ConnectorConfiguration),
		TargetConnectorConfiguration: maps.Clone(c.ConnectorConfiguration),
	}

	if update.Capacity != nil {
		op.Type = connectorOperationTypeWorkerSetting
		op.Steps = []ConnectorOperationStep{
			{StepType: connectorOperationStepUpdateWorkerSetting, StepState: connectorOperationStepStateCompleted},
		}

		origin := c.Capacity.clone()
		op.OriginCapacity = &origin
		c.Capacity = update.Capacity.clone()
		target := c.Capacity.clone()
		op.TargetCapacity = &target
	} else {
		op.Type = connectorOperationTypeConfiguration
		op.Steps = []ConnectorOperationStep{
			{StepType: connectorOperationStepUpdateConfiguration, StepState: connectorOperationStepStateCompleted},
		}

		op.TargetConnectorConfiguration = maps.Clone(update.ConnectorConfiguration)
		c.ConnectorConfiguration = maps.Clone(update.ConnectorConfiguration)
	}

	op.EndTime = time.Now().UTC()
	c.CurrentVersion = newVersion()

	b.connectorOperations.Put(op)

	return c.clone(), op.clone(), nil
}

// DeleteConnector deletes a connector under optimistic lock (currentVersion,
// when supplied), returning a snapshot with State set to DELETING to mirror
// AWS's synchronous delete response.
func (b *InMemoryBackend) DeleteConnector(connectorArn, currentVersion string) (*Connector, error) {
	b.mu.Lock("DeleteConnector")
	defer b.mu.Unlock()

	c, ok := b.connectors.Get(connectorArn)
	if !ok {
		return nil, ErrConnectorNotFound
	}

	if currentVersion != "" && c.CurrentVersion != currentVersion {
		return nil, ErrVersionMismatch
	}

	out := c.clone()
	out.State = deletingState

	b.connectors.Delete(connectorArn)

	return out, nil
}

// DescribeConnectorOperation returns the details of a single connector operation.
func (b *InMemoryBackend) DescribeConnectorOperation(operationArn string) (*ConnectorOperation, error) {
	b.mu.RLock("DescribeConnectorOperation")
	defer b.mu.RUnlock()

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
	b.mu.RLock("ListConnectorOperations")
	defer b.mu.RUnlock()

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
