package kafkaconnect

import "time"

const (
	provisionDelay = 500 * time.Millisecond
	deletionDelay  = 300 * time.Millisecond
)

// settleLocked advances every transient resource whose deadline has passed.
// Caller must hold b.mu for writing.
func (b *InMemoryBackend) settleLocked(now time.Time) {
	for _, c := range b.connectors.All() {
		b.settleConnectorLocked(c, now)
	}

	for _, p := range b.customPlugins.All() {
		switch {
		case p.State == deletingState && !now.Before(p.PendingUntil):
			b.customPlugins.Delete(p.ARN)
		case p.State == customPluginStateCreating && !now.Before(p.PendingUntil):
			p.State = customPluginStateActive
			if p.FailureMessage != "" {
				p.State = customPluginStateCreateFailed
			}
		}
	}

	for _, w := range b.workerConfigurations.All() {
		if w.State == deletingState && !now.Before(w.PendingUntil) {
			b.workerConfigurations.Delete(w.ARN)
		}
	}
}

func (b *InMemoryBackend) settleConnectorLocked(c *Connector, now time.Time) {
	if c.State == connectorStateRunning || now.Before(c.PendingUntil) {
		return
	}

	if c.State == deletingState {
		b.connectors.Delete(c.ARN)
		b.deleteOperationsLocked(c.ARN)

		return
	}

	c.State = connectorStateRunning
	b.completeOperationsLocked(c.ARN, c.PendingUntil)
}

func (b *InMemoryBackend) deleteOperationsLocked(connectorArn string) {
	for _, op := range b.connectorOperations.All() {
		if op.ConnectorArn == connectorArn {
			b.connectorOperations.Delete(op.ARN)
		}
	}
}

func (b *InMemoryBackend) completeOperationsLocked(connectorArn string, at time.Time) {
	for _, op := range b.connectorOperations.All() {
		if op.ConnectorArn != connectorArn || !op.EndTime.IsZero() {
			continue
		}

		switch op.State {
		case connectorOperationStateInProgress:
			op.State = connectorOperationStateComplete
		case connectorOperationStateRestartInProgress:
			op.State = connectorOperationStateRestartComplete
		default:
			continue
		}

		for i := range op.Steps {
			op.Steps[i].StepState = connectorOperationStepStateCompleted
		}

		op.EndTime = at
	}
}
