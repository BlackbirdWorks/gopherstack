package kafka

import (
	"bytes"
	"context"
	"fmt"
	"time"
)

// ClientCAResolver returns the PEM certificate (and chain) of a certificate authority named in
// tls.certificateAuthorityArnList, typically backed by the ACM PCA emulator.
type ClientCAResolver func(ctx context.Context, caArn string) ([]byte, error)

// SetClientCAResolver registers the resolver used to build the broker truststore for mutual TLS.
// Without one, brokers trust only the emulator's own broker CA.
func (b *InMemoryBackend) SetClientCAResolver(r ClientCAResolver) {
	b.mu.Lock("SetClientCAResolver")
	defer b.mu.Unlock()

	b.clientCAs = r
}

// clientTrust assembles the PEM truststore a mutual-TLS broker verifies client certificates against.
func (b *InMemoryBackend) clientTrust(ctx context.Context, plan brokerPlan) ([]byte, error) {
	if !plan.mtls {
		return nil, nil
	}

	b.mu.RLock("clientTrust")
	resolve, e := b.clientCAs, b.engine
	b.mu.RUnlock()

	if resolve == nil {
		if e == nil || e.cfg.CA == nil {
			return nil, errNoBrokerCA
		}

		return e.cfg.CA.CertPEM(), nil
	}

	var out bytes.Buffer

	for _, arn := range plan.caArns {
		pem, err := resolve(ctx, arn)
		if err != nil {
			return nil, fmt.Errorf("resolve client CA %s: %w", arn, err)
		}

		out.Write(pem)
		out.WriteByte('\n')
	}

	return out.Bytes(), nil
}

// relaunchForSecurityLocked replaces a docker-backed cluster's broker so it picks up new
// authentication and encryption settings. The cluster reports UPDATING and op stays in progress
// until the new broker is reachable. It returns the superseded broker for the caller to reap
// once the lock is released.
func (b *InMemoryBackend) relaunchForSecurityLocked(c *Cluster, op *ClusterOperation) *liveBroker {
	if b.engine == nil || c.ClusterType != ClusterTypeProvisioned {
		return nil
	}

	stored, ok := b.clusterOperations.Get(op.ClusterOperationArn)
	if !ok {
		return nil
	}

	stored.OperationState, stored.EndTime, stored.awaitBroker = ClusterOperationStateUpdateInProgress, "", true
	op.OperationState, op.EndTime = stored.OperationState, ""

	old := b.detachBrokerLocked(c.ClusterArn)

	c.State, c.StateInfo = ClusterStateUpdating, nil
	c.ActiveOperationArn = op.ClusterOperationArn
	c.readyAt, c.pendingState, c.pendingOpArn = time.Time{}, "", ""

	b.launchBrokerLocked(c.ClusterArn, c.ClusterName, op.ClusterOperationArn)

	return old
}

// settleBrokerOpLocked records the outcome of the operation a broker relaunch was started for.
func (b *InMemoryBackend) settleBrokerOpLocked(clusterArn, opArn, state string) {
	if opArn == "" {
		return
	}

	if op, ok := b.clusterOperations.Get(opArn); ok {
		op.OperationState, op.awaitBroker = state, false
		op.EndTime = b.now().UTC().Format(time.RFC3339)
	}

	if c, ok := b.clusters.Get(clusterArn); ok && c.ActiveOperationArn == opArn {
		c.ActiveOperationArn = ""
	}
}
