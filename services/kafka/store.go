// Package kafka provides an in-memory stub of AWS MSK (Managed Streaming for Apache Kafka).
package kafka

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// MSK resources are isolated per region: every backend operation resolves the
// caller's region from the request context (for create/list operations) or from
// the resource ARN (for operations that target an existing ARN) and operates only
// on that region's nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

// InMemoryBackend stores MSK state in memory.
//
// Every ARN-keyed resource table below is keyed by the resource's own ARN,
// which already encodes its region (arn:partition:service:region:account:
// resource, see regionFromARN) — so same-named resources in different
// regions are fully isolated without an outer region-keyed map layer.
// Operations that take an existing resource ARN resolve their region from
// the ARN itself when they need to build a NEW related ARN; point lookups by
// an existing ARN need no region resolution at all. See store_setup.go for
// the full table/index registration and the rationale for each one.
type InMemoryBackend struct {
	registry                   *store.Registry
	clusters                   *store.Table[Cluster]
	clustersByRegion           *store.Index[Cluster]
	clustersByName             *store.Index[Cluster]
	configurations             *store.Table[Configuration]
	configurationsByRegion     *store.Index[Configuration]
	replicators                *store.Table[Replicator]
	replicatorsByRegion        *store.Index[Replicator]
	topics                     *store.Table[Topic]
	topicsByCluster            *store.Index[Topic]
	vpcConnections             *store.Table[VpcConnection]
	vpcConnectionsByRegion     *store.Index[VpcConnection]
	vpcConnectionsByCluster    *store.Index[VpcConnection]
	clusterOperations          *store.Table[ClusterOperation]
	clusterOperationsByCluster *store.Index[ClusterOperation]
	scramSecrets               map[string][]string // clusterArn → []secretArn (raw: slice-valued, not *T)
	clusterPolicies            map[string]string   // clusterArn → policy document (raw: string-valued, not *T)
	mu                         *lockmetrics.RWMutex
	accountID                  string
	region                     string
}

// NewInMemoryBackend creates a new in-memory MSK backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		scramSecrets:    make(map[string][]string),
		clusterPolicies: make(map[string]string),
		mu:              lockmetrics.New("kafka"),
		accountID:       accountID,
		region:          region,
	}
	registerAllTables(b)

	return b
}

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all state, returning the backend to a clean empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.scramSecrets = make(map[string][]string)
	b.clusterPolicies = make(map[string]string)
}

// clusterARN builds an ARN for an MSK cluster in region.
func (b *InMemoryBackend) clusterARN(region, name string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("cluster/%s/%s", name, uuid.New().String()),
	)
}

// configurationARN builds an ARN for an MSK configuration in region.
func (b *InMemoryBackend) configurationARN(region, name string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("configuration/%s/%s", name, uuid.New().String()),
	)
}

// replicatorARN builds an ARN for an MSK replicator in region.
func (b *InMemoryBackend) replicatorARN(region, name string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("replicator/%s/%s", name, uuid.New().String()),
	)
}

// vpcConnectionARN builds an ARN for an MSK VPC connection in region.
func (b *InMemoryBackend) vpcConnectionARN(region, clusterArn, vpcID string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("vpc-connection/%s/%s/%s", clusterArn, vpcID, uuid.New().String()),
	)
}

// clusterOperationARN builds an ARN for an MSK cluster operation in region.
func (b *InMemoryBackend) clusterOperationARN(region, clusterArn string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("cluster-operation/%s/%s", clusterArn, uuid.New().String()),
	)
}

// topicKey returns the composite key used to store a topic in memory.
func topicKey(clusterArn, topicName string) string {
	return clusterArn + "|" + topicName
}

// nextVersionToken returns a new opaque optimistic-lock token, formatted like
// the 14-character uppercase alphanumeric tokens real MSK issues for
// Cluster.CurrentVersion / Replicator.CurrentVersion (e.g. "K3AEGXETSR30VB").
// Real MSK bumps these on every successful mutating operation so that a
// subsequent update must supply the version returned by the most recent
// describe/create/update -- see newClusterOperationLocked and
// InMemoryBackend.UpdateReplicationInfo.
func nextVersionToken() string {
	const tokenLen = 14

	raw := strings.ToUpper(strings.ReplaceAll(uuid.New().String(), "-", ""))

	return raw[:tokenLen]
}

// collectClusterChildrenLocked verifies the cluster exists, then returns the
// clones of every value idx groups under clusterArn, sorted ascending by
// sortKey. Callers must hold b.mu (read lock).
func collectClusterChildrenLocked[T any](
	clusters *store.Table[Cluster],
	idx *store.Index[T],
	clusterArn string,
	clone func(*T) *T,
	sortKey func(*T) string,
) ([]*T, error) {
	if !clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	items := idx.Get(clusterArn)
	out := make([]*T, 0, len(items))

	for _, item := range items {
		out = append(out, clone(item))
	}

	slices.SortFunc(out, func(a, b *T) int { return strings.Compare(sortKey(a), sortKey(b)) })

	return out, nil
}

// cloneMutableClusterInfo deep-copies a MutableClusterInfo, reusing the existing
// per-field clone helpers so the returned operation does not alias backend state.
func cloneMutableClusterInfo(m *MutableClusterInfo) *MutableClusterInfo {
	if m == nil {
		return nil
	}

	clone := &MutableClusterInfo{
		ConnectivityInfo:     cloneConnectivityInfo(m.ConnectivityInfo),
		OpenMonitoring:       cloneOpenMonitoring(m.OpenMonitoring),
		LoggingInfo:          cloneLoggingInfo(m.LoggingInfo),
		ClientAuthentication: cloneClientAuth(m.ClientAuthentication),
		EncryptionInfo:       cloneEncryptionInfo(m.EncryptionInfo),
		StorageMode:          m.StorageMode,
		EnhancedMonitoring:   m.EnhancedMonitoring,
		NumberOfBrokerNodes:  m.NumberOfBrokerNodes,
	}

	if len(m.BrokerEBSVolumeInfo) > 0 {
		clone.BrokerEBSVolumeInfo = make([]BrokerEBSVolumeInfo, len(m.BrokerEBSVolumeInfo))
		for i, v := range m.BrokerEBSVolumeInfo {
			cv := BrokerEBSVolumeInfo{
				KafkaBrokerNodeID: v.KafkaBrokerNodeID,
				VolumeSizeGB:      v.VolumeSizeGB,
			}
			if v.ProvisionedThroughput != nil {
				pt := *v.ProvisionedThroughput
				cv.ProvisionedThroughput = &pt
			}
			clone.BrokerEBSVolumeInfo[i] = cv
		}
	}

	return clone
}

// nonNilTagsCopy returns a new non-nil copy of tags; an empty map if tags is nil.
func nonNilTagsCopy(tags map[string]string) map[string]string {
	if tags == nil {
		return make(map[string]string)
	}

	return maps.Clone(tags)
}
