package redshift

import (
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Serverless snapshot copy configurations
// ---------------------------------------------------------------------------

// CreateSnapshotCopyConfigurationSL creates a cross-region snapshot copy
// configuration for namespaceName. destinationRegion and namespaceName are
// required by the real CreateSnapshotCopyConfigurationRequest.
func (b *InMemoryBackend) CreateSnapshotCopyConfigurationSL(
	namespaceName, destinationRegion, destinationKmsKeyID string, retentionPeriod int,
) (*ServerlessSnapshotCopyConfiguration, error) {
	b.mu.Lock("CreateSnapshotCopyConfigurationSL")
	defer b.mu.Unlock()

	if _, ok := b.slNamespaces.Get(namespaceName); !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	id := randomHex(slIDHexBytes)
	cfg := &ServerlessSnapshotCopyConfiguration{
		SnapshotCopyConfigurationID: id,
		SnapshotCopyConfigurationArn: arn.Build(
			"redshift-serverless", b.region, b.accountID, "snapshotcopyconfiguration/"+id,
		),
		NamespaceName:           namespaceName,
		DestinationRegion:       destinationRegion,
		DestinationKmsKeyID:     destinationKmsKeyID,
		SnapshotRetentionPeriod: retentionPeriod,
	}
	b.slSnapshotCopyConfig.Put(cfg)
	b.slSnapshotCopyConfigIdx.insert(id)

	cp := *cfg

	return &cp, nil
}

// UpdateSnapshotCopyConfigurationSL updates the retention period of an
// existing snapshot copy configuration -- the only mutable field on
// UpdateSnapshotCopyConfigurationInput besides its identifying ID.
func (b *InMemoryBackend) UpdateSnapshotCopyConfigurationSL(
	id string, retentionPeriod int,
) (*ServerlessSnapshotCopyConfiguration, error) {
	b.mu.Lock("UpdateSnapshotCopyConfigurationSL")
	defer b.mu.Unlock()

	cfg, ok := b.slSnapshotCopyConfig.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: snapshot copy configuration %q not found", ErrSnapshotCopyConfigSLNotFound, id)
	}

	cfg.SnapshotRetentionPeriod = retentionPeriod

	cp := *cfg

	return &cp, nil
}

// DeleteSnapshotCopyConfigurationSL removes a snapshot copy configuration and
// returns the deleted object (DeleteSnapshotCopyConfigurationResponse echoes
// it, unlike most other serverless Delete* responses).
func (b *InMemoryBackend) DeleteSnapshotCopyConfigurationSL(id string) (*ServerlessSnapshotCopyConfiguration, error) {
	b.mu.Lock("DeleteSnapshotCopyConfigurationSL")
	defer b.mu.Unlock()

	cfg, ok := b.slSnapshotCopyConfig.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: snapshot copy configuration %q not found", ErrSnapshotCopyConfigSLNotFound, id)
	}

	cp := *cfg
	b.slSnapshotCopyConfig.Delete(id)
	b.slSnapshotCopyConfigIdx.remove(id)

	return &cp, nil
}

// ListSnapshotCopyConfigurationsSL returns configurations, optionally
// filtered by namespaceName.
func (b *InMemoryBackend) ListSnapshotCopyConfigurationsSL(
	namespaceName string, maxResults int, nextToken string,
) ([]*ServerlessSnapshotCopyConfiguration, string) {
	b.mu.RLock("ListSnapshotCopyConfigurationsSL")
	defer b.mu.RUnlock()

	keys := b.slSnapshotCopyConfigIdx.ordered()
	list := make([]*ServerlessSnapshotCopyConfiguration, 0, len(keys))

	for _, id := range keys {
		cfg, ok := b.slSnapshotCopyConfig.Get(id)
		if !ok {
			continue
		}

		if namespaceName != "" && cfg.NamespaceName != namespaceName {
			continue
		}

		cp := *cfg
		list = append(list, &cp)
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := decodeServerlessPageToken(nextToken)

	if startIdx >= len(list) {
		return []*ServerlessSnapshotCopyConfiguration{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}
