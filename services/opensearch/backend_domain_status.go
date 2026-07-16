package opensearch

import (
	"fmt"
	"time"
)

// GetDomainHealth returns computed health metrics for a domain.
func (b *InMemoryBackend) GetDomainHealth(domainName string) (map[string]any, error) {
	b.mu.RLock("GetDomainHealth")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	instanceCount := d.ClusterConfig.InstanceCount
	if instanceCount == 0 {
		instanceCount = 1
	}

	totalShards := instanceCount * defaultShardsPerNode

	warmNodes := 0
	if d.ClusterConfig.WarmEnabled {
		warmNodes = d.ClusterConfig.WarmCount
	}

	dedicatedMaster := d.ClusterConfig.DedicatedMasterEnabled

	docCount := 0
	for _, idx := range b.domainIndexesByDomain.Get(domainName) {
		docCount += idx.DocumentCount
	}

	return map[string]any{
		"DomainState":                 domainStatusActive,
		"TotalShards":                 totalShards,
		"ActiveShards":                totalShards,
		"UnAssignedShards":            0,
		"DataNodeCount":               instanceCount,
		"WarmNodeCount":               warmNodes,
		"DedicatedMaster":             dedicatedMaster,
		"ActiveAvailabilityZoneCount": 1,
		"DocumentCount":               docCount,
	}, nil
}

// GetDomainNodes returns a list of node descriptors based on cluster config.
func (b *InMemoryBackend) GetDomainNodes(domainName string) ([]map[string]any, error) {
	b.mu.RLock("GetDomainNodes")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	count := d.ClusterConfig.InstanceCount
	if count == 0 {
		count = 1
	}

	nodes := make([]map[string]any, 0, count)

	storageVolumeType := "EBS"
	if d.EBSOptions != nil && d.EBSOptions.VolumeType != "" {
		storageVolumeType = d.EBSOptions.VolumeType
	}

	for i := range count {
		nodes = append(nodes, map[string]any{
			"NodeId":            fmt.Sprintf("node-%d", i),
			"NodeType":          nodeRoleData,
			jsonKeyInstanceType: d.ClusterConfig.InstanceType,
			"NodeStatus":        domainStatusActive,
			"StorageVolumeType": storageVolumeType,
			"AvailabilityZone":  fmt.Sprintf("%sa", b.region),
		})
	}

	return nodes, nil
}

// GetDryRunProgress returns dry-run progress for a domain. Creates a default entry if none exists.
func (b *InMemoryBackend) GetDryRunProgress(domainName string) (*DryRunStatus, error) {
	b.mu.Lock("GetDryRunProgress")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	dr, exists := b.dryRuns.Get(domainName)
	if !exists {
		now := time.Now().UTC().Format(time.RFC3339)
		dr = &DryRunStatus{
			DryRunID:           fmt.Sprintf("dryrun-%s-%d", domainName, time.Now().UnixNano()),
			DryRunStatus:       softwareUpdateCompleted,
			CreationDate:       now,
			UpdateDate:         now,
			ValidationFailures: []map[string]any{},
			DomainName:         domainName,
		}
		b.dryRuns.Put(dr)
	}

	if dr.ValidationFailures == nil {
		dr.ValidationFailures = []map[string]any{}
	}

	cp := *dr

	return &cp, nil
}

// GetChangeProgress returns the last change progress for a domain.
func (b *InMemoryBackend) GetChangeProgress(domainName string) (map[string]any, error) {
	b.mu.RLock("GetChangeProgress")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	changeID := d.LastChangeID
	if changeID == "" {
		changeID = changeProgressStub
	}

	now := time.Now().UTC().Format(time.RFC3339)

	return map[string]any{
		"ChangeId":            changeID,
		jsonKeyStatus:         softwareUpdateCompleted,
		"CompletedProperties": []any{},
		"PendingProperties":   []any{},
		"TotalNumberOfStages": 0,
		"StartTime":           now,
		"LastUpdatedTime":     now,
	}, nil
}
