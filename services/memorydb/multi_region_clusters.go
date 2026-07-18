package memorydb

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateMultiRegionCluster creates a new multi-region cluster.
func (b *InMemoryBackend) CreateMultiRegionCluster(
	ctx context.Context,
	req *createMultiRegionClusterRequest,
) (*MultiRegionCluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	fullName := "virv-" + req.MultiRegionClusterNameSuffix

	if _, exists := b.multiRegionClusters.Get(fullName); exists {
		return nil, ErrMultiRegionClusterAlreadyExists
	}

	mrARN := arn.Build("memorydb", region, b.accountID, "multiregioncluster/"+fullName)

	engineVersion := req.EngineVersion
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	engine := req.Engine
	if engine == "" {
		engine = engineRedis
	}

	mrc := &MultiRegionCluster{
		MultiRegionClusterName:        fullName,
		ARN:                           mrARN,
		Description:                   req.Description,
		NodeType:                      req.NodeType,
		Engine:                        engine,
		EngineVersion:                 engineVersion,
		MultiRegionParameterGroupName: req.MultiRegionParameterGroupName,
		Status:                        multiRegionClusterStatusAvailable,
		Tags:                          tagsFromSlice(req.Tags),
		CreatedAt:                     time.Now(),
	}

	b.multiRegionClusters.Put(mrc)
	b.arnToResourceStore(region)[mrARN] = resourceRef{Kind: resourceKindMultiRegionCluster, Name: fullName}

	// Clone: mrc stays in the registry and its Tags map can be mutated in
	// place by a concurrent TagResource/UntagResource or UpdateMultiRegionCluster
	// call after this method returns and b.mu is released.
	return cloneMultiRegionCluster(mrc), nil
}

// DeleteMultiRegionCluster removes a multi-region cluster.
func (b *InMemoryBackend) DeleteMultiRegionCluster(ctx context.Context, name string) (*MultiRegionCluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	mrc, ok := b.multiRegionClusters.Get(name)
	if !ok {
		return nil, ErrMultiRegionClusterNotFound
	}

	b.multiRegionClusters.Delete(name)
	delete(b.arnToResourceStore(region), mrc.ARN)

	return cloneMultiRegionCluster(mrc), nil
}

// DescribeMultiRegionClusters returns multi-region clusters, optionally filtered by name.
func (b *InMemoryBackend) DescribeMultiRegionClusters(_ context.Context, name string) ([]*MultiRegionCluster, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		mrc, ok := b.multiRegionClusters.Get(name)
		if !ok {
			return nil, ErrMultiRegionClusterNotFound
		}

		return []*MultiRegionCluster{cloneMultiRegionCluster(mrc)}, nil
	}

	all := b.multiRegionClusters.All()
	result := make([]*MultiRegionCluster, 0, len(all))
	for _, mrc := range all {
		result = append(result, cloneMultiRegionCluster(mrc))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].MultiRegionClusterName < result[j].MultiRegionClusterName
	})

	return result, nil
}

// UpdateMultiRegionCluster modifies an existing multi-region cluster.
func (b *InMemoryBackend) UpdateMultiRegionCluster(
	_ context.Context,
	req *updateMultiRegionClusterRequest,
) (*MultiRegionCluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	mrc, ok := b.multiRegionClusters.Get(req.MultiRegionClusterName)
	if !ok {
		return nil, ErrMultiRegionClusterNotFound
	}

	if req.Description != "" {
		mrc.Description = req.Description
	}

	if req.NodeType != "" {
		mrc.NodeType = req.NodeType
	}
	if req.EngineVersion != "" {
		mrc.EngineVersion = req.EngineVersion
	}

	if req.MultiRegionParameterGroupName != "" {
		mrc.MultiRegionParameterGroupName = req.MultiRegionParameterGroupName
	}

	return cloneMultiRegionCluster(mrc), nil
}

// -- MultiRegionParameterGroup operations ----------------------------------------

// DescribeMultiRegionParameterGroups returns multi-region parameter groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeMultiRegionParameterGroups(
	_ context.Context,
	name string,
) ([]*MultiRegionParameterGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		mrpg, ok := b.multiRegionParameterGroups.Get(name)
		if !ok {
			return nil, ErrMultiRegionParameterGroupNotFound
		}

		return []*MultiRegionParameterGroup{cloneMultiRegionParameterGroup(mrpg)}, nil
	}

	all := b.multiRegionParameterGroups.All()
	result := make([]*MultiRegionParameterGroup, 0, len(all))
	for _, mrpg := range all {
		result = append(result, cloneMultiRegionParameterGroup(mrpg))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// -- ParameterGroup parameter operations -----------------------------------------

// ListAllowedMultiRegionClusterUpdates returns the set of node types a multi-region cluster can be updated to.
func (b *InMemoryBackend) ListAllowedMultiRegionClusterUpdates(
	_ context.Context,
	clusterName string,
) ([]string, error) {
	b.mu.RLock()

	defer b.mu.RUnlock()

	if _, ok := b.multiRegionClusters.Get(clusterName); !ok {
		return nil, ErrMultiRegionClusterNotFound
	}

	return allowedNodeTypes(), nil
}

// DescribeMultiRegionParameters returns the parameters for a multi-region parameter group.
func (b *InMemoryBackend) DescribeMultiRegionParameters(
	_ context.Context,
	parameterGroupName string,
) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if parameterGroupName == "" {
		return nil, fmt.Errorf("parameter group name is required: %w", ErrValidation)
	}

	mrpg, ok := b.multiRegionParameterGroups.Get(parameterGroupName)
	if !ok {
		return nil, ErrMultiRegionParameterGroupNotFound
	}

	return maps.Clone(mrpg.Parameters), nil
}

// cloneMultiRegionCluster returns a shallow copy of the multi-region cluster with separate tags.
func cloneMultiRegionCluster(mrc *MultiRegionCluster) *MultiRegionCluster {
	if mrc == nil {
		return nil
	}

	cp := *mrc
	cp.Tags = maps.Clone(mrc.Tags)

	return &cp
}

// cloneMultiRegionParameterGroup returns a shallow copy with separate tags.
func cloneMultiRegionParameterGroup(mrpg *MultiRegionParameterGroup) *MultiRegionParameterGroup {
	if mrpg == nil {
		return nil
	}

	cp := *mrpg
	cp.Tags = maps.Clone(mrpg.Tags)

	return &cp
}

// -- Seed helpers (for testing) --------------------------------------------------

// AddMultiRegionParameterGroupInternal inserts a multi-region parameter group directly into the backend for testing.
func (b *InMemoryBackend) AddMultiRegionParameterGroupInternal(name, family string) *MultiRegionParameterGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	mrpgARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "multiregionparametergroup/"+name)
	mrpg := &MultiRegionParameterGroup{
		Name:       name,
		ARN:        mrpgARN,
		Family:     family,
		Tags:       make(map[string]string),
		Parameters: make(map[string]string),
		CreatedAt:  time.Now(),
	}
	b.multiRegionParameterGroups.Put(mrpg)

	return mrpg
}
