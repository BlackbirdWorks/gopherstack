package elasticache

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) getGlobalReplicationGroup(id string) (*GlobalReplicationGroup, bool) {
	return b.globalReplicationGroups.Get(id)
}

func (b *InMemoryBackend) listGlobalReplicationGroups() []*GlobalReplicationGroup {
	return b.globalReplicationGroups.All()
}

func (b *InMemoryBackend) putGlobalReplicationGroup(_ string, grg *GlobalReplicationGroup) {
	b.globalReplicationGroups.Put(grg)
}

func (b *InMemoryBackend) deleteGlobalReplicationGroup(id string) {
	b.globalReplicationGroups.Delete(id)
}

func (b *InMemoryBackend) globalReplicationGroupARN(id string) string {
	return arn.Build("elasticache", b.region, b.accountID, "globalreplicationgroup:"+id)
}

// CreateGlobalReplicationGroup creates a new global replication group.
func (b *InMemoryBackend) CreateGlobalReplicationGroup(
	ctx context.Context,
	globalReplicationGroupIDSuffix, description, primaryReplicationGroupID string,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("CreateGlobalReplicationGroup")
	defer b.mu.Unlock()

	id := "ldgnf-" + globalReplicationGroupIDSuffix
	if _, exists := b.getGlobalReplicationGroup(id); exists {
		return nil, ErrGlobalReplicationGroupExists
	}

	region := getRegion(ctx, b.region)
	engine := engineRedis
	engineVersion := versionRedis710
	if rg, ok := b.replicationGroupsStore(region).Get(primaryReplicationGroupID); ok {
		if rg.EngineVersion != "" {
			engineVersion = rg.EngineVersion
		}
		if rg.Engine != "" {
			engine = rg.Engine
		}
	}

	nodeGroupCount := int32(1)
	if rg, ok := b.replicationGroupsStore(region).Get(primaryReplicationGroupID); ok && len(rg.NodeGroups) > 0 {
		var cnt int32
		for range rg.NodeGroups {
			cnt++
		}
		nodeGroupCount = cnt
	}

	grg := &GlobalReplicationGroup{
		GlobalReplicationGroupID:      id,
		Description:                   description,
		Status:                        statusAvailable,
		ARN:                           b.globalReplicationGroupARN(id),
		Engine:                        engine,
		EngineVersion:                 engineVersion,
		PrimaryReplicationGroupRegion: region,
		SecondaryReplicationGroups:    make(map[string]string),
		CreatedAt:                     time.Now(),
		Tags:                          tags.New("elasticache.grg." + id + ".tags"),
		NodeGroupCount:                nodeGroupCount,
	}
	b.markCreatingLocked(&grg.PendingStatus, &grg.AvailableAt)
	b.putGlobalReplicationGroup(id, grg)
	b.appendEventLocked(id, "global-replication-group", "global replication group created")

	return b.globalReplicationGroupView(grg), nil
}

// ----------------------------------------
// CreateServerlessCache
// ----------------------------------------

// AddGlobalReplicationGroupInternal seeds a global replication group for testing.
func (b *InMemoryBackend) AddGlobalReplicationGroupInternal(grg *GlobalReplicationGroup) {
	b.mu.Lock("AddGlobalReplicationGroupInternal")
	defer b.mu.Unlock()
	b.putGlobalReplicationGroup(grg.GlobalReplicationGroupID, grg)
}

// DeleteGlobalReplicationGroup deletes a global replication group.
func (b *InMemoryBackend) DeleteGlobalReplicationGroup(
	_ context.Context,
	id string,
	_ bool,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("DeleteGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok || isReaped(b.now(), grg.PendingStatus, grg.AvailableAt) {
		return nil, ErrGlobalReplicationGroupNotFound
	}
	if err := b.requireAvailableLocked(
		grg.Status, grg.PendingStatus, grg.AvailableAt, ErrGlobalReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	if d := b.pendingUntil(); !d.IsZero() {
		grg.PendingStatus = statusDeleting
		grg.AvailableAt = d
		b.appendEventLocked(id, "global-replication-group", "global replication group deleting")

		return b.globalReplicationGroupView(grg), nil
	}

	result := *grg
	grg.Tags.Close()
	b.deleteGlobalReplicationGroup(id)

	b.appendEventLocked(id, "global-replication-group", "global replication group deleted")

	return &result, nil
}

// DescribeGlobalReplicationGroups returns a paginated list of global replication groups.
func (b *InMemoryBackend) DescribeGlobalReplicationGroups(
	_ context.Context,
	id, marker string,
	maxRecords int,
) (page.Page[GlobalReplicationGroup], error) {
	b.mu.RLock("DescribeGlobalReplicationGroups")
	defer b.mu.RUnlock()

	now := b.now()
	if id != "" {
		grg, ok := b.getGlobalReplicationGroup(id)

		if !ok || isReaped(now, grg.PendingStatus, grg.AvailableAt) {
			return page.Page[GlobalReplicationGroup]{}, ErrGlobalReplicationGroupNotFound
		}

		view := *b.globalReplicationGroupView(grg)

		return page.Page[GlobalReplicationGroup]{Data: []GlobalReplicationGroup{view}}, nil
	}

	grgs := b.listGlobalReplicationGroups()
	out := make([]GlobalReplicationGroup, 0, len(grgs))
	for _, grg := range grgs {
		if isReaped(now, grg.PendingStatus, grg.AvailableAt) {
			continue
		}
		out = append(out, *b.globalReplicationGroupView(grg))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].GlobalReplicationGroupID < out[j].GlobalReplicationGroupID
	})

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// DisassociateGlobalReplicationGroup removes a secondary replication group from a global replication group.
func (b *InMemoryBackend) DisassociateGlobalReplicationGroup(
	_ context.Context,
	id, _, _ string,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("DisassociateGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}
	if err := b.requireAvailableLocked(
		grg.Status, grg.PendingStatus, grg.AvailableAt, ErrGlobalReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	result := *grg

	return &result, nil
}

// FailoverGlobalReplicationGroup promotes a secondary region to primary.
func (b *InMemoryBackend) FailoverGlobalReplicationGroup(
	_ context.Context,
	id, _, _ string,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("FailoverGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}
	if err := b.requireAvailableLocked(
		grg.Status, grg.PendingStatus, grg.AvailableAt, ErrGlobalReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	result := *grg

	return &result, nil
}

// IncreaseNodeGroupsInGlobalReplicationGroup increases the node group count.
// AWS documents ApplyImmediately as required, stating "the only permitted
// value for this parameter is true", so applyImmediately=false is rejected.
func (b *InMemoryBackend) IncreaseNodeGroupsInGlobalReplicationGroup(
	_ context.Context,
	id string,
	nodeGroupCount int32,
	applyImmediately bool,
) (*GlobalReplicationGroup, error) {
	if !applyImmediately {
		return nil, ErrApplyImmediatelyRequired
	}

	b.mu.Lock("IncreaseNodeGroupsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}
	if err := b.requireAvailableLocked(
		grg.Status, grg.PendingStatus, grg.AvailableAt, ErrGlobalReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	if nodeGroupCount > grg.NodeGroupCount {
		grg.NodeGroupCount = nodeGroupCount
	}

	result := *grg

	return &result, nil
}

// DecreaseNodeGroupsInGlobalReplicationGroup decreases the node group count.
// See IncreaseNodeGroupsInGlobalReplicationGroup's doc comment: AWS documents
// ApplyImmediately=false as unsupported for this operation too.
func (b *InMemoryBackend) DecreaseNodeGroupsInGlobalReplicationGroup(
	_ context.Context,
	id string,
	nodeGroupCount int32,
	applyImmediately bool,
) (*GlobalReplicationGroup, error) {
	if !applyImmediately {
		return nil, ErrApplyImmediatelyRequired
	}

	b.mu.Lock("DecreaseNodeGroupsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}
	if err := b.requireAvailableLocked(
		grg.Status, grg.PendingStatus, grg.AvailableAt, ErrGlobalReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	if nodeGroupCount > 0 && nodeGroupCount < grg.NodeGroupCount {
		grg.NodeGroupCount = nodeGroupCount
	}

	result := *grg

	return &result, nil
}

// ModifyGlobalReplicationGroup modifies a global replication group.
// applyImmediately is accepted (it is a required wire member) but is not a
// gate on timing here: AWS's own docs for this operation state that GRG
// modifications "cannot be requested to be applied in PreferredMaintenceWindow"
// -- there is no deferred path to honour, on the real API or in this
// backend's GlobalReplicationGroup model (which, unlike ReplicationGroup, has
// no PendingModifiedValues concept). Both true and false apply immediately.
func (b *InMemoryBackend) ModifyGlobalReplicationGroup(
	_ context.Context,
	id, description, engineVersion string,
	automaticFailoverEnabled, applyImmediately bool,
) (*GlobalReplicationGroup, error) {
	_ = applyImmediately

	b.mu.Lock("ModifyGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}
	if err := b.requireAvailableLocked(
		grg.Status, grg.PendingStatus, grg.AvailableAt, ErrGlobalReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	if description != "" {
		grg.Description = description
	}

	if engineVersion != "" {
		grg.EngineVersion = engineVersion
	}

	_ = automaticFailoverEnabled
	b.markTransitionLocked(&grg.PendingStatus, &grg.AvailableAt, statusModifying)

	return b.globalReplicationGroupView(grg), nil
}

// RebalanceSlotsInGlobalReplicationGroup rebalances slots. applyImmediately is
// accepted (it is a required wire member) but, like ModifyGlobalReplicationGroup,
// is not a genuine timing gate: this backend has no background scheduler to
// defer a redistribution onto, so both true and false rebalance synchronously.
func (b *InMemoryBackend) RebalanceSlotsInGlobalReplicationGroup(
	_ context.Context,
	id string,
	applyImmediately bool,
) (*GlobalReplicationGroup, error) {
	_ = applyImmediately

	b.mu.Lock("RebalanceSlotsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}
	if err := b.requireAvailableLocked(
		grg.Status, grg.PendingStatus, grg.AvailableAt, ErrGlobalReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	result := *grg

	return &result, nil
}
