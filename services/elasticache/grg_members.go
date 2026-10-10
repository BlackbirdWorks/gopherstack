package elasticache

import (
	"context"
	"fmt"
	"sort"
)

const (
	memberRolePrimary   = "PRIMARY"
	memberRoleSecondary = "SECONDARY"
	memberStatus        = "associated"
)

// globalMembersLocked lists the replication groups making up grg. Must hold b.mu.
func (b *InMemoryBackend) globalMembersLocked(grg *GlobalReplicationGroup) []GlobalReplicationGroupMember {
	var out []GlobalReplicationGroupMember

	if grg.PrimaryReplicationGroupID != "" {
		out = append(out, b.globalMember(grg.PrimaryReplicationGroupID, grg.PrimaryReplicationGroupRegion,
			memberRolePrimary))
	}

	ids := make([]string, 0, len(grg.SecondaryReplicationGroups))
	for id := range grg.SecondaryReplicationGroups {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	for _, id := range ids {
		out = append(out, b.globalMember(id, grg.SecondaryReplicationGroups[id], memberRoleSecondary))
	}

	return out
}

func (b *InMemoryBackend) globalMember(id, region, role string) GlobalReplicationGroupMember {
	failover := statusDisabled

	if rg, ok := b.replicationGroupsStoreRO(region).Get(id); ok && rg.AutomaticFailover != "" {
		failover = rg.AutomaticFailover
	}

	return GlobalReplicationGroupMember{
		ReplicationGroupID: id,
		Region:             region,
		Role:               role,
		AutomaticFailover:  failover,
		Status:             memberStatus,
	}
}

// linkGlobalGroupLocked records rg as a secondary of grgID (no-op when empty).
func (b *InMemoryBackend) linkGlobalGroupLocked(rg *ReplicationGroup, region, grgID string) {
	if grgID == "" {
		return
	}

	grg, ok := b.getGlobalReplicationGroup(grgID)
	if !ok {
		return
	}

	rg.GlobalReplicationGroupID = grgID
	rg.GlobalReplicationGroupRole = groupRoleSecondary

	if grg.SecondaryReplicationGroups == nil {
		grg.SecondaryReplicationGroups = make(map[string]string)
	}

	grg.SecondaryReplicationGroups[rg.ReplicationGroupID] = region
}

// unlinkGlobalMemberLocked clears the global-datastore link of the replication group id in region.
func (b *InMemoryBackend) unlinkGlobalMemberLocked(id, region string) {
	if rg, ok := b.replicationGroupsStore(region).Get(id); ok {
		rg.GlobalReplicationGroupID = ""
		rg.GlobalReplicationGroupRole = ""
	}
}

// DisassociateGlobalReplicationGroup removes a secondary replication group from a global replication group.
func (b *InMemoryBackend) DisassociateGlobalReplicationGroup(
	_ context.Context,
	id, replicationGroupID, replicationGroupRegion string,
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

	if region, member := grg.SecondaryReplicationGroups[replicationGroupID]; !member ||
		region != replicationGroupRegion {
		return nil, fmt.Errorf("%w: %s in %s is not a secondary of %s", ErrInvalidParameterValue,
			replicationGroupID, replicationGroupRegion, id)
	}

	delete(grg.SecondaryReplicationGroups, replicationGroupID)
	b.unlinkGlobalMemberLocked(replicationGroupID, replicationGroupRegion)

	return b.globalReplicationGroupView(grg), nil
}

// FailoverGlobalReplicationGroup promotes a secondary replication group to primary.
func (b *InMemoryBackend) FailoverGlobalReplicationGroup(
	_ context.Context,
	id, primaryRegion, primaryReplicationGroupID string,
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

	if grg.PrimaryReplicationGroupID == "" && len(grg.SecondaryReplicationGroups) == 0 {
		return b.globalReplicationGroupView(grg), nil
	}

	if region, member := grg.SecondaryReplicationGroups[primaryReplicationGroupID]; !member || region != primaryRegion {
		return nil, fmt.Errorf("%w: %s in %s is not a secondary of %s", ErrInvalidParameterValue,
			primaryReplicationGroupID, primaryRegion, id)
	}

	oldID, oldRegion := grg.PrimaryReplicationGroupID, grg.PrimaryReplicationGroupRegion

	delete(grg.SecondaryReplicationGroups, primaryReplicationGroupID)

	if oldID != "" {
		grg.SecondaryReplicationGroups[oldID] = oldRegion

		if rg, found := b.replicationGroupsStore(oldRegion).Get(oldID); found {
			rg.GlobalReplicationGroupRole = groupRoleSecondary
		}
	}

	grg.PrimaryReplicationGroupID, grg.PrimaryReplicationGroupRegion = primaryReplicationGroupID, primaryRegion

	if rg, found := b.replicationGroupsStore(primaryRegion).Get(primaryReplicationGroupID); found {
		rg.GlobalReplicationGroupRole = groupRolePrimary
	}

	return b.globalReplicationGroupView(grg), nil
}
