package managedblockchain

import (
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// memberStatusAvailable is the status for a ready member.
const memberStatusAvailable = "AVAILABLE"

// memberARN builds the ARN for a Managed Blockchain member.
func memberARN(region, accountID, memberID string) string {
	return arn.Build("managedblockchain", region, accountID, "members/"+memberID)
}

// cloneMember returns a deep copy of m with the Tags map cloned.
func cloneMember(m *Member) *Member {
	cp := *m
	cp.Tags = maps.Clone(m.Tags)

	return &cp
}

// CreateMember creates a new member in an existing network.
func (b *InMemoryBackend) CreateMember(
	region, accountID, networkID, name, description string,
	tags map[string]string,
) (*Member, error) {
	b.mu.Lock("CreateMember")
	defer b.mu.Unlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return nil, ErrNetworkNotFound
	}

	now := time.Now().UTC()
	memberID := uuid.NewString()

	t := make(map[string]string)
	maps.Copy(t, tags)

	member := &Member{
		ID:           memberID,
		Arn:          memberARN(region, accountID, memberID),
		Name:         name,
		Description:  description,
		NetworkID:    networkID,
		Status:       memberStatusAvailable,
		CreationDate: &now,
		Tags:         t,
		IsOwned:      true,
	}

	b.members.Put(member)
	b.arnToResource[member.Arn] = member

	return cloneMember(member), nil
}

// GetMember returns a member by network ID and member ID.
func (b *InMemoryBackend) GetMember(networkID, memberID string) (*Member, error) {
	b.mu.RLock("GetMember")
	defer b.mu.RUnlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return nil, ErrNetworkNotFound
	}

	member, exists := b.members.Get(memberKey(networkID, memberID))
	if !exists {
		return nil, ErrMemberNotFound
	}

	return cloneMember(member), nil
}

// ListMembers returns all members in a network, optionally filtered.
func (b *InMemoryBackend) ListMembers(networkID string, filter ListMembersFilter) ([]*Member, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return nil, ErrNetworkNotFound
	}

	members := b.membersByNetwork.Get(networkID)
	all := make([]*Member, 0, len(members))

	for _, m := range members {
		if filter.Name != "" && m.Name != filter.Name {
			continue
		}

		if filter.Status != "" && m.Status != filter.Status {
			continue
		}

		if filter.IsOwned != nil && m.IsOwned != *filter.IsOwned {
			continue
		}

		all = append(all, cloneMember(m))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, nil
}

// DeleteMember removes a member from a network, cascading the delete to all of its nodes.
func (b *InMemoryBackend) DeleteMember(networkID, memberID string) error {
	b.mu.Lock("DeleteMember")
	defer b.mu.Unlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return ErrNetworkNotFound
	}

	m, exists := b.members.Get(memberKey(networkID, memberID))
	if !exists {
		return ErrMemberNotFound
	}

	delete(b.arnToResource, m.Arn)
	b.members.Delete(memberKey(networkID, memberID))

	b.deleteNodesForMemberLocked(networkID, memberID)

	return nil
}

// AddMemberInternal adds a member directly to the backend (for testing and seeding).
// The network must already exist.
func (b *InMemoryBackend) AddMemberInternal(region, accountID, networkID, name string) *Member {
	b.mu.Lock("AddMemberInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	memberID := uuid.NewString()

	member := &Member{
		ID:           memberID,
		Arn:          memberARN(region, accountID, memberID),
		Name:         name,
		NetworkID:    networkID,
		Status:       memberStatusAvailable,
		CreationDate: &now,
		Tags:         make(map[string]string),
		IsOwned:      true,
	}

	b.members.Put(member)
	b.arnToResource[member.Arn] = member

	return cloneMember(member)
}

// UpdateMember updates a member's log publishing configuration.
func (b *InMemoryBackend) UpdateMember(
	networkID, memberID string,
	logConfig *MemberLogPublishingConfigState,
) (*Member, error) {
	b.mu.Lock("UpdateMember")
	defer b.mu.Unlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return nil, ErrNetworkNotFound
	}

	m, exists := b.members.Get(memberKey(networkID, memberID))
	if !exists {
		return nil, ErrMemberNotFound
	}

	if logConfig != nil {
		m.LogPublishingConfiguration = cloneMemberLogConfig(logConfig)
	}

	return cloneMember(m), nil
}

// cloneMemberLogConfig returns a deep copy of MemberLogPublishingConfigState.
func cloneMemberLogConfig(c *MemberLogPublishingConfigState) *MemberLogPublishingConfigState {
	if c == nil {
		return nil
	}

	cp := &MemberLogPublishingConfigState{}

	if c.Fabric != nil {
		fabric := &MemberFabricLogState{}

		if c.Fabric.CALogs != nil {
			caLogs := cloneLogConfig(c.Fabric.CALogs)
			fabric.CALogs = caLogs
		}

		cp.Fabric = fabric
	}

	return cp
}

// cloneLogConfig returns a deep copy of LogConfigState. Shared by both
// cloneMemberLogConfig above and cloneNodeLogConfig in nodes.go.
func cloneLogConfig(c *LogConfigState) *LogConfigState {
	if c == nil {
		return nil
	}

	cp := &LogConfigState{}

	if c.CloudWatch != nil {
		cw := *c.CloudWatch
		cp.CloudWatch = &cw
	}

	return cp
}
