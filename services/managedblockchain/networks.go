package managedblockchain

import (
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	// networkStatusAvailable is the status for a ready network.
	networkStatusAvailable = "AVAILABLE"
	// defaultFramework is the default framework for new networks.
	defaultFramework = "HYPERLEDGER_FABRIC"
	// defaultFrameworkVersion is the default framework version.
	defaultFrameworkVersion = "1.4"
)

// networkARN builds the ARN for a Managed Blockchain network.
func networkARN(region, accountID, networkID string) string {
	return arn.Build("managedblockchain", region, accountID, "networks/"+networkID)
}

// CreateNetwork creates a new Managed Blockchain network and its first member.
func (b *InMemoryBackend) CreateNetwork(
	region, accountID, name, description, framework, frameworkVersion, memberName, memberDescription string,
	tags map[string]string,
	votingPolicy *VotingPolicy,
) (*Network, *Member, error) {
	b.mu.Lock("CreateNetwork")
	defer b.mu.Unlock()

	for _, n := range b.networks.All() {
		if n.Name == name {
			return nil, nil, ErrNetworkAlreadyExists
		}
	}

	now := time.Now().UTC()
	networkID := uuid.NewString()
	memberID := uuid.NewString()

	fw := framework
	if fw == "" {
		fw = defaultFramework
	}

	fwv := frameworkVersion
	if fwv == "" {
		fwv = defaultFrameworkVersion
	}

	t := make(map[string]string)
	maps.Copy(t, tags)

	network := &Network{
		ID:               networkID,
		Arn:              networkARN(region, accountID, networkID),
		Name:             name,
		Description:      description,
		Framework:        fw,
		FrameworkVersion: fwv,
		Status:           networkStatusAvailable,
		CreationDate:     &now,
		Tags:             t,
		VotingPolicy:     cloneVotingPolicy(votingPolicy),
	}

	b.networks.Put(network)
	b.arnToResource[network.Arn] = network

	member := &Member{
		ID:           memberID,
		Arn:          memberARN(region, accountID, memberID),
		Name:         memberName,
		Description:  memberDescription,
		NetworkID:    networkID,
		Status:       memberStatusAvailable,
		CreationDate: &now,
		Tags:         make(map[string]string),
		IsOwned:      true,
	}

	b.members.Put(member)
	b.arnToResource[member.Arn] = member

	return cloneNetwork(network), cloneMember(member), nil
}

// cloneVotingPolicy returns a deep copy of a VotingPolicy.
func cloneVotingPolicy(vp *VotingPolicy) *VotingPolicy {
	if vp == nil {
		return nil
	}

	cp := *vp

	if vp.ApprovalThresholdPolicy != nil {
		atp := *vp.ApprovalThresholdPolicy
		cp.ApprovalThresholdPolicy = &atp
	}

	return &cp
}

// cloneNetwork returns a deep copy of n with the Tags map cloned.
func cloneNetwork(n *Network) *Network {
	cp := *n
	cp.Tags = maps.Clone(n.Tags)
	cp.VotingPolicy = cloneVotingPolicy(n.VotingPolicy)

	return &cp
}

// GetNetwork returns the details of a network by ID.
func (b *InMemoryBackend) GetNetwork(networkID string) (*Network, error) {
	b.mu.RLock("GetNetwork")
	defer b.mu.RUnlock()

	network, exists := b.networks.Get(networkID)
	if !exists {
		return nil, ErrNetworkNotFound
	}

	return cloneNetwork(network), nil
}

// ListNetworks returns all networks, optionally filtered.
func (b *InMemoryBackend) ListNetworks(filter ListNetworksFilter) ([]*Network, error) {
	b.mu.RLock("ListNetworks")
	defer b.mu.RUnlock()

	all := make([]*Network, 0, b.networks.Len())

	for _, n := range b.networks.All() {
		if filter.Name != "" && n.Name != filter.Name {
			continue
		}

		if filter.Framework != "" && n.Framework != filter.Framework {
			continue
		}

		if filter.Status != "" && n.Status != filter.Status {
			continue
		}

		all = append(all, cloneNetwork(n))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, nil
}

// AddNetworkInternal adds a network directly to the backend (for testing and seeding).
func (b *InMemoryBackend) AddNetworkInternal(region, accountID, name string) *Network {
	b.mu.Lock("AddNetworkInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	networkID := uuid.NewString()

	network := &Network{
		ID:               networkID,
		Arn:              networkARN(region, accountID, networkID),
		Name:             name,
		Framework:        defaultFramework,
		FrameworkVersion: defaultFrameworkVersion,
		Status:           networkStatusAvailable,
		CreationDate:     &now,
		Tags:             make(map[string]string),
	}

	b.networks.Put(network)
	b.arnToResource[network.Arn] = network

	return cloneNetwork(network)
}
