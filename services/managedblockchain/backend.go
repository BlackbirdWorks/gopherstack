package managedblockchain

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNetworkNotFound is returned when a network does not exist.
	ErrNetworkNotFound = awserr.New("ResourceNotFoundException: network not found", awserr.ErrNotFound)
	// ErrMemberNotFound is returned when a member does not exist.
	ErrMemberNotFound = awserr.New("ResourceNotFoundException: member not found", awserr.ErrNotFound)
	// ErrResourceNotFound is returned when a resource (network or member) cannot be found by ARN.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException: resource not found", awserr.ErrNotFound)
	// ErrNetworkAlreadyExists is returned when a network already exists.
	ErrNetworkAlreadyExists = awserr.New(
		"ResourceAlreadyExistsException: network already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrMissingNetworkName is returned when the network name is missing.
	ErrMissingNetworkName = errors.New("Name is required for CreateNetwork")
	// ErrMissingMemberName is returned when the member name is missing.
	ErrMissingMemberName = errors.New("Name is required for member configuration")
	// ErrMissingNetworkID is returned when the network ID is missing from a path.
	ErrMissingNetworkID = errors.New("networkId is required")
)

const (
	// networkStatusAvailable is the status for a ready network.
	networkStatusAvailable = "AVAILABLE"
	// memberStatusAvailable is the status for a ready member.
	memberStatusAvailable = "AVAILABLE"
	// defaultFramework is the default framework for new networks.
	defaultFramework = "HYPERLEDGER_FABRIC"
	// defaultFrameworkVersion is the default framework version.
	defaultFrameworkVersion = "1.4"
)

// StorageBackend is the interface for the Managed Blockchain in-memory backend.
type StorageBackend interface {
	CreateNetwork(
		region, accountID, name, description, framework, frameworkVersion, memberName, memberDescription string,
		tags map[string]string,
	) (*Network, *Member, error)
	GetNetwork(networkID string) (*Network, error)
	ListNetworks() ([]*Network, error)
	CreateMember(region, accountID, networkID, name, description string, tags map[string]string) (*Member, error)
	GetMember(networkID, memberID string) (*Member, error)
	ListMembers(networkID string) ([]*Member, error)
	DeleteMember(networkID, memberID string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	networks map[string]*Network
	members  map[string]map[string]*Member // networkID → memberID → Member
	mu       sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory Managed Blockchain backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		networks: make(map[string]*Network),
		members:  make(map[string]map[string]*Member),
	}
}

// networkARN builds the ARN for a Managed Blockchain network.
func networkARN(region, accountID, networkID string) string {
	return arn.Build("managedblockchain", region, accountID, fmt.Sprintf("networks/%s", networkID))
}

// memberARN builds the ARN for a Managed Blockchain member.
func memberARN(region, accountID, memberID string) string {
	return arn.Build("managedblockchain", region, accountID, fmt.Sprintf("members/%s", memberID))
}

// CreateNetwork creates a new Managed Blockchain network and its first member.
func (b *InMemoryBackend) CreateNetwork(
	region, accountID, name, description, framework, frameworkVersion, memberName, memberDescription string,
	tags map[string]string,
) (*Network, *Member, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, n := range b.networks {
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
	}

	b.networks[networkID] = network
	b.members[networkID] = make(map[string]*Member)

	member := &Member{
		ID:           memberID,
		Arn:          memberARN(region, accountID, memberID),
		Name:         memberName,
		Description:  memberDescription,
		NetworkID:    networkID,
		Status:       memberStatusAvailable,
		CreationDate: &now,
		Tags:         make(map[string]string),
	}

	b.members[networkID][memberID] = member

	return network, member, nil
}

// GetNetwork returns the details of a network by ID.
func (b *InMemoryBackend) GetNetwork(networkID string) (*Network, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	network, exists := b.networks[networkID]
	if !exists {
		return nil, ErrNetworkNotFound
	}

	return network, nil
}

// ListNetworks returns all networks.
func (b *InMemoryBackend) ListNetworks() ([]*Network, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := make([]*Network, 0, len(b.networks))

	for _, n := range b.networks {
		all = append(all, n)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, nil
}

// CreateMember creates a new member in an existing network.
func (b *InMemoryBackend) CreateMember(
	region, accountID, networkID, name, description string,
	tags map[string]string,
) (*Member, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
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
	}

	if b.members[networkID] == nil {
		b.members[networkID] = make(map[string]*Member)
	}

	b.members[networkID][memberID] = member

	return member, nil
}

// GetMember returns a member by network ID and member ID.
func (b *InMemoryBackend) GetMember(networkID, memberID string) (*Member, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	members, ok := b.members[networkID]
	if !ok {
		return nil, ErrMemberNotFound
	}

	member, exists := members[memberID]
	if !exists {
		return nil, ErrMemberNotFound
	}

	return member, nil
}

// ListMembers returns all members in a network.
func (b *InMemoryBackend) ListMembers(networkID string) ([]*Member, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	members := b.members[networkID]
	all := make([]*Member, 0, len(members))

	for _, m := range members {
		all = append(all, m)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, nil
}

// DeleteMember removes a member from a network.
func (b *InMemoryBackend) DeleteMember(networkID, memberID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
		return ErrNetworkNotFound
	}

	members, ok := b.members[networkID]
	if !ok || members[memberID] == nil {
		return ErrMemberNotFound
	}

	delete(members, memberID)

	return nil
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, network := range b.networks {
		if network.Arn == resourceARN {
			result := make(map[string]string, len(network.Tags))
			maps.Copy(result, network.Tags)

			return result, nil
		}

		for _, member := range b.members[network.ID] {
			if member.Arn == resourceARN {
				result := make(map[string]string, len(member.Tags))
				maps.Copy(result, member.Tags)

				return result, nil
			}
		}
	}

	return nil, ErrResourceNotFound
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, network := range b.networks {
		if network.Arn == resourceARN {
			if network.Tags == nil {
				network.Tags = make(map[string]string)
			}

			maps.Copy(network.Tags, tags)

			return nil
		}

		for _, member := range b.members[network.ID] {
			if member.Arn == resourceARN {
				if member.Tags == nil {
					member.Tags = make(map[string]string)
				}

				maps.Copy(member.Tags, tags)

				return nil
			}
		}
	}

	return ErrResourceNotFound
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, network := range b.networks {
		if network.Arn == resourceARN {
			for _, k := range tagKeys {
				delete(network.Tags, k)
			}

			return nil
		}

		for _, member := range b.members[network.ID] {
			if member.Arn == resourceARN {
				for _, k := range tagKeys {
					delete(member.Tags, k)
				}

				return nil
			}
		}
	}

	return ErrResourceNotFound
}
