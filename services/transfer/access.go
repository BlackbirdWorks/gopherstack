package transfer

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

// CreateAccessInput holds all optional fields for CreateAccess.
type CreateAccessInput struct {
	PosixProfile          *PosixProfile
	Tags                  map[string]string
	ServerID              string
	ExternalID            string
	Role                  string
	HomeDir               string
	HomeDirectoryType     string
	Policy                string
	HomeDirectoryMappings []HomeDirectoryMapEntry
}

// CreateAccess creates an access policy entry on an existing server.
func (b *InMemoryBackend) CreateAccess(
	serverID, externalID, role, homeDir string,
	tags map[string]string,
) (*Access, error) {
	return b.CreateAccessFull(&CreateAccessInput{
		ServerID:   serverID,
		ExternalID: externalID,
		Role:       role,
		HomeDir:    homeDir,
		Tags:       tags,
	})
}

// CreateAccessFull creates an access entry with full configuration.
func (b *InMemoryBackend) CreateAccessFull(in *CreateAccessInput) (*Access, error) {
	b.mu.Lock("CreateAccess")
	defer b.mu.Unlock()

	if !b.servers.Has(in.ServerID) {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, in.ServerID)
	}

	// ExternalId must be unique per server.
	if b.accesses.Has(accessKey(in.ServerID, in.ExternalID)) {
		return nil, fmt.Errorf(
			"%w: access with ExternalId %s already exists on server %s",
			ErrAccessAlreadyExists,
			in.ExternalID,
			in.ServerID,
		)
	}

	merged := make(map[string]string, len(in.Tags))
	maps.Copy(merged, in.Tags)

	homeDirectoryType := in.HomeDirectoryType
	if homeDirectoryType == "" {
		homeDirectoryType = homeDirectoryTypePath
	}

	a := &Access{
		ExternalID:            in.ExternalID,
		ServerID:              in.ServerID,
		Role:                  in.Role,
		HomeDir:               in.HomeDir,
		HomeDirectoryType:     homeDirectoryType,
		HomeDirectoryMappings: in.HomeDirectoryMappings,
		Policy:                in.Policy,
		PosixProfile:          in.PosixProfile,
		CreatedAt:             time.Now(),
		Tags:                  merged,
		AccountID:             b.accountID,
		Region:                b.region,
	}
	b.accesses.Put(a)

	return cloneAccess(a), nil
}

// DeleteAccess removes an access entry from a server.
func (b *InMemoryBackend) DeleteAccess(serverID, externalID string) error {
	b.mu.Lock("DeleteAccess")
	defer b.mu.Unlock()

	if !b.servers.Has(serverID) {
		return fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	key := accessKey(serverID, externalID)
	if !b.accesses.Has(key) {
		return fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	b.accesses.Delete(key)

	return nil
}

// DescribeAccess returns an access entry from a server.
func (b *InMemoryBackend) DescribeAccess(serverID, externalID string) (*Access, error) {
	b.mu.RLock("DescribeAccess")
	defer b.mu.RUnlock()

	a, ok := b.accesses.Get(accessKey(serverID, externalID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	return cloneAccess(a), nil
}

// ListAccesses returns all accesses on a server sorted by externalID.
func (b *InMemoryBackend) ListAccesses(serverID string) ([]*Access, error) {
	b.mu.RLock("ListAccesses")
	defer b.mu.RUnlock()

	if !b.servers.Has(serverID) {
		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	accesses := b.accessesByServer.Get(serverID)
	out := make([]*Access, 0, len(accesses))

	for _, a := range accesses {
		out = append(out, cloneAccess(a))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ExternalID < out[j].ExternalID
	})

	return out, nil
}

// UpdateAccess updates mutable fields on an access entry.
func (b *InMemoryBackend) UpdateAccess(
	serverID, externalID, role, homeDir string,
) (*Access, error) {
	b.mu.Lock("UpdateAccess")
	defer b.mu.Unlock()

	a, ok := b.accesses.Get(accessKey(serverID, externalID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound,
			externalID,
			serverID,
		)
	}

	if role != "" {
		a.Role = role
	}

	if homeDir != "" {
		a.HomeDir = homeDir
	}

	return cloneAccess(a), nil
}

// UpdateAccessInput holds all mutable fields for UpdateAccessFull.
type UpdateAccessInput struct {
	PosixProfile             *PosixProfile
	ServerID                 string
	ExternalID               string
	Role                     string
	HomeDir                  string
	HomeDirectoryType        string
	Policy                   string
	HomeDirectoryMappings    []HomeDirectoryMapEntry
	SetPosixProfile          bool
	SetHomeDirectoryType     bool
	SetPolicy                bool
	SetHomeDirectoryMappings bool
}

// UpdateAccessFull updates all mutable fields on an access entry.
func (b *InMemoryBackend) UpdateAccessFull(in *UpdateAccessInput) (*Access, error) {
	b.mu.Lock("UpdateAccessFull")
	defer b.mu.Unlock()

	a, ok := b.accesses.Get(accessKey(in.ServerID, in.ExternalID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: access %s not found on server %s",
			ErrAccessNotFound, in.ExternalID, in.ServerID,
		)
	}

	if in.Role != "" {
		a.Role = in.Role
	}

	if in.HomeDir != "" {
		a.HomeDir = in.HomeDir
	}

	if in.SetHomeDirectoryType {
		a.HomeDirectoryType = in.HomeDirectoryType
	}

	if in.SetPolicy {
		a.Policy = in.Policy
	}

	if in.SetPosixProfile {
		a.PosixProfile = in.PosixProfile
	}

	if in.SetHomeDirectoryMappings {
		if in.HomeDirectoryMappings != nil {
			cp := make([]HomeDirectoryMapEntry, len(in.HomeDirectoryMappings))
			copy(cp, in.HomeDirectoryMappings)
			a.HomeDirectoryMappings = cp
		} else {
			a.HomeDirectoryMappings = nil
		}
	}

	return cloneAccess(a), nil
}
