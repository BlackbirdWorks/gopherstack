package codeconnections

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Connections        map[string]map[string]*Connection        `json:"connections"`
	ConnectionsByName  map[string]map[string]string             `json:"connectionsByName"`
	Hosts              map[string]map[string]*Host              `json:"hosts"`
	HostsByName        map[string]map[string]string             `json:"hostsByName"`
	RepositoryLinks    map[string]map[string]*RepositoryLink    `json:"repositoryLinks"`
	SyncConfigurations map[string]map[string]*SyncConfiguration `json:"syncConfigurations"`
	AccountID          string                                   `json:"accountID"`
	Region             string                                   `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.Connections == nil {
		s.Connections = make(map[string]map[string]*Connection)
	}

	if s.ConnectionsByName == nil {
		s.ConnectionsByName = make(map[string]map[string]string)
	}

	if s.Hosts == nil {
		s.Hosts = make(map[string]map[string]*Host)
	}

	if s.HostsByName == nil {
		s.HostsByName = make(map[string]map[string]string)
	}

	if s.RepositoryLinks == nil {
		s.RepositoryLinks = make(map[string]map[string]*RepositoryLink)
	}

	if s.SyncConfigurations == nil {
		s.SyncConfigurations = make(map[string]map[string]*SyncConfiguration)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Connections:        b.connections,
		ConnectionsByName:  b.connectionsByName,
		Hosts:              b.hosts,
		HostsByName:        b.hostsByName,
		RepositoryLinks:    b.repositoryLinks,
		SyncConfigurations: b.syncConfigurations,
		AccountID:          b.accountID,
		Region:             b.defaultRegion,
	}

	return persistence.MarshalSnapshot(ctx, "codeconnections", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "codeconnections", data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.connections = snap.Connections
	b.connectionsByName = snap.ConnectionsByName
	b.hosts = snap.Hosts
	b.hostsByName = snap.HostsByName
	b.repositoryLinks = snap.RepositoryLinks
	b.syncConfigurations = snap.SyncConfigurations
	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region

	return nil
}
