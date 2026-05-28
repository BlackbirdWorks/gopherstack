package codeconnections

import "encoding/json"

type backendSnapshot struct {
	Connections        map[string]*Connection        `json:"connections"`
	ConnectionsByName  map[string]string             `json:"connectionsByName"`
	Hosts              map[string]*Host              `json:"hosts"`
	HostsByName        map[string]string             `json:"hostsByName"`
	RepositoryLinks    map[string]*RepositoryLink    `json:"repositoryLinks"`
	SyncConfigurations map[string]*SyncConfiguration `json:"syncConfigurations"`
	AccountID          string                        `json:"accountID"`
	Region             string                        `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
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
		Region:             b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Connections == nil {
		snap.Connections = make(map[string]*Connection)
	}

	if snap.ConnectionsByName == nil {
		snap.ConnectionsByName = make(map[string]string)
	}

	if snap.Hosts == nil {
		snap.Hosts = make(map[string]*Host)
	}

	if snap.HostsByName == nil {
		snap.HostsByName = make(map[string]string)
	}

	if snap.RepositoryLinks == nil {
		snap.RepositoryLinks = make(map[string]*RepositoryLink)
	}

	if snap.SyncConfigurations == nil {
		snap.SyncConfigurations = make(map[string]*SyncConfiguration)
	}

	b.connections = snap.Connections
	b.connectionsByName = snap.ConnectionsByName
	b.hosts = snap.Hosts
	b.hostsByName = snap.HostsByName
	b.repositoryLinks = snap.RepositoryLinks
	b.syncConfigurations = snap.SyncConfigurations
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
