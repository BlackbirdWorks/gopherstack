package transfer

import (
	"encoding/json"
	"log/slog"
	"maps"
)

// backendSnapshot is the serialisable representation of InMemoryBackend state.
type backendSnapshot struct {
	Servers       map[string]*Server                             `json:"servers"`
	Users         map[string]map[string]*User                    `json:"users"`
	Accesses      map[string]map[string]*Access                  `json:"accesses"`
	Agreements    map[string]map[string]*Agreement               `json:"agreements"`
	Connectors    map[string]*Connector                          `json:"connectors"`
	Profiles      map[string]*Profile                            `json:"profiles"`
	WebApps       map[string]*WebApp                             `json:"web_apps"`
	Workflows     map[string]*Workflow                           `json:"workflows"`
	Certificates  map[string]*Certificate                        `json:"certificates"`
	HostKeys      map[string]map[string]*HostKey                 `json:"host_keys"`
	SshPublicKeys map[string]map[string]map[string]*SshPublicKey `json:"ssh_public_keys"`
	TagsStore     map[string]map[string]string                   `json:"tags_store"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Servers:       make(map[string]*Server, len(b.servers)),
		Users:         make(map[string]map[string]*User, len(b.users)),
		Accesses:      make(map[string]map[string]*Access, len(b.accesses)),
		Agreements:    make(map[string]map[string]*Agreement, len(b.agreements)),
		Connectors:    make(map[string]*Connector, len(b.connectors)),
		Profiles:      make(map[string]*Profile, len(b.profiles)),
		WebApps:       make(map[string]*WebApp, len(b.webApps)),
		Workflows:     make(map[string]*Workflow, len(b.workflows)),
		Certificates:  make(map[string]*Certificate, len(b.certificates)),
		HostKeys:      make(map[string]map[string]*HostKey, len(b.hostKeys)),
		SshPublicKeys: make(map[string]map[string]map[string]*SshPublicKey, len(b.sshPublicKeys)),
		TagsStore:     make(map[string]map[string]string, len(b.tagsStore)),
	}

	for k, v := range b.servers {
		snap.Servers[k] = cloneServer(v)
	}

	for sid, userMap := range b.users {
		m := make(map[string]*User, len(userMap))
		for k, v := range userMap {
			m[k] = cloneUser(v)
		}
		snap.Users[sid] = m
	}

	for sid, accessMap := range b.accesses {
		m := make(map[string]*Access, len(accessMap))
		for k, v := range accessMap {
			m[k] = cloneAccess(v)
		}
		snap.Accesses[sid] = m
	}

	for sid, agMap := range b.agreements {
		m := make(map[string]*Agreement, len(agMap))
		for k, v := range agMap {
			m[k] = cloneAgreement(v)
		}
		snap.Agreements[sid] = m
	}

	for k, v := range b.connectors {
		snap.Connectors[k] = cloneConnector(v)
	}

	for k, v := range b.profiles {
		snap.Profiles[k] = cloneProfile(v)
	}

	for k, v := range b.webApps {
		snap.WebApps[k] = cloneWebApp(v)
	}

	for k, v := range b.workflows {
		snap.Workflows[k] = cloneWorkflow(v)
	}

	for k, v := range b.certificates {
		cp := *v
		cp.Tags = make(map[string]string, len(v.Tags))
		maps.Copy(cp.Tags, v.Tags)
		snap.Certificates[k] = &cp
	}

	for sid, keyMap := range b.hostKeys {
		m := make(map[string]*HostKey, len(keyMap))
		for k, v := range keyMap {
			m[k] = cloneHostKey(v)
		}
		snap.HostKeys[sid] = m
	}

	for sid, userMap := range b.sshPublicKeys {
		um := make(map[string]map[string]*SshPublicKey, len(userMap))
		for userName, keyMap := range userMap {
			km := make(map[string]*SshPublicKey, len(keyMap))
			for k, v := range keyMap {
				cp := *v
				km[k] = &cp
			}
			um[userName] = km
		}
		snap.SshPublicKeys[sid] = um
	}

	for arn, tagMap := range b.tagsStore {
		m := make(map[string]string, len(tagMap))
		maps.Copy(m, tagMap)
		snap.TagsStore[arn] = m
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("transfer: failed to marshal snapshot", "error", err)

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

	ensureNonNilMaps(&snap)

	b.servers = snap.Servers
	b.users = snap.Users
	b.accesses = snap.Accesses
	b.agreements = snap.Agreements
	b.connectors = snap.Connectors
	b.profiles = snap.Profiles
	b.webApps = snap.WebApps
	b.workflows = snap.Workflows
	b.certificates = snap.Certificates
	b.hostKeys = snap.HostKeys
	b.sshPublicKeys = snap.SshPublicKeys
	b.tagsStore = snap.TagsStore

	return nil
}

// ensureNonNilMaps guarantees no map field in the snapshot is nil after
// unmarshalling.
func ensureNonNilMaps(s *backendSnapshot) {
	if s.Servers == nil {
		s.Servers = make(map[string]*Server)
	}

	if s.Users == nil {
		s.Users = make(map[string]map[string]*User)
	}

	if s.Accesses == nil {
		s.Accesses = make(map[string]map[string]*Access)
	}

	if s.Agreements == nil {
		s.Agreements = make(map[string]map[string]*Agreement)
	}

	if s.Connectors == nil {
		s.Connectors = make(map[string]*Connector)
	}

	if s.Profiles == nil {
		s.Profiles = make(map[string]*Profile)
	}

	if s.WebApps == nil {
		s.WebApps = make(map[string]*WebApp)
	}

	if s.Workflows == nil {
		s.Workflows = make(map[string]*Workflow)
	}

	if s.Certificates == nil {
		s.Certificates = make(map[string]*Certificate)
	}

	if s.HostKeys == nil {
		s.HostKeys = make(map[string]map[string]*HostKey)
	}

	if s.SshPublicKeys == nil {
		s.SshPublicKeys = make(map[string]map[string]map[string]*SshPublicKey)
	}

	if s.TagsStore == nil {
		s.TagsStore = make(map[string]map[string]string)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(data)
	}

	return nil
}
