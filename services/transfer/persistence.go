package transfer

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// backendSnapshot is the serialisable representation of InMemoryBackend state.
type backendSnapshot struct {
	Servers             map[string]*Server                             `json:"servers"`
	Users               map[string]map[string]*User                    `json:"users"`
	Accesses            map[string]map[string]*Access                  `json:"accesses"`
	Agreements          map[string]map[string]*Agreement               `json:"agreements"`
	Connectors          map[string]*Connector                          `json:"connectors"`
	Profiles            map[string]*Profile                            `json:"profiles"`
	WebApps             map[string]*WebApp                             `json:"web_apps"`
	Workflows           map[string]*Workflow                           `json:"workflows"`
	Certificates        map[string]*Certificate                        `json:"certificates"`
	HostKeys            map[string]map[string]*HostKey                 `json:"host_keys"`
	SSHPublicKeys       map[string]map[string]map[string]*SSHPublicKey `json:"ssh_public_keys"`
	Executions          map[string]map[string]*Execution               `json:"executions"`
	TagsStore           map[string]map[string]string                   `json:"tags_store"`
	FileTransferResults map[string]*FileTransferResult                 `json:"transfer_records"`
	AsyncOperations     map[string]*AsyncOperationRecord               `json:"async_operations"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := b.buildSnapshot()

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "transfer: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// buildSnapshot constructs the serialisable snapshot from backend state.
func (b *InMemoryBackend) buildSnapshot() backendSnapshot {
	snap := backendSnapshot{
		Servers:             snapshotServers(b.servers),
		Users:               snapshotNestedMap(b.users, cloneUser),
		Accesses:            snapshotNestedMap(b.accesses, cloneAccess),
		Agreements:          snapshotNestedMap(b.agreements, cloneAgreement),
		Connectors:          snapshotFlatMap(b.connectors, cloneConnector),
		Profiles:            snapshotFlatMap(b.profiles, cloneProfile),
		WebApps:             snapshotFlatMap(b.webApps, cloneWebApp),
		Workflows:           snapshotFlatMap(b.workflows, cloneWorkflow),
		Certificates:        snapshotCertificates(b.certificates),
		HostKeys:            snapshotNestedMap(b.hostKeys, cloneHostKey),
		SSHPublicKeys:       snapshotSSHPublicKeys(b.sshPublicKeys),
		Executions:          snapshotExecutions(b.executions),
		TagsStore:           snapshotTagsStore(b.tagsStore),
		FileTransferResults: snapshotFileTransferResults(b.transferRecords),
		AsyncOperations:     snapshotAsyncOperations(b.asyncOperations),
	}

	return snap
}

// snapshotFileTransferResults clones the transfer records map.
func snapshotFileTransferResults(src map[string]*FileTransferResult) map[string]*FileTransferResult {
	dst := make(map[string]*FileTransferResult, len(src))
	for k, v := range src {
		cp := *v
		if v.Files != nil {
			cp.Files = make([]string, len(v.Files))
			copy(cp.Files, v.Files)
		}
		dst[k] = &cp
	}

	return dst
}

// snapshotAsyncOperations clones the async operations map.
func snapshotAsyncOperations(src map[string]*AsyncOperationRecord) map[string]*AsyncOperationRecord {
	dst := make(map[string]*AsyncOperationRecord, len(src))
	for k, v := range src {
		cp := *v
		dst[k] = &cp
	}

	return dst
}

// snapshotServers clones the servers map.
func snapshotServers(src map[string]*Server) map[string]*Server {
	dst := make(map[string]*Server, len(src))
	for k, v := range src {
		dst[k] = cloneServer(v)
	}

	return dst
}

// snapshotFlatMap clones a flat map using the provided clone function.
func snapshotFlatMap[V any](src map[string]*V, clone func(*V) *V) map[string]*V {
	dst := make(map[string]*V, len(src))
	for k, v := range src {
		dst[k] = clone(v)
	}

	return dst
}

// snapshotNestedMap clones a two-level nested map using the provided clone function.
func snapshotNestedMap[V any](
	src map[string]map[string]*V,
	clone func(*V) *V,
) map[string]map[string]*V {
	dst := make(map[string]map[string]*V, len(src))
	for sid, inner := range src {
		m := make(map[string]*V, len(inner))
		for k, v := range inner {
			m[k] = clone(v)
		}
		dst[sid] = m
	}

	return dst
}

// snapshotCertificates clones the certificates map including tag maps.
func snapshotCertificates(src map[string]*Certificate) map[string]*Certificate {
	dst := make(map[string]*Certificate, len(src))
	for k, v := range src {
		cp := *v
		cp.Tags = make(map[string]string, len(v.Tags))
		maps.Copy(cp.Tags, v.Tags)
		dst[k] = &cp
	}

	return dst
}

// snapshotSSHPublicKeys clones the three-level SSH public key map.
func snapshotSSHPublicKeys(
	src map[string]map[string]map[string]*SSHPublicKey,
) map[string]map[string]map[string]*SSHPublicKey {
	dst := make(map[string]map[string]map[string]*SSHPublicKey, len(src))
	for sid, userMap := range src {
		um := make(map[string]map[string]*SSHPublicKey, len(userMap))
		for userName, keyMap := range userMap {
			km := make(map[string]*SSHPublicKey, len(keyMap))
			for k, v := range keyMap {
				cp := *v
				km[k] = &cp
			}
			um[userName] = km
		}
		dst[sid] = um
	}

	return dst
}

// snapshotExecutions clones the two-level executions map.
func snapshotExecutions(src map[string]map[string]*Execution) map[string]map[string]*Execution {
	dst := make(map[string]map[string]*Execution, len(src))
	for wid, execMap := range src {
		em := make(map[string]*Execution, len(execMap))
		for k, v := range execMap {
			cp := *v
			em[k] = &cp
		}
		dst[wid] = em
	}

	return dst
}

// snapshotTagsStore clones the tags store map.
func snapshotTagsStore(src map[string]map[string]string) map[string]map[string]string {
	dst := make(map[string]map[string]string, len(src))
	for arn, tagMap := range src {
		m := make(map[string]string, len(tagMap))
		maps.Copy(m, tagMap)
		dst[arn] = m
	}

	return dst
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "transfer", data, &snap); err != nil {
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
	b.sshPublicKeys = snap.SSHPublicKeys
	b.executions = snap.Executions
	b.tagsStore = snap.TagsStore
	b.transferRecords = snap.FileTransferResults
	b.asyncOperations = snap.AsyncOperations

	return nil
}

// ensureNonNilMaps guarantees no map field in the snapshot is nil after
// unmarshalling.
func ensureNonNilMaps(s *backendSnapshot) {
	ensureNonNilCoreMaps(s)
	ensureNonNilSecondaryMaps(s)
}

// ensureNonNilCoreMaps ensures nil core resource maps are initialised.
func ensureNonNilCoreMaps(s *backendSnapshot) {
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
}

// ensureNonNilSecondaryMaps ensures nil secondary resource maps are initialised.
func ensureNonNilSecondaryMaps(s *backendSnapshot) {
	if s.Certificates == nil {
		s.Certificates = make(map[string]*Certificate)
	}

	if s.HostKeys == nil {
		s.HostKeys = make(map[string]map[string]*HostKey)
	}

	if s.SSHPublicKeys == nil {
		s.SSHPublicKeys = make(map[string]map[string]map[string]*SSHPublicKey)
	}

	if s.Executions == nil {
		s.Executions = make(map[string]map[string]*Execution)
	}

	if s.TagsStore == nil {
		s.TagsStore = make(map[string]map[string]string)
	}

	if s.FileTransferResults == nil {
		s.FileTransferResults = make(map[string]*FileTransferResult)
	}

	if s.AsyncOperations == nil {
		s.AsyncOperations = make(map[string]*AsyncOperationRecord)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(ctx, data)
	}

	return nil
}
