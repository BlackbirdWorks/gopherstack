package iam

import (
	"encoding/json"
)

type backendSnapshot struct {
	Users            map[string]User            `json:"users"`
	Roles            map[string]Role            `json:"roles"`
	Policies         map[string]Policy          `json:"policies"`
	Groups           map[string]Group           `json:"groups"`
	AccessKeys       map[string]AccessKey       `json:"accessKeys"`
	InstanceProfiles map[string]InstanceProfile `json:"instanceProfiles"`
	UserPolicies     map[string][]string        `json:"userPolicies"`
	RolePolicies     map[string][]string        `json:"rolePolicies"`
	AccountID        string                     `json:"accountID"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Users:            b.users,
		Roles:            b.roles,
		Policies:         b.policies,
		Groups:           b.groups,
		AccessKeys:       b.accessKeys,
		InstanceProfiles: b.instanceProfiles,
		UserPolicies:     b.userPolicies,
		RolePolicies:     b.rolePolicies,
		AccountID:        b.accountID,
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

	if snap.Users == nil {
		snap.Users = make(map[string]User)
	}

	if snap.Roles == nil {
		snap.Roles = make(map[string]Role)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]Policy)
	}

	if snap.Groups == nil {
		snap.Groups = make(map[string]Group)
	}

	if snap.AccessKeys == nil {
		snap.AccessKeys = make(map[string]AccessKey)
	}

	if snap.InstanceProfiles == nil {
		snap.InstanceProfiles = make(map[string]InstanceProfile)
	}

	if snap.UserPolicies == nil {
		snap.UserPolicies = make(map[string][]string)
	}

	if snap.RolePolicies == nil {
		snap.RolePolicies = make(map[string][]string)
	}

	b.users = snap.Users
	b.roles = snap.Roles
	b.policies = snap.Policies
	b.groups = snap.Groups
	b.accessKeys = snap.AccessKeys
	b.instanceProfiles = snap.InstanceProfiles
	b.userPolicies = snap.UserPolicies
	b.rolePolicies = snap.RolePolicies
	b.accountID = snap.AccountID

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
