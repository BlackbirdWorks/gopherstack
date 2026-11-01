package azurearm

import "maps"

type authorizationSnapshot struct {
	RoleDefs      map[string]*storedRoleDef      `json:"roleDefinitions,omitempty"`
	RoleAssigns   map[string]*storedRoleAssign   `json:"roleAssignments,omitempty"`
	Locks         map[string]*storedLock         `json:"locks,omitempty"`
	PolicyDefs    map[string]*storedPolicy       `json:"policyDefinitions,omitempty"`
	PolicySets    map[string]*storedPolicy       `json:"policySetDefinitions,omitempty"`
	PolicyAssigns map[string]*storedPolicyAssign `json:"policyAssignments,omitempty"`
}

func (p *AuthorizationProvider) snapshot() *authorizationSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	return &authorizationSnapshot{
		RoleDefs: maps.Clone(p.roleDefs), RoleAssigns: maps.Clone(p.roleAssigns), Locks: maps.Clone(p.locks),
		PolicyDefs: maps.Clone(p.policyDefs), PolicySets: maps.Clone(p.policySets),
		PolicyAssigns: maps.Clone(p.policyAssigns),
	}
}

// restore replaces the provider's state; nil clears it.
func (p *AuthorizationProvider) restore(snap *authorizationSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.resetMaps()

	if snap == nil {
		return
	}

	copyNonNil(p.roleDefs, snap.RoleDefs)
	copyNonNil(p.roleAssigns, snap.RoleAssigns)
	copyNonNil(p.locks, snap.Locks)
	copyNonNil(p.policyDefs, snap.PolicyDefs)
	copyNonNil(p.policySets, snap.PolicySets)
	copyNonNil(p.policyAssigns, snap.PolicyAssigns)
}

func copyNonNil[T any](dst, src map[string]*T) {
	for k, v := range src {
		if v != nil {
			dst[k] = v
		}
	}
}
