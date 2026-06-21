package iam

import (
	"context"
	"encoding/json"
)

type backendSnapshot struct {
	RoleInlinePolicies    map[string]map[string]string         `json:"roleInlinePolicies,omitempty"`
	VirtualMFADevices     map[string]VirtualMFADevice          `json:"virtualMFADevices,omitempty"`
	Policies              map[string]Policy                    `json:"policies,omitempty"`
	Groups                map[string]Group                     `json:"groups,omitempty"`
	AccessKeys            map[string]AccessKey                 `json:"accessKeys,omitempty"`
	InstanceProfiles      map[string]InstanceProfile           `json:"instanceProfiles,omitempty"`
	SAMLProviders         map[string]SAMLProvider              `json:"samlProviders,omitempty"`
	OIDCProviders         map[string]OIDCProvider              `json:"oidcProviders,omitempty"`
	LoginProfiles         map[string]LoginProfile              `json:"loginProfiles,omitempty"`
	GroupMembers          map[string][]string                  `json:"groupMembers,omitempty"`
	Roles                 map[string]Role                      `json:"roles,omitempty"`
	Users                 map[string]User                      `json:"users,omitempty"`
	UserPolicies          map[string][]string                  `json:"userPolicies,omitempty"`
	UserInlinePolicies    map[string]map[string]string         `json:"userInlinePolicies,omitempty"`
	GroupPolicies         map[string][]string                  `json:"groupPolicies,omitempty"`
	RolePolicies          map[string][]string                  `json:"rolePolicies,omitempty"`
	PasswordPolicy        *PasswordPolicy                      `json:"passwordPolicy,omitempty"`
	PolicyVersions        map[string][]StoredPolicyVersion     `json:"policyVersions,omitempty"`
	PolicyVersionCounters map[string]int                       `json:"policyVersionCounters,omitempty"`
	ServiceSpecificCreds  map[string]ServiceSpecificCredential `json:"serviceSpecificCreds,omitempty"`
	GroupInlinePolicies   map[string]map[string]string         `json:"groupInlinePolicies,omitempty"`
	ServerCertificates    map[string]ServerCertificate         `json:"serverCertificates,omitempty"`
	DelegationRequests    map[string]DelegationRequest         `json:"delegationRequests,omitempty"`
	PolicyByARN           map[string]string                    `json:"policyByARN,omitempty"`
	RoleByARN             map[string]string                    `json:"roleByARN,omitempty"`
	PolicyAttachments     map[string]policyAttachmentRefs      `json:"policyAttachments,omitempty"`
	DeletedV1Policies     map[string]bool                      `json:"deletedV1Policies,omitempty"`
	SigningCertificates   map[string]SigningCertificate        `json:"signingCertificates,omitempty"`
	AccountID             string                               `json:"accountID,omitempty"`
	AccountAliases        []string                             `json:"accountAliases,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Users:                 b.users,
		Roles:                 b.roles,
		Policies:              b.policies,
		Groups:                b.groups,
		AccessKeys:            b.accessKeys,
		InstanceProfiles:      b.instanceProfiles,
		SAMLProviders:         b.samlProviders,
		OIDCProviders:         b.oidcProviders,
		LoginProfiles:         b.loginProfiles,
		UserPolicies:          b.userPolicies,
		RolePolicies:          b.rolePolicies,
		GroupPolicies:         b.groupPolicies,
		GroupMembers:          b.groupMembers,
		UserInlinePolicies:    b.userInlinePolicies,
		RoleInlinePolicies:    b.roleInlinePolicies,
		GroupInlinePolicies:   b.groupInlinePolicies,
		AccountAliases:        b.accountAliases,
		PolicyVersions:        b.policyVersions,
		PolicyVersionCounters: b.policyVersionCounters,
		ServiceSpecificCreds:  b.serviceSpecificCreds,
		VirtualMFADevices:     b.virtualMFADevices,
		DelegationRequests:    b.delegationRequests,
		AccountID:             b.accountID,
		PolicyByARN:           b.policyByARN,
		RoleByARN:             b.roleByARN,
		PolicyAttachments:     b.policyAttachments,
		DeletedV1Policies:     b.deletedV1Policies,
		SigningCertificates:   b.signingCertificates,
		ServerCertificates:    b.serverCertificates,
		PasswordPolicy:        b.passwordPolicy,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	normalizeSnapshot(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.users = snap.Users
	b.roles = snap.Roles
	b.policies = snap.Policies
	b.groups = snap.Groups
	b.accessKeys = snap.AccessKeys
	b.instanceProfiles = snap.InstanceProfiles
	b.samlProviders = snap.SAMLProviders
	b.oidcProviders = snap.OIDCProviders
	b.loginProfiles = snap.LoginProfiles
	b.userPolicies = snap.UserPolicies
	b.rolePolicies = snap.RolePolicies
	b.groupPolicies = snap.GroupPolicies
	b.groupMembers = snap.GroupMembers
	b.userInlinePolicies = snap.UserInlinePolicies
	b.roleInlinePolicies = snap.RoleInlinePolicies
	b.groupInlinePolicies = snap.GroupInlinePolicies
	b.accountAliases = snap.AccountAliases
	b.policyVersions = snap.PolicyVersions
	b.policyVersionCounters = snap.PolicyVersionCounters
	b.serviceSpecificCreds = snap.ServiceSpecificCreds
	b.virtualMFADevices = snap.VirtualMFADevices
	b.delegationRequests = snap.DelegationRequests
	b.accountID = snap.AccountID
	b.rebuildIndexesLocked()

	if snap.PolicyByARN != nil {
		b.policyByARN = snap.PolicyByARN
	} else {
		b.policyByARN = make(map[string]string)
	}
	if snap.RoleByARN != nil {
		b.roleByARN = snap.RoleByARN
	} else {
		b.roleByARN = make(map[string]string)
	}
	if snap.PolicyAttachments != nil {
		b.policyAttachments = snap.PolicyAttachments
	} else {
		b.policyAttachments = make(map[string]policyAttachmentRefs)
	}
	if snap.DeletedV1Policies != nil {
		b.deletedV1Policies = snap.DeletedV1Policies
	} else {
		b.deletedV1Policies = make(map[string]bool)
	}
	if snap.SigningCertificates != nil {
		b.signingCertificates = snap.SigningCertificates
	} else {
		b.signingCertificates = make(map[string]SigningCertificate)
	}
	if snap.ServerCertificates != nil {
		b.serverCertificates = snap.ServerCertificates
	} else {
		b.serverCertificates = make(map[string]ServerCertificate)
	}
	b.passwordPolicy = snap.PasswordPolicy

	return nil
}

// normalizeSnapshot ensures all map fields in snap are non-nil so callers
// can assign them directly without nil-pointer risk.
func normalizeSnapshot(snap *backendSnapshot) {
	normalizeSnapshotEntities(snap)
	normalizeSnapshotPolicies(snap)
	normalizeSnapshotNewOps(snap)
}

// normalizeSnapshotEntities initialises entity maps in snap to non-nil empty maps.
func normalizeSnapshotEntities(snap *backendSnapshot) {
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

	if snap.SAMLProviders == nil {
		snap.SAMLProviders = make(map[string]SAMLProvider)
	}

	if snap.OIDCProviders == nil {
		snap.OIDCProviders = make(map[string]OIDCProvider)
	}

	if snap.LoginProfiles == nil {
		snap.LoginProfiles = make(map[string]LoginProfile)
	}
}

// normalizeSnapshotPolicies initialises policy maps in snap to non-nil empty maps.
func normalizeSnapshotPolicies(snap *backendSnapshot) {
	if snap.UserPolicies == nil {
		snap.UserPolicies = make(map[string][]string)
	}

	if snap.RolePolicies == nil {
		snap.RolePolicies = make(map[string][]string)
	}

	if snap.GroupPolicies == nil {
		snap.GroupPolicies = make(map[string][]string)
	}

	if snap.GroupMembers == nil {
		snap.GroupMembers = make(map[string][]string)
	}

	if snap.UserInlinePolicies == nil {
		snap.UserInlinePolicies = make(map[string]map[string]string)
	}

	if snap.RoleInlinePolicies == nil {
		snap.RoleInlinePolicies = make(map[string]map[string]string)
	}

	if snap.GroupInlinePolicies == nil {
		snap.GroupInlinePolicies = make(map[string]map[string]string)
	}
}

// normalizeSnapshotNewOps initialises new-ops maps in snap to non-nil empty values.
func normalizeSnapshotNewOps(snap *backendSnapshot) {
	if snap.PolicyVersions == nil {
		snap.PolicyVersions = make(map[string][]StoredPolicyVersion)
	}

	if snap.ServiceSpecificCreds == nil {
		snap.ServiceSpecificCreds = make(map[string]ServiceSpecificCredential)
	}

	if snap.VirtualMFADevices == nil {
		snap.VirtualMFADevices = make(map[string]VirtualMFADevice)
	}

	if snap.DelegationRequests == nil {
		snap.DelegationRequests = make(map[string]DelegationRequest)
	}

	if snap.PolicyVersionCounters == nil {
		snap.PolicyVersionCounters = rebuildVersionCounters(snap.PolicyVersions)
	}
}

// rebuildVersionCounters derives a monotonic-counter map from stored policy versions.
// It is used when restoring snapshots that pre-date the counter field, ensuring that
// newly created versions do not collide with existing IDs.
func rebuildVersionCounters(versions map[string][]StoredPolicyVersion) map[string]int {
	counters := make(map[string]int, len(versions))

	for policyArn, pvs := range versions {
		counters[policyArn] = maxVersionNumber(pvs)
	}

	return counters
}

// maxVersionNumber returns max(1, highest vN suffix) across pvs.
// The result is stored as the counter so that counter++ yields the next unused ID.
func maxVersionNumber(pvs []StoredPolicyVersion) int {
	maxNum := 1 // v1 is always implicit

	for _, v := range pvs {
		if n := parseVersionNum(v.VersionID); n > maxNum {
			maxNum = n
		}
	}

	// counter = maxNum - 1 so counter++ produces maxNum+1 as the next version number.
	return maxNum - 1
}

// decimalBase is the numeric base used when parsing version ID suffixes.
const decimalBase = 10

// parseVersionNum extracts the integer suffix from a "vN" version ID string.
// Returns 0 if the ID does not match the "v<digits>" pattern.
func parseVersionNum(id string) int {
	if len(id) < 2 || id[0] != 'v' {
		return 0
	}

	n := 0

	for _, ch := range id[1:] {
		if ch < '0' || ch > '9' {
			return 0
		}

		n = n*decimalBase + int(ch-'0')
	}

	return n
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
