package ec2

import (
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// ipam_policy.go implements the IPAM Policy / Governance sub-family layered on top of
// the core Ipam/IpamPool state in advanced_networking.go: IPAM policies
// (Create/Describe/DeleteIpamPolicy), their enablement for the current account and for
// Organizations targets (Enable/DisableIpamPolicy, GetEnabledIpamPolicy,
// GetIpamPolicyOrganizationTargets), a policy's public IPv4 allocation rules
// (Get/ModifyIpamPolicyAllocationRules), the IPAM Organizations delegated admin account setting
// (Enable/DisableIpamOrganizationAdminAccount), and moving an existing BYOIP CIDR into an IPAM
// pool (MoveByoipCidrToIpam).

// ---- Errors ----

var (
	// ErrIpamPolicyNotFound is returned when an IPAM policy ID does not exist.
	ErrIpamPolicyNotFound = errors.New("InvalidIpamPolicyId.NotFound")
	// ErrIpamOrgAdminAccountNotFound is returned when disabling an IPAM Organizations delegated
	// admin account that does not match the currently-enabled admin account.
	ErrIpamOrgAdminAccountNotFound = errors.New("InvalidParameterValue")
)

// ---- Constants ----

const (
	// ipamPolicyOrgTargetAccount is the internal key used in ipamPolicyEnabledTargets to
	// represent "enabled for the current account" (i.e. no OrganizationTargetId supplied),
	// as opposed to a specific Organizations target ID.
	ipamPolicyOrgTargetAccount = ""

	// ipamPolicyManagedByAccount is the steady-state IpamPolicyManagedBy value returned by
	// GetEnabledIpamPolicy for a policy the caller's own account manages (this mock does not
	// model cross-account Organizations delegation for the enabled-policy view).
	ipamPolicyManagedByAccount = "account"
)

// ---- Data types ----

// IpamPolicyAllocationRule mirrors AWS's IpamPolicyAllocationRule: a single source IPAM pool
// entry within an IPAM policy allocation rule document.
type IpamPolicyAllocationRule struct {
	SourceIpamPoolID string `json:"sourceIpamPoolId,omitempty"`
}

// IpamPolicyDocument represents the allocation rules configured for one (Locale, ResourceType)
// pair of an IPAM policy, as returned/replaced by Get/ModifyIpamPolicyAllocationRules.
type IpamPolicyDocument struct {
	IpamPolicyID    string                     `json:"ipamPolicyId,omitempty"`
	Locale          string                     `json:"locale,omitempty"`
	ResourceType    string                     `json:"resourceType,omitempty"`
	AllocationRules []IpamPolicyAllocationRule `json:"allocationRules,omitempty"`
}

// IpamPolicy represents an AWS IPAM policy: a set of rules that define how public IPv4
// addresses from IPAM pools are allocated to AWS resources.
type IpamPolicy struct {
	AllocationDocs   map[string]*IpamPolicyDocument `json:"allocationDocs,omitempty"`
	IpamPolicyID     string                         `json:"ipamPolicyId,omitempty"`
	IpamPolicyARN    string                         `json:"ipamPolicyArn,omitempty"`
	IpamPolicyRegion string                         `json:"ipamPolicyRegion,omitempty"`
	IpamID           string                         `json:"ipamId,omitempty"`
	OwnerID          string                         `json:"ownerId,omitempty"`
	State            string                         `json:"state,omitempty"`
	StateMessage     string                         `json:"stateMessage,omitempty"`
}

// ---- Reset helpers ----

// resetIpamPolicyMapsLocked re-initialises all maps owned by this file. Must be called with
// b.mu held.
func (b *InMemoryBackend) resetIpamPolicyMapsLocked() {
	b.ipamPolicyEnabledTargets = make(map[string]string)
	b.ipamOrgAdminAccountID = ""
}

// copyIpamPolicy returns a deep copy of an IPAM policy so callers cannot mutate backend state
// (in particular its AllocationDocs map) through the returned pointer.
func copyIpamPolicy(p *IpamPolicy) *IpamPolicy {
	cp := *p
	cp.AllocationDocs = make(map[string]*IpamPolicyDocument, len(p.AllocationDocs))

	for k, doc := range p.AllocationDocs {
		docCopy := *doc
		docCopy.AllocationRules = append([]IpamPolicyAllocationRule(nil), doc.AllocationRules...)
		cp.AllocationDocs[k] = &docCopy
	}

	return &cp
}

func ipamPolicyDocKey(locale, resourceType string) string {
	return locale + "|" + resourceType
}

// ---- CreateIpamPolicy / DescribeIpamPolicies / DeleteIpamPolicy ----

// CreateIpamPolicy creates a new IPAM policy under the given IPAM.
func (b *InMemoryBackend) CreateIpamPolicy(ipamID string) (*IpamPolicy, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIpamPolicy")
	defer b.mu.Unlock()

	if _, ok := b.ipams.Get(ipamID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	id := "ipam-policy-" + uuid.New().String()[:8]
	policy := &IpamPolicy{
		IpamPolicyID:     id,
		IpamPolicyARN:    "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-policy/" + id,
		IpamPolicyRegion: b.Region,
		IpamID:           ipamID,
		OwnerID:          b.AccountID,
		State:            ipamStateCreateComplete,
		AllocationDocs:   make(map[string]*IpamPolicyDocument),
	}
	b.ipamPolicies.Put(policy)

	return copyIpamPolicy(policy), nil
}

// DeleteIpamPolicy removes an IPAM policy and clears any enablement referencing it.
func (b *InMemoryBackend) DeleteIpamPolicy(id string) (*IpamPolicy, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPolicyId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamPolicy")
	defer b.mu.Unlock()

	policy, ok := b.ipamPolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPolicyNotFound, id)
	}
	b.ipamPolicies.Delete(id)

	for target, enabledID := range b.ipamPolicyEnabledTargets {
		if enabledID == id {
			delete(b.ipamPolicyEnabledTargets, target)
		}
	}

	cp := copyIpamPolicy(policy)
	cp.State = ipamStateDeleteComplete

	return cp, nil
}

// DescribeIpamPolicies returns IPAM policies, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamPolicies(ids []string) []*IpamPolicy {
	b.mu.RLock("DescribeIpamPolicies")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamPolicy, 0, b.ipamPolicies.Len())

	for _, p := range b.ipamPolicies.All() {
		if len(idSet) > 0 && !idSet[p.IpamPolicyID] {
			continue
		}

		out = append(out, copyIpamPolicy(p))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPolicyID < out[j].IpamPolicyID
	})

	return out
}

// ---- Enablement ----

// EnableIpamPolicy enables an IPAM policy for the current account, or for a specific
// Organizations target when orgTargetID is non-empty.
func (b *InMemoryBackend) EnableIpamPolicy(id, orgTargetID string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamPolicyId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableIpamPolicy")
	defer b.mu.Unlock()

	if _, ok := b.ipamPolicies.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrIpamPolicyNotFound, id)
	}

	target := orgTargetID
	if target == "" {
		target = ipamPolicyOrgTargetAccount
	}

	b.ipamPolicyEnabledTargets[target] = id

	return nil
}

// DisableIpamPolicy disables an IPAM policy for the current account, or for a specific
// Organizations target when orgTargetID is non-empty. Disabling a policy that is not
// currently enabled for that target is a no-op, matching AWS's idempotent Disable semantics.
func (b *InMemoryBackend) DisableIpamPolicy(id, orgTargetID string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamPolicyId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableIpamPolicy")
	defer b.mu.Unlock()

	if _, ok := b.ipamPolicies.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrIpamPolicyNotFound, id)
	}

	target := orgTargetID
	if target == "" {
		target = ipamPolicyOrgTargetAccount
	}

	if b.ipamPolicyEnabledTargets[target] == id {
		delete(b.ipamPolicyEnabledTargets, target)
	}

	return nil
}

// GetEnabledIpamPolicy returns the IPAM policy currently enabled for the current account (the
// ipamPolicyOrgTargetAccount target), whether one is enabled at all, and who manages it.
func (b *InMemoryBackend) GetEnabledIpamPolicy() (string, bool, string) {
	b.mu.RLock("GetEnabledIpamPolicy")
	defer b.mu.RUnlock()

	id, ok := b.ipamPolicyEnabledTargets[ipamPolicyOrgTargetAccount]
	if !ok {
		return "", false, ""
	}

	return id, true, ipamPolicyManagedByAccount
}

// GetIpamPolicyOrganizationTargets returns the Organizations target IDs for which the given
// IPAM policy is currently enabled.
func (b *InMemoryBackend) GetIpamPolicyOrganizationTargets(id string) ([]string, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPolicyId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPolicyOrganizationTargets")
	defer b.mu.RUnlock()

	if _, ok := b.ipamPolicies.Get(id); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPolicyNotFound, id)
	}

	var targets []string

	for target, enabledID := range b.ipamPolicyEnabledTargets {
		if enabledID == id && target != ipamPolicyOrgTargetAccount {
			targets = append(targets, target)
		}
	}

	sort.Strings(targets)

	return targets, nil
}

// ---- Allocation rules ----

// GetIpamPolicyAllocationRules returns the allocation rule documents of an IPAM policy,
// optionally filtered by locale and/or resource type.
func (b *InMemoryBackend) GetIpamPolicyAllocationRules(
	id, locale, resourceType string,
) ([]*IpamPolicyDocument, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPolicyId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPolicyAllocationRules")
	defer b.mu.RUnlock()

	policy, ok := b.ipamPolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPolicyNotFound, id)
	}

	out := make([]*IpamPolicyDocument, 0, len(policy.AllocationDocs))

	for _, doc := range policy.AllocationDocs {
		if locale != "" && doc.Locale != locale {
			continue
		}

		if resourceType != "" && doc.ResourceType != resourceType {
			continue
		}

		docCopy := *doc
		docCopy.AllocationRules = append([]IpamPolicyAllocationRule(nil), doc.AllocationRules...)
		out = append(out, &docCopy)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Locale != out[j].Locale {
			return out[i].Locale < out[j].Locale
		}

		return out[i].ResourceType < out[j].ResourceType
	})

	return out, nil
}

// ModifyIpamPolicyAllocationRules replaces the allocation rules of an IPAM policy for the given
// (locale, resourceType) pair.
func (b *InMemoryBackend) ModifyIpamPolicyAllocationRules(
	id, locale, resourceType string, rules []IpamPolicyAllocationRule,
) (*IpamPolicyDocument, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPolicyId is required", ErrInvalidParameter)
	}

	if locale == "" {
		return nil, fmt.Errorf("%w: Locale is required", ErrInvalidParameter)
	}

	if resourceType == "" {
		return nil, fmt.Errorf("%w: ResourceType is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamPolicyAllocationRules")
	defer b.mu.Unlock()

	policy, ok := b.ipamPolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPolicyNotFound, id)
	}

	doc := &IpamPolicyDocument{
		IpamPolicyID:    id,
		Locale:          locale,
		ResourceType:    resourceType,
		AllocationRules: append([]IpamPolicyAllocationRule(nil), rules...),
	}
	policy.AllocationDocs[ipamPolicyDocKey(locale, resourceType)] = doc
	policy.State = ipamStateModifyComplete

	docCopy := *doc
	docCopy.AllocationRules = append([]IpamPolicyAllocationRule(nil), doc.AllocationRules...)

	return &docCopy, nil
}

// ---- Organizations delegated admin account ----

// EnableIpamOrganizationAdminAccount enables an Organizations member account as the IPAM
// delegated administrator account.
func (b *InMemoryBackend) EnableIpamOrganizationAdminAccount(accountID string) error {
	if accountID == "" {
		return fmt.Errorf("%w: DelegatedAdminAccountId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableIpamOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.ipamOrgAdminAccountID = accountID

	return nil
}

// DisableIpamOrganizationAdminAccount disables the IPAM Organizations delegated administrator
// account. Returns ErrIpamOrgAdminAccountNotFound if accountID does not match the currently
// enabled admin account.
func (b *InMemoryBackend) DisableIpamOrganizationAdminAccount(accountID string) error {
	if accountID == "" {
		return fmt.Errorf("%w: DelegatedAdminAccountId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableIpamOrganizationAdminAccount")
	defer b.mu.Unlock()

	if b.ipamOrgAdminAccountID == "" || b.ipamOrgAdminAccountID != accountID {
		return fmt.Errorf("%w: %s", ErrIpamOrgAdminAccountNotFound, accountID)
	}

	b.ipamOrgAdminAccountID = ""

	return nil
}

// ---- MoveByoipCidrToIpam ----

// MoveByoipCidrToIpam associates an existing BYOIPv4 CIDR with an IPAM pool, making it
// allocatable from that pool going forward. poolOwner (the AWS account ID that owns the IPAM
// pool) is required by the AWS API but, since this mock only models a single account, is not
// otherwise validated against pool ownership.
func (b *InMemoryBackend) MoveByoipCidrToIpam(cidr, poolID, poolOwner string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	if poolOwner == "" {
		return nil, fmt.Errorf("%w: IpamPoolOwner is required", ErrInvalidParameter)
	}

	b.mu.Lock("MoveByoipCidrToIpam")
	defer b.mu.Unlock()

	entry, ok := b.byoipCidrs.Get(cidr)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrByoipCidrNotFound, cidr)
	}

	if _, poolOK := b.ipamPools.Get(poolID); !poolOK {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	entry.State = ipamPoolCidrStateProvisioned

	found := false

	for _, c := range b.ipamPoolCidrs[poolID] {
		if c.Cidr == cidr {
			found = true

			break
		}
	}

	if !found {
		b.ipamPoolCidrs[poolID] = append(
			b.ipamPoolCidrs[poolID], &IpamPoolCidr{Cidr: cidr, State: ipamPoolCidrStateProvisioned},
		)
	}

	cp := *entry

	return &cp, nil
}
