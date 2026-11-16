package iam

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "iam"
	quotaRoles       = "L-F4A5425F"
	quotaUsers       = "L-F55AF5E4"
	quotaGroups      = "L-BF35879D"
	quotaPolicies    = "L-0DA4ABF3"
	quotaProfiles    = "L-E95E4862"
)

// SetQuotaLimiter makes the IAM create operations honour Service Quotas (roles, users, groups, customer
// managed policies and instance profiles); nil disables it.
func (b *InMemoryBackend) SetQuotaLimiter(f quotalimit.Func) { b.quotaGate.Set(f) }

// RoleCount returns the number of roles.
func (b *InMemoryBackend) RoleCount() int {
	b.mu.RLock("RoleCount")
	defer b.mu.RUnlock()

	return len(b.sortedRoleNames)
}

// UserCount returns the number of users.
func (b *InMemoryBackend) UserCount() int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	return len(b.sortedUserNames)
}

// GroupCount returns the number of groups.
func (b *InMemoryBackend) GroupCount() int {
	b.mu.RLock("GroupCount")
	defer b.mu.RUnlock()

	return len(b.sortedGroupNames)
}

// PolicyCount returns the number of customer managed policies.
func (b *InMemoryBackend) PolicyCount() int {
	b.mu.RLock("PolicyCount")
	defer b.mu.RUnlock()

	return len(b.sortedPolicyNames)
}

// InstanceProfileCount returns the number of instance profiles.
func (b *InMemoryBackend) InstanceProfileCount() int {
	b.mu.RLock("InstanceProfileCount")
	defer b.mu.RUnlock()

	return len(b.sortedIPNames)
}

func (b *InMemoryBackend) quotaLimit(code string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, "", quotaServiceCode, code)
}

func quotaExceeded(kind string, limit quotalimit.Limit) error {
	return fmt.Errorf("%w: cannot exceed quota for %s: %.0f", ErrLimitExceeded, kind, limit.Value)
}
