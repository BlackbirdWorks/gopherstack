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
)

// SetQuotaLimiter makes CreateRole, CreateUser and CreateGroup honour Service Quotas; nil disables it.
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

func (b *InMemoryBackend) quotaLimit(code string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, "", quotaServiceCode, code)
}

func quotaExceeded(kind string, limit quotalimit.Limit) error {
	return fmt.Errorf("%w: cannot exceed quota for %s: %.0f", ErrLimitExceeded, kind, limit.Value)
}
