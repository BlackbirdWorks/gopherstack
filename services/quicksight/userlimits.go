package quicksight

import "fmt"

const (
	resourceTypeIndexStorage = "INDEX_STORAGE"
	resourceTypeAgentHours   = "AGENT_HOURS"

	limitSourceSystemDefault = "SYSTEM_DEFAULT"

	// systemDefaultProfileID is a reserved sentinel EffectiveLimit.ProfileId
	// for the SYSTEM_DEFAULT source. ProfileId is a required output member
	// even though "the built-in subscription entitlement" (per AWS's
	// documented resolution hierarchy) has no real LimitsProfile record
	// backing it: the pinned SDK has no LimitsProfile-assignment operation
	// at all (CreateLimitsProfile/UpdateLimitsProfile carry no user/group/
	// role/account target field), so DIRECT_USER/GROUP/ROLE/ACCOUNT sources
	// can never be honestly resolved by this backend. This value matches
	// the real profile ID pattern (lp-[a-f0-9-]+) without claiming to
	// identify an actual profile.
	systemDefaultProfileID = "lp-00000000-0000-0000-0000-000000000000"

	// Index storage per-user default entitlement, by subscription tier
	// (docs.aws.amazon.com/quick/latest/userguide/manage-data-capacity.html:
	// "Professional users get 25 GB of storage per user and Enterprise
	// users get 50 GB per user"). This backend's Edition field (STANDARD/
	// ENTERPRISE/ENTERPRISE_AND_Q) has no literal "Professional" value --
	// STANDARD maps to the documented Professional-tier default, ENTERPRISE
	// and ENTERPRISE_AND_Q to the Enterprise-tier default, the closest
	// honest correspondence this backend's Edition model supports.
	indexStorageDefaultGBStandard   = 25
	indexStorageDefaultGBEnterprise = 50

	// Agent-hours-per-month default entitlement, same tier mapping. AWS's
	// API reference does not publish a number for this one (verified:
	// API_EffectiveLimit.html states only the MB|GB|HOURS|DAYS unit and a
	// minimum-value-0 constraint, no default). This is the one specific
	// figure found in third-party Quick-pricing coverage, not a primary AWS
	// source -- lower confidence than the index-storage figures above,
	// disclosed as such in PARITY.md rather than presented as
	// authoritative.
	agentHoursDefaultStandard   = 4
	agentHoursDefaultEnterprise = 8
)

// systemDefaultLimit resolves the SYSTEM_DEFAULT EffectiveLimit for
// resourceType given the account's edition -- see the const block above for
// where each number comes from.
func systemDefaultLimit(resourceType, edition string) EffectiveLimit {
	enterprise := edition == "ENTERPRISE" || edition == "ENTERPRISE_AND_Q"

	limit := EffectiveLimit{
		ProfileID:    systemDefaultProfileID,
		ResourceType: resourceType,
		Source:       limitSourceSystemDefault,
	}

	if resourceType == resourceTypeAgentHours {
		limit.LimitUnit = "HOURS"
		limit.LimitValue = agentHoursDefaultStandard

		if enterprise {
			limit.LimitValue = agentHoursDefaultEnterprise
		}

		return limit
	}

	limit.LimitUnit = "GB"
	limit.LimitValue = indexStorageDefaultGBStandard

	if enterprise {
		limit.LimitValue = indexStorageDefaultGBEnterprise
	}

	return limit
}

// BatchDescribeUserLimits resolves each requested user's effective
// per-resource-type limits. This backend has no LimitsProfile assignment
// mechanism at all (see systemDefaultProfileID's doc comment), so every
// resolved limit's Source is SYSTEM_DEFAULT -- never a fabricated
// DIRECT_USER/GROUP/ROLE/ACCOUNT source this backend can't back with real
// assignment state. A requested user this backend has no record for is
// reported in the returned errors, not silently given default limits anyway.
func (b *InMemoryBackend) BatchDescribeUserLimits(
	accountID string, users []UserLimitsQuery, resourceTypes []string,
) ([]UserLimits, []UserLimitsError) {
	b.mu.RLock("BatchDescribeUserLimits")
	defer b.mu.RUnlock()

	edition := editionStandard
	if s, ok := b.accountSubscriptions[accountID]; ok && s.Edition != "" {
		edition = s.Edition
	}

	types := resourceTypes
	if len(types) == 0 {
		types = []string{resourceTypeIndexStorage, resourceTypeAgentHours}
	}

	result := make([]UserLimits, 0, len(users))

	var errs []UserLimitsError

	for _, q := range users {
		u, ok := b.users.Get(userKey(accountID, q.Namespace, q.UserName))
		if !ok {
			errs = append(errs, UserLimitsError{
				UserName:  q.UserName,
				Namespace: q.Namespace,
				ErrorCode: errResourceNotFound,
				Message:   fmt.Sprintf("user %s not found in namespace %s", q.UserName, q.Namespace),
			})

			continue
		}

		limits := make([]EffectiveLimit, 0, len(types))
		for _, rt := range types {
			limits = append(limits, systemDefaultLimit(rt, edition))
		}

		result = append(result, UserLimits{
			UserName:        u.UserName,
			Namespace:       q.Namespace,
			EffectiveLimits: limits,
		})
	}

	return result, errs
}
