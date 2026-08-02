package networkmanager

// This file implements PARITY.md family V: AWS Organizations integration
// (2 ops) -- enabling/disabling the NetworkManager AWS-Organizations
// service-linked-role trust relationship, account-wide.
//
// This repo has no independent AWS Organizations backend to bind against
// (PARITY.md's own open question, not resolved here); OrganizationId is a
// synthetic, deterministically-generated-once identifier (same
// hex-suffix convention as every other ID in this package), not a
// fabricated-as-real value pulled from nowhere -- it only ever appears
// after StartOrganizationServiceAccessUpdate(ENABLE) actually runs.

const (
	orgAccessEnable  = "ENABLE"
	orgAccessDisable = "DISABLE"
)

// StartOrganizationServiceAccessUpdate flips the account's
// OrganizationAwsServiceAccessStatus per action ("ENABLE"/"DISABLE" --
// PARITY.md notes this is a free-form *string on the wire, not a closed
// SDK enum).
func (b *InMemoryBackend) StartOrganizationServiceAccessUpdate(action string) *organizationStatus {
	b.mu.Lock("StartOrganizationServiceAccessUpdate")
	defer b.mu.Unlock()

	switch action {
	case orgAccessEnable:
		if b.orgStatus.OrganizationID == "" {
			b.orgStatus.OrganizationID = "o-" + randomHexID()
		}

		b.orgStatus.OrganizationAwsServiceAccessStatus = "ENABLED"
		b.orgStatus.SLRDeploymentStatus = "SUCCEEDED"
	case orgAccessDisable:
		b.orgStatus.OrganizationAwsServiceAccessStatus = orgAccessStatusDisabled
		b.orgStatus.SLRDeploymentStatus = ""
	}

	return b.orgStatus.clone()
}

// ListOrganizationServiceAccessStatus returns the current organization
// status -- the only op in this 95-op surface with zero typed exception
// cases (PARITY.md), so its handler never returns an apiError.
func (b *InMemoryBackend) ListOrganizationServiceAccessStatus() *organizationStatus {
	b.mu.RLock("ListOrganizationServiceAccessStatus")
	defer b.mu.RUnlock()

	return b.orgStatus.clone()
}
