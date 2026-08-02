package grafana

import "time"

// licenseValidityPeriod is the simulated validity window applied when a
// license is associated (real durations are set by the Grafana Labs token
// exchange this emulator does not perform).
const licenseValidityPeriod = 365 * 24 * time.Hour

// AssociateLicense assigns a Grafana Enterprise license to a workspace.
// ENTERPRISE_FREE_TRIAL is rejected: "Amazon Managed Grafana workspaces no
// longer support Grafana Enterprise free trials" (CreateWorkspaceApiKeyInput's
// sibling AssociateLicenseInput.LicenseType doc comment) is a real-service
// behavior fact this emulator encodes rather than silently accepting.
//
// The not-ACTIVE precondition below is reported as ValidationException, NOT
// ConflictException: unlike UpdateWorkspace/UpdateWorkspaceConfiguration,
// AssociateLicense's own deserializeOpErrorAssociateLicense in the real
// SDK's deserializers.go does not list ConflictException among the error
// shapes it recognizes for this operation -- returning one here would
// deserialize client-side as an untyped smithy.GenericAPIError instead of
// a matchable *types.ConflictException, breaking real caller error handling.
func (b *InMemoryBackend) AssociateLicense(id, licenseType, grafanaToken string) (*Workspace, error) {
	if licenseType != LicenseEnterprise {
		return nil, validationError("licenseType " + licenseType +
			" is not supported: Grafana Enterprise free trials are no longer available")
	}

	b.mu.Lock("AssociateLicense")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return nil, notFoundError(resourceTypeWorkspace, id)
	}

	if w.Status != StatusActive && w.Status != StatusDegraded {
		return nil, validationError("workspace is not in a state that can be updated: " + w.Status)
	}

	now := time.Now().UTC()
	w.LicenseType = licenseType
	w.LicenseExpiration = now.Add(licenseValidityPeriod)
	w.GrafanaToken = grafanaToken
	w.Status = StatusUpgrading
	w.Modified = now
	b.scheduleWorkspaceTransition(id, StatusUpgrading)

	cp := *w

	return &cp, nil
}

// DisassociateLicense removes the Grafana Enterprise license from a
// workspace. Disassociating a license type the workspace does not currently
// have is treated as an idempotent no-op (the workspace is returned
// unchanged) rather than an error: deserializeOpErrorDisassociateLicense in
// the real SDK's deserializers.go does not list ConflictException among the
// error shapes this operation can return, so there is no wire-accurate way
// to signal "nothing to remove" as an error here (unlike, e.g.,
// AssociateLicense's ValidationException path for an unsupported license
// type, this isn't a malformed-request condition either).
func (b *InMemoryBackend) DisassociateLicense(id, licenseType string) (*Workspace, error) {
	b.mu.Lock("DisassociateLicense")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return nil, notFoundError(resourceTypeWorkspace, id)
	}

	if w.LicenseType != licenseType {
		cp := *w

		return &cp, nil
	}

	w.LicenseType = ""
	w.LicenseExpiration = time.Time{}
	w.GrafanaToken = ""
	w.Modified = time.Now().UTC()

	cp := *w

	return &cp, nil
}
