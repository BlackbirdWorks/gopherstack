package gcpresourcemanager

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
)

var _ gcpenforce.Enforcer = (*InMemoryBackend)(nil)

// CheckProject returns CONSUMER_INVALID for service unless project (id or number) exists and is ACTIVE.
func (b *InMemoryBackend) CheckProject(project, service string) error {
	b.mu.RLock("CheckProject")
	defer b.mu.RUnlock()

	p, ok := b.findProjectLocked(project)
	if !ok || p.requested() {
		return errConsumerFor(kindProjects+"/"+strings.TrimPrefix(project, kindProjects+"/"), service)
	}

	return nil
}

// SetEnforcement turns the opt-in project and API-enablement checks of the other GCP services on or off.
func (h *Handler) SetEnforcement(s gcpenforce.Settings) { h.enforce = s }

// EnforcementGate returns the Gate the other GCP services consult, or nil when enforcement is off.
func (h *Handler) EnforcementGate() *gcpenforce.Gate {
	if !h.enforce.Enabled() {
		return nil
	}

	return &gcpenforce.Gate{Enforcer: h.Backend, Settings: h.enforce}
}
