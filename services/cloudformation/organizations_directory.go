package cloudformation

import "errors"

// OrganizationsDirectory resolves the account IDs beneath an OU or root, for
// SERVICE_MANAGED StackSet deployment-target expansion (DeploymentTargets.
// OrganizationalUnitIds). Satisfied structurally by
// organizations.InMemoryBackend.ResolveAccountIDsUnderParent.
type OrganizationsDirectory interface {
	ResolveAccountIDsUnderParent(parentID string) ([]string, error)
}

// SetOrganizationsDirectory wires an OrganizationsDirectory so SERVICE_MANAGED
// StackSet operations can expand OU-based deployment targets against the real
// Organizations hierarchy. Passing nil disables OU-based deployment targets.
// Intended to be called once during service wiring, before the backend serves
// traffic.
func (b *InMemoryBackend) SetOrganizationsDirectory(d OrganizationsDirectory) {
	b.mu.Lock("SetOrganizationsDirectory")
	defer b.mu.Unlock()
	b.orgDirectory = d
}

var (
	// ErrOrganizationsNotWired is returned when a SERVICE_MANAGED deployment
	// target names an OU but no Organizations backend has been wired.
	ErrOrganizationsNotWired = errors.New("no Organizations service wired for OU-based deployment targets")

	// ErrOrganizationsAccessNotActive is returned when OU-based deployment
	// targets are used before ActivateOrganizationsAccess.
	ErrOrganizationsAccessNotActive = errors.New(
		"organizations trusted access is not activated: call ActivateOrganizationsAccess first",
	)

	// ErrServiceManagedRequired is returned when OU-based deployment targets
	// are used against a non-SERVICE_MANAGED StackSet.
	ErrServiceManagedRequired = errors.New(
		"OU-based deployment targets require a SERVICE_MANAGED StackSet",
	)
)
