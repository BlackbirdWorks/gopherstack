package ec2

import "fmt"

// managedResourceVisibilityHidden and managedResourceVisibilityVisible are the
// AWS-defined ManagedResourceDefaultVisibility values: "hidden" (the AWS
// default — AWS-managed resources are excluded from Describe responses
// unless explicitly requested) and "visible" (AWS-managed resources are
// included by default).
const (
	managedResourceVisibilityHidden  = "hidden"
	managedResourceVisibilityVisible = "visible"
)

// managedResourceVisibilityValid are the AWS-defined
// ManagedResourceDefaultVisibility values accepted by
// ModifyManagedResourceVisibility.
//
//nolint:gochecknoglobals // lookup set, analogous to vpcEncryptionControlValidModes
var managedResourceVisibilityValid = map[string]bool{
	managedResourceVisibilityHidden:  true,
	managedResourceVisibilityVisible: true,
}

// GetManagedResourceVisibility returns the account's current default
// visibility setting for AWS-managed resources.
func (b *InMemoryBackend) GetManagedResourceVisibility() string {
	b.mu.RLock("GetManagedResourceVisibility")
	defer b.mu.RUnlock()

	return b.managedResourceDefaultVisibility
}

// ModifyManagedResourceVisibility updates the account's default visibility
// setting for AWS-managed resources.
func (b *InMemoryBackend) ModifyManagedResourceVisibility(defaultVisibility string) (string, error) {
	if !managedResourceVisibilityValid[defaultVisibility] {
		return "", fmt.Errorf("%w: DefaultVisibility %q is invalid", ErrInvalidParameter, defaultVisibility)
	}

	b.mu.Lock("ModifyManagedResourceVisibility")
	defer b.mu.Unlock()

	b.managedResourceDefaultVisibility = defaultVisibility

	return b.managedResourceDefaultVisibility, nil
}
