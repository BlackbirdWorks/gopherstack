package gcpiam

import (
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const maxBindings = 1500

func memberValid(m string) bool {
	if m == "allUsers" || m == "allAuthenticatedUsers" {
		return true
	}

	for _, prefix := range []string{
		"user:", "serviceAccount:", "group:", "domain:", "deleted:",
		"projectOwner:", "projectEditor:", "projectViewer:", "principal:", "principalSet:",
	} {
		if rest, ok := strings.CutPrefix(m, prefix); ok && rest != "" {
			return true
		}
	}

	return false
}

// validateBindingsLocked checks member syntax, role format and existence of referenced custom roles.
func (b *InMemoryBackend) validateBindingsLocked(p *iampb.Policy) error {
	if len(p.GetBindings()) > maxBindings {
		return errInvalid("A policy may contain at most %d bindings.", maxBindings)
	}

	for _, bd := range p.GetBindings() {
		if bd.GetRole() == "" || len(bd.GetMembers()) == 0 {
			return errInvalid("Every binding needs a role and at least one member.")
		}

		for _, m := range bd.GetMembers() {
			if !memberValid(m) {
				return errInvalid("Invalid member %q.", m)
			}
		}

		if err := b.roleRefValidLocked(bd.GetRole()); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) roleRefValidLocked(role string) error {
	if rest, ok := strings.CutPrefix(role, "roles/"); ok {
		if rest == "" || strings.Contains(rest, "/") {
			return errInvalid("Invalid role %q.", role)
		}

		return nil
	}

	if err := validateCustomRoleName(role); err != nil {
		return errInvalid("Invalid role %q.", role)
	}

	if r, ok := b.roles[role]; !ok || !r.deletedAt.IsZero() {
		return errInvalid("Role %s does not exist in the resource's hierarchy.", role)
	}

	return nil
}

// GetIamPolicy returns a service account's IAM policy at the requested version, or an empty one.
func (b *InMemoryBackend) GetIamPolicy(resource string, version int32) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	rec, err := b.findAccountLocked(resource)
	if err != nil {
		return nil, err
	}

	return gcpiampolicy.View(rec.policy, version), nil
}

// SetIamPolicy replaces a service account's policy; a non-empty etag must match the stored one.
func (b *InMemoryBackend) SetIamPolicy(req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(req.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	rec, err := b.findAccountLocked(req.GetResource())
	if err != nil {
		return nil, err
	}

	if err = b.validateBindingsLocked(req.GetPolicy()); err != nil {
		return nil, err
	}

	next, err := gcpiampolicy.Apply(rec.policy, req.GetPolicy(), b.nextEtag)
	if err != nil {
		return nil, err
	}

	rec.policy = next

	return gcpiampolicy.SetView(next), nil
}

// TestIamPermissions reports the requested IAM permissions as granted: the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	if _, err := b.findAccountLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(req.GetPermissions(), "iam.")}, nil
}
