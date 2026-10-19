package gcpiam

import (
	"bytes"
	"slices"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/proto"
)

const maxBindings = 1500

func emptyPolicyEtag() []byte { return []byte{0x00, 0x20, 0x01} }

func clonePolicy(p *iampb.Policy) *iampb.Policy {
	out, _ := proto.Clone(p).(*iampb.Policy)

	return out
}

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

// GetIamPolicy returns a service account's IAM policy, or an empty one with the initial etag.
func (b *InMemoryBackend) GetIamPolicy(resource string) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	rec, err := b.findAccountLocked(resource)
	if err != nil {
		return nil, err
	}

	if rec.policy != nil {
		return clonePolicy(rec.policy), nil
	}

	return &iampb.Policy{Etag: emptyPolicyEtag()}, nil
}

// SetIamPolicy replaces a service account's policy; a non-empty etag must match the stored one.
func (b *InMemoryBackend) SetIamPolicy(req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if req.GetPolicy() == nil {
		return nil, errInvalid("policy must be specified.")
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

	current := emptyPolicyEtag()
	if rec.policy != nil {
		current = rec.policy.GetEtag()
	}

	if et := req.GetPolicy().GetEtag(); len(et) > 0 && !bytes.Equal(et, current) {
		return nil, errAborted("There were concurrent policy changes. " +
			"Please retry the whole read-modify-write with exponential backoff.")
	}

	next := clonePolicy(req.GetPolicy())
	next.Etag = b.nextEtag()
	rec.policy = next

	return clonePolicy(next), nil
}

// TestIamPermissions reports every requested permission as granted: the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	if _, err := b.findAccountLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: slices.Clone(req.GetPermissions())}, nil
}
