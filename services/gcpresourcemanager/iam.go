package gcpresourcemanager

import (
	"slices"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const publicMemberAllUsers, publicMemberAllAuth = "allUsers", "allAuthenticatedUsers"

// policyKeyLocked resolves the resource an IAM call names to the key its policy is stored under.
func (b *InMemoryBackend) policyKeyLocked(resource string, write bool) (string, error) {
	kind, id, ok := strings.Cut(resource, "/")
	if !ok || id == "" || strings.Contains(id, "/") {
		return "", errInvalid("Invalid resource name %q.", resource)
	}

	switch kind {
	case kindOrganizations:
		if id != b.org.ID {
			return "", errDenied("resourcemanager.organizations.getIamPolicy", resource)
		}
	case kindFolders:
		f, err := b.folderLocked(resource, permFolderGet)
		if err != nil {
			return "", err
		}

		if write && f.requested() {
			return "", errPrecondition("Folder %s is in the DELETE_REQUESTED state.", resource)
		}
	case kindProjects:
		var (
			p   *projectRec
			err error
		)

		if write {
			p, err = b.activeProjectLocked(resource, "resourcemanager.projects.setIamPolicy")
		} else {
			p, err = b.projectLocked(resource, "resourcemanager.projects.getIamPolicy")
		}

		if err != nil {
			return "", err
		}

		return kindProjects + "/" + p.Number, nil
	case kindTagKeys:
		if _, ok := b.tagKeys[id]; !ok {
			return "", errDenied("resourcemanager.tagKeys.getIamPolicy", resource)
		}
	case kindTagValues:
		if _, ok := b.tagValues[id]; !ok {
			return "", errDenied("resourcemanager.tagValues.getIamPolicy", resource)
		}
	default:
		return "", errInvalid("IAM policies are available for organizations, folders, projects, tagKeys and tagValues, not %q.", resource)
	}

	return resource, nil
}

// GetIamPolicy returns the policy of a resource at the requested version.
func (b *InMemoryBackend) GetIamPolicy(in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	b.mu.Lock("GetIamPolicy")
	defer b.mu.Unlock()

	b.purgeLocked()

	key, err := b.policyKeyLocked(in.GetResource(), false)
	if err != nil {
		return nil, err
	}

	return b.policies.Get(key, in.GetOptions().GetRequestedPolicyVersion()), nil
}

func rejectPublicMembers(p *iampb.Policy) error {
	for _, bd := range p.GetBindings() {
		if slices.ContainsFunc(bd.GetMembers(), func(m string) bool { return m == publicMemberAllUsers || m == publicMemberAllAuth }) {
			return errInvalid("Invalid IAM policy: %s and %s cannot be granted a role on a project.",
				publicMemberAllUsers, publicMemberAllAuth)
		}
	}

	return nil
}

// SetIamPolicy replaces a policy; a stale etag aborts the call.
func (b *InMemoryBackend) SetIamPolicy(in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(in.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	b.purgeLocked()

	key, err := b.policyKeyLocked(in.GetResource(), true)
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(key, kindProjects+"/") {
		if err = rejectPublicMembers(in.GetPolicy()); err != nil {
			return nil, err
		}
	}

	return b.policies.Set(key, in.GetPolicy())
}

// TestIamPermissions reports every requested permission as granted: the emulator authorises all callers.
func (b *InMemoryBackend) TestIamPermissions(in *iampb.TestIamPermissionsRequest) (
	*iampb.TestIamPermissionsResponse, error,
) {
	b.mu.Lock("TestIamPermissions")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, err := b.policyKeyLocked(in.GetResource(), false); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(in.GetPermissions(), "")}, nil
}
