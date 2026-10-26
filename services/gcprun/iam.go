package gcprun

import (
	"context"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const (
	permServices = "run.services."
	permJobs     = "run.jobs."
)

// iamTargetLocked validates that name is a service or job and returns the permission prefix for its type.
func (b *InMemoryBackend) iamTargetLocked(name string) (string, error) {
	if n, err := parseName(name, "services"); err == nil {
		if _, ok := b.services[name]; !ok {
			return "", errNotFound("SERVICE", n.ids[0], n.location, n.project)
		}

		return permServices, nil
	}

	if n, err := parseName(name, "jobs"); err == nil {
		if _, ok := b.jobs[name]; !ok {
			return "", errNotFound("JOB", n.ids[0], n.location, n.project)
		}

		return permJobs, nil
	}

	return "", errInvalid("Invalid resource name %q: expected a service or a job.", name)
}

// SetIamPolicy replaces the policy of a service or job; a non-empty etag must match the stored policy.
func (b *InMemoryBackend) SetIamPolicy(in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(in.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	if _, err := b.iamTargetLocked(in.GetResource()); err != nil {
		return nil, err
	}

	return b.iam.Set(in.GetResource(), in.GetPolicy())
}

// GetIamPolicy returns the policy of a service or job, empty when none was set.
func (b *InMemoryBackend) GetIamPolicy(in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	if _, err := b.iamTargetLocked(in.GetResource()); err != nil {
		return nil, err
	}

	return b.iam.Get(in.GetResource(), in.GetOptions().GetRequestedPolicyVersion()), nil
}

// TestIamPermissions reports the requested permissions that apply to the resource type as granted:
// the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	if len(in.GetPermissions()) == 0 {
		return nil, errInvalid("permissions must not be empty.")
	}

	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	prefix, err := b.iamTargetLocked(in.GetResource())
	if err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(in.GetPermissions(), prefix)}, nil
}

func (s *servicesServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *servicesServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in)
}

func (s *servicesServer) TestIamPermissions(
	_ context.Context,
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

func (s *jobsServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *jobsServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in)
}

func (s *jobsServer) TestIamPermissions(
	_ context.Context,
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}
