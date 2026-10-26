package gcppubsub

import (
	"context"
	"slices"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const (
	iamCollSchemas  = "schemas"
	iamCollTopics   = "topics"
	iamCollSubs     = "subscriptions"
	iamCollSnapshot = "snapshots"
)

// iamKind returns the collection of an IAM-capable resource name, or "" when the name is not one.
func iamKind(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != projectsSeg || parts[1] == "" || parts[3] == "" {
		return ""
	}

	if slices.Contains([]string{iamCollTopics, iamCollSubs, iamCollSnapshot, iamCollSchemas}, parts[2]) {
		return parts[2]
	}

	return ""
}

func (b *InMemoryBackend) iamExistsLocked(name, kind string) bool {
	switch kind {
	case iamCollTopics:
		_, ok := b.topics[name]

		return ok
	case iamCollSubs:
		_, err := b.liveSubLocked(name)

		return err == nil
	case iamCollSnapshot:
		_, err := b.liveSnapLocked(name)

		return err == nil
	default:
		_, ok := b.schemas[name]

		return ok
	}
}

func (b *InMemoryBackend) iamTargetLocked(name string) (string, error) {
	kind := iamKind(name)
	if kind == "" {
		return "", errInvalid("Invalid resource name given (name=%s).", name)
	}

	if !b.iamExistsLocked(name, kind) {
		return "", errNotFound(strings.TrimSuffix(kind, "s"), name)
	}

	return kind, nil
}

// SetIamPolicy replaces a resource's IAM policy; a non-empty etag must match the stored policy.
func (b *InMemoryBackend) SetIamPolicy(req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(req.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	if _, err := b.iamTargetLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return b.iam.Set(req.GetResource(), req.GetPolicy())
}

// GetIamPolicy returns a resource's IAM policy, or an empty policy when none is set.
func (b *InMemoryBackend) GetIamPolicy(req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	if _, err := b.iamTargetLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return b.iam.Get(req.GetResource(), req.GetOptions().GetRequestedPolicyVersion()), nil
}

// TestIamPermissions reports the requested permissions that apply to the resource type as granted:
// the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	if len(req.GetPermissions()) == 0 {
		return nil, errInvalid("permissions must not be empty.")
	}

	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	kind, err := b.iamTargetLocked(req.GetResource())
	if err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{
		Permissions: gcpiampolicy.Granted(req.GetPermissions(), "pubsub."+kind+"."),
	}, nil
}

func (b *InMemoryBackend) dropPolicyLocked(name string) { b.iam.Drop(name) }

type iamServer struct {
	iampb.UnimplementedIAMPolicyServer
	b *InMemoryBackend
}

func (s *iamServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *iamServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in)
}

func (s *iamServer) TestIamPermissions(
	_ context.Context,
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}
