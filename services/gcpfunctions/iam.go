package gcpfunctions

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const permFunctions = "cloudfunctions.functions."

func (b *InMemoryBackend) iamTargetLocked(name string) error {
	n, err := parseName(name)
	if err != nil {
		return err
	}

	if _, ok := b.functions[n.full()]; !ok {
		return errNotFound(n)
	}

	return nil
}

// SetIamPolicy replaces a function's policy; a non-empty etag must match the stored policy.
func (b *InMemoryBackend) SetIamPolicy(in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(in.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	if err := b.iamTargetLocked(in.GetResource()); err != nil {
		return nil, err
	}

	return b.iam.Set(in.GetResource(), in.GetPolicy())
}

// GetIamPolicy returns a function's policy, empty when none was set.
func (b *InMemoryBackend) GetIamPolicy(in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	if err := b.iamTargetLocked(in.GetResource()); err != nil {
		return nil, err
	}

	return b.iam.Get(in.GetResource(), in.GetOptions().GetRequestedPolicyVersion()), nil
}

// TestIamPermissions reports the cloudfunctions.functions.* permissions as granted: nothing is authorised.
func (b *InMemoryBackend) TestIamPermissions(
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	if len(in.GetPermissions()) == 0 {
		return nil, errInvalid("permissions must not be empty.")
	}

	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	if err := b.iamTargetLocked(in.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{
		Permissions: gcpiampolicy.Granted(in.GetPermissions(), permFunctions),
	}, nil
}

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
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

func marshalPolicies(m map[string]*iampb.Policy) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(m))

	for k, p := range m {
		if raw, err := protojson.Marshal(p); err == nil {
			out[k] = raw
		}
	}

	return out
}

func unmarshalPolicies(raw map[string]json.RawMessage) (map[string]*iampb.Policy, error) {
	out := make(map[string]*iampb.Policy, len(raw))

	for k, r := range raw {
		p := &iampb.Policy{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(r, p); err != nil {
			return nil, fmt.Errorf("gcpfunctions: restore iam policy %q: %w", k, err)
		}

		out[k] = p
	}

	return out, nil
}
