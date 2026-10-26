package gcpspanner

import (
	"encoding/binary"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const etagLen = 8

const permPrefix = "spanner."

func (b *InMemoryBackend) nextEtag() []byte {
	b.etagSeq++
	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.etagSeq)

	return raw
}

// GetIamPolicy returns the policy of an instance at the requested version.
func (b *InMemoryBackend) GetIamPolicy(resource string, version int32) (*iampb.Policy, error) {
	if _, _, err := parseInstance(resource); err != nil {
		return nil, err
	}

	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	rec, ok := b.instances[resource]
	if !ok {
		return nil, errInstanceNotFound(resource)
	}

	return gcpiampolicy.View(rec.policy, version), nil
}

// SetIamPolicy replaces the policy of an instance; a stale etag aborts the call.
func (b *InMemoryBackend) SetIamPolicy(in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if _, _, err := parseInstance(in.GetResource()); err != nil {
		return nil, err
	}

	if err := gcpiampolicy.Validate(in.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	rec, ok := b.instances[in.GetResource()]
	if !ok {
		return nil, errInstanceNotFound(in.GetResource())
	}

	next, err := gcpiampolicy.Apply(rec.policy, in.GetPolicy(), b.nextEtag)
	if err != nil {
		return nil, err
	}

	rec.policy = next

	return gcpiampolicy.SetView(next), nil
}

// TestIamPermissions reports the requested Spanner permissions as granted: the emulator authorises all callers.
func (b *InMemoryBackend) TestIamPermissions(
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	if _, err := b.GetInstance(in.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(in.GetPermissions(), permPrefix)}, nil
}
