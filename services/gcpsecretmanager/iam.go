package gcpsecretmanager

import (
	"encoding/binary"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const (
	etagLen    = 8
	permPrefix = "secretmanager."
)

func (b *InMemoryBackend) nextPolicyEtag() []byte {
	b.etagSeq++

	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.etagSeq)

	return raw
}

// SetIamPolicy replaces a secret's IAM policy; a non-empty etag must match the stored policy.
func (b *InMemoryBackend) SetIamPolicy(req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(req.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(req.GetResource())
	if err != nil {
		return nil, err
	}

	next, err := gcpiampolicy.Apply(rec.policy, req.GetPolicy(), b.nextPolicyEtag)
	if err != nil {
		return nil, err
	}

	rec.policy = next

	return gcpiampolicy.SetView(next), nil
}

// GetIamPolicy returns a secret's IAM policy at the requested version, or an empty policy when none is set.
func (b *InMemoryBackend) GetIamPolicy(resource string, version int32) (*iampb.Policy, error) {
	b.mu.Lock("GetIamPolicy")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(resource)
	if err != nil {
		return nil, err
	}

	return gcpiampolicy.View(rec.policy, version), nil
}

// TestIamPermissions reports the requested Secret Manager permissions as granted; there is no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.Lock("TestIamPermissions")
	defer b.mu.Unlock()

	b.sweepLocked()

	if _, err := b.getLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(req.GetPermissions(), permPrefix)}, nil
}
