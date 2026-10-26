package gcpkms

import (
	"encoding/binary"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const (
	etagLen    = 8
	permPrefix = "cloudkms."
)

func (b *InMemoryBackend) nextPolicyEtag() []byte {
	b.etagSeq++

	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.etagSeq)

	return raw
}

// SetIamPolicy replaces a resource's IAM policy; a non-empty etag must match the stored policy.
func (b *InMemoryBackend) SetIamPolicy(req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(req.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	b.sweepLocked()

	if err := b.resourceLocked(req.GetResource()); err != nil {
		return nil, err
	}

	next, err := gcpiampolicy.Apply(b.policies[req.GetResource()], req.GetPolicy(), b.nextPolicyEtag)
	if err != nil {
		return nil, err
	}

	b.policies[req.GetResource()] = next

	return gcpiampolicy.SetView(next), nil
}

// GetIamPolicy returns a resource's IAM policy at the requested version, or an empty policy when none is set.
func (b *InMemoryBackend) GetIamPolicy(resource string, version int32) (*iampb.Policy, error) {
	b.mu.Lock("GetIamPolicy")
	defer b.mu.Unlock()

	b.sweepLocked()

	if err := b.resourceLocked(resource); err != nil {
		return nil, err
	}

	return gcpiampolicy.View(b.policies[resource], version), nil
}

// TestIamPermissions reports the requested Cloud KMS permissions as granted: the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.Lock("TestIamPermissions")
	defer b.mu.Unlock()

	b.sweepLocked()

	if err := b.resourceLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(req.GetPermissions(), permPrefix)}, nil
}

// resourceLocked checks that name is an existing key ring, crypto key or import job.
func (b *InMemoryBackend) resourceLocked(name string) error {
	if _, ok := b.rings[name]; ok {
		return nil
	}

	if _, ok := b.keys[name]; ok {
		return nil
	}

	if _, ok := b.jobs[name]; ok {
		return nil
	}

	return errNotFound("Resource", name)
}
