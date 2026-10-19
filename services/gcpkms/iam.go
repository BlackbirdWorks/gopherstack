package gcpkms

import (
	"bytes"
	"encoding/binary"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// emptyPolicyEtag is the etag IAM reports for a resource with no policy set.
func emptyPolicyEtag() []byte { return []byte{0x00, 0x20, 0x01} }

const etagLen = 8

func (b *InMemoryBackend) nextPolicyEtag() []byte {
	b.etagSeq++

	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.etagSeq)

	return raw
}

func clonePolicy(p *iampb.Policy) *iampb.Policy {
	out, _ := proto.Clone(p).(*iampb.Policy)

	return out
}

func validatePolicy(p *iampb.Policy) error {
	for _, bd := range p.GetBindings() {
		if bd.GetRole() == "" || len(bd.GetMembers()) == 0 {
			return errInvalid("Every binding needs a role and at least one member.")
		}
	}

	return nil
}

// SetIamPolicy replaces a resource's IAM policy; a non-empty etag must match the stored policy.
func (b *InMemoryBackend) SetIamPolicy(req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if req.GetPolicy() == nil {
		return nil, errInvalid("policy must be specified.")
	}

	if err := validatePolicy(req.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	b.sweepLocked()

	if err := b.resourceLocked(req.GetResource()); err != nil {
		return nil, err
	}

	current := emptyPolicyEtag()
	if p := b.policies[req.GetResource()]; p != nil {
		current = p.GetEtag()
	}

	if et := req.GetPolicy().GetEtag(); len(et) > 0 && !bytes.Equal(et, current) {
		return nil, status.Error(codes.Aborted,
			"There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff.")
	}

	next := clonePolicy(req.GetPolicy())
	next.Etag = b.nextPolicyEtag()
	b.policies[req.GetResource()] = next

	return clonePolicy(next), nil
}

// GetIamPolicy returns a resource's IAM policy, or an empty policy when none is set.
func (b *InMemoryBackend) GetIamPolicy(resource string) (*iampb.Policy, error) {
	b.mu.Lock("GetIamPolicy")
	defer b.mu.Unlock()

	b.sweepLocked()

	if err := b.resourceLocked(resource); err != nil {
		return nil, err
	}

	p := b.policies[resource]
	if p == nil {
		return &iampb.Policy{Etag: emptyPolicyEtag()}, nil
	}

	return clonePolicy(p), nil
}

// TestIamPermissions reports every requested permission as granted: the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.Lock("TestIamPermissions")
	defer b.mu.Unlock()

	b.sweepLocked()

	if err := b.resourceLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: append([]string(nil), req.GetPermissions()...)}, nil
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
