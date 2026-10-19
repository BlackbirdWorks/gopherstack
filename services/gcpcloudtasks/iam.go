package gcpcloudtasks

import (
	"bytes"
	"encoding/binary"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const etagLen = 8

func emptyPolicyEtag() []byte { return []byte{0x00, 0x20, 0x01} }

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

func (b *InMemoryBackend) queueExistsLocked(name string) error {
	if _, err := parseQueueName(name); err != nil {
		return err
	}

	if _, ok := b.queues[name]; !ok {
		return errQueueMissing()
	}

	return nil
}

// SetIamPolicy replaces a queue's IAM policy; a non-empty etag must match the stored policy.
func (b *InMemoryBackend) SetIamPolicy(req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if req.GetPolicy() == nil {
		return nil, errInvalid("policy must be specified.")
	}

	for _, bd := range req.GetPolicy().GetBindings() {
		if bd.GetRole() == "" || len(bd.GetMembers()) == 0 {
			return nil, errInvalid("Every binding needs a role and at least one member.")
		}
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	if err := b.queueExistsLocked(req.GetResource()); err != nil {
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

// GetIamPolicy returns a queue's IAM policy, or an empty policy when none is set.
func (b *InMemoryBackend) GetIamPolicy(resource string) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	if err := b.queueExistsLocked(resource); err != nil {
		return nil, err
	}

	if p := b.policies[resource]; p != nil {
		return clonePolicy(p), nil
	}

	return &iampb.Policy{Etag: emptyPolicyEtag()}, nil
}

// TestIamPermissions reports every requested permission as granted: the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	if err := b.queueExistsLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: append([]string(nil), req.GetPermissions()...)}, nil
}
