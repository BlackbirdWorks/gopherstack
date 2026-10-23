package gcpspanner

import (
	"encoding/base64"
	"encoding/binary"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const etagLen = 8

func clonePolicy(p *iampb.Policy) *iampb.Policy {
	out, _ := proto.Clone(p).(*iampb.Policy)

	return out
}

func (b *InMemoryBackend) nextEtag() []byte {
	b.etagSeq++
	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.etagSeq)

	return []byte(base64.StdEncoding.EncodeToString(raw))
}

func emptyPolicy() *iampb.Policy { return &iampb.Policy{Etag: []byte("ACAB")} }

func validatePolicy(p *iampb.Policy) error {
	for _, bind := range p.GetBindings() {
		if bind.GetRole() == "" || len(bind.GetMembers()) == 0 {
			return errInvalid("Every binding needs a role and at least one member.")
		}
	}

	return nil
}

// GetIamPolicy returns the policy of an instance.
func (b *InMemoryBackend) GetIamPolicy(resource string) (*iampb.Policy, error) {
	if _, _, err := parseInstance(resource); err != nil {
		return nil, err
	}

	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	rec, ok := b.instances[resource]
	if !ok {
		return nil, errInstanceNotFound(resource)
	}

	if rec.policy == nil {
		return emptyPolicy(), nil
	}

	return clonePolicy(rec.policy), nil
}

// SetIamPolicy replaces the policy of an instance; a stale etag aborts the call.
func (b *InMemoryBackend) SetIamPolicy(in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if _, _, err := parseInstance(in.GetResource()); err != nil {
		return nil, err
	}

	if in.GetPolicy() == nil {
		return nil, errInvalid("policy is required.")
	}

	if err := validatePolicy(in.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	rec, ok := b.instances[in.GetResource()]
	if !ok {
		return nil, errInstanceNotFound(in.GetResource())
	}

	cur := emptyPolicy()
	if rec.policy != nil {
		cur = rec.policy
	}

	if len(in.GetPolicy().GetEtag()) > 0 && string(in.GetPolicy().GetEtag()) != string(cur.GetEtag()) {
		return nil, status.Error(codes.Aborted, "The policy etag does not match the current policy; read it again.")
	}

	next := clonePolicy(in.GetPolicy())
	next.Etag = b.nextEtag()
	rec.policy = next

	return clonePolicy(next), nil
}

// TestIamPermissions reports every requested permission as granted: the emulator authorises all callers.
func (b *InMemoryBackend) TestIamPermissions(
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	if _, err := b.GetInstance(in.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: in.GetPermissions()}, nil
}
