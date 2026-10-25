package gcpbigtable

import (
	"encoding/base64"
	"encoding/binary"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
)

const etagLen = 8

func (b *InMemoryBackend) nextEtag() []byte {
	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.nextSeq())

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

// policySlot returns the policy pointer of an instance, table or backup resource.
func (b *InMemoryBackend) policySlot(resource string) (**iampb.Policy, error) {
	switch {
	case strings.Contains(resource, "/backups/"):
		inst, err := parseBackup(resource)
		if err != nil {
			return nil, err
		}

		rec, err := b.instanceLocked(inst)
		if err != nil {
			return nil, err
		}

		bk, ok := rec.backups[resource]
		if !ok {
			return nil, errBackupNotFound(resource)
		}

		return &bk.policy, nil
	case strings.Contains(resource, "/tables/"):
		inst, id, err := parseTable(resource)
		if err != nil {
			return nil, err
		}

		rec, err := b.instanceLocked(inst)
		if err != nil {
			return nil, err
		}

		t, ok := rec.tables[id]
		if !ok {
			return nil, errTableNotFound(resource)
		}

		return &t.policy, nil
	default:
		rec, err := b.instanceLocked(resource)
		if err != nil {
			return nil, err
		}

		return &rec.policy, nil
	}
}

// GetIamPolicy returns the policy of an instance, table or backup.
func (b *InMemoryBackend) GetIamPolicy(resource string) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	slot, err := b.policySlot(resource)
	if err != nil {
		return nil, err
	}

	if *slot == nil {
		return emptyPolicy(), nil
	}

	return clone(*slot), nil
}

// SetIamPolicy replaces a policy; a stale etag aborts the call.
func (b *InMemoryBackend) SetIamPolicy(in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if in.GetPolicy() == nil {
		return nil, errInvalid("policy is required.")
	}

	if err := validatePolicy(in.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	slot, err := b.policySlot(in.GetResource())
	if err != nil {
		return nil, err
	}

	cur := emptyPolicy()
	if *slot != nil {
		cur = *slot
	}

	if len(in.GetPolicy().GetEtag()) > 0 && string(in.GetPolicy().GetEtag()) != string(cur.GetEtag()) {
		return nil, errAborted("The policy etag does not match the current policy; read it again.")
	}

	next := clone(in.GetPolicy())
	next.Etag = b.nextEtag()
	*slot = next

	return clone(next), nil
}

// TestIamPermissions reports every requested permission as granted: the emulator authorises all callers.
func (b *InMemoryBackend) TestIamPermissions(
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	if _, err := b.policySlot(in.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: in.GetPermissions()}, nil
}
