package gcpbigtable

import (
	"encoding/binary"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const (
	etagLen    = 8
	permPrefix = "bigtable."
)

func (b *InMemoryBackend) nextEtag() []byte {
	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.nextSeq())

	return raw
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

// GetIamPolicy returns the policy of an instance, table or backup at the requested version.
func (b *InMemoryBackend) GetIamPolicy(resource string, version int32) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	slot, err := b.policySlot(resource)
	if err != nil {
		return nil, err
	}

	return gcpiampolicy.View(*slot, version), nil
}

// SetIamPolicy replaces a policy; a stale etag aborts the call.
func (b *InMemoryBackend) SetIamPolicy(in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(in.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	slot, err := b.policySlot(in.GetResource())
	if err != nil {
		return nil, err
	}

	next, err := gcpiampolicy.Apply(*slot, in.GetPolicy(), b.nextEtag)
	if err != nil {
		return nil, err
	}

	*slot = next

	return gcpiampolicy.SetView(next), nil
}

// TestIamPermissions reports the requested Bigtable permissions as granted: the emulator authorises all callers.
func (b *InMemoryBackend) TestIamPermissions(
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	if _, err := b.policySlot(in.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(in.GetPermissions(), permPrefix)}, nil
}
