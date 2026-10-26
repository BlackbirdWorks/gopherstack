package gcpcloudtasks

import (
	"encoding/binary"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const (
	etagLen    = 8
	permPrefix = "cloudtasks."
)

func (b *InMemoryBackend) nextPolicyEtag() []byte {
	b.etagSeq++

	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.etagSeq)

	return raw
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
	if err := gcpiampolicy.Validate(req.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	if err := b.queueExistsLocked(req.GetResource()); err != nil {
		return nil, err
	}

	next, err := gcpiampolicy.Apply(b.policies[req.GetResource()], req.GetPolicy(), b.nextPolicyEtag)
	if err != nil {
		return nil, err
	}

	b.policies[req.GetResource()] = next

	return gcpiampolicy.SetView(next), nil
}

// GetIamPolicy returns a queue's IAM policy at the requested version, or an empty policy when none is set.
func (b *InMemoryBackend) GetIamPolicy(resource string, version int32) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	if err := b.queueExistsLocked(resource); err != nil {
		return nil, err
	}

	return gcpiampolicy.View(b.policies[resource], version), nil
}

// TestIamPermissions reports the requested Cloud Tasks permissions as granted: the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	if err := b.queueExistsLocked(req.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(req.GetPermissions(), permPrefix)}, nil
}
