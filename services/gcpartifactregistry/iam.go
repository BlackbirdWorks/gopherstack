package gcpartifactregistry

import (
	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
	"github.com/google/uuid"
)

const permPrefix = "artifactregistry."

func newEtag() []byte { return []byte(uuid.NewString()) }

// GetIamPolicy returns a repository's policy at the requested version, or an empty one with an etag if none was set.
func (b *InMemoryBackend) GetIamPolicy(resource string, version int32) (*iampb.Policy, error) {
	ref, err := parseRepo(resource)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	rec, ok := b.repos[ref.full()]
	if !ok {
		return nil, errNotFound("Repository %q not found.", ref.full())
	}

	return gcpiampolicy.View(rec.iam, version), nil
}

// SetIamPolicy replaces a repository's policy; a stale etag aborts the call.
func (b *InMemoryBackend) SetIamPolicy(resource string, policy *iampb.Policy) (*iampb.Policy, error) {
	ref, err := parseRepo(resource)
	if err != nil {
		return nil, err
	}

	if err = gcpiampolicy.Validate(policy); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	rec, ok := b.repos[ref.full()]
	if !ok {
		return nil, errNotFound("Repository %q not found.", ref.full())
	}

	next, err := gcpiampolicy.Apply(rec.iam, policy, newEtag)
	if err != nil {
		return nil, err
	}

	rec.iam = next

	return gcpiampolicy.SetView(next), nil
}

// TestIamPermissions reports the requested Artifact Registry permissions as granted: the emulator does not enforce IAM.
func (b *InMemoryBackend) TestIamPermissions(resource string, perms []string) ([]string, error) {
	if _, err := b.GetIamPolicy(resource, 1); err != nil {
		return nil, err
	}

	return gcpiampolicy.Granted(perms, permPrefix), nil
}
