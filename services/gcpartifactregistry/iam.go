package gcpartifactregistry

import (
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func clonePolicy(p *iampb.Policy) *iampb.Policy {
	out, _ := proto.Clone(p).(*iampb.Policy)

	return out
}

// GetIamPolicy returns a repository's policy, or an empty one with an etag if none was set.
func (b *InMemoryBackend) GetIamPolicy(resource string) (*iampb.Policy, error) {
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

	if rec.iam == nil {
		return &iampb.Policy{Etag: []byte("ACAB")}, nil
	}

	return clonePolicy(rec.iam), nil
}

// SetIamPolicy replaces a repository's policy; a stale etag is ABORTED-style FAILED_PRECONDITION.
func (b *InMemoryBackend) SetIamPolicy(resource string, policy *iampb.Policy) (*iampb.Policy, error) {
	ref, err := parseRepo(resource)
	if err != nil {
		return nil, err
	}

	if policy == nil {
		return nil, errInvalid("policy is required.")
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	rec, ok := b.repos[ref.full()]
	if !ok {
		return nil, errNotFound("Repository %q not found.", ref.full())
	}

	if len(policy.GetEtag()) > 0 && rec.iam != nil && string(policy.GetEtag()) != string(rec.iam.GetEtag()) {
		return nil, errPrecondition("The policy was modified concurrently; fetch it again.")
	}

	next := clonePolicy(policy)
	next.Etag = []byte(uuid.NewString())
	rec.iam = next

	return clonePolicy(next), nil
}
