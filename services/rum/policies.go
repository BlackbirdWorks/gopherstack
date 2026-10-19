package rum

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

// PutResourcePolicy attaches or replaces the resource policy of an app monitor.
func (b *InMemoryBackend) PutResourcePolicy(
	ctx context.Context, name, doc, revision string,
) (string, string, error) {
	if len(doc) > maxPolicyBytes {
		return "", "", policySizeExceeded("the policy document can be at most %d bytes", maxPolicyBytes)
	}

	var parsed struct {
		Statement json.RawMessage `json:"Statement"`
	}

	if doc == "" || json.Unmarshal([]byte(doc), &parsed) != nil || len(parsed.Statement) == 0 {
		return "", "", malformedPolicy("PolicyDocument must be a JSON policy with a Statement")
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	m, err := b.monitorByNameLocked(sc, name)
	if err != nil {
		return "", "", err
	}

	if revision != "" && m.Policy != "" && revision != m.PolicyRevision {
		return "", "", invalidRevision("PolicyRevisionId %s is not the current revision", revision)
	}

	cp := *m
	cp.Policy, cp.PolicyRevision = doc, awsid.New("")
	b.monitors.Put(&cp)

	return cp.Policy, cp.PolicyRevision, nil
}

// GetResourcePolicy returns the policy and revision of an app monitor.
func (b *InMemoryBackend) GetResourcePolicy(ctx context.Context, name string) (string, string, error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	m, err := b.monitorByNameLocked(sc, name)
	if err != nil {
		return "", "", err
	}

	if m.Policy == "" {
		return "", "", policyNotFound("app monitor %s has no resource policy", name)
	}

	return m.Policy, m.PolicyRevision, nil
}

// DeleteResourcePolicy removes the policy and returns the revision that was deleted.
func (b *InMemoryBackend) DeleteResourcePolicy(ctx context.Context, name, revision string) (string, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	m, err := b.monitorByNameLocked(sc, name)
	if err != nil {
		return "", err
	}

	if m.Policy == "" {
		return "", policyNotFound("app monitor %s has no resource policy", name)
	}

	if revision != "" && revision != m.PolicyRevision {
		return "", invalidRevision("PolicyRevisionId %s is not the current revision", revision)
	}

	deleted := m.PolicyRevision
	cp := *m
	cp.Policy, cp.PolicyRevision = "", ""
	b.monitors.Put(&cp)

	return deleted, nil
}
