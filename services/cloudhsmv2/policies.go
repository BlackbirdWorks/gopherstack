package cloudhsmv2

import (
	"context"
	"encoding/json"
	"strings"
)

func (b *InMemoryBackend) policyBackupLocked(sc scope, resourceARN string) (*Backup, error) {
	if resourceARN == "" {
		return nil, fail(ErrInvalidRequest, "ResourceArn is required")
	}

	if !strings.Contains(resourceARN, ":backup/") {
		return nil, fail(ErrInvalidRequest, "resource policies are only supported on backups")
	}

	return b.backupLocked(sc, resourceARN)
}

// PutResourcePolicy attaches a policy to a READY backup the caller owns.
func (b *InMemoryBackend) PutResourcePolicy(ctx context.Context, resourceARN, policy string) (string, error) {
	var doc struct {
		Statement json.RawMessage `json:"Statement"`
	}

	if policy == "" || json.Unmarshal([]byte(policy), &doc) != nil || len(doc.Statement) == 0 {
		return "", fail(ErrInvalidRequest, "Policy must be a JSON policy document with a Statement")
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.reapLocked()

	bk, err := b.policyBackupLocked(sc, resourceARN)
	if err != nil {
		return "", err
	}

	if state := backupState(bk, b.now()); state != stateReady {
		return "", fail(ErrInvalidRequest, "backup %s is %s; only a READY backup can be shared", bk.ID, state)
	}

	bk.Policy = policy

	return policy, nil
}

// GetResourcePolicy returns the policy attached to a backup.
func (b *InMemoryBackend) GetResourcePolicy(ctx context.Context, resourceARN string) (string, error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	bk, err := b.policyBackupLocked(sc, resourceARN)
	if err != nil {
		return "", err
	}

	if bk.Policy == "" {
		return "", fail(ErrResourceNotFound, "no resource policy is attached to %s", resourceARN)
	}

	return bk.Policy, nil
}

// DeleteResourcePolicy removes and returns the policy attached to a backup.
func (b *InMemoryBackend) DeleteResourcePolicy(ctx context.Context, resourceARN string) (string, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	b.reapLocked()

	bk, err := b.policyBackupLocked(sc, resourceARN)
	if err != nil {
		return "", err
	}

	if bk.Policy == "" {
		return "", fail(ErrResourceNotFound, "no resource policy is attached to %s", resourceARN)
	}

	policy := bk.Policy
	bk.Policy = ""

	return policy, nil
}
