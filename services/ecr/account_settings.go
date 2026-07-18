package ecr

import (
	"context"
	"sort"
	"time"
)

// GetAccountSetting returns a registry account setting.
func (b *InMemoryBackend) GetAccountSetting(
	ctx context.Context, //nolint:revive // existing issue.
	name string,
) (string, error) {
	b.mu.RLock("GetAccountSetting")
	defer b.mu.RUnlock()

	if entry, ok := b.accountSettings.Get(name); ok {
		return entry.Value, nil
	}

	return "", nil
}

// PutAccountSetting updates a registry account setting.
func (b *InMemoryBackend) PutAccountSetting(
	ctx context.Context, //nolint:revive // existing issue.
	name, value string,
) (string, error) {
	b.mu.Lock("PutAccountSetting")
	defer b.mu.Unlock()

	b.accountSettings.Put(&accountSettingEntry{Name: name, Value: value})

	return value, nil
}

// RegisterPullTimeUpdateExclusion creates a pull time update exclusion.
func (b *InMemoryBackend) RegisterPullTimeUpdateExclusion(
	ctx context.Context, //nolint:revive // existing issue.
	principalArn string,
) (*PullTimeUpdateExclusion, error) {
	b.mu.Lock("RegisterPullTimeUpdateExclusion")
	defer b.mu.Unlock()

	exclusion := &PullTimeUpdateExclusion{CreatedAt: time.Now(), PrincipalArn: principalArn}
	b.pullTimeUpdateExclusions.Put(exclusion)
	cp := *exclusion

	return &cp, nil
}

// DeregisterPullTimeUpdateExclusion deletes a pull time update exclusion.
func (b *InMemoryBackend) DeregisterPullTimeUpdateExclusion(
	ctx context.Context, //nolint:revive // existing issue.
	principalArn string,
) (*PullTimeUpdateExclusion, error) {
	b.mu.Lock("DeregisterPullTimeUpdateExclusion")
	defer b.mu.Unlock()

	exclusion, ok := b.pullTimeUpdateExclusions.Get(principalArn)
	if !ok {
		return &PullTimeUpdateExclusion{PrincipalArn: principalArn}, nil
	}

	b.pullTimeUpdateExclusions.Delete(principalArn)
	cp := *exclusion

	return &cp, nil
}

// ListPullTimeUpdateExclusions lists pull time update exclusions.
func (b *InMemoryBackend) ListPullTimeUpdateExclusions(
	ctx context.Context, //nolint:revive // existing issue.
) ([]PullTimeUpdateExclusion, error) {
	b.mu.RLock("ListPullTimeUpdateExclusions")
	defer b.mu.RUnlock()

	out := make([]PullTimeUpdateExclusion, 0, b.pullTimeUpdateExclusions.Len())
	for _, exclusion := range b.pullTimeUpdateExclusions.All() {
		out = append(out, *exclusion)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PrincipalArn < out[j].PrincipalArn })

	return out, nil
}
