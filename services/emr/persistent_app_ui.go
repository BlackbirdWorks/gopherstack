package emr

import (
	"context"
	"fmt"
	"time"
)

func (b *InMemoryBackend) persistentAppUIGet(region, id string) (*PersistentAppUI, bool) {
	return b.persistentAppUIs.Get(regionKey(region, id))
}

func (b *InMemoryBackend) persistentAppUIPut(v *PersistentAppUI) { b.persistentAppUIs.Put(v) }

// DescribePersistentAppUI returns a persistent app UI by ID.
func (b *InMemoryBackend) DescribePersistentAppUI(ctx context.Context, id string) (*PersistentAppUI, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribePersistentAppUI")
	defer b.mu.RUnlock()

	ui, ok := b.persistentAppUIGet(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: persistent app UI %s not found", ErrNotFound, id)
	}

	cp := *ui

	return &cp, nil
}

// GetOnClusterPresignedURL returns a presigned URL for an on-cluster app UI, verifying cluster exists.
func (b *InMemoryBackend) GetOnClusterPresignedURL(_ context.Context, clusterID, region string) (string, error) {
	b.mu.RLock("GetOnClusterPresignedURL")
	defer b.mu.RUnlock()

	if _, ok := b.clusterGet(region, clusterID); !ok {
		return "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	return b.GetPresignedURL(clusterID, region), nil
}

// GetPresignedURL returns a synthetic presigned URL for a persistent app UI.
func (b *InMemoryBackend) GetPresignedURL(id, region string) string {
	return fmt.Sprintf(
		"https://%s.%s.persistent-emr.amazonaws.com?X-Amz-Signature=fakesig-%s",
		id,
		region,
		id,
	)
}

// GetClusterSessionCredentials returns synthesized credentials for cluster session access.
func (b *InMemoryBackend) GetClusterSessionCredentials(
	ctx context.Context,
	clusterID, executionRoleArn string,
) (map[string]any, time.Time, error) {
	if executionRoleArn == "" {
		return nil, time.Time{}, fmt.Errorf("%w: ExecutionRoleArn is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetClusterSessionCredentials")
	defer b.mu.RUnlock()

	if _, ok := b.clusterGet(region, clusterID); !ok {
		return nil, time.Time{}, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	expiry := time.Now().Add(sessionCredentialExpiry)
	creds := map[string]any{
		"UsernamePassword": map[string]string{
			"Username": "admin-" + clusterID,
			"Password": "fake-password-" + clusterID,
		},
	}

	return creds, expiry, nil
}

// CreatePersistentAppUI creates a new persistent application user interface.
func (b *InMemoryBackend) CreatePersistentAppUI(
	ctx context.Context,
	targetResourceArn string,
) (*PersistentAppUI, error) {
	if targetResourceArn == "" {
		return nil, fmt.Errorf("%w: TargetResourceArn is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreatePersistentAppUI")
	defer b.mu.Unlock()

	id := b.nextPersistentAppUIID()
	ui := &PersistentAppUI{
		ID:                        id,
		TargetResourceArn:         targetResourceArn,
		RuntimeRoleEnabledCluster: false,
		region:                    region,
		CreatedAt:                 time.Now(),
	}

	b.persistentAppUIPut(ui)
	cp := *ui

	return &cp, nil
}

// AddPersistentAppUIInternal seeds a persistent app UI directly into the backend for testing.
func (b *InMemoryBackend) AddPersistentAppUIInternal(ctx context.Context, ui PersistentAppUI) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddPersistentAppUIInternal")
	defer b.mu.Unlock()

	cp := ui
	cp.region = region
	b.persistentAppUIPut(&cp)
}
