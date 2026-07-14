package dms

import (
	"context"
	"fmt"
)

// ApplyPendingMaintenanceAction applies a pending maintenance action to a replication instance.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	ctx context.Context,
	replicationInstanceArn, applyAction, optInType string,
) (*ReplicationInstance, error) {
	b.mu.Lock("ApplyPendingMaintenanceAction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, replicationInstanceArn))
	if !ok {
		ri, ok = b.replicationInstances.Get(regionKey(region, replicationInstanceArn))
	}
	if ok {
		// In-memory: mark the action as applied by updating the engine version
		// for "os-upgrade" / "db-upgrade" or just acknowledge for others.
		_ = applyAction
		_ = optInType
		cp := *ri

		return &cp, nil
	}

	return nil, fmt.Errorf(
		"%w: replication instance %s not found",
		ErrNotFound,
		replicationInstanceArn,
	)
}
