package secretsmanager

import (
	"context"
	"fmt"
)

// replicationStatusInSync is the status used for in-sync replicas.
const (
	replicationStatusFailed     = "Failed"
	replicationStatusInProgress = "InProgress"
	replicationStatusInSync     = "InSync"
)

// ReplicateSecretToRegions adds replication configuration for the specified regions.
func (b *InMemoryBackend) ReplicateSecretToRegions(
	ctx context.Context,
	input *ReplicateSecretToRegionsInput,
) (*ReplicateSecretToRegionsOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("ReplicateSecretToRegions")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	configs := b.replicationConfigsStore(region)
	existing := configs[name]
	existingByRegion := make(map[string]int, len(existing))

	for i, r := range existing {
		existingByRegion[r.Region] = i
	}

	for _, replica := range input.AddReplicaRegions {
		if _, found := existingByRegion[replica.Region]; found && !input.ForceOverwriteReplicaSecret {
			return nil, fmt.Errorf(
				"%w: a replica already exists in region %s; use ForceOverwriteReplicaSecret to overwrite",
				ErrSecretAlreadyExists, replica.Region,
			)
		}

		status := ReplicationStatusType{
			Region:        replica.Region,
			KmsKeyID:      replica.KmsKeyID,
			Status:        replicationStatusInProgress,
			StatusMessage: "replication queued",
		}

		if idx, found := existingByRegion[replica.Region]; found {
			existing[idx] = status
		} else {
			existing = append(existing, status)
		}
	}

	configs[name] = existing
	b.syncReplicationStatusLocked(region, secret)

	return &ReplicateSecretToRegionsOutput{
		ARN:               secret.ARN,
		ReplicationStatus: configs[name],
	}, nil
}

// RemoveRegionsFromReplication removes replication configuration for the specified regions.
func (b *InMemoryBackend) RemoveRegionsFromReplication(
	ctx context.Context,
	input *RemoveRegionsFromReplicationInput,
) (*RemoveRegionsFromReplicationOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveRegionsFromReplication")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	toRemove := make(map[string]struct{}, len(input.RemoveReplicaRegions))

	for _, r := range input.RemoveReplicaRegions {
		toRemove[r] = struct{}{}
	}

	configs := b.replicationConfigsStore(region)
	existing := configs[name]
	remaining := make([]ReplicationStatusType, 0, len(existing))

	for _, r := range existing {
		if _, remove := toRemove[r.Region]; !remove {
			remaining = append(remaining, r)
		}
	}

	configs[name] = remaining

	return &RemoveRegionsFromReplicationOutput{
		ARN:               secret.ARN,
		ReplicationStatus: remaining,
	}, nil
}

// StopReplicationToReplica promotes a replica secret to a standalone secret.
func (b *InMemoryBackend) StopReplicationToReplica(
	ctx context.Context,
	input *StopReplicationToReplicaInput,
) (*StopReplicationToReplicaOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopReplicationToReplica")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	// In the in-memory backend, we simply remove any replication config for this secret.
	delete(b.replicationConfigsStore(region), name)

	return &StopReplicationToReplicaOutput{
		ARN: secret.ARN,
	}, nil
}

func (b *InMemoryBackend) syncReplicationStatusLocked(region string, secret *Secret) {
	configs := b.replicationConfigsStore(region)
	statuses, exists := configs[secret.Name]
	if !exists || len(statuses) == 0 {
		return
	}

	currentVer := b.findVersion(secret, "", StagingLabelCurrent)
	if currentVer == nil {
		for i := range statuses {
			statuses[i].Status = replicationStatusFailed
			statuses[i].StatusMessage = "no current secret version to replicate"
		}
		configs[secret.Name] = statuses

		return
	}

	for i := range statuses {
		statuses[i].Status = replicationStatusInSync
		statuses[i].StatusMessage = "replicated version " + currentVer.VersionID
	}

	configs[secret.Name] = statuses
}
