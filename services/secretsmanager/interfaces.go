package secretsmanager

import "context"

// StorageBackend defines the interface for the Secrets Manager in-memory backend.
// The region for each operation is resolved from the supplied context (falling back
// to the backend's default region).
type StorageBackend interface {
	CreateSecret(ctx context.Context, input *CreateSecretInput) (*CreateSecretOutput, error)
	GetSecretValue(ctx context.Context, input *GetSecretValueInput) (*GetSecretValueOutput, error)
	PutSecretValue(ctx context.Context, input *PutSecretValueInput) (*PutSecretValueOutput, error)
	DeleteSecret(ctx context.Context, input *DeleteSecretInput) (*DeleteSecretOutput, error)
	ListSecrets(ctx context.Context, input *ListSecretsInput) (*ListSecretsOutput, error)
	ListSecretVersionIDs(ctx context.Context, input *ListSecretVersionIDsInput) (*ListSecretVersionIDsOutput, error)
	DescribeSecret(ctx context.Context, input *DescribeSecretInput) (*DescribeSecretOutput, error)
	UpdateSecret(ctx context.Context, input *UpdateSecretInput) (*UpdateSecretOutput, error)
	RestoreSecret(ctx context.Context, input *RestoreSecretInput) (*RestoreSecretOutput, error)
	TagResource(ctx context.Context, input *TagResourceInput) error
	UntagResource(ctx context.Context, input *UntagResourceInput) error
	RotateSecret(ctx context.Context, input *RotateSecretInput) (*RotateSecretOutput, error)
	GetRandomPassword(input *GetRandomPasswordInput) (*GetRandomPasswordOutput, error)
	ListAll() []SecretListEntry
	BatchGetSecretValue(ctx context.Context, input *BatchGetSecretValueInput) (*BatchGetSecretValueOutput, error)
	CancelRotateSecret(ctx context.Context, input *CancelRotateSecretInput) (*CancelRotateSecretOutput, error)
	GetResourcePolicy(ctx context.Context, input *GetResourcePolicyInput) (*GetResourcePolicyOutput, error)
	PutResourcePolicy(ctx context.Context, input *PutResourcePolicyInput) (*PutResourcePolicyOutput, error)
	DeleteResourcePolicy(ctx context.Context, input *DeleteResourcePolicyInput) (*DeleteResourcePolicyOutput, error)
	ReplicateSecretToRegions(
		ctx context.Context, input *ReplicateSecretToRegionsInput,
	) (*ReplicateSecretToRegionsOutput, error)
	RemoveRegionsFromReplication(
		ctx context.Context, input *RemoveRegionsFromReplicationInput,
	) (*RemoveRegionsFromReplicationOutput, error)
	StopReplicationToReplica(
		ctx context.Context, input *StopReplicationToReplicaInput,
	) (*StopReplicationToReplicaOutput, error)
	UpdateSecretVersionStage(
		ctx context.Context, input *UpdateSecretVersionStageInput,
	) (*UpdateSecretVersionStageOutput, error)
	ValidateResourcePolicy(
		ctx context.Context, input *ValidateResourcePolicyInput,
	) (*ValidateResourcePolicyOutput, error)
}

// ensure InMemoryBackend satisfies StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)
