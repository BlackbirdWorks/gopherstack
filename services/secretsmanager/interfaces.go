package secretsmanager

// StorageBackend defines the interface for the Secrets Manager in-memory backend.
type StorageBackend interface {
	CreateSecret(input *CreateSecretInput) (*CreateSecretOutput, error)
	GetSecretValue(input *GetSecretValueInput) (*GetSecretValueOutput, error)
	PutSecretValue(input *PutSecretValueInput) (*PutSecretValueOutput, error)
	DeleteSecret(input *DeleteSecretInput) (*DeleteSecretOutput, error)
	ListSecrets(input *ListSecretsInput) (*ListSecretsOutput, error)
	ListSecretVersionIDs(input *ListSecretVersionIDsInput) (*ListSecretVersionIDsOutput, error)
	DescribeSecret(input *DescribeSecretInput) (*DescribeSecretOutput, error)
	UpdateSecret(input *UpdateSecretInput) (*UpdateSecretOutput, error)
	RestoreSecret(input *RestoreSecretInput) (*RestoreSecretOutput, error)
	TagResource(input *TagResourceInput) error
	UntagResource(input *UntagResourceInput) error
	RotateSecret(input *RotateSecretInput) (*RotateSecretOutput, error)
	GetRandomPassword(input *GetRandomPasswordInput) (*GetRandomPasswordOutput, error)
	ListAll() []SecretListEntry
	BatchGetSecretValue(input *BatchGetSecretValueInput) (*BatchGetSecretValueOutput, error)
	CancelRotateSecret(input *CancelRotateSecretInput) (*CancelRotateSecretOutput, error)
	GetResourcePolicy(input *GetResourcePolicyInput) (*GetResourcePolicyOutput, error)
	PutResourcePolicy(input *PutResourcePolicyInput) (*PutResourcePolicyOutput, error)
	DeleteResourcePolicy(input *DeleteResourcePolicyInput) (*DeleteResourcePolicyOutput, error)
	ReplicateSecretToRegions(input *ReplicateSecretToRegionsInput) (*ReplicateSecretToRegionsOutput, error)
	RemoveRegionsFromReplication(input *RemoveRegionsFromReplicationInput) (*RemoveRegionsFromReplicationOutput, error)
	StopReplicationToReplica(input *StopReplicationToReplicaInput) (*StopReplicationToReplicaOutput, error)
	UpdateSecretVersionStage(input *UpdateSecretVersionStageInput) (*UpdateSecretVersionStageOutput, error)
	ValidateResourcePolicy(input *ValidateResourcePolicyInput) (*ValidateResourcePolicyOutput, error)
}

// ensure InMemoryBackend satisfies StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)
