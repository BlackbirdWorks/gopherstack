// Package kms provides a mock AWS Key Management Service (KMS) implementation.
package kms

import "time"

const (
	// nanoToSeconds converts nanoseconds to seconds.
	nanoToSeconds = 1e9
	// MockAccountID is the mock AWS account ID.
	MockAccountID = "000000000000"
	// MockRegion is the mock AWS region.
	MockRegion = "us-east-1"
)

// KeyStateEnabled is the string constant for an enabled key.
const KeyStateEnabled = "Enabled"

// KeyStateDisabled is the string constant for a disabled key.
const KeyStateDisabled = "Disabled"

// KeyUsageEncryptDecrypt is the string constant for the default key usage.
const KeyUsageEncryptDecrypt = "ENCRYPT_DECRYPT"

// Note: Go fields use KeyID (Go convention) while JSON tags use KeyId (AWS API wire format).
// This intentional difference matches both Go naming best practices and AWS API compatibility.

// Key represents a KMS customer-managed key.
type Key struct {
	// KeyId is the UUID identifier for the key.
	KeyID string `json:"KeyId"`
	// Arn is the full ARN of the key.
	Arn string `json:"Arn"`
	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`
	// KeyState is the current state: Enabled, Disabled, or PendingDeletion.
	KeyState string `json:"KeyState"`
	// KeyUsage is the cryptographic operation: ENCRYPT_DECRYPT.
	KeyUsage string `json:"KeyUsage"`
	// KeySpec is the key spec, e.g., "SYMMETRIC_DEFAULT".
	KeySpec string `json:"KeySpec,omitempty"`
	// CreationDate is the Unix timestamp when the key was created.
	CreationDate float64 `json:"CreationDate"`
	// RotationEnabled indicates whether automatic key rotation is enabled.
	RotationEnabled bool `json:"RotationEnabled"`
	// Enabled indicates whether the key is currently enabled.
	Enabled bool `json:"Enabled"`
	// DeletionDate is the Unix timestamp when the key will be deleted (PendingDeletion state).
	DeletionDate float64 `json:"DeletionDate,omitempty"`
}

// KeyMetadata is the metadata for a KMS key returned in API responses.
type KeyMetadata struct {
	// KeyId is the UUID identifier for the key.
	KeyID string `json:"KeyId"`
	// Arn is the full ARN of the key.
	Arn string `json:"Arn"`
	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`
	// KeyState is the current state: Enabled or Disabled.
	KeyState string `json:"KeyState"`
	// KeyUsage is the cryptographic operation: ENCRYPT_DECRYPT.
	KeyUsage string `json:"KeyUsage"`
	// KeyManager is always "CUSTOMER" for customer-managed keys.
	KeyManager string `json:"KeyManager,omitempty"`
	// Origin is always "AWS_KMS" for keys created in KMS.
	Origin string `json:"Origin,omitempty"`
	// KeySpec is the key spec, e.g., "SYMMETRIC_DEFAULT".
	KeySpec string `json:"KeySpec,omitempty"`
	// EncryptionAlgorithms lists the encryption algorithms supported by this key.
	EncryptionAlgorithms []string `json:"EncryptionAlgorithms,omitempty"`
	// MultiRegion indicates whether this is a multi-region key.
	MultiRegion bool `json:"MultiRegion"`
	// CreationDate is the Unix timestamp when the key was created.
	CreationDate float64 `json:"CreationDate"`
}

// Alias represents a KMS alias pointing to a key.
type Alias struct {
	// AliasName is the alias name (e.g., alias/my-key).
	AliasName string `json:"AliasName"`
	// AliasArn is the full ARN of the alias.
	AliasArn string `json:"AliasArn"`
	// TargetKeyId is the key ID that this alias points to.
	TargetKeyID string `json:"TargetKeyId,omitempty"`
}

// CreateKeyInput is the request payload for CreateKey.
type CreateKeyInput struct {
	// Description is an optional description.
	Description string `json:"Description,omitempty"`
	// KeyUsage is the cryptographic operation (default ENCRYPT_DECRYPT).
	KeyUsage string `json:"KeyUsage,omitempty"`
	// Region is the AWS region for ARN construction (optional; defaults to backend region).
	Region string `json:"-"`
}

// CreateKeyOutput is the response payload for CreateKey.
type CreateKeyOutput struct {
	// KeyMetadata contains the newly created key metadata.
	KeyMetadata KeyMetadata `json:"KeyMetadata"`
}

// DescribeKeyInput is the request payload for DescribeKey.
type DescribeKeyInput struct {
	// KeyId is the key ID or alias to describe.
	KeyID string `json:"KeyId"`
}

// DescribeKeyOutput is the response payload for DescribeKey.
type DescribeKeyOutput struct {
	// KeyMetadata contains the key metadata.
	KeyMetadata KeyMetadata `json:"KeyMetadata"`
}

// KeyListEntry is a brief key reference used in ListKeys.
type KeyListEntry struct {
	// KeyId is the UUID of the key.
	KeyID string `json:"KeyId"`
	// KeyArn is the full ARN of the key.
	KeyArn string `json:"KeyArn"`
}

// ListKeysInput is the request payload for ListKeys.
type ListKeysInput struct {
	// Limit caps the number of results returned.
	Limit *int32 `json:"Limit,omitempty"`
	// Marker is the pagination cursor from a previous call.
	Marker string `json:"Marker,omitempty"`
}

// ListKeysOutput is the response payload for ListKeys.
type ListKeysOutput struct {
	NextMarker string         `json:"NextMarker,omitempty"`
	Keys       []KeyListEntry `json:"Keys"`
	Truncated  bool           `json:"Truncated"`
}

// EncryptInput is the request payload for Encrypt.
type EncryptInput struct {
	// KeyId identifies the KMS key to use for encryption.
	KeyID string `json:"KeyId"`
	// Plaintext is the data to encrypt (base64-encoded in JSON wire format).
	Plaintext []byte `json:"Plaintext"`
}

// EncryptOutput is the response payload for Encrypt.
type EncryptOutput struct {
	KeyID          string `json:"KeyId"`
	CiphertextBlob []byte `json:"CiphertextBlob"`
}

// DecryptInput is the request payload for Decrypt.
type DecryptInput struct {
	KeyID          string `json:"KeyId,omitempty"`
	CiphertextBlob []byte `json:"CiphertextBlob"`
}

// DecryptOutput is the response payload for Decrypt.
type DecryptOutput struct {
	KeyID     string `json:"KeyId"`
	Plaintext []byte `json:"Plaintext"`
}

// GenerateDataKeyInput is the request payload for GenerateDataKey.
type GenerateDataKeyInput struct {
	NumberOfBytes *int32 `json:"NumberOfBytes,omitempty"`
	KeyID         string `json:"KeyId"`
	KeySpec       string `json:"KeySpec,omitempty"`
}

// GenerateDataKeyOutput is the response payload for GenerateDataKey.
type GenerateDataKeyOutput struct {
	KeyID          string `json:"KeyId"`
	CiphertextBlob []byte `json:"CiphertextBlob"`
	Plaintext      []byte `json:"Plaintext"`
}

// ReEncryptInput is the request payload for ReEncrypt.
type ReEncryptInput struct {
	DestinationKeyID string `json:"DestinationKeyId"`
	SourceKeyID      string `json:"SourceKeyId,omitempty"`
	CiphertextBlob   []byte `json:"CiphertextBlob"`
}

// ReEncryptOutput is the response payload for ReEncrypt.
type ReEncryptOutput struct {
	KeyID          string `json:"KeyId"`
	SourceKeyID    string `json:"SourceKeyId"`
	CiphertextBlob []byte `json:"CiphertextBlob"`
}

// CreateAliasInput is the request payload for CreateAlias.
type CreateAliasInput struct {
	// AliasName is the name of the alias (must begin with alias/).
	AliasName string `json:"AliasName"`
	// TargetKeyId is the key ID the alias should point to.
	TargetKeyID string `json:"TargetKeyId"`
}

// DeleteAliasInput is the request payload for DeleteAlias.
type DeleteAliasInput struct {
	// AliasName is the name of the alias to delete.
	AliasName string `json:"AliasName"`
}

// ListAliasesInput is the request payload for ListAliases.
type ListAliasesInput struct {
	// KeyId optionally filters aliases to those pointing to this key.
	KeyID string `json:"KeyId,omitempty"`
	// Limit caps the number of results returned.
	Limit *int32 `json:"Limit,omitempty"`
	// Marker is the pagination cursor from a previous call.
	Marker string `json:"Marker,omitempty"`
}

// ListAliasesOutput is the response payload for ListAliases.
type ListAliasesOutput struct {
	NextMarker string  `json:"NextMarker,omitempty"`
	Aliases    []Alias `json:"Aliases"`
	Truncated  bool    `json:"Truncated"`
}

// EnableKeyRotationInput is the request payload for EnableKeyRotation.
type EnableKeyRotationInput struct {
	// KeyId is the key to enable rotation for.
	KeyID string `json:"KeyId"`
}

// DisableKeyRotationInput is the request payload for DisableKeyRotation.
type DisableKeyRotationInput struct {
	// KeyId is the key to disable rotation for.
	KeyID string `json:"KeyId"`
}

// GetKeyRotationStatusInput is the request payload for GetKeyRotationStatus.
type GetKeyRotationStatusInput struct {
	// KeyId is the key to query rotation status for.
	KeyID string `json:"KeyId"`
}

// GetKeyRotationStatusOutput is the response payload for GetKeyRotationStatus.
type GetKeyRotationStatusOutput struct {
	KeyID              string `json:"KeyId"`
	KeyRotationEnabled bool   `json:"KeyRotationEnabled"`
}

// KeyStatePendingDeletion is the string constant for a key pending deletion.
const KeyStatePendingDeletion = "PendingDeletion"

// DisableKeyInput is the request payload for DisableKey.
type DisableKeyInput struct {
	KeyID string `json:"KeyId"`
}

// EnableKeyInput is the request payload for EnableKey.
type EnableKeyInput struct {
	KeyID string `json:"KeyId"`
}

// ScheduleKeyDeletionInput is the request payload for ScheduleKeyDeletion.
type ScheduleKeyDeletionInput struct {
	KeyID               string `json:"KeyId"`
	PendingWindowInDays int    `json:"PendingWindowInDays,omitempty"`
}

// ScheduleKeyDeletionOutput is the response payload for ScheduleKeyDeletion.
type ScheduleKeyDeletionOutput struct {
	KeyID        string  `json:"KeyId"`
	KeyState     string  `json:"KeyState"`
	DeletionDate float64 `json:"DeletionDate"`
}

// CancelKeyDeletionInput is the request payload for CancelKeyDeletion.
type CancelKeyDeletionInput struct {
	KeyID string `json:"KeyId"`
}

// ErrorResponse is the KMS JSON error response format.
type ErrorResponse struct {
	// Type is the error type string.
	Type string `json:"__type"`
	// Message is the human-readable error message.
	Message string `json:"message"`
}

// UnixTimeFloat converts a time value to a Unix timestamp float.
func UnixTimeFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / nanoToSeconds
}
