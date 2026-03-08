// Package secretsmanager provides a mock AWS Secrets Manager implementation.
package secretsmanager

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	// nanoToSeconds converts nanoseconds to seconds.
	nanoToSeconds = 1e9
	// MockAccountID is the mock AWS account ID.
	MockAccountID = config.DefaultAccountID
	// MockRegion is the mock AWS region.
	MockRegion = config.DefaultRegion
	// StagingLabelCurrent is the staging label for the current secret version.
	StagingLabelCurrent = "AWSCURRENT"
	// StagingLabelPrevious is the staging label for the previous secret version.
	StagingLabelPrevious = "AWSPREVIOUS"
)

// SecretVersion represents a single version of a secret.
type SecretVersion struct {
	// VersionId is the unique UUID of this version.
	VersionID string `json:"VersionId"`
	// SecretString is the secret value as a string (mutually exclusive with SecretBinary).
	SecretString string `json:"SecretString,omitempty"`
	// SecretBinary is the secret value as bytes (base64-encoded in JSON).
	SecretBinary []byte `json:"SecretBinary,omitempty"`
	// StagingLabels are the labels attached to this version.
	StagingLabels []string `json:"VersionStages,omitempty"`
	// CreatedDate is the Unix timestamp when this version was created.
	CreatedDate float64 `json:"CreatedDate"`
}

// Secret represents a stored secret including all versions.
type Secret struct {
	// ARN is the full ARN of the secret.
	ARN string `json:"ARN"`
	// Name is the human-readable name of the secret.
	Name string `json:"Name"`
	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`
	// Tags is a map of key/value tag pairs.
	Tags *tags.Tags `json:"Tags,omitempty"`
	// DeletedDate is set when the secret is deleted; nil means active.
	DeletedDate *float64 `json:"DeletedDate,omitempty"`
	// Versions holds all versions keyed by VersionId.
	Versions map[string]*SecretVersion `json:"-"`
	// CurrentVersionId is the VersionId with the AWSCURRENT label.
	CurrentVersionID string `json:"-"`
}

// CreateSecretInput is the request payload for CreateSecret.
type CreateSecretInput struct {
	Name         string `json:"Name"`
	Description  string `json:"Description,omitempty"`
	SecretString string `json:"SecretString,omitempty"`
	Region       string `json:"-"`
	SecretBinary []byte `json:"SecretBinary,omitempty"`
	Tags         []Tag  `json:"Tags,omitempty"`
}

// Tag represents a key/value tag pair in the Secrets Manager wire format.
type Tag struct {
	// Key is the tag key.
	Key string `json:"Key"`
	// Value is the tag value.
	Value string `json:"Value"`
}

// CreateSecretOutput is the response payload for CreateSecret.
type CreateSecretOutput struct {
	// ARN is the full ARN of the created secret.
	ARN string `json:"ARN"`
	// Name is the name of the created secret.
	Name string `json:"Name"`
	// VersionId is the initial version UUID.
	VersionID string `json:"VersionId,omitempty"`
}

// GetSecretValueInput is the request payload for GetSecretValue.
type GetSecretValueInput struct {
	// SecretId is the name or ARN of the secret.
	SecretID string `json:"SecretId"`
	// VersionId retrieves a specific version (default: AWSCURRENT).
	VersionID string `json:"VersionId,omitempty"`
	// VersionStage retrieves the version with this staging label (default: AWSCURRENT).
	VersionStage string `json:"VersionStage,omitempty"`
}

// GetSecretValueOutput is the response payload for GetSecretValue.
type GetSecretValueOutput struct {
	// ARN is the full ARN of the secret.
	ARN string `json:"ARN"`
	// Name is the name of the secret.
	Name string `json:"Name"`
	// VersionId is the UUID of the version returned.
	VersionID string `json:"VersionId"`
	// SecretString is the string value (when the secret stores a string).
	SecretString string `json:"SecretString,omitempty"`
	// SecretBinary is the binary value (when the secret stores binary data).
	SecretBinary []byte `json:"SecretBinary,omitempty"`
	// VersionStages are the staging labels attached to this version.
	VersionStages []string `json:"VersionStages,omitempty"`
	// CreatedDate is the Unix timestamp when this version was created.
	CreatedDate float64 `json:"CreatedDate"`
}

// PutSecretValueInput is the request payload for PutSecretValue.
type PutSecretValueInput struct {
	SecretID           string `json:"SecretId"`
	SecretString       string `json:"SecretString,omitempty"`
	ClientRequestToken string `json:"ClientRequestToken,omitempty"`
	SecretBinary       []byte `json:"SecretBinary,omitempty"`
}

// PutSecretValueOutput is the response payload for PutSecretValue.
type PutSecretValueOutput struct {
	// ARN is the full ARN of the secret.
	ARN string `json:"ARN"`
	// Name is the name of the secret.
	Name string `json:"Name"`
	// VersionId is the UUID of the new version.
	VersionID string `json:"VersionId"`
	// VersionStages are the staging labels attached to the new version.
	VersionStages []string `json:"VersionStages"`
}

// DeleteSecretInput is the request payload for DeleteSecret.
type DeleteSecretInput struct {
	// SecretId is the name or ARN of the secret to delete.
	SecretID string `json:"SecretId"`
	// ForceDeleteWithoutRecovery deletes immediately when true.
	ForceDeleteWithoutRecovery bool `json:"ForceDeleteWithoutRecovery,omitempty"`
}

// DeleteSecretOutput is the response payload for DeleteSecret.
type DeleteSecretOutput struct {
	// ARN is the full ARN of the deleted secret.
	ARN string `json:"ARN"`
	// Name is the name of the deleted secret.
	Name string `json:"Name"`
	// DeletionDate is the Unix timestamp when the secret was deleted.
	DeletionDate float64 `json:"DeletionDate"`
}

// SecretListEntry is a brief secret descriptor used in ListSecrets.
type SecretListEntry struct {
	DeletedDate *float64   `json:"DeletedDate,omitempty"`
	Tags        *tags.Tags `json:"Tags,omitempty"`
	ARN         string     `json:"ARN"`
	Name        string     `json:"Name"`
	Description string     `json:"Description,omitempty"`
}

// ListSecretsInput is the request payload for ListSecrets.
type ListSecretsInput struct {
	// MaxResults limits the number of results returned.
	MaxResults *int64 `json:"MaxResults,omitempty"`
	// NextToken is the pagination cursor from a previous call.
	NextToken string `json:"NextToken,omitempty"`
	// IncludeDeleted controls whether deleted secrets are included.
	IncludeDeleted bool `json:"IncludeDeleted,omitempty"`
}

// ListSecretsOutput is the response payload for ListSecrets.
type ListSecretsOutput struct {
	NextToken  string            `json:"NextToken,omitempty"`
	SecretList []SecretListEntry `json:"SecretList"`
}

// DescribeSecretInput is the request payload for DescribeSecret.
type DescribeSecretInput struct {
	// SecretId is the name or ARN of the secret.
	SecretID string `json:"SecretId"`
}

// DescribeSecretOutput is the response payload for DescribeSecret.
type DescribeSecretOutput struct {
	Tags               *tags.Tags          `json:"Tags,omitempty"`
	DeletedDate        *float64            `json:"DeletedDate,omitempty"`
	VersionIDsToStages map[string][]string `json:"VersionIdsToStages,omitempty"`
	ARN                string              `json:"ARN"`
	Name               string              `json:"Name"`
	Description        string              `json:"Description,omitempty"`
}

// UpdateSecretInput is the request payload for UpdateSecret.
type UpdateSecretInput struct {
	// SecretId is the name or ARN of the secret.
	SecretID string `json:"SecretId"`
	// Description is the new description (empty string clears it).
	Description string `json:"Description,omitempty"`
	// SecretString is a new string value, creating a new version.
	SecretString string `json:"SecretString,omitempty"`
	// SecretBinary is a new binary value, creating a new version.
	SecretBinary []byte `json:"SecretBinary,omitempty"`
}

// UpdateSecretOutput is the response payload for UpdateSecret.
type UpdateSecretOutput struct {
	// ARN is the full ARN of the updated secret.
	ARN string `json:"ARN"`
	// Name is the name of the updated secret.
	Name string `json:"Name"`
	// VersionId is the new version UUID when a value was also updated.
	VersionID string `json:"VersionId,omitempty"`
}

// RestoreSecretInput is the request payload for RestoreSecret.
type RestoreSecretInput struct {
	// SecretId is the name or ARN of the secret to restore.
	SecretID string `json:"SecretId"`
}

// RestoreSecretOutput is the response payload for RestoreSecret.
type RestoreSecretOutput struct {
	// ARN is the full ARN of the restored secret.
	ARN string `json:"ARN"`
	// Name is the name of the restored secret.
	Name string `json:"Name"`
}

// TagResourceInput is the request payload for TagResource.
type TagResourceInput struct {
	SecretID string `json:"SecretId"`
	Tags     []Tag  `json:"Tags"`
}

// UntagResourceInput is the request payload for UntagResource.
type UntagResourceInput struct {
	SecretID string   `json:"SecretId"`
	TagKeys  []string `json:"TagKeys"`
}

// RotateSecretInput is the request payload for RotateSecret.
type RotateSecretInput struct {
	SecretID           string `json:"SecretId"`
	RotationLambdaARN  string `json:"RotationLambdaARN,omitempty"`
	ClientRequestToken string `json:"ClientRequestToken,omitempty"`
}

// RotateSecretOutput is the response payload for RotateSecret.
type RotateSecretOutput struct {
	ARN       string `json:"ARN"`
	Name      string `json:"Name"`
	VersionID string `json:"VersionId,omitempty"`
}

// GetRandomPasswordInput is the request payload for GetRandomPassword.
type GetRandomPasswordInput struct {
	PasswordLength          *int64 `json:"PasswordLength,omitempty"`
	ExcludeCharacters       string `json:"ExcludeCharacters,omitempty"`
	ExcludeNumbers          bool   `json:"ExcludeNumbers,omitempty"`
	ExcludePunctuation      bool   `json:"ExcludePunctuation,omitempty"`
	ExcludeUppercase        bool   `json:"ExcludeUppercase,omitempty"`
	ExcludeLowercase        bool   `json:"ExcludeLowercase,omitempty"`
	IncludeSpace            bool   `json:"IncludeSpace,omitempty"`
	RequireEachIncludedType bool   `json:"RequireEachIncludedType,omitempty"`
}

// GetRandomPasswordOutput is the response payload for GetRandomPassword.
type GetRandomPasswordOutput struct {
	// RandomPassword is the generated password string.
	RandomPassword string `json:"RandomPassword"`
}

// ErrorResponse is the Secrets Manager JSON error response format.
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
