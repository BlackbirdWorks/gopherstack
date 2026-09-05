package iam

import "errors"

var (
	// ErrUserNotFound is returned when a requested user does not exist.
	ErrUserNotFound = errors.New("NoSuchEntity: user")
	// ErrUserAlreadyExists is returned when creating a user that already exists.
	ErrUserAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrRoleNotFound is returned when a requested role does not exist.
	ErrRoleNotFound = errors.New("NoSuchEntity: role")
	// ErrRoleAlreadyExists is returned when creating a role that already exists.
	ErrRoleAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrPolicyNotFound is returned when a requested policy does not exist.
	ErrPolicyNotFound = errors.New("NoSuchEntity: policy")
	// ErrPolicyAlreadyExists is returned when creating a policy that already exists.
	ErrPolicyAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrGroupNotFound is returned when a requested group does not exist.
	ErrGroupNotFound = errors.New("NoSuchEntity: group")
	// ErrGroupAlreadyExists is returned when creating a group that already exists.
	ErrGroupAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrAccessKeyNotFound is returned when a requested access key does not exist.
	ErrAccessKeyNotFound = errors.New("NoSuchEntity: access key")
	// ErrInstanceProfileNotFound is returned when a requested instance profile does not exist.
	ErrInstanceProfileNotFound = errors.New("NoSuchEntity: instance profile")
	// ErrInstanceProfileAlreadyExists is returned when creating a profile that already exists.
	ErrInstanceProfileAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrInvalidAction is returned when an unknown IAM action is requested.
	ErrInvalidAction = errors.New("InvalidAction")
	// ErrMalformedPolicyDocument is returned when a policy document is not valid JSON.
	ErrMalformedPolicyDocument = errors.New("MalformedPolicyDocument")
	// ErrDeleteConflict is returned when an entity has attached resources that prevent deletion.
	ErrDeleteConflict = errors.New("DeleteConflict")
	// ErrInlinePolicyNotFound is returned when a requested inline policy does not exist.
	ErrInlinePolicyNotFound = errors.New("NoSuchEntity: inline policy")
	// ErrSAMLProviderNotFound is returned when a requested SAML provider does not exist.
	ErrSAMLProviderNotFound = errors.New("NoSuchEntity: SAML provider")
	// ErrSAMLProviderAlreadyExists is returned when creating a SAML provider that already exists.
	ErrSAMLProviderAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrOIDCProviderNotFound is returned when a requested OIDC provider does not exist.
	ErrOIDCProviderNotFound = errors.New("NoSuchEntity: OIDC provider")
	// ErrOIDCProviderAlreadyExists is returned when creating an OIDC provider that already exists.
	ErrOIDCProviderAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrInvalidAuthenticationCode is returned when MFA code is invalid.
	ErrInvalidAuthenticationCode = errors.New("InvalidAuthenticationCode")
	// ErrInvalidInput is returned when an input parameter is invalid.
	ErrInvalidInput = errors.New("InvalidInput")
	// ErrLoginProfileNotFound is returned when a requested login profile does not exist.
	ErrLoginProfileNotFound = errors.New("NoSuchEntity: login profile")
	// ErrLoginProfileAlreadyExists is returned when creating a login profile that already exists.
	ErrLoginProfileAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrInvalidOIDCProviderURL is returned when an OIDC provider URL cannot be parsed.
	ErrInvalidOIDCProviderURL = errors.New("InvalidInput")
	// ErrInvalidPassword is returned when a password fails validation (e.g., empty).
	ErrInvalidPassword = errors.New("InvalidInput")
	// ErrLimitExceeded is returned when an inline policy or other entity exceeds an AWS quota.
	ErrLimitExceeded = errors.New("LimitExceeded")
	// ErrValidationError is returned when a parameter fails AWS constraint validation (e.g. MaxSessionDuration bounds).
	ErrValidationError = errors.New("ValidationError")
	// ErrOldPasswordIncorrect is returned when ChangePassword's required OldPassword
	// is missing or does not match the account's current password.
	ErrOldPasswordIncorrect = errors.New("PasswordPolicyViolation")
	// ErrMalformedCertificate is returned when certificate/key material is not
	// well-formed PEM (UploadServerCertificate's own declared error set).
	ErrMalformedCertificate = errors.New("MalformedCertificate")
	// ErrUnrecognizedPublicKeyEncoding is returned when GetSSHPublicKey's Encoding
	// is not SSH/PEM, or the stored key can't be converted to the requested one.
	ErrUnrecognizedPublicKeyEncoding = errors.New("UnrecognizedPublicKeyEncoding")
	// ErrDelegationRequestNotFound is returned when a requested delegation request does not exist.
	ErrDelegationRequestNotFound = errors.New("NoSuchEntity: delegation request")
	// ErrAccountAliasNotFound is returned when DeleteAccountAlias is called with
	// an alias that isn't the account's current one. DeleteAccountAlias's own
	// deserializeOpError switch (iam@v1.58.1) models NoSuchEntity, not InvalidAction.
	ErrAccountAliasNotFound = errors.New("NoSuchEntity: account alias")
	// ErrMFADeviceNotFound is returned when an MFA operation references a virtual
	// MFA device serial number that doesn't exist. EnableMFADevice and
	// DeactivateMFADevice's own deserializeOpError switches (iam@v1.58.1) both
	// model NoSuchEntity, not InvalidAction.
	ErrMFADeviceNotFound = errors.New("NoSuchEntity: virtual MFA device")
	// ErrMFADeviceAlreadyEnabled is returned when EnableMFADevice is called on a
	// device that is already enabled. EnableMFADevice's own deserializeOpError
	// switch (iam@v1.58.1) models EntityAlreadyExists, not InvalidAction.
	ErrMFADeviceAlreadyEnabled = errors.New("EntityAlreadyExists: virtual MFA device already enabled")
)
