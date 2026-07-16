package kms

import (
	"errors"
)

var (
	// ErrKeyNotFound is returned when the specified key does not exist.
	ErrKeyNotFound = errors.New("NotFoundException")

	// ErrMalformedPolicyDocument is returned when the provided policy is invalid.
	ErrMalformedPolicyDocument = errors.New("MalformedPolicyDocumentException")
	// ErrAliasNotFound is returned when the specified alias does not exist.
	ErrAliasNotFound = errors.New("NotFoundException")
	// ErrAliasAlreadyExists is returned when an alias with the given name already exists.
	ErrAliasAlreadyExists = errors.New("AlreadyExistsException")
	// ErrCustomKeyStoreAlreadyExists is returned when a custom key store with the given name already exists.
	ErrCustomKeyStoreAlreadyExists = errors.New("CustomKeyStoreNameInUseException")
	// ErrCustomKeyStoreNotFound is returned when a custom key store ID does not exist.
	ErrCustomKeyStoreNotFound = errors.New("CustomKeyStoreNotFoundException")
	// ErrKeyDisabled is returned when an operation is attempted on a disabled key.
	ErrKeyDisabled = errors.New("DisabledException")
	// ErrKeyInvalidState is returned when a key is in a state that does not allow the requested
	// operation (e.g. PendingDeletion).
	ErrKeyInvalidState = errors.New("KMSInvalidStateException")
	// ErrInvalidKeyUsage is returned when the key is used for an operation incompatible with its
	// KeyUsage (e.g. encrypting with a SIGN_VERIFY key).
	ErrInvalidKeyUsage = errors.New("InvalidKeyUsageException")
	// ErrInvalidCiphertext is returned when the ciphertext cannot be decrypted.
	ErrInvalidCiphertext = errors.New("InvalidCiphertextException")
	// ErrIncorrectKey is returned when the KMS key identified by a caller-supplied KeyId
	// (Decrypt) or SourceKeyId (ReEncrypt) is not the key that encrypted the ciphertext.
	ErrIncorrectKey = errors.New("IncorrectKeyException")
	// ErrGrantNotFound is returned when the specified grant does not exist.
	ErrGrantNotFound = errors.New("NotFoundException: grant not found")
	// ErrCiphertextTooShort is returned when the ciphertext is too short.
	ErrCiphertextTooShort = errors.New("ciphertext too short")
	// ErrInvalidDataKeySize is returned when a data key size is invalid or too large.
	ErrInvalidDataKeySize = errors.New("ValidationException: invalid data key size")
	// ErrInvalidSignature is returned when a signature verification fails.
	ErrInvalidSignature = errors.New("KMSInvalidSignatureException")
	// ErrKeyMaterialUnavailable is returned when key material is missing (e.g. restored from
	// an older snapshot that predates key material persistence).
	ErrKeyMaterialUnavailable = errors.New("key material unavailable for this key")
	// ErrUnsupportedOrigin is returned when an operation is incompatible with the key's origin.
	ErrUnsupportedOrigin = errors.New("UnsupportedOperationException")
	// ErrValidation is returned for invalid request parameters (maps to ValidationException).
	ErrValidation = errors.New("ValidationException")
	// ErrExpiredKeyMaterial is returned when a key's imported material has passed its ValidTo date.
	ErrExpiredKeyMaterial = errors.New("ExpiredImportTokenException")
	// ErrInvalidGrantToken is returned when a grant token is expired or malformed.
	ErrInvalidGrantToken = errors.New("InvalidGrantTokenException")
	// ErrLimitExceeded is returned when a service limit is exceeded (e.g. grants per key).
	ErrLimitExceeded = errors.New("LimitExceededException")
	// ErrInvalidAlgorithm is returned when an algorithm is not valid for the key spec.
	ErrInvalidAlgorithm = errors.New("InvalidAlgorithmException")
)
