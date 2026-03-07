package kms

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var (
	// ErrKeyNotFound is returned when the specified key does not exist.
	ErrKeyNotFound = errors.New("NotFoundException")
	// ErrAliasNotFound is returned when the specified alias does not exist.
	ErrAliasNotFound = errors.New("NotFoundException")
	// ErrAliasAlreadyExists is returned when an alias with the given name already exists.
	ErrAliasAlreadyExists = errors.New("AlreadyExistsException")
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
)

const (
	// keySpecRSA3072 is the key spec for RSA-3072 asymmetric keys.
	keySpecRSA3072 = "RSA_3072"
	// keySpecRSA4096 is the key spec for RSA-4096 asymmetric keys.
	keySpecRSA4096 = "RSA_4096"
	// keySpecECCP256 is the key spec for ECC NIST P-256 asymmetric keys.
	keySpecECCP256 = "ECC_NIST_P256"
	// keySpecECCP384 is the key spec for ECC NIST P-384 asymmetric keys.
	keySpecECCP384 = "ECC_NIST_P384"
	// keySpecECCP521 is the key spec for ECC NIST P-521 asymmetric keys.
	keySpecECCP521 = "ECC_NIST_P521"
	// messageTypeRaw is the message type for raw (un-hashed) messages.
	messageTypeRaw = "RAW"
	// maxDataKeyBytes limits the maximum size of a generated data key when NumberOfBytes is specified.
	maxDataKeyBytes = 4096
)

const (
	// keyIDPrefixLen is the length of the key ID prefix embedded in ciphertext blobs.
	keyIDPrefixLen = 36
	// defaultListLimit is the default maximum number of results for list operations.
	defaultListLimit = 100
	// aes256Bytes is the size of an AES-256 data key in bytes.
	aes256Bytes = 32
	// aes128Bytes is the size of an AES-128 data key in bytes.
	aes128Bytes = 16
)

// StorageBackend defines the interface for the KMS in-memory backend.
type StorageBackend interface {
	CreateKey(input *CreateKeyInput) (*CreateKeyOutput, error)
	DescribeKey(input *DescribeKeyInput) (*DescribeKeyOutput, error)
	ListKeys(input *ListKeysInput) (*ListKeysOutput, error)
	Encrypt(input *EncryptInput) (*EncryptOutput, error)
	Decrypt(input *DecryptInput) (*DecryptOutput, error)
	GenerateDataKey(input *GenerateDataKeyInput) (*GenerateDataKeyOutput, error)
	GenerateDataKeyWithoutPlaintext(
		input *GenerateDataKeyWithoutPlaintextInput,
	) (*GenerateDataKeyWithoutPlaintextOutput, error)
	ReEncrypt(input *ReEncryptInput) (*ReEncryptOutput, error)
	Sign(input *SignInput) (*SignOutput, error)
	Verify(input *VerifyInput) (*VerifyOutput, error)
	GetPublicKey(input *GetPublicKeyInput) (*GetPublicKeyOutput, error)
	CreateAlias(input *CreateAliasInput) error
	DeleteAlias(input *DeleteAliasInput) error
	ListAliases(input *ListAliasesInput) (*ListAliasesOutput, error)
	EnableKeyRotation(input *EnableKeyRotationInput) error
	DisableKeyRotation(input *DisableKeyRotationInput) error
	GetKeyRotationStatus(input *GetKeyRotationStatusInput) (*GetKeyRotationStatusOutput, error)
	DisableKey(input *DisableKeyInput) error
	EnableKey(input *EnableKeyInput) error
	ScheduleKeyDeletion(input *ScheduleKeyDeletionInput) (*ScheduleKeyDeletionOutput, error)
	CancelKeyDeletion(input *CancelKeyDeletionInput) error
	CreateGrant(input *CreateGrantInput) (*CreateGrantOutput, error)
	ListGrants(input *ListGrantsInput) (*ListGrantsOutput, error)
	RevokeGrant(input *RevokeGrantInput) error
	RetireGrant(input *RetireGrantInput) error
	ListRetirableGrants(input *ListRetirableGrantsInput) (*ListGrantsOutput, error)
	PutKeyPolicy(input *PutKeyPolicyInput) error
	GetKeyPolicy(input *GetKeyPolicyInput) (*GetKeyPolicyOutput, error)
}

// InMemoryBackend is a concurrency-safe in-memory KMS backend.
type InMemoryBackend struct {
	keys         map[string]*Key
	aliases      map[string]*Alias
	grants       map[string]*Grant
	policies     map[string]string
	keyMaterials map[string]*keyMaterial
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates and returns a new empty KMS backend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion)
}

// NewInMemoryBackendWithConfig creates a new KMS backend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		keys:         make(map[string]*Key),
		aliases:      make(map[string]*Alias),
		grants:       make(map[string]*Grant),
		policies:     make(map[string]string),
		keyMaterials: make(map[string]*keyMaterial),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("kms"),
	}
}

// resolveKeyID resolves an alias name or ARN to a plain key UUID.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) resolveKeyID(keyID string) (string, error) {
	if strings.HasPrefix(keyID, "alias/") {
		alias, ok := b.aliases[keyID]
		if !ok {
			return "", ErrAliasNotFound
		}

		return alias.TargetKeyID, nil
	}

	if strings.HasPrefix(keyID, "arn:") {
		parts := strings.Split(keyID, "/")

		return parts[len(parts)-1], nil
	}

	return keyID, nil
}

// encryptData encrypts plaintext using the per-key AES-256-GCM material, embedding the key ID.
// Kept as a compatibility shim; callers should use encryptSymmetric directly.
func encryptData(plaintext []byte, keyID string, km *keyMaterial) ([]byte, error) {
	return encryptSymmetric(plaintext, keyID, km)
}

// decryptData decrypts a ciphertext blob produced by encryptData.
// Returns (plaintext, resolvedKeyID, error).
func decryptData(blob []byte, km *keyMaterial) ([]byte, string, error) {
	return decryptSymmetric(blob, km)
}

// requireKeyMaterial returns the key material for keyID or an error if absent.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) requireKeyMaterial(keyID string) (*keyMaterial, error) {
	km, ok := b.keyMaterials[keyID]
	if !ok || km == nil {
		return nil, fmt.Errorf("%w: keyID %q", ErrKeyMaterialUnavailable, keyID)
	}

	return km, nil
}

// validateKeySpecUsage returns an error when keySpec and keyUsage are incompatible.
// Symmetric specs (SYMMETRIC_DEFAULT) are only valid for ENCRYPT_DECRYPT;
// asymmetric specs (RSA_*, ECC_*) are only valid for SIGN_VERIFY.
func validateKeySpecUsage(keySpec, keyUsage string) error {
	switch keySpec {
	case keySpecSymmetric:
		if keyUsage != "" && keyUsage != KeyUsageEncryptDecrypt {
			return fmt.Errorf(
				"%w: key spec %q is not compatible with key usage %q; symmetric keys require ENCRYPT_DECRYPT",
				ErrInvalidKeyUsage, keySpec, keyUsage,
			)
		}
	case keySpecRSA2048, keySpecRSA3072, keySpecRSA4096,
		keySpecECCP256, keySpecECCP384, keySpecECCP521:
		if keyUsage != "" && keyUsage != KeyUsageSignVerify {
			return fmt.Errorf(
				"%w: key spec %q is not compatible with key usage %q; asymmetric keys require SIGN_VERIFY",
				ErrInvalidKeyUsage, keySpec, keyUsage,
			)
		}
	}

	return nil
}

// CreateKey creates a new KMS key and stores it in the backend.
func (b *InMemoryBackend) CreateKey(input *CreateKeyInput) (*CreateKeyOutput, error) {
	b.mu.Lock("CreateKey")
	defer b.mu.Unlock()

	keyID := uuid.New().String()
	keyUsage := input.KeyUsage
	keySpec := input.KeySpec

	// Validate that KeySpec and KeyUsage are compatible when both are specified.
	if err := validateKeySpecUsage(keySpec, keyUsage); err != nil {
		return nil, err
	}

	// Derive keyUsage from keySpec when not explicitly specified.
	if keyUsage == "" {
		switch keySpec {
		case keySpecSymmetric, "":
			keyUsage = KeyUsageEncryptDecrypt
		default:
			keyUsage = KeyUsageSignVerify
		}
	}

	// Derive keySpec from keyUsage when not explicitly specified.
	if keySpec == "" {
		switch keyUsage {
		case KeyUsageEncryptDecrypt:
			keySpec = keySpecSymmetric
		case KeyUsageSignVerify:
			keySpec = keySpecRSA2048
		default:
			keySpec = keySpecSymmetric
		}
	}

	region := b.region
	if input.Region != "" {
		region = input.Region
	}

	keyARN := arn.Build("kms", region, b.accountID, "key/"+keyID)
	key := &Key{
		KeyID:        keyID,
		Arn:          keyARN,
		Description:  input.Description,
		KeyState:     KeyStateEnabled,
		KeyUsage:     keyUsage,
		KeySpec:      keySpec,
		CreationDate: UnixTimeFloat(time.Now()),
		Enabled:      true,
	}

	km, err := generateKeyMaterial(keySpec)
	if err != nil {
		return nil, fmt.Errorf("generating key material for spec %q: %w", keySpec, err)
	}

	b.keys[keyID] = key
	b.keyMaterials[keyID] = km

	return &CreateKeyOutput{
		KeyMetadata: keyToMetadata(key),
	}, nil
}

// DescribeKey returns metadata for the specified key.
func (b *InMemoryBackend) DescribeKey(input *DescribeKeyInput) (*DescribeKeyOutput, error) {
	b.mu.RLock("DescribeKey")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	return &DescribeKeyOutput{
		KeyMetadata: keyToMetadata(key),
	}, nil
}

// ListKeys returns a paginated list of all keys.
func (b *InMemoryBackend) ListKeys(input *ListKeysInput) (*ListKeysOutput, error) {
	b.mu.RLock("ListKeys")
	defer b.mu.RUnlock()

	entries := make([]KeyListEntry, 0, len(b.keys))

	for _, k := range b.keys {
		entries = append(entries, KeyListEntry{KeyID: k.KeyID, KeyArn: k.Arn, Description: k.Description})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].KeyID < entries[j].KeyID
	})

	startIdx := parseMarker(input.Marker)
	limit := int32(defaultListLimit)

	if input.Limit != nil && *input.Limit > 0 {
		limit = *input.Limit
	}

	if startIdx >= len(entries) {
		return &ListKeysOutput{Keys: []KeyListEntry{}}, nil
	}

	end := startIdx + int(limit)

	var nextMarker string

	if end < len(entries) {
		nextMarker = strconv.Itoa(end)
	} else {
		end = len(entries)
	}

	return &ListKeysOutput{
		Keys:       entries[startIdx:end],
		NextMarker: nextMarker,
		Truncated:  nextMarker != "",
	}, nil
}

// Encrypt encrypts the given plaintext using the specified key.
func (b *InMemoryBackend) Encrypt(input *EncryptInput) (*EncryptOutput, error) {
	b.mu.RLock("Encrypt")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: key %q is not usable for encryption", ErrInvalidKeyUsage, key.KeyID)
	}

	km, err := b.requireKeyMaterial(key.KeyID)
	if err != nil {
		return nil, err
	}

	blob, encErr := encryptData(input.Plaintext, key.KeyID, km)
	if encErr != nil {
		return nil, encErr
	}

	return &EncryptOutput{
		CiphertextBlob: blob,
		KeyID:          key.Arn,
	}, nil
}

// Decrypt decrypts the given ciphertext blob.
func (b *InMemoryBackend) Decrypt(input *DecryptInput) (*DecryptOutput, error) {
	b.mu.RLock("Decrypt")
	defer b.mu.RUnlock()

	// Extract the key ID from the blob prefix first, then look up material.
	if len(input.CiphertextBlob) < keyIDPrefixLen {
		return nil, ErrCiphertextTooShort
	}

	keyID := strings.TrimRight(string(input.CiphertextBlob[:keyIDPrefixLen]), "\x00")

	key, lookupErr := b.lookupKey(keyID)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: key %q is not usable for decryption", ErrInvalidKeyUsage, key.KeyID)
	}

	km, err := b.requireKeyMaterial(key.KeyID)
	if err != nil {
		return nil, err
	}

	plaintext, _, err := decryptData(input.CiphertextBlob, km)
	if err != nil {
		return nil, err
	}

	return &DecryptOutput{
		Plaintext: plaintext,
		KeyID:     key.Arn,
	}, nil
}

// GenerateDataKey generates a random data key, returning both plaintext and encrypted forms.
func (b *InMemoryBackend) GenerateDataKey(input *GenerateDataKeyInput) (*GenerateDataKeyOutput, error) {
	b.mu.RLock("GenerateDataKey")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: key %q is not usable for data key generation", ErrInvalidKeyUsage, key.KeyID)
	}

	// Validate requested data key size to prevent excessive memory allocation.
	if input.NumberOfBytes != nil {
		if *input.NumberOfBytes <= 0 || *input.NumberOfBytes > maxDataKeyBytes {
			return nil, ErrInvalidDataKeySize
		}
	}

	keyBytes := min(dataKeySize(input.KeySpec, input.NumberOfBytes), maxDataKeyBytes)

	plaintextKey := make([]byte, keyBytes)
	if _, randErr := io.ReadFull(rand.Reader, plaintextKey); randErr != nil {
		return nil, randErr
	}

	km, err := b.requireKeyMaterial(key.KeyID)
	if err != nil {
		return nil, err
	}

	blob, encErr := encryptData(plaintextKey, key.KeyID, km)
	if encErr != nil {
		return nil, encErr
	}

	return &GenerateDataKeyOutput{
		CiphertextBlob: blob,
		Plaintext:      plaintextKey,
		KeyID:          key.Arn,
	}, nil
}

// ReEncrypt decrypts a ciphertext and re-encrypts it under a different key.
func (b *InMemoryBackend) ReEncrypt(input *ReEncryptInput) (*ReEncryptOutput, error) {
	b.mu.RLock("ReEncrypt")
	defer b.mu.RUnlock()

	// Extract source key ID from blob to look up key metadata and material.
	if len(input.CiphertextBlob) < keyIDPrefixLen {
		return nil, ErrCiphertextTooShort
	}

	sourceKeyID := strings.TrimRight(string(input.CiphertextBlob[:keyIDPrefixLen]), "\x00")

	// Validate source key state and usage before decrypting.
	sourceKey, sourceErr := b.lookupKey(sourceKeyID)
	if sourceErr != nil {
		return nil, sourceErr
	}

	if sourceKey.KeyState != KeyStateEnabled {
		return nil, keyStateError(sourceKey)
	}

	if sourceKey.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: source key %q is not usable for decryption", ErrInvalidKeyUsage, sourceKey.KeyID)
	}

	sourceKM, err := b.requireKeyMaterial(sourceKeyID)
	if err != nil {
		return nil, err
	}

	plaintext, _, err := decryptData(input.CiphertextBlob, sourceKM)
	if err != nil {
		return nil, err
	}

	destKey, lookupErr := b.lookupKey(input.DestinationKeyID)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if destKey.KeyState != KeyStateEnabled {
		return nil, keyStateError(destKey)
	}

	if destKey.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: destination key %q is not usable for encryption", ErrInvalidKeyUsage, destKey.KeyID)
	}

	destKM, err := b.requireKeyMaterial(destKey.KeyID)
	if err != nil {
		return nil, err
	}

	blob, encErr := encryptData(plaintext, destKey.KeyID, destKM)
	if encErr != nil {
		return nil, encErr
	}

	return &ReEncryptOutput{
		CiphertextBlob: blob,
		KeyID:          destKey.Arn,
		SourceKeyID:    sourceKey.Arn,
	}, nil
}

// Sign creates a digital signature for the specified message using an asymmetric KMS key.
func (b *InMemoryBackend) Sign(input *SignInput) (*SignOutput, error) {
	b.mu.RLock("Sign")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageSignVerify {
		return nil, fmt.Errorf("%w: key %q is not usable for signing", ErrInvalidKeyUsage, key.KeyID)
	}

	if algErr := validateSigningAlgorithm(input.SigningAlgorithm, key.KeySpec); algErr != nil {
		return nil, algErr
	}

	km, err := b.requireKeyMaterial(key.KeyID)
	if err != nil {
		return nil, err
	}

	messageType := input.MessageType
	if messageType == "" {
		messageType = messageTypeRaw
	}

	sig, signErr := signWithKeyMaterial(input.Message, messageType, input.SigningAlgorithm, km)
	if signErr != nil {
		return nil, signErr
	}

	return &SignOutput{
		KeyID:            key.Arn,
		Signature:        sig,
		SigningAlgorithm: input.SigningAlgorithm,
	}, nil
}

// Verify verifies a digital signature using an asymmetric KMS key.
func (b *InMemoryBackend) Verify(input *VerifyInput) (*VerifyOutput, error) {
	b.mu.RLock("Verify")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageSignVerify {
		return nil, fmt.Errorf("%w: key %q is not usable for verification", ErrInvalidKeyUsage, key.KeyID)
	}

	if algErr := validateSigningAlgorithm(input.SigningAlgorithm, key.KeySpec); algErr != nil {
		return nil, algErr
	}

	km, err := b.requireKeyMaterial(key.KeyID)
	if err != nil {
		return nil, err
	}

	messageType := input.MessageType
	if messageType == "" {
		messageType = messageTypeRaw
	}

	valid, verifyErr := verifyWithKeyMaterial(input.Message, input.Signature, messageType, input.SigningAlgorithm, km)
	if verifyErr != nil {
		return nil, verifyErr
	}

	return &VerifyOutput{
		KeyID:            key.Arn,
		SignatureValid:   valid,
		SigningAlgorithm: input.SigningAlgorithm,
	}, nil
}

// GetPublicKey returns the public key for an asymmetric KMS key.
func (b *InMemoryBackend) GetPublicKey(input *GetPublicKeyInput) (*GetPublicKeyOutput, error) {
	b.mu.RLock("GetPublicKey")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageSignVerify {
		return nil, fmt.Errorf("%w: key %q does not have an asymmetric public key", ErrInvalidKeyUsage, key.KeyID)
	}

	km, err := b.requireKeyMaterial(key.KeyID)
	if err != nil {
		return nil, err
	}

	der, pubErr := publicKeyDER(km)
	if pubErr != nil {
		return nil, pubErr
	}

	return &GetPublicKeyOutput{
		KeyID:             key.KeyID,
		PublicKey:         der,
		KeySpec:           key.KeySpec,
		KeyUsage:          key.KeyUsage,
		SigningAlgorithms: defaultSigningAlgorithms(key.KeySpec),
	}, nil
}

// CreateAlias creates an alias pointing to a key.
func (b *InMemoryBackend) CreateAlias(input *CreateAliasInput) error {
	b.mu.Lock("CreateAlias")
	defer b.mu.Unlock()

	if _, exists := b.aliases[input.AliasName]; exists {
		return ErrAliasAlreadyExists
	}

	targetID, err := b.resolveKeyID(input.TargetKeyID)
	if err != nil {
		return err
	}

	if _, exists := b.keys[targetID]; !exists {
		return ErrKeyNotFound
	}

	aliasArn := arn.Build("kms", b.region, b.accountID, input.AliasName)
	b.aliases[input.AliasName] = &Alias{
		AliasName:   input.AliasName,
		AliasArn:    aliasArn,
		TargetKeyID: targetID,
	}

	return nil
}

// DeleteAlias removes an alias.
func (b *InMemoryBackend) DeleteAlias(input *DeleteAliasInput) error {
	b.mu.Lock("DeleteAlias")
	defer b.mu.Unlock()

	if _, exists := b.aliases[input.AliasName]; !exists {
		return ErrAliasNotFound
	}

	delete(b.aliases, input.AliasName)

	return nil
}

// ListAliases returns a paginated list of aliases, optionally filtered by key.
func (b *InMemoryBackend) ListAliases(input *ListAliasesInput) (*ListAliasesOutput, error) {
	b.mu.RLock("ListAliases")
	defer b.mu.RUnlock()

	var resolvedKeyID string

	if input.KeyID != "" {
		var err error

		resolvedKeyID, err = b.resolveKeyID(input.KeyID)
		if err != nil {
			return nil, err
		}
	}

	aliases := make([]Alias, 0, len(b.aliases))

	for _, a := range b.aliases {
		if resolvedKeyID != "" && a.TargetKeyID != resolvedKeyID {
			continue
		}

		aliases = append(aliases, *a)
	}

	sort.Slice(aliases, func(i, j int) bool {
		return aliases[i].AliasName < aliases[j].AliasName
	})

	startIdx := parseMarker(input.Marker)
	limit := int32(defaultListLimit)

	if input.Limit != nil && *input.Limit > 0 {
		limit = *input.Limit
	}

	if startIdx >= len(aliases) {
		return &ListAliasesOutput{Aliases: []Alias{}}, nil
	}

	end := startIdx + int(limit)

	var nextMarker string

	if end < len(aliases) {
		nextMarker = strconv.Itoa(end)
	} else {
		end = len(aliases)
	}

	return &ListAliasesOutput{
		Aliases:    aliases[startIdx:end],
		NextMarker: nextMarker,
		Truncated:  nextMarker != "",
	}, nil
}

// EnableKeyRotation enables automatic key rotation for the specified key.
func (b *InMemoryBackend) EnableKeyRotation(input *EnableKeyRotationInput) error {
	b.mu.Lock("EnableKeyRotation")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(input.KeyID)
	if err != nil {
		return err
	}

	key.RotationEnabled = true

	return nil
}

// DisableKeyRotation disables automatic key rotation for the specified key.
func (b *InMemoryBackend) DisableKeyRotation(input *DisableKeyRotationInput) error {
	b.mu.Lock("DisableKeyRotation")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(input.KeyID)
	if err != nil {
		return err
	}

	key.RotationEnabled = false

	return nil
}

// GetKeyRotationStatus returns whether rotation is enabled for the specified key.
func (b *InMemoryBackend) GetKeyRotationStatus(input *GetKeyRotationStatusInput) (*GetKeyRotationStatusOutput, error) {
	b.mu.RLock("GetKeyRotationStatus")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	return &GetKeyRotationStatusOutput{
		KeyRotationEnabled: key.RotationEnabled,
		KeyID:              key.KeyID,
	}, nil
}

// DisableKey disables the specified key.
func (b *InMemoryBackend) DisableKey(input *DisableKeyInput) error {
	b.mu.Lock("DisableKey")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(input.KeyID)
	if err != nil {
		return err
	}

	key.KeyState = KeyStateDisabled
	key.Enabled = false

	return nil
}

// EnableKey enables the specified key.
func (b *InMemoryBackend) EnableKey(input *EnableKeyInput) error {
	b.mu.Lock("EnableKey")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(input.KeyID)
	if err != nil {
		return err
	}

	key.KeyState = KeyStateEnabled
	key.Enabled = true

	return nil
}

const defaultPendingWindowDays = 30

// ScheduleKeyDeletion schedules a key for deletion.
func (b *InMemoryBackend) ScheduleKeyDeletion(input *ScheduleKeyDeletionInput) (*ScheduleKeyDeletionOutput, error) {
	b.mu.Lock("ScheduleKeyDeletion")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(input.KeyID)
	if err != nil {
		return nil, err
	}

	days := input.PendingWindowInDays
	if days <= 0 {
		days = defaultPendingWindowDays
	}

	deletionDate := time.Now().UTC().AddDate(0, 0, days)
	key.KeyState = KeyStatePendingDeletion
	key.Enabled = false
	key.DeletionDate = UnixTimeFloat(deletionDate)

	return &ScheduleKeyDeletionOutput{
		KeyID:        key.KeyID,
		DeletionDate: key.DeletionDate,
		KeyState:     key.KeyState,
	}, nil
}

// CancelKeyDeletion cancels a pending key deletion and sets the key to Disabled.
func (b *InMemoryBackend) CancelKeyDeletion(input *CancelKeyDeletionInput) error {
	b.mu.Lock("CancelKeyDeletion")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(input.KeyID)
	if err != nil {
		return err
	}

	key.KeyState = KeyStateDisabled
	key.Enabled = false
	key.DeletionDate = 0

	return nil
}

// lookupKey finds a key by ID, alias, or ARN. Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupKey(keyID string) (*Key, error) {
	resolved, err := b.resolveKeyID(keyID)
	if err != nil {
		return nil, err
	}

	key, ok := b.keys[resolved]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return key, nil
}

// lookupKeyWrite finds a key by ID, alias, or ARN. Caller must hold a write lock.
func (b *InMemoryBackend) lookupKeyWrite(keyID string) (*Key, error) {
	return b.lookupKey(keyID)
}

// keyStateError returns the appropriate error for a key that is not in the Enabled state.
// Disabled keys return ErrKeyDisabled; keys in any other non-enabled state (e.g. PendingDeletion)
// return ErrKeyInvalidState, matching the KMSInvalidStateException that AWS raises.
func keyStateError(key *Key) error {
	if key.KeyState == KeyStateDisabled {
		return ErrKeyDisabled
	}

	return ErrKeyInvalidState
}

// keyToMetadata converts a Key to its KeyMetadata representation.
func keyToMetadata(k *Key) KeyMetadata {
	meta := KeyMetadata{
		KeyID:        k.KeyID,
		Arn:          k.Arn,
		Description:  k.Description,
		KeyState:     k.KeyState,
		KeyUsage:     k.KeyUsage,
		KeySpec:      k.KeySpec,
		CreationDate: k.CreationDate,
		KeyManager:   "CUSTOMER",
		Origin:       "AWS_KMS",
		MultiRegion:  false,
	}

	switch k.KeyUsage {
	case KeyUsageEncryptDecrypt:
		meta.EncryptionAlgorithms = []string{"SYMMETRIC_DEFAULT"}
	case KeyUsageSignVerify:
		meta.SigningAlgorithms = defaultSigningAlgorithms(k.KeySpec)
	}

	return meta
}

// dataKeySize returns the number of bytes for a data key based on spec and override.
func dataKeySize(keySpec string, numBytes *int32) int {
	if numBytes != nil && *numBytes > 0 {
		return int(*numBytes)
	}

	if keySpec == "AES_128" {
		return aes128Bytes
	}

	return aes256Bytes
}

// parseMarker converts a pagination marker string to an integer start index.
func parseMarker(marker string) int {
	if marker == "" {
		return 0
	}

	idx, err := strconv.Atoi(marker)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// CreateGrant creates a new grant on the specified key.
func (b *InMemoryBackend) CreateGrant(input *CreateGrantInput) (*CreateGrantOutput, error) {
	b.mu.Lock("CreateGrant")
	defer b.mu.Unlock()

	keyID, err := b.resolveKeyID(input.KeyID)
	if err != nil {
		return nil, err
	}

	if _, ok := b.keys[keyID]; !ok {
		return nil, ErrKeyNotFound
	}

	grantID := uuid.New().String()
	grantToken := uuid.New().String()
	grant := &Grant{
		GrantID:          grantID,
		KeyID:            keyID,
		GranteePrincipal: input.GranteePrincipal,
		Operations:       input.Operations,
		Name:             input.Name,
		GrantToken:       grantToken,
		CreationDate:     UnixTimeFloat(time.Now()),
	}
	b.grants[grantID] = grant

	return &CreateGrantOutput{GrantID: grantID, GrantToken: grantToken}, nil
}

// ListGrants returns the grants for a specified key.
func (b *InMemoryBackend) ListGrants(input *ListGrantsInput) (*ListGrantsOutput, error) {
	b.mu.RLock("ListGrants")
	defer b.mu.RUnlock()

	keyID, err := b.resolveKeyID(input.KeyID)
	if err != nil {
		return nil, err
	}

	if _, ok := b.keys[keyID]; !ok {
		return nil, ErrKeyNotFound
	}

	var grants []Grant
	for _, g := range b.grants {
		if g.KeyID == keyID {
			grants = append(grants, *g)
		}
	}

	sort.Slice(grants, func(i, j int) bool { return grants[i].GrantID < grants[j].GrantID })

	return &ListGrantsOutput{Grants: grants}, nil
}

// RevokeGrant revokes a grant by ID.
func (b *InMemoryBackend) RevokeGrant(input *RevokeGrantInput) error {
	b.mu.Lock("RevokeGrant")
	defer b.mu.Unlock()

	keyID, err := b.resolveKeyID(input.KeyID)
	if err != nil {
		return err
	}

	if _, ok := b.keys[keyID]; !ok {
		return ErrKeyNotFound
	}

	grant, ok := b.grants[input.GrantID]
	if !ok || grant.KeyID != keyID {
		return ErrGrantNotFound
	}

	delete(b.grants, input.GrantID)

	return nil
}

// RetireGrant retires a grant by grant token or grant ID + key ID.
func (b *InMemoryBackend) RetireGrant(input *RetireGrantInput) error {
	b.mu.Lock("RetireGrant")
	defer b.mu.Unlock()

	if input.GrantToken != "" {
		for grantID, g := range b.grants {
			if g.GrantToken == input.GrantToken {
				delete(b.grants, grantID)

				return nil
			}
		}

		return ErrGrantNotFound
	}

	if input.GrantID == "" {
		return ErrGrantNotFound
	}

	grant, ok := b.grants[input.GrantID]
	if !ok {
		return ErrGrantNotFound
	}

	if input.KeyID != "" {
		keyID, err := b.resolveKeyID(input.KeyID)
		if err != nil {
			return err
		}

		if grant.KeyID != keyID {
			return ErrGrantNotFound
		}
	}

	delete(b.grants, input.GrantID)

	return nil
}

// ListRetirableGrants returns all grants for which the given principal is the retiring principal.
func (b *InMemoryBackend) ListRetirableGrants(_ *ListRetirableGrantsInput) (*ListGrantsOutput, error) {
	b.mu.RLock("ListRetirableGrants")
	defer b.mu.RUnlock()

	grants := make([]Grant, 0, len(b.grants))
	for _, g := range b.grants {
		grants = append(grants, *g)
	}

	sort.Slice(grants, func(i, j int) bool { return grants[i].GrantID < grants[j].GrantID })

	return &ListGrantsOutput{Grants: grants}, nil
}

// GenerateDataKeyWithoutPlaintext generates a data key but returns only the encrypted copy.
func (b *InMemoryBackend) GenerateDataKeyWithoutPlaintext(
	input *GenerateDataKeyWithoutPlaintextInput,
) (*GenerateDataKeyWithoutPlaintextOutput, error) {
	out, err := b.GenerateDataKey(&GenerateDataKeyInput{
		KeyID:         input.KeyID,
		KeySpec:       input.KeySpec,
		NumberOfBytes: input.NumberOfBytes,
	})
	if err != nil {
		return nil, err
	}

	return &GenerateDataKeyWithoutPlaintextOutput{
		KeyID:          out.KeyID,
		CiphertextBlob: out.CiphertextBlob,
	}, nil
}

// PutKeyPolicy stores a key policy for a KMS key.
func (b *InMemoryBackend) PutKeyPolicy(input *PutKeyPolicyInput) error {
	b.mu.Lock("PutKeyPolicy")
	defer b.mu.Unlock()

	keyID, err := b.resolveKeyID(input.KeyID)
	if err != nil {
		return err
	}

	if _, ok := b.keys[keyID]; !ok {
		return ErrKeyNotFound
	}

	b.policies[keyID] = input.Policy

	return nil
}

// GetKeyPolicy retrieves the key policy for a KMS key.
func (b *InMemoryBackend) GetKeyPolicy(input *GetKeyPolicyInput) (*GetKeyPolicyOutput, error) {
	b.mu.RLock("GetKeyPolicy")
	defer b.mu.RUnlock()

	keyID, err := b.resolveKeyID(input.KeyID)
	if err != nil {
		return nil, err
	}

	if _, ok := b.keys[keyID]; !ok {
		return nil, ErrKeyNotFound
	}

	policy, ok := b.policies[keyID]
	if !ok {
		// Return default policy
		rootARN := arn.Build("iam", "", b.accountID, "root")
		policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":"` + rootARN + `"},"Action":"kms:*","Resource":"*"}]}`
	}

	policyName := input.PolicyName
	if policyName == "" {
		policyName = "default"
	}

	return &GetKeyPolicyOutput{Policy: policy, PolicyName: policyName}, nil
}
