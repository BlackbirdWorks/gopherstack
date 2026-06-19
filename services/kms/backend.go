package kms

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	gopherarn "github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// regionContextKey is the context key type for the per-request AWS region.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion.
func getRegion(ctx context.Context, defaultRegion string) string {
	if v, ok := ctx.Value(regionContextKey{}).(string); ok && v != "" {
		return v
	}

	return defaultRegion
}

var (
	// ErrKeyNotFound is returned when the specified key does not exist.
	ErrKeyNotFound = errors.New("NotFoundException")
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

const (
	algoRSAESOAEPSHA1 = "RSAES_OAEP_SHA_1"
	algoECDH          = "ECDH"

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
	// AWS KMS enforces a maximum of 1024 bytes for GenerateDataKey.
	maxDataKeyBytes = 1024
	// getParametersImportPublicKeyBytes is the mock wrapping public key length for GetParametersForImport.
	getParametersImportPublicKeyBytes = 64
	// getParametersValidityWindow is the validity duration used by GetParametersForImport.
	getParametersValidityWindow = 24 * time.Hour
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
	// minPendingWindowDays is the minimum number of days allowed for key deletion pending window.
	minPendingWindowDays = 7
	// defaultPendingWindowDays is the default pending window when not specified.
	// AWS KMS defaults to 30 days, which is also the maximum.
	defaultPendingWindowDays = 30
	// maxPendingWindowDays is the maximum number of days allowed for key deletion pending window.
	// Per AWS docs, the range is [7, 30]. The default and maximum share the same value.
	maxPendingWindowDays = 30
	// defaultRotationPeriodDays is the default automatic rotation period when not specified.
	// AWS KMS defaults to 365 days.
	defaultRotationPeriodDays = 365
	// minRotationPeriodDays is the minimum rotation period AWS KMS allows.
	minRotationPeriodDays = 90
	// maxRotationPeriodDays is the maximum rotation period AWS KMS allows.
	maxRotationPeriodDays = 2560
	// maxPlaintextBytes is the maximum plaintext size for Encrypt (4096 bytes per AWS).
	maxPlaintextBytes = 4096
	// maxEncryptionContextBytes caps the encoded EncryptionContext size (4096 bytes per AWS).
	// Oversize contexts are rejected to mirror AWS ValidationException behavior.
	maxEncryptionContextBytes = 4096
	// maxKeyMaterialHistoryEntries caps how many rotated key materials are retained per key
	// to bound memory growth in long-running mock instances.
	maxKeyMaterialHistoryEntries = 100
	// maxSignMessageBytes is the maximum message size for Sign/Verify with RAW message type.
	maxSignMessageBytes = 4096
	// maxOnDemandRotationsPerDay is the maximum number of on-demand rotations allowed per key per rolling 24-hour window.
	maxOnDemandRotationsPerDay = 10
	// maxGrantNameLength is the maximum length of a grant name (AWS allows up to 256 characters).
	maxGrantNameLength = 256
	// maxTagKeyLength is the maximum length of a tag key per AWS KMS.
	maxTagKeyLength = 128
	// maxTagValueLength is the maximum length of a tag value per AWS KMS.
	maxTagValueLength = 256
	// expirationModelExpires means the imported key material expires at the ValidTo time.
	expirationModelExpires = "KEY_MATERIAL_EXPIRES"
	// expirationModelNoExpiry means the imported key material does not expire.
	expirationModelNoExpiry = "KEY_MATERIAL_DOES_NOT_EXPIRE"
	// defaultKeyPolicyName is the only policy name supported by AWS KMS.
	defaultKeyPolicyName = "default"
	// maxGrantsPerKey is the AWS KMS default service limit for grants per key.
	maxGrantsPerKey = 50000
	// grantTokenTTL is the lifetime of a grant token per AWS KMS (approximately 5 minutes).
	grantTokenTTL = 5 * time.Minute
)

// isValidAliasName reports whether an alias name uses only the characters allowed by AWS KMS.
// AWS permits letters, digits, colons, forward slashes, underscores, and hyphens.
// The caller is responsible for ensuring the name starts with "alias/" before calling this.
func isValidAliasName(name string) bool {
	for _, ch := range name {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') &&
			(ch < '0' || ch > '9') && ch != ':' && ch != '/' && ch != '_' && ch != '-' {
			return false
		}
	}

	return true
}

// isValidGrantOperation reports whether op is a grant operation permitted by AWS KMS.
func isValidGrantOperation(op string) bool {
	switch op {
	case "Decrypt", "Encrypt", "GenerateDataKey", "GenerateDataKeyWithoutPlaintext",
		"ReEncryptFrom", "ReEncryptTo", "Sign", "Verify", "GetPublicKey",
		"CreateGrant", "RetireGrant", "DescribeKey", "GenerateMac", "VerifyMac",
		"DeriveSharedSecret", "GenerateDataKeyPair", "GenerateDataKeyPairWithoutPlaintext":
		return true
	}

	return false
}

// StorageBackend defines the interface for the KMS in-memory backend.
type StorageBackend interface {
	CreateKey(ctx context.Context, input *CreateKeyInput) (*CreateKeyOutput, error)
	DescribeKey(ctx context.Context, input *DescribeKeyInput) (*DescribeKeyOutput, error)
	ListKeys(ctx context.Context, input *ListKeysInput) (*ListKeysOutput, error)
	Encrypt(ctx context.Context, input *EncryptInput) (*EncryptOutput, error)
	Decrypt(ctx context.Context, input *DecryptInput) (*DecryptOutput, error)
	GenerateDataKey(ctx context.Context, input *GenerateDataKeyInput) (*GenerateDataKeyOutput, error)
	GenerateDataKeyWithoutPlaintext(
		ctx context.Context, input *GenerateDataKeyWithoutPlaintextInput,
	) (*GenerateDataKeyWithoutPlaintextOutput, error)
	ReEncrypt(ctx context.Context, input *ReEncryptInput) (*ReEncryptOutput, error)
	Sign(ctx context.Context, input *SignInput) (*SignOutput, error)
	Verify(ctx context.Context, input *VerifyInput) (*VerifyOutput, error)
	GetPublicKey(ctx context.Context, input *GetPublicKeyInput) (*GetPublicKeyOutput, error)
	CreateAlias(ctx context.Context, input *CreateAliasInput) error
	UpdateAlias(ctx context.Context, input *UpdateAliasInput) error
	DeleteAlias(ctx context.Context, input *DeleteAliasInput) error
	ListAliases(ctx context.Context, input *ListAliasesInput) (*ListAliasesOutput, error)
	EnableKeyRotation(ctx context.Context, input *EnableKeyRotationInput) error
	DisableKeyRotation(ctx context.Context, input *DisableKeyRotationInput) error
	GetKeyRotationStatus(ctx context.Context, input *GetKeyRotationStatusInput) (*GetKeyRotationStatusOutput, error)
	DisableKey(ctx context.Context, input *DisableKeyInput) error
	EnableKey(ctx context.Context, input *EnableKeyInput) error
	ScheduleKeyDeletion(ctx context.Context, input *ScheduleKeyDeletionInput) (*ScheduleKeyDeletionOutput, error)
	CancelKeyDeletion(ctx context.Context, input *CancelKeyDeletionInput) (*CancelKeyDeletionOutput, error)
	CreateGrant(ctx context.Context, input *CreateGrantInput) (*CreateGrantOutput, error)
	ListGrants(ctx context.Context, input *ListGrantsInput) (*ListGrantsOutput, error)
	RevokeGrant(ctx context.Context, input *RevokeGrantInput) error
	RetireGrant(ctx context.Context, input *RetireGrantInput) error
	ListRetirableGrants(ctx context.Context, input *ListRetirableGrantsInput) (*ListGrantsOutput, error)
	PutKeyPolicy(ctx context.Context, input *PutKeyPolicyInput) error
	GetKeyPolicy(ctx context.Context, input *GetKeyPolicyInput) (*GetKeyPolicyOutput, error)
	GetParametersForImport(
		ctx context.Context,
		input *GetParametersForImportInput,
	) (*GetParametersForImportOutput, error)
	ListKeyPolicies(ctx context.Context, input *ListKeyPoliciesInput) (*ListKeyPoliciesOutput, error)
	ListKeyRotations(ctx context.Context, input *ListKeyRotationsInput) (*ListKeyRotationsOutput, error)
	ImportKeyMaterial(ctx context.Context, input *ImportKeyMaterialInput) error
	DeleteImportedKeyMaterial(ctx context.Context, input *DeleteImportedKeyMaterialInput) error
	ReplicateKey(ctx context.Context, input *ReplicateKeyInput) (*ReplicateKeyOutput, error)
	RotateKeyOnDemand(ctx context.Context, input *RotateKeyOnDemandInput) (*RotateKeyOnDemandOutput, error)
	ConnectCustomKeyStore(ctx context.Context, input *ConnectCustomKeyStoreInput) error
	CreateCustomKeyStore(ctx context.Context, input *CreateCustomKeyStoreInput) (*CreateCustomKeyStoreOutput, error)
	DeleteCustomKeyStore(ctx context.Context, input *DeleteCustomKeyStoreInput) error
	DeriveSharedSecret(ctx context.Context, input *DeriveSharedSecretInput) (*DeriveSharedSecretOutput, error)
	DescribeCustomKeyStores(
		ctx context.Context,
		input *DescribeCustomKeyStoresInput,
	) (*DescribeCustomKeyStoresOutput, error)
	DisconnectCustomKeyStore(ctx context.Context, input *DisconnectCustomKeyStoreInput) error
	UpdateCustomKeyStore(ctx context.Context, input *UpdateCustomKeyStoreInput) error
	UpdateKeyDescription(ctx context.Context, input *UpdateKeyDescriptionInput) error
	UpdatePrimaryRegion(ctx context.Context, input *UpdatePrimaryRegionInput) error
	GenerateDataKeyPair(ctx context.Context, input *GenerateDataKeyPairInput) (*GenerateDataKeyPairOutput, error)
	GenerateDataKeyPairWithoutPlaintext(
		ctx context.Context, input *GenerateDataKeyPairWithoutPlaintextInput,
	) (*GenerateDataKeyPairWithoutPlaintextOutput, error)
	GenerateMac(ctx context.Context, input *GenerateMacInput) (*GenerateMacOutput, error)
	GenerateRandom(ctx context.Context, input *GenerateRandomInput) (*GenerateRandomOutput, error)
	VerifyMac(ctx context.Context, input *VerifyMacInput) (*VerifyMacOutput, error)
}

// ensure InMemoryBackend satisfies StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)

// InMemoryBackend is a concurrency-safe in-memory KMS backend.
type InMemoryBackend struct {
	keys    map[string]map[string]*Key
	aliases map[string]map[string]*Alias
	grants  map[string]map[string]*Grant
	// grantsByToken indexes grants by their GrantToken for O(1) lookup on the
	// encrypt/decrypt grant-validation hot path. Kept consistent with grants on
	// every create/revoke/retire.
	grantsByToken map[string]map[string]*Grant
	// grantsByKey indexes grants by keyID for O(1) ListGrants and grant-count
	// checks on the CreateGrant hot path. Kept consistent with grants on every
	// create/revoke/retire.
	grantsByKey          map[string]map[string]map[string]*Grant
	policies             map[string]map[string]string
	keyMaterials         map[string]map[string]*keyMaterial
	keyMaterialHistory   map[string]map[string][]*keyMaterial
	customKeyStores      map[string]map[string]*CustomKeyStore
	mu                   *lockmetrics.RWMutex
	accountID            string
	defaultRegion        string
	keyIDResolutionCache sync.Map
}

// NewInMemoryBackend creates and returns a new empty KMS backend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion)
}

// NewInMemoryBackendWithConfig creates a new KMS backend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		keys:               make(map[string]map[string]*Key),
		aliases:            make(map[string]map[string]*Alias),
		grants:             make(map[string]map[string]*Grant),
		grantsByToken:      make(map[string]map[string]*Grant),
		grantsByKey:        make(map[string]map[string]map[string]*Grant),
		policies:           make(map[string]map[string]string),
		keyMaterials:       make(map[string]map[string]*keyMaterial),
		keyMaterialHistory: make(map[string]map[string][]*keyMaterial),
		customKeyStores:    make(map[string]map[string]*CustomKeyStore),
		accountID:          accountID,
		defaultRegion:      region,
		mu:                 lockmetrics.New("kms"),
	}
}

// keysStore returns (creating lazily) the per-region keys map.
func (b *InMemoryBackend) keysStore(region string) map[string]*Key {
	if m, ok := b.keys[region]; ok {
		return m
	}

	m := make(map[string]*Key)
	b.keys[region] = m

	return m
}

// aliasesStore returns (creating lazily) the per-region aliases map.
func (b *InMemoryBackend) aliasesStore(region string) map[string]*Alias {
	if m, ok := b.aliases[region]; ok {
		return m
	}

	m := make(map[string]*Alias)
	b.aliases[region] = m

	return m
}

// grantsStore returns (creating lazily) the per-region grants map.
func (b *InMemoryBackend) grantsStore(region string) map[string]*Grant {
	if m, ok := b.grants[region]; ok {
		return m
	}

	m := make(map[string]*Grant)
	b.grants[region] = m

	return m
}

// grantsByTokenStore returns (creating lazily) the per-region grantsByToken map.
func (b *InMemoryBackend) grantsByTokenStore(region string) map[string]*Grant {
	if m, ok := b.grantsByToken[region]; ok {
		return m
	}

	m := make(map[string]*Grant)
	b.grantsByToken[region] = m

	return m
}

// grantsByKeyStore returns (creating lazily) the per-key grants map for a region.
func (b *InMemoryBackend) grantsByKeyStore(region, keyID string) map[string]*Grant {
	if b.grantsByKey[region] == nil {
		b.grantsByKey[region] = make(map[string]map[string]*Grant)
	}

	if b.grantsByKey[region][keyID] == nil {
		b.grantsByKey[region][keyID] = make(map[string]*Grant)
	}

	return b.grantsByKey[region][keyID]
}

// policiesStore returns (creating lazily) the per-region policies map.
func (b *InMemoryBackend) policiesStore(region string) map[string]string {
	if m, ok := b.policies[region]; ok {
		return m
	}

	m := make(map[string]string)
	b.policies[region] = m

	return m
}

// keyMaterialsStore returns (creating lazily) the per-region keyMaterials map.
func (b *InMemoryBackend) keyMaterialsStore(region string) map[string]*keyMaterial {
	if m, ok := b.keyMaterials[region]; ok {
		return m
	}

	m := make(map[string]*keyMaterial)
	b.keyMaterials[region] = m

	return m
}

// keyMaterialHistoryStore returns (creating lazily) the per-region keyMaterialHistory map.
func (b *InMemoryBackend) keyMaterialHistoryStore(region string) map[string][]*keyMaterial {
	if m, ok := b.keyMaterialHistory[region]; ok {
		return m
	}

	m := make(map[string][]*keyMaterial)
	b.keyMaterialHistory[region] = m

	return m
}

// customKeyStoresStore returns (creating lazily) the per-region customKeyStores map.
func (b *InMemoryBackend) customKeyStoresStore(region string) map[string]*CustomKeyStore {
	if m, ok := b.customKeyStores[region]; ok {
		return m
	}

	m := make(map[string]*CustomKeyStore)
	b.customKeyStores[region] = m

	return m
}

// resolveKeyID resolves an alias name or ARN to a plain key UUID and region.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) resolveKeyID(
	ctx context.Context,
	keyID string,
) (string, string, error) {
	ctxRegion := getRegion(ctx, b.defaultRegion)

	if cached, ok := b.keyIDResolutionCache.Load(keyID); ok {
		resolved, resolvedOK := cached.(string)
		if !resolvedOK {
			return "", "", fmt.Errorf("%w: invalid key resolution cache entry", ErrValidation)
		}

		return resolved, ctxRegion, nil
	}

	if strings.HasPrefix(keyID, "alias/") {
		alias, ok := b.aliasesStore(ctxRegion)[keyID]
		if !ok {
			return "", "", ErrAliasNotFound
		}

		b.keyIDResolutionCache.Store(keyID, alias.TargetKeyID)

		return alias.TargetKeyID, ctxRegion, nil
	}

	if strings.HasPrefix(keyID, "arn:") {
		resolved, arnRegion, arnErr := b.resolveARNKeyID(keyID)
		if arnErr != nil {
			return "", "", arnErr
		}

		b.keyIDResolutionCache.Store(keyID, resolved)

		return resolved, arnRegion, nil
	}

	return keyID, ctxRegion, nil
}

func (b *InMemoryBackend) resolveARNKeyID(keyID string) (string, string, error) {
	parsed, parseErr := awsarn.Parse(keyID)
	if parseErr != nil {
		return "", "", fmt.Errorf("%w: invalid key ARN %q", ErrValidation, keyID)
	}

	if strings.HasPrefix(parsed.Resource, "alias/") {
		alias, ok := b.aliasesStore(parsed.Region)[parsed.Resource]
		if !ok {
			return "", "", ErrAliasNotFound
		}

		return alias.TargetKeyID, parsed.Region, nil
	}

	if after, ok := strings.CutPrefix(parsed.Resource, "key/"); ok {
		return after, parsed.Region, nil
	}

	return "", "", fmt.Errorf("%w: unsupported KMS ARN resource %q", ErrValidation, parsed.Resource)
}

func (b *InMemoryBackend) clearResolutionCache() {
	b.keyIDResolutionCache.Range(func(key, _ any) bool {
		b.keyIDResolutionCache.Delete(key)

		return true
	})
}

func (b *InMemoryBackend) keyRegion(keyARN string) string {
	parsed, err := awsarn.Parse(keyARN)
	if err != nil {
		return b.defaultRegion
	}

	return parsed.Region
}

// encryptData encrypts plaintext using the per-key AES-256-GCM material, embedding the key ID.
// Kept as a compatibility shim; callers should use encryptSymmetric directly.
func encryptData(plaintext []byte, keyID string, encCtx map[string]string, km *keyMaterial) ([]byte, error) {
	return encryptSymmetric(plaintext, keyID, encCtx, km)
}

// decryptData decrypts a ciphertext blob produced by encryptData.
// Returns (plaintext, resolvedKeyID, error).
func decryptData(blob []byte, encCtx map[string]string, km *keyMaterial) ([]byte, string, error) {
	return decryptSymmetric(blob, encCtx, km)
}

// checkKeyMaterialExpiry returns ErrExpiredKeyMaterial if the key's imported material
// has passed its ValidTo date. Must be called with at least a read lock held.
func (*InMemoryBackend) checkKeyMaterialExpiry(key *Key) error {
	if key.Origin != KeyOriginExternal {
		return nil
	}

	if key.ExpirationModel != expirationModelExpires || key.ValidTo == 0 {
		return nil
	}

	now := float64(time.Now().UnixNano()) / nanoToSeconds
	if now >= key.ValidTo {
		return fmt.Errorf("%w: key %q imported material has expired", ErrExpiredKeyMaterial, key.KeyID)
	}

	return nil
}

// requireKeyMaterial returns the key material for keyID in the given region or an error if absent.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) requireKeyMaterial(region, keyID string) (*keyMaterial, error) {
	km, ok := b.keyMaterialsStore(region)[keyID]
	if !ok || km == nil {
		return nil, fmt.Errorf("%w: keyID %q", ErrKeyMaterialUnavailable, keyID)
	}

	return km, nil
}

// validateKeySpecUsage returns an error when keySpec and keyUsage are incompatible.
// Symmetric specs (SYMMETRIC_DEFAULT) are only valid for ENCRYPT_DECRYPT;
// RSA specs (RSA_*) are valid for SIGN_VERIFY or ENCRYPT_DECRYPT (RSA-OAEP);
// ECC specs (ECC_*) are valid for SIGN_VERIFY or KEY_AGREEMENT;
// HMAC specs (HMAC_*) are only valid for GENERATE_VERIFY_MAC.
func validateKeySpecUsage(keySpec, keyUsage string) error {
	switch keySpec {
	case keySpecSymmetric:
		if keyUsage != "" && keyUsage != KeyUsageEncryptDecrypt {
			return fmt.Errorf(
				"%w: key spec %q is not compatible with key usage %q; symmetric keys require ENCRYPT_DECRYPT",
				ErrInvalidKeyUsage, keySpec, keyUsage,
			)
		}
	case keySpecRSA2048, keySpecRSA3072, keySpecRSA4096:
		if keyUsage != "" && keyUsage != KeyUsageSignVerify && keyUsage != KeyUsageEncryptDecrypt {
			return fmt.Errorf(
				"%w: key spec %q supports KeyUsage=%s or KeyUsage=%s only",
				ErrInvalidKeyUsage, keySpec, KeyUsageSignVerify, KeyUsageEncryptDecrypt,
			)
		}
	case keySpecECCP256, keySpecECCP384, keySpecECCP521:
		if keyUsage != "" && keyUsage != KeyUsageSignVerify && keyUsage != KeyUsageKeyAgreement {
			return fmt.Errorf(
				"%w: key spec %q is not compatible with key usage %q; ECC keys require SIGN_VERIFY or KEY_AGREEMENT",
				ErrInvalidKeyUsage, keySpec, keyUsage,
			)
		}
	case keySpecHMAC256, keySpecHMAC384, keySpecHMAC512:
		if keyUsage != "" && keyUsage != KeyUsageGenerateMac {
			return fmt.Errorf(
				"%w: key spec %q is not compatible with key usage %q; HMAC keys require GENERATE_VERIFY_MAC",
				ErrInvalidKeyUsage, keySpec, keyUsage,
			)
		}
	}

	return nil
}

// deriveKeySpecUsage fills in missing KeySpec and KeyUsage defaults, returning the resolved pair.
// If keyUsage is empty, it is inferred from keySpec; if keySpec is empty it is inferred from keyUsage.
func deriveKeySpecUsage(keySpec, keyUsage string) (string, string) {
	if keyUsage == "" {
		switch keySpec {
		case keySpecSymmetric, "":
			keyUsage = KeyUsageEncryptDecrypt
		case keySpecHMAC256, keySpecHMAC384, keySpecHMAC512:
			keyUsage = KeyUsageGenerateMac
		default:
			// RSA and ECC specs default to SIGN_VERIFY unless the caller specified otherwise.
			keyUsage = KeyUsageSignVerify
		}
	}

	if keySpec == "" {
		switch keyUsage {
		case KeyUsageEncryptDecrypt:
			keySpec = keySpecSymmetric
		case KeyUsageGenerateMac:
			keySpec = keySpecHMAC256
		case KeyUsageSignVerify:
			keySpec = keySpecRSA2048
		case KeyUsageKeyAgreement:
			keySpec = keySpecECCP256
		default:
			keySpec = keySpecSymmetric
		}
	}

	return keySpec, keyUsage
}

// CreateKey creates a new KMS key and stores it in the backend.
func (b *InMemoryBackend) CreateKey(ctx context.Context, input *CreateKeyInput) (*CreateKeyOutput, error) {
	if len(input.Description) > maxDescriptionLength {
		return nil, fmt.Errorf(
			"%w: Description exceeds maximum length of %d characters",
			ErrValidation, maxDescriptionLength,
		)
	}

	if len(input.Tags) > maxTagsPerKey {
		return nil, fmt.Errorf(
			"%w: number of tags (%d) exceeds the maximum of %d",
			ErrValidation, len(input.Tags), maxTagsPerKey,
		)
	}

	b.mu.Lock("CreateKey")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	if input.Region != "" {
		region = input.Region
	}

	keyID := uuid.New().String()
	keyUsage := input.KeyUsage
	keySpec := input.KeySpec

	// Validate that KeySpec and KeyUsage are compatible when both are specified.
	if err := validateKeySpecUsage(keySpec, keyUsage); err != nil {
		return nil, err
	}

	keySpec, keyUsage = deriveKeySpecUsage(keySpec, keyUsage)

	// HMAC keys do not support MultiRegion.
	if input.MultiRegion {
		switch keySpec {
		case keySpecHMAC256, keySpecHMAC384, keySpecHMAC512:
			return nil, fmt.Errorf(
				"%w: HMAC keys (spec %q) do not support MultiRegion",
				ErrInvalidKeyUsage, keySpec,
			)
		}
	}

	// Resolve origin: EXTERNAL keys require the caller to import key material later.
	origin := input.Origin
	if origin == "" {
		origin = KeyOriginAWSKMS
	}

	keyARN := gopherarn.Build("kms", region, b.accountID, "key/"+keyID)

	// External-origin keys start in PendingImport; no key material is generated.
	keyState := KeyStateEnabled
	if origin == KeyOriginExternal {
		keyState = KeyStatePendingImport
	}

	key := &Key{
		KeyID:         keyID,
		Arn:           keyARN,
		Description:   input.Description,
		KeyState:      keyState,
		KeyUsage:      keyUsage,
		KeySpec:       keySpec,
		Origin:        origin,
		PrimaryRegion: region,
		CreationDate:  UnixTimeFloat(time.Now()),
		Enabled:       keyState == KeyStateEnabled,
		MultiRegion:   input.MultiRegion,
	}

	if origin != KeyOriginExternal {
		km, err := generateKeyMaterial(keySpec)
		if err != nil {
			return nil, fmt.Errorf("generating key material for spec %q: %w", keySpec, err)
		}

		b.keyMaterialsStore(region)[keyID] = km
	}

	b.keysStore(region)[keyID] = key

	out := &CreateKeyOutput{
		KeyMetadata: keyToMetadata(key),
	}

	return out, nil
}

// DescribeKey returns metadata for the specified key.
func (b *InMemoryBackend) DescribeKey(ctx context.Context, input *DescribeKeyInput) (*DescribeKeyOutput, error) {
	b.mu.RLock("DescribeKey")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	meta := keyToMetadata(key)
	meta.MultiRegionConfiguration = b.buildMultiRegionConfig(ctx, key)

	return &DescribeKeyOutput{KeyMetadata: meta}, nil
}

// ListKeys returns a paginated list of all keys.
func (b *InMemoryBackend) ListKeys(ctx context.Context, input *ListKeysInput) (*ListKeysOutput, error) {
	b.mu.RLock("ListKeys")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	entries := make([]KeyListEntry, 0, len(b.keysStore(region)))

	for _, k := range b.keysStore(region) {
		entries = append(entries, KeyListEntry{KeyID: k.KeyID, KeyArn: k.Arn, Description: k.Description})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].KeyID < entries[j].KeyID
	})

	startIdx := parseMarker(input.Marker)
	limit := int32(defaultListLimit)

	if input.Limit != nil {
		if *input.Limit < 1 || *input.Limit > 1000 {
			return nil, fmt.Errorf("%w: Limit must be between 1 and 1000", ErrValidation)
		}

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

// encryptionAlgorithmForSpec returns the EncryptionAlgorithm string for a given key spec.
// Returns encryptionAlgorithmRSAOAEP for RSA keys and "SYMMETRIC_DEFAULT" for symmetric keys.
func encryptionAlgorithmForSpec(keySpec string) string {
	switch keySpec {
	case keySpecRSA2048, keySpecRSA3072, keySpecRSA4096:
		return encryptionAlgorithmRSAOAEP
	default:
		return encryptionAlgorithmSymmetric
	}
}

// Encrypt encrypts the given plaintext using the specified key.
func (b *InMemoryBackend) Encrypt(ctx context.Context, input *EncryptInput) (*EncryptOutput, error) {
	if len(input.Plaintext) > maxPlaintextBytes {
		return nil, fmt.Errorf(
			"%w: plaintext must not exceed %d bytes, got %d",
			ErrValidation, maxPlaintextBytes, len(input.Plaintext),
		)
	}

	if err := validateEncryptionContextSize(input.EncryptionContext); err != nil {
		return nil, err
	}

	b.mu.RLock("Encrypt")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: key %q is not usable for encryption", ErrInvalidKeyUsage, key.KeyID)
	}

	if err = b.checkKeyMaterialExpiry(key); err != nil {
		return nil, err
	}

	km, err := b.requireKeyMaterial(region, key.KeyID)
	if err != nil {
		return nil, err
	}

	if err = b.validateGrantTokenConstraints(ctx, input.GrantTokens, input.EncryptionContext); err != nil {
		return nil, err
	}

	blob, err := b.encryptPayload(input.Plaintext, key.KeyID, input.EncryptionContext, km)
	if err != nil {
		return nil, err
	}

	return &EncryptOutput{
		CiphertextBlob:      blob,
		KeyID:               key.Arn,
		EncryptionAlgorithm: encryptionAlgorithmForSpec(key.KeySpec),
	}, nil
}

// encryptPayload dispatches to RSA-OAEP or symmetric encryption depending on key type.
// Must be called with at least a read lock held.
func (*InMemoryBackend) encryptPayload(
	plaintext []byte,
	keyID string,
	encCtx map[string]string,
	km *keyMaterial,
) ([]byte, error) {
	if km.rsaKey != nil {
		// RSA ENCRYPT_DECRYPT keys use RSA-OAEP-SHA256.
		// Prepend the key ID prefix so Decrypt can identify the key.
		rsaBlob, encErr := encryptRSAOAEP(plaintext, km)
		if encErr != nil {
			return nil, encErr
		}

		full := make([]byte, keyIDPrefixLen+len(rsaBlob))
		copy(full[:keyIDPrefixLen], padKeyID(keyID))
		copy(full[keyIDPrefixLen:], rsaBlob)

		return full, nil
	}

	return encryptData(plaintext, keyID, encCtx, km)
}

// Decrypt decrypts the given ciphertext blob.
func (b *InMemoryBackend) Decrypt(ctx context.Context, input *DecryptInput) (*DecryptOutput, error) {
	if err := validateEncryptionContextSize(input.EncryptionContext); err != nil {
		return nil, err
	}

	b.mu.RLock("Decrypt")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	// Extract the key ID from the blob prefix first, then look up material.
	if len(input.CiphertextBlob) < keyIDPrefixLen {
		return nil, ErrCiphertextTooShort
	}

	keyID := strings.TrimRight(string(input.CiphertextBlob[:keyIDPrefixLen]), "\x00")

	// If the caller provided a KeyId hint, verify it matches the embedded key ID.
	if input.KeyID != "" {
		hintResolved, _, hintErr := b.resolveKeyID(ctx, input.KeyID)
		if hintErr != nil {
			return nil, hintErr
		}

		if hintResolved != keyID {
			return nil, fmt.Errorf(
				"%w: provided KeyId %q does not match the key that encrypted the ciphertext",
				ErrInvalidCiphertext, input.KeyID,
			)
		}
	}

	key, lookupErr := b.lookupKey(ctx, keyID)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: key %q is not usable for decryption", ErrInvalidKeyUsage, key.KeyID)
	}

	if err := b.checkKeyMaterialExpiry(key); err != nil {
		return nil, err
	}

	km, err := b.requireKeyMaterial(region, key.KeyID)
	if err != nil {
		return nil, err
	}

	if err = b.validateGrantTokenConstraints(ctx, input.GrantTokens, input.EncryptionContext); err != nil {
		return nil, err
	}

	cipherPayload := input.CiphertextBlob[keyIDPrefixLen:]

	plaintext, err := b.decryptPayload(region, input.CiphertextBlob, cipherPayload, input.EncryptionContext, key, km)
	if err != nil {
		return nil, err
	}

	// Defense-in-depth: a tampered ciphertext that decrypts could in theory exceed
	// the maximum plaintext that AWS allows on encrypt. Reject to mirror behavior.
	if len(plaintext) > maxPlaintextBytes {
		return nil, fmt.Errorf(
			"%w: decrypted plaintext exceeds %d bytes",
			ErrInvalidCiphertext, maxPlaintextBytes,
		)
	}

	return &DecryptOutput{
		Plaintext:           plaintext,
		KeyID:               key.Arn,
		EncryptionAlgorithm: encryptionAlgorithmForSpec(key.KeySpec),
	}, nil
}

// decryptPayload dispatches to RSA-OAEP or symmetric decryption depending on key type.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) decryptPayload(
	region string,
	fullBlob, rsaPayload []byte,
	encCtx map[string]string,
	key *Key,
	km *keyMaterial,
) ([]byte, error) {
	if km.rsaKey != nil {
		return decryptRSAOAEP(rsaPayload, km)
	}

	plaintext, _, err := decryptData(fullBlob, encCtx, km)
	if err == nil {
		return plaintext, nil
	}

	// Try previous key material versions (produced by key rotation).
	return b.decryptWithHistory(region, fullBlob, encCtx, key.KeyID)
}

// decryptWithHistory attempts to decrypt a blob using previous key material versions.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) decryptWithHistory(
	region string,
	blob []byte,
	encCtx map[string]string,
	keyID string,
) ([]byte, error) {
	history := b.keyMaterialHistoryStore(region)[keyID]
	for _, v := range slices.Backward(history) {
		plaintext, _, err := decryptData(blob, encCtx, v)
		if err == nil {
			return plaintext, nil
		}
	}

	return nil, ErrInvalidCiphertext
}

// GenerateDataKey generates a random data key, returning both plaintext and encrypted forms.
func (b *InMemoryBackend) GenerateDataKey(
	ctx context.Context,
	input *GenerateDataKeyInput,
) (*GenerateDataKeyOutput, error) {
	if err := validateGenerateDataKeyInput(input); err != nil {
		return nil, err
	}

	b.mu.RLock("GenerateDataKey")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKey(ctx, input.KeyID)
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

	km, err := b.requireKeyMaterial(region, key.KeyID)
	if err != nil {
		return nil, err
	}

	if err = b.validateGrantTokenConstraints(ctx, input.GrantTokens, input.EncryptionContext); err != nil {
		return nil, err
	}

	blob, encErr := encryptData(plaintextKey, key.KeyID, input.EncryptionContext, km)
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
func (b *InMemoryBackend) ReEncrypt(ctx context.Context, input *ReEncryptInput) (*ReEncryptOutput, error) {
	if err := validateReEncryptInput(input); err != nil {
		return nil, err
	}

	b.mu.RLock("ReEncrypt")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	// Extract source key ID from blob to look up key metadata and material.
	if len(input.CiphertextBlob) < keyIDPrefixLen {
		return nil, ErrCiphertextTooShort
	}

	sourceKeyID := strings.TrimRight(string(input.CiphertextBlob[:keyIDPrefixLen]), "\x00")

	// Validate source key state and usage before decrypting.
	sourceKey, sourceErr := b.lookupKey(ctx, sourceKeyID)
	if sourceErr != nil {
		return nil, sourceErr
	}

	if sourceKey.KeyState != KeyStateEnabled {
		return nil, keyStateError(sourceKey)
	}

	if sourceKey.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: source key %q is not usable for decryption", ErrInvalidKeyUsage, sourceKey.KeyID)
	}

	sourceKM, err := b.requireKeyMaterial(region, sourceKeyID)
	if err != nil {
		return nil, err
	}

	plaintext, _, decErr := decryptData(input.CiphertextBlob, input.SourceEncryptionContext, sourceKM)
	if decErr != nil {
		// Try previous key material versions produced by rotation.
		plaintext, decErr = b.decryptWithHistory(
			region,
			input.CiphertextBlob,
			input.SourceEncryptionContext,
			sourceKey.KeyID,
		)
		if decErr != nil {
			return nil, decErr
		}
	}

	destKey, lookupErr := b.lookupKey(ctx, input.DestinationKeyID)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if destKey.KeyState != KeyStateEnabled {
		return nil, keyStateError(destKey)
	}

	if destKey.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: destination key %q is not usable for encryption", ErrInvalidKeyUsage, destKey.KeyID)
	}

	destKM, err := b.requireKeyMaterial(region, destKey.KeyID)
	if err != nil {
		return nil, err
	}

	blob, encErr := encryptData(plaintext, destKey.KeyID, input.DestinationEncryptionContext, destKM)
	if encErr != nil {
		return nil, encErr
	}

	return &ReEncryptOutput{
		CiphertextBlob:                 blob,
		KeyID:                          destKey.Arn,
		SourceKeyID:                    sourceKey.Arn,
		SourceEncryptionAlgorithm:      encryptionAlgorithmForSpec(sourceKey.KeySpec),
		DestinationEncryptionAlgorithm: encryptionAlgorithmForSpec(destKey.KeySpec),
	}, nil
}

// Sign creates a digital signature for the specified message using an asymmetric KMS key.
func (b *InMemoryBackend) Sign(ctx context.Context, input *SignInput) (*SignOutput, error) {
	// Validate message size: AWS limits RAW messages to 4096 bytes.
	msgType := input.MessageType
	if msgType == "" {
		msgType = messageTypeRaw
	}

	if msgType == messageTypeRaw && len(input.Message) > maxSignMessageBytes {
		return nil, fmt.Errorf(
			"%w: message must not exceed %d bytes for RAW message type, got %d",
			ErrValidation, maxSignMessageBytes, len(input.Message),
		)
	}

	b.mu.RLock("Sign")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKey(ctx, input.KeyID)
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

	km, err := b.requireKeyMaterial(region, key.KeyID)
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
func (b *InMemoryBackend) Verify(ctx context.Context, input *VerifyInput) (*VerifyOutput, error) {
	// Validate message size: AWS limits RAW messages to 4096 bytes.
	msgType := input.MessageType
	if msgType == "" {
		msgType = messageTypeRaw
	}

	if msgType == messageTypeRaw && len(input.Message) > maxSignMessageBytes {
		return nil, fmt.Errorf(
			"%w: message must not exceed %d bytes for RAW message type, got %d",
			ErrValidation, maxSignMessageBytes, len(input.Message),
		)
	}

	b.mu.RLock("Verify")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKey(ctx, input.KeyID)
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

	km, err := b.requireKeyMaterial(region, key.KeyID)
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
func (b *InMemoryBackend) GetPublicKey(ctx context.Context, input *GetPublicKeyInput) (*GetPublicKeyOutput, error) {
	b.mu.RLock("GetPublicKey")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageSignVerify && key.KeyUsage != KeyUsageKeyAgreement &&
		key.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf(
			"%w: key %q does not have an asymmetric public key (KeyUsage=%s)",
			ErrInvalidKeyUsage,
			key.KeyID,
			key.KeyUsage,
		)
	}

	// Symmetric keys do not have a public key.
	if key.KeySpec == keySpecSymmetric {
		return nil, fmt.Errorf("%w: key %q is a symmetric key and has no public key", ErrInvalidKeyUsage, key.KeyID)
	}

	km, err := b.requireKeyMaterial(region, key.KeyID)
	if err != nil {
		return nil, err
	}

	der, pubErr := publicKeyDER(km)
	if pubErr != nil {
		return nil, pubErr
	}

	out := &GetPublicKeyOutput{
		KeyID:     key.Arn,
		PublicKey: der,
		KeySpec:   key.KeySpec,
		KeyUsage:  key.KeyUsage,
	}

	switch key.KeyUsage {
	case KeyUsageSignVerify:
		out.SigningAlgorithms = defaultSigningAlgorithmsForUsage(key.KeySpec, key.KeyUsage)
	case KeyUsageKeyAgreement:
		out.KeyAgreementAlgorithms = keyAgreementAlgorithms(key.KeyUsage)
	case KeyUsageEncryptDecrypt:
		out.EncryptionAlgorithms = []string{algoRSAESOAEPSHA1, encryptionAlgorithmRSAOAEP}
	}

	return out, nil
}

// CreateAlias creates an alias pointing to a key.
func (b *InMemoryBackend) CreateAlias(ctx context.Context, input *CreateAliasInput) error {
	if !strings.HasPrefix(input.AliasName, "alias/") {
		return fmt.Errorf("%w: alias name must start with alias/", ErrValidation)
	}

	if strings.HasPrefix(input.AliasName, "alias/aws/") {
		return fmt.Errorf("%w: alias names that begin with alias/aws/ are reserved for AWS managed keys", ErrValidation)
	}

	if len(input.AliasName) > maxAliasNameLength {
		return fmt.Errorf(
			"%w: alias name exceeds maximum length of %d characters",
			ErrValidation, maxAliasNameLength,
		)
	}

	if !isValidAliasName(input.AliasName) {
		return fmt.Errorf(
			"%w: alias name %q contains invalid characters; "+
				"allowed: letters, numbers, colons, forward slashes, underscores, and hyphens",
			ErrValidation, input.AliasName,
		)
	}

	b.mu.Lock("CreateAlias")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if _, exists := b.aliasesStore(region)[input.AliasName]; exists {
		return ErrAliasAlreadyExists
	}

	targetID, _, err := b.resolveKeyID(ctx, input.TargetKeyID)
	if err != nil {
		return err
	}

	if _, exists := b.keysStore(region)[targetID]; !exists {
		return ErrKeyNotFound
	}

	now := UnixTimeFloat(time.Now())
	aliasArn := gopherarn.Build("kms", region, b.accountID, input.AliasName)
	b.aliasesStore(region)[input.AliasName] = &Alias{
		AliasName:       input.AliasName,
		AliasArn:        aliasArn,
		TargetKeyID:     targetID,
		CreationDate:    now,
		LastUpdatedDate: now,
	}
	b.clearResolutionCache()

	return nil
}

// UpdateAlias redirects an existing alias to a different key.
// The alias must already exist; the target key must exist and not be in PendingDeletion state.
func (b *InMemoryBackend) UpdateAlias(ctx context.Context, input *UpdateAliasInput) error {
	b.mu.Lock("UpdateAlias")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	alias, exists := b.aliasesStore(region)[input.AliasName]
	if !exists {
		return ErrAliasNotFound
	}

	targetID, _, err := b.resolveKeyID(ctx, input.TargetKeyID)
	if err != nil {
		return err
	}

	targetKey, ok := b.keysStore(region)[targetID]
	if !ok {
		return ErrKeyNotFound
	}

	// AWS rejects alias updates pointing to a key in PendingDeletion state.
	if targetKey.KeyState == KeyStatePendingDeletion {
		return fmt.Errorf(
			"%w: cannot update alias to a key in PendingDeletion state (key %q)",
			ErrKeyInvalidState, targetID,
		)
	}

	alias.TargetKeyID = targetID
	alias.LastUpdatedDate = UnixTimeFloat(time.Now())
	b.clearResolutionCache()

	return nil
}

// DeleteAlias removes an alias.
// Per AWS KMS behaviour, an alias pointing to a key in PendingDeletion state
// cannot be deleted — the caller must cancel the deletion first.
func (b *InMemoryBackend) DeleteAlias(ctx context.Context, input *DeleteAliasInput) error {
	b.mu.Lock("DeleteAlias")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	alias, exists := b.aliasesStore(region)[input.AliasName]
	if !exists {
		return ErrAliasNotFound
	}

	// Prevent deleting an alias that targets a key scheduled for deletion.
	if alias.TargetKeyID != "" {
		if key, ok := b.keysStore(region)[alias.TargetKeyID]; ok && key.KeyState == KeyStatePendingDeletion {
			return fmt.Errorf(
				"%w: key %s is pending deletion; cancel the deletion before deleting the alias",
				ErrKeyInvalidState, alias.TargetKeyID,
			)
		}
	}

	delete(b.aliasesStore(region), input.AliasName)
	b.clearResolutionCache()

	return nil
}

// ListAliases returns a paginated list of aliases, optionally filtered by key.
func (b *InMemoryBackend) ListAliases(ctx context.Context, input *ListAliasesInput) (*ListAliasesOutput, error) {
	b.mu.RLock("ListAliases")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	var resolvedKeyID string

	if input.KeyID != "" {
		var err error

		resolvedKeyID, _, err = b.resolveKeyID(ctx, input.KeyID)
		if err != nil {
			return nil, err
		}
	}

	aliases := make([]Alias, 0, len(b.aliasesStore(region)))

	for _, a := range b.aliasesStore(region) {
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

	if input.Limit != nil {
		if *input.Limit < 1 || *input.Limit > 1000 {
			return nil, fmt.Errorf("%w: Limit must be between 1 and 1000", ErrValidation)
		}

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
// The rotation period defaults to 365 days. Rotation is NOT performed immediately;
// it is scheduled starting from the key's creation date or last rotation date.
// The key must be in the Enabled state.
func (b *InMemoryBackend) EnableKeyRotation(ctx context.Context, input *EnableKeyRotationInput) error {
	b.mu.Lock("EnableKeyRotation")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return err
	}

	// Only SYMMETRIC_DEFAULT keys with AWS_KMS origin support rotation.
	if key.KeySpec != keySpecSymmetric {
		return fmt.Errorf(
			"%w: key rotation is only supported for symmetric SYMMETRIC_DEFAULT keys; key %q has spec %s",
			ErrUnsupportedOrigin, key.KeyID, key.KeySpec,
		)
	}

	if key.Origin == KeyOriginExternal {
		return fmt.Errorf(
			"%w: key rotation is not supported for EXTERNAL-origin keys",
			ErrUnsupportedOrigin,
		)
	}

	// AWS requires the key to be in Enabled state to enable rotation.
	if key.KeyState != KeyStateEnabled {
		return keyStateError(key)
	}

	rotationPeriod := int32(defaultRotationPeriodDays)

	if input.RotationPeriodInDays != nil && *input.RotationPeriodInDays > 0 {
		period := *input.RotationPeriodInDays
		if period < minRotationPeriodDays || period > maxRotationPeriodDays {
			return fmt.Errorf(
				"%w: RotationPeriodInDays must be between %d and %d, got %d",
				ErrValidation, minRotationPeriodDays, maxRotationPeriodDays, period,
			)
		}

		rotationPeriod = period
	}

	key.RotationEnabled = true
	key.RotationPeriodInDays = rotationPeriod

	return nil
}

// DisableKeyRotation disables automatic key rotation for the specified key.
// Asymmetric keys and EXTERNAL-origin keys do not support rotation and return ErrUnsupportedOrigin.
func (b *InMemoryBackend) DisableKeyRotation(ctx context.Context, input *DisableKeyRotationInput) error {
	b.mu.Lock("DisableKeyRotation")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return err
	}

	if key.KeySpec != keySpecSymmetric {
		return fmt.Errorf(
			"%w: key rotation is only supported for symmetric SYMMETRIC_DEFAULT keys; key %q has spec %s",
			ErrUnsupportedOrigin, key.KeyID, key.KeySpec,
		)
	}

	if key.Origin == KeyOriginExternal {
		return fmt.Errorf(
			"%w: key rotation is not supported for EXTERNAL-origin keys",
			ErrUnsupportedOrigin,
		)
	}

	key.RotationEnabled = false
	key.RotationPeriodInDays = 0

	return nil
}

// RotateKeyOnDemand rotates key material immediately without changing automatic rotation status.
func (b *InMemoryBackend) RotateKeyOnDemand(
	ctx context.Context,
	input *RotateKeyOnDemandInput,
) (*RotateKeyOnDemandOutput, error) {
	b.mu.Lock("RotateKeyOnDemand")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeySpec != keySpecSymmetric {
		return nil, fmt.Errorf(
			"%w: key rotation is only supported for symmetric SYMMETRIC_DEFAULT keys; key %q has spec %s",
			ErrUnsupportedOrigin, key.KeyID, key.KeySpec,
		)
	}

	if key.Origin == KeyOriginExternal {
		return nil, fmt.Errorf(
			"%w: key rotation is not supported for EXTERNAL-origin keys",
			ErrUnsupportedOrigin,
		)
	}

	// AWS allows at most maxOnDemandRotationsPerDay on-demand rotations per 24-hour window.
	now := time.Now()
	cutoff := UnixTimeFloat(now.Add(-24 * time.Hour))
	recentCount := 0

	for _, r := range key.Rotations {
		if r.RotationType == rotationTypeImported && r.Date >= cutoff {
			recentCount++
		}
	}

	if recentCount >= maxOnDemandRotationsPerDay {
		return nil, fmt.Errorf(
			"%w: on-demand rotation limit of %d per 24-hour window exceeded for key %q",
			ErrValidation, maxOnDemandRotationsPerDay, key.KeyID,
		)
	}

	if err = b.rotateKeyMaterialLocked(region, key, rotationTypeImported); err != nil {
		return nil, err
	}

	key.OnDemandRotationDates = append(key.OnDemandRotationDates, UnixTimeFloat(now))

	return &RotateKeyOnDemandOutput{KeyID: key.KeyID}, nil
}

// GetKeyRotationStatus returns rotation configuration and schedule for the specified key.
func (b *InMemoryBackend) GetKeyRotationStatus(
	ctx context.Context,
	input *GetKeyRotationStatusInput,
) (*GetKeyRotationStatusOutput, error) {
	b.mu.RLock("GetKeyRotationStatus")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	// AWS raises UnsupportedOperationException for asymmetric or HMAC keys.
	if key.KeySpec != keySpecSymmetric || key.Origin == KeyOriginExternal {
		return nil, fmt.Errorf(
			"%w: GetKeyRotationStatus is only supported for symmetric keys with AWS_KMS origin; key %q has spec %s origin %s",
			ErrUnsupportedOrigin,
			key.KeyID,
			key.KeySpec,
			key.Origin,
		)
	}

	out := &GetKeyRotationStatusOutput{
		KeyRotationEnabled: key.RotationEnabled,
		KeyID:              key.KeyID,
	}

	if key.RotationEnabled {
		b.populateNextRotationDate(key, out)
	}

	out.OnDemandRotationStartDate = b.lastOnDemandRotationDate(key)

	return out, nil
}

// populateNextRotationDate fills NextRotationDate and RotationPeriodInDays on out.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) populateNextRotationDate(key *Key, out *GetKeyRotationStatusOutput) {
	period := key.RotationPeriodInDays
	if period <= 0 {
		period = defaultRotationPeriodDays
	}

	out.RotationPeriodInDays = period

	lastRotation := b.lastScheduledRotationDate(key)
	out.NextRotationDate = lastRotation + float64(period)*float64(24*time.Hour/time.Second)
}

// lastScheduledRotationDate returns the date of the most recent AWS_KMS scheduled rotation,
// falling back to legacy slices and finally the key creation date.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) lastScheduledRotationDate(key *Key) float64 {
	for _, v := range slices.Backward(key.Rotations) {
		if v.RotationType == rotationTypeAWSKMS {
			return v.Date
		}
	}

	if len(key.RotationDates) > 0 {
		return key.RotationDates[len(key.RotationDates)-1]
	}

	return key.CreationDate
}

// lastOnDemandRotationDate returns the date of the most recent on-demand rotation,
// falling back to the legacy OnDemandRotationDates slice.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) lastOnDemandRotationDate(key *Key) float64 {
	for _, v := range slices.Backward(key.Rotations) {
		if v.RotationType == rotationTypeImported {
			return v.Date
		}
	}

	if len(key.OnDemandRotationDates) > 0 {
		return key.OnDemandRotationDates[len(key.OnDemandRotationDates)-1]
	}

	return 0
}

// DisableKey disables the specified key.
// AWS raises KMSInvalidStateException for keys in PendingDeletion or PendingImport states.
func (b *InMemoryBackend) DisableKey(ctx context.Context, input *DisableKeyInput) error {
	b.mu.Lock("DisableKey")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return err
	}

	if key.KeyState == KeyStatePendingDeletion || key.KeyState == KeyStatePendingImport {
		return keyStateError(key)
	}

	key.KeyState = KeyStateDisabled
	key.Enabled = false

	return nil
}

// EnableKey enables the specified key.
// AWS raises KMSInvalidStateException for keys in PendingDeletion or PendingImport states.
func (b *InMemoryBackend) EnableKey(ctx context.Context, input *EnableKeyInput) error {
	b.mu.Lock("EnableKey")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return err
	}

	if key.KeyState == KeyStatePendingDeletion || key.KeyState == KeyStatePendingImport {
		return keyStateError(key)
	}

	key.KeyState = KeyStateEnabled
	key.Enabled = true

	return nil
}

// ScheduleKeyDeletion schedules a key for deletion.
// PendingWindowInDays must be in the range [7, 30]; values outside this range are rejected.
// AWS raises ValidationException for out-of-range values and KMSInvalidStateException
// for keys already in PendingDeletion.
func (b *InMemoryBackend) ScheduleKeyDeletion(
	ctx context.Context,
	input *ScheduleKeyDeletionInput,
) (*ScheduleKeyDeletionOutput, error) {
	b.mu.Lock("ScheduleKeyDeletion")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState == KeyStatePendingDeletion {
		return nil, keyStateError(key)
	}

	days := input.PendingWindowInDays
	if days == 0 {
		days = defaultPendingWindowDays
	}

	if days < minPendingWindowDays || days > maxPendingWindowDays {
		return nil, fmt.Errorf(
			"%w: PendingWindowInDays must be between %d and %d, got %d",
			ErrValidation, minPendingWindowDays, maxPendingWindowDays, days,
		)
	}

	deletionDate := time.Now().UTC().AddDate(0, 0, days)
	key.KeyState = KeyStatePendingDeletion
	key.Enabled = false
	key.DeletionDate = UnixTimeFloat(deletionDate)
	key.PendingWindowInDays = days

	return &ScheduleKeyDeletionOutput{
		KeyID:               key.KeyID,
		DeletionDate:        key.DeletionDate,
		KeyState:            key.KeyState,
		PendingWindowInDays: days,
	}, nil
}

// CancelKeyDeletion cancels a pending key deletion and sets the key to Disabled.
// AWS raises KMSInvalidStateException if the key is not in PendingDeletion state.
func (b *InMemoryBackend) CancelKeyDeletion(
	ctx context.Context,
	input *CancelKeyDeletionInput,
) (*CancelKeyDeletionOutput, error) {
	b.mu.Lock("CancelKeyDeletion")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStatePendingDeletion {
		return nil, keyStateError(key)
	}

	key.KeyState = KeyStateDisabled
	key.Enabled = false
	key.DeletionDate = 0

	return &CancelKeyDeletionOutput{KeyID: key.KeyID, KeyState: key.KeyState}, nil
}

// lookupKey finds a key by ID, alias, or ARN. Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupKey(ctx context.Context, keyID string) (*Key, error) {
	resolved, region, err := b.resolveKeyID(ctx, keyID)
	if err != nil {
		return nil, err
	}

	key, ok := b.keysStore(region)[resolved]
	if !ok {
		// For plain UUID lookups (not ARN or alias), fall back to searching all regions.
		// This preserves mock compatibility: multi-region tests create replicas in a target
		// region and look them up without specifying the target region in ctx.
		if !strings.HasPrefix(keyID, "arn:") && !strings.HasPrefix(keyID, "alias/") {
			if key = b.findKeyInAnyRegion(resolved); key != nil {
				return key, nil
			}
		}

		return nil, ErrKeyNotFound
	}

	return key, nil
}

// lookupKeyWrite finds a key by ID, alias, or ARN. Caller must hold a write lock.
func (b *InMemoryBackend) lookupKeyWrite(ctx context.Context, keyID string) (*Key, error) {
	return b.lookupKey(ctx, keyID)
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

func (b *InMemoryBackend) rotateKeyMaterialLocked(region string, key *Key, rotationType string) error {
	if key.KeyState != KeyStateEnabled {
		return keyStateError(key)
	}

	if key.KeySpec != keySpecSymmetric {
		return fmt.Errorf(
			"%w: key rotation is only supported for symmetric SYMMETRIC_DEFAULT keys; key %q has spec %s",
			ErrUnsupportedOrigin, key.KeyID, key.KeySpec,
		)
	}

	if key.Origin == KeyOriginExternal {
		return fmt.Errorf(
			"%w: key rotation is not supported for EXTERNAL-origin keys; material is managed by the caller",
			ErrUnsupportedOrigin,
		)
	}

	newKM, kmErr := generateKeyMaterial(key.KeySpec)
	if kmErr != nil {
		return fmt.Errorf("rotating key material: %w", kmErr)
	}

	kms := b.keyMaterialsStore(region)
	kmh := b.keyMaterialHistoryStore(region)

	if current := kms[key.KeyID]; current != nil {
		kmh[key.KeyID] = append(kmh[key.KeyID], current)
		// Cap retained history to bound long-running mock memory growth.
		hist := kmh[key.KeyID]
		if len(hist) > maxKeyMaterialHistoryEntries {
			kmh[key.KeyID] = hist[len(hist)-maxKeyMaterialHistoryEntries:]
		}
	}

	kms[key.KeyID] = newKM
	ts := UnixTimeFloat(time.Now())
	key.RotationDates = append(key.RotationDates, ts)
	key.Rotations = append(key.Rotations, RotationRecord{Date: ts, RotationType: rotationType})

	return nil
}

// keyToMetadata converts a Key to its KeyMetadata representation.
func keyToMetadata(k *Key) KeyMetadata {
	origin := k.Origin
	if origin == "" {
		origin = KeyOriginAWSKMS
	}

	meta := KeyMetadata{
		KeyID:                 k.KeyID,
		Arn:                   k.Arn,
		Description:           k.Description,
		KeyState:              k.KeyState,
		KeyUsage:              k.KeyUsage,
		KeySpec:               k.KeySpec,
		CustomerMasterKeySpec: k.KeySpec,
		CreationDate:          k.CreationDate,
		KeyManager:            "CUSTOMER",
		Origin:                origin,
		MultiRegion:           k.MultiRegion,
		PrimaryRegion:         k.PrimaryRegion,
		Enabled:               k.Enabled,
	}

	// DeletionDate and PendingWindowInDays are only meaningful for PendingDeletion keys.
	if k.KeyState == KeyStatePendingDeletion {
		meta.DeletionDate = k.DeletionDate
		meta.PendingDeletionWindowInDays = k.PendingWindowInDays
	}

	applyExpirationFields(k, &meta)
	applyMultiRegionType(k, &meta)
	applyAlgorithmFields(k, &meta)

	return meta
}

// applyExpirationFields sets ValidTo and ExpirationModel on meta for EXTERNAL keys.
func applyExpirationFields(k *Key, meta *KeyMetadata) {
	if k.Origin != KeyOriginExternal {
		return
	}

	if k.ValidTo > 0 {
		meta.ValidTo = k.ValidTo
		meta.ExpirationModel = expirationModelExpires

		return
	}

	if k.ExpirationModel == expirationModelNoExpiry {
		meta.ExpirationModel = expirationModelNoExpiry

		return
	}

	meta.ExpirationModel = k.ExpirationModel
}

// applyMultiRegionType sets MultiRegionKeyType on meta for multi-region keys.
func applyMultiRegionType(k *Key, meta *KeyMetadata) {
	if !k.MultiRegion || k.PrimaryRegion == "" {
		return
	}

	if k.PrimaryRegion == extractRegionFromARN(k.Arn) {
		meta.MultiRegionKeyType = "PRIMARY"
	} else {
		meta.MultiRegionKeyType = "REPLICA"
	}
}

// buildMultiRegionConfig constructs the MultiRegionConfiguration for a key, following
// the same PRIMARY/REPLICA logic used by AWS DescribeKey. Returns nil for non-multi-region keys.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) buildMultiRegionConfig(_ context.Context, key *Key) *MultiRegionConfiguration {
	if !key.MultiRegion {
		return nil
	}

	keyRegion := extractRegionFromARN(key.Arn)

	if key.PrimaryRegion == "" || key.PrimaryRegion == keyRegion {
		return b.buildPrimaryMultiRegionConfig(key, keyRegion)
	}

	return b.buildReplicaMultiRegionConfig(key)
}

// buildPrimaryMultiRegionConfig returns the MultiRegionConfiguration for a primary key.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) buildPrimaryMultiRegionConfig(key *Key, keyRegion string) *MultiRegionConfiguration {
	cfg := &MultiRegionConfiguration{
		MultiRegionKeyType: "PRIMARY",
		PrimaryKey:         &MultiRegionKeyRef{Arn: key.Arn, Region: keyRegion},
	}

	for _, replicaID := range key.ReplicaKeyIDs {
		// replica keys may live in any region — search all regions
		if rk := b.findKeyInAnyRegion(replicaID); rk != nil {
			cfg.ReplicaKeys = append(cfg.ReplicaKeys, MultiRegionKeyRef{
				Arn:    rk.Arn,
				Region: extractRegionFromARN(rk.Arn),
			})
		}
	}

	return cfg
}

// buildReplicaMultiRegionConfig returns the MultiRegionConfiguration for a replica key by
// scanning keys to locate the primary. Must be called with at least a read lock held.
func (b *InMemoryBackend) buildReplicaMultiRegionConfig(key *Key) *MultiRegionConfiguration {
	cfg := &MultiRegionConfiguration{
		MultiRegionKeyType: "REPLICA",
	}

	primaryKey := b.findPrimaryKeyForReplica(key)
	if primaryKey != nil {
		cfg.PrimaryKey = &MultiRegionKeyRef{
			Arn:    primaryKey.Arn,
			Region: key.PrimaryRegion,
		}

		for _, replicaID := range primaryKey.ReplicaKeyIDs {
			if rk := b.findKeyInAnyRegion(replicaID); rk != nil {
				cfg.ReplicaKeys = append(cfg.ReplicaKeys, MultiRegionKeyRef{
					Arn:    rk.Arn,
					Region: extractRegionFromARN(rk.Arn),
				})
			}
		}
	} else {
		cfg.PrimaryKey = &MultiRegionKeyRef{Region: key.PrimaryRegion}
	}

	return cfg
}

// findKeyInAnyRegion searches all region stores for a key with the given keyID.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) findKeyInAnyRegion(keyID string) *Key {
	for _, regionKeys := range b.keys {
		if k, ok := regionKeys[keyID]; ok {
			return k
		}
	}

	return nil
}

// findPrimaryKeyForReplica locates the primary key that lists replicaKey.KeyID in its
// ReplicaKeyIDs. Must be called with at least a read lock held.
func (b *InMemoryBackend) findPrimaryKeyForReplica(replicaKey *Key) *Key {
	for _, regionKeys := range b.keys {
		for _, k := range regionKeys {
			if !k.MultiRegion || extractRegionFromARN(k.Arn) != replicaKey.PrimaryRegion {
				continue
			}

			if slices.Contains(k.ReplicaKeyIDs, replicaKey.KeyID) {
				return k
			}
		}
	}

	return nil
}

// applyAlgorithmFields sets the algorithm lists on meta based on key usage and spec.
func applyAlgorithmFields(k *Key, meta *KeyMetadata) {
	switch k.KeyUsage {
	case KeyUsageEncryptDecrypt:
		if k.KeySpec == keySpecRSA2048 || k.KeySpec == keySpecRSA3072 || k.KeySpec == keySpecRSA4096 {
			meta.EncryptionAlgorithms = []string{algoRSAESOAEPSHA1, encryptionAlgorithmRSAOAEP}
		} else {
			meta.EncryptionAlgorithms = []string{"SYMMETRIC_DEFAULT"}
		}
	case KeyUsageSignVerify:
		meta.SigningAlgorithms = defaultSigningAlgorithms(k.KeySpec)
	case KeyUsageGenerateMac:
		meta.MacAlgorithms = defaultMacAlgorithms(k.KeySpec)
	case KeyUsageKeyAgreement:
		meta.KeyAgreementAlgorithms = []string{algoECDH}
	}
}

// extractRegionFromARN parses the region component from a KMS ARN.
func extractRegionFromARN(arnStr string) string {
	parsed, err := awsarn.Parse(arnStr)
	if err != nil {
		return ""
	}

	return parsed.Region
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
func (b *InMemoryBackend) CreateGrant(ctx context.Context, input *CreateGrantInput) (*CreateGrantOutput, error) {
	if strings.TrimSpace(input.GranteePrincipal) == "" {
		return nil, fmt.Errorf("%w: GranteePrincipal must not be empty", ErrValidation)
	}

	if len(input.Operations) == 0 {
		return nil, fmt.Errorf("%w: Operations must contain at least one entry", ErrValidation)
	}

	// Validate grant name length.
	if len(input.Name) > maxGrantNameLength {
		return nil, fmt.Errorf(
			"%w: grant name must not exceed %d characters, got %d",
			ErrValidation, maxGrantNameLength, len(input.Name),
		)
	}

	// Validate each operation against the allowed set.
	for _, op := range input.Operations {
		if !isValidGrantOperation(op) {
			return nil, fmt.Errorf(
				"%w: invalid grant operation %q; must be one of the allowed KMS grant operations",
				ErrValidation, op,
			)
		}
	}

	b.mu.Lock("CreateGrant")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	keyID, _, err := b.resolveKeyID(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	key, ok := b.keysStore(region)[keyID]
	if !ok {
		return nil, ErrKeyNotFound
	}

	if key.KeyState == KeyStatePendingDeletion || key.KeyState == KeyStatePendingImport {
		return nil, keyStateError(key)
	}

	if len(b.grantsByKeyStore(region, keyID)) >= maxGrantsPerKey {
		return nil, fmt.Errorf("%w: grant limit of %d exceeded for key %q", ErrLimitExceeded, maxGrantsPerKey, keyID)
	}

	now := time.Now()
	grantID := uuid.New().String()
	grantToken := uuid.New().String()
	grant := &Grant{
		GrantID:           grantID,
		KeyID:             keyID,
		GranteePrincipal:  input.GranteePrincipal,
		RetiringPrincipal: input.RetiringPrincipal,
		Operations:        input.Operations,
		Name:              input.Name,
		GrantToken:        grantToken,
		TokenIssuedAt:     now,
		Constraints:       input.Constraints,
		CreationDate:      UnixTimeFloat(now),
	}
	b.grantsStore(region)[grantID] = grant
	b.grantsByTokenStore(region)[grantToken] = grant
	b.grantsByKeyStore(region, keyID)[grantID] = grant

	return &CreateGrantOutput{GrantID: grantID, GrantToken: grantToken}, nil
}

// grantConstraintsSatisfied reports whether the provided encryption context satisfies
// the grant's constraints. A nil Constraints field always passes.
func grantConstraintsSatisfied(c *GrantConstraints, encCtx map[string]string) bool {
	if c == nil {
		return true
	}

	if len(c.EncryptionContextEquals) > 0 {
		if len(encCtx) != len(c.EncryptionContextEquals) {
			return false
		}

		for k, v := range c.EncryptionContextEquals {
			if encCtx[k] != v {
				return false
			}
		}
	}

	for k, v := range c.EncryptionContextSubset {
		if encCtx[k] != v {
			return false
		}
	}

	return true
}

// findGrantByToken returns the first grant whose GrantToken matches any of the provided tokens.
// Searches all regions. Must be called with at least a read lock held.
func (b *InMemoryBackend) findGrantByToken(grantTokens []string) *Grant {
	for _, regionMap := range b.grantsByToken {
		for _, token := range grantTokens {
			if g, ok := regionMap[token]; ok {
				return g
			}
		}
	}

	return nil
}

// validateGrantTokenConstraints checks that, if a grant token is provided, the encryption
// context satisfies the grant's constraints. No-op when grantTokens is empty.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) validateGrantTokenConstraints(
	_ context.Context,
	grantTokens []string,
	encCtx map[string]string,
) error {
	if len(grantTokens) == 0 {
		return nil
	}

	grant := b.findGrantByToken(grantTokens)
	if grant == nil {
		return fmt.Errorf("%w: grant token not found", ErrInvalidGrantToken)
	}

	// AWS grant tokens are valid for approximately 5 minutes after issuance.
	if !grant.TokenIssuedAt.IsZero() && time.Since(grant.TokenIssuedAt) > grantTokenTTL {
		return fmt.Errorf("%w: grant token has expired", ErrInvalidGrantToken)
	}

	if !grantConstraintsSatisfied(grant.Constraints, encCtx) {
		return fmt.Errorf(
			"%w: encryption context does not satisfy grant constraints",
			ErrKeyInvalidState,
		)
	}

	return nil
}

// ListGrants returns the grants for a specified key with optional pagination and GrantId filter.
func (b *InMemoryBackend) ListGrants(ctx context.Context, input *ListGrantsInput) (*ListGrantsOutput, error) {
	b.mu.RLock("ListGrants")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	keyID, _, err := b.resolveKeyID(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if _, ok := b.keysStore(region)[keyID]; !ok {
		return nil, ErrKeyNotFound
	}

	var grants []Grant
	for grantID, g := range b.grantsByKey[region][keyID] {
		// Filter by GrantId if specified.
		if input.GrantID != "" && grantID != input.GrantID {
			continue
		}
		grants = append(grants, *g)
	}

	sort.Slice(grants, func(i, j int) bool { return grants[i].GrantID < grants[j].GrantID })

	startIdx := parseMarker(input.Marker)
	limit := int32(defaultListLimit)

	if input.Limit != nil && *input.Limit > 0 {
		limit = *input.Limit
	}

	if startIdx >= len(grants) {
		return &ListGrantsOutput{Grants: []Grant{}}, nil
	}

	end := startIdx + int(limit)

	var nextMarker string
	if end < len(grants) {
		nextMarker = strconv.Itoa(end)
	} else {
		end = len(grants)
	}

	return &ListGrantsOutput{
		Grants:     grants[startIdx:end],
		NextMarker: nextMarker,
		Truncated:  nextMarker != "",
	}, nil
}

// RevokeGrant revokes a grant by ID.
func (b *InMemoryBackend) RevokeGrant(ctx context.Context, input *RevokeGrantInput) error {
	b.mu.Lock("RevokeGrant")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	keyID, _, err := b.resolveKeyID(ctx, input.KeyID)
	if err != nil {
		return err
	}

	if _, ok := b.keysStore(region)[keyID]; !ok {
		return ErrKeyNotFound
	}

	grant, ok := b.grantsStore(region)[input.GrantID]
	if !ok || grant.KeyID != keyID {
		return ErrGrantNotFound
	}

	delete(b.grantsStore(region), input.GrantID)
	delete(b.grantsByTokenStore(region), grant.GrantToken)
	if rm := b.grantsByKey[region]; rm != nil {
		delete(rm[grant.KeyID], input.GrantID)
	}

	return nil
}

// RetireGrant retires a grant by grant token or grant ID + key ID.
func (b *InMemoryBackend) RetireGrant(ctx context.Context, input *RetireGrantInput) error {
	b.mu.Lock("RetireGrant")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if input.GrantToken != "" {
		// Search all regions for the grant token.
		for r, regionMap := range b.grantsByToken {
			if g, ok := regionMap[input.GrantToken]; ok {
				delete(b.grantsStore(r), g.GrantID)
				delete(regionMap, input.GrantToken)
				if rm := b.grantsByKey[r]; rm != nil {
					delete(rm[g.KeyID], g.GrantID)
				}

				return nil
			}
		}

		return ErrGrantNotFound
	}

	if input.GrantID == "" {
		return ErrGrantNotFound
	}

	grant, ok := b.grantsStore(region)[input.GrantID]
	if !ok {
		return ErrGrantNotFound
	}

	if input.KeyID != "" {
		keyID, _, err := b.resolveKeyID(ctx, input.KeyID)
		if err != nil {
			return err
		}

		if grant.KeyID != keyID {
			return ErrGrantNotFound
		}
	}

	delete(b.grantsStore(region), input.GrantID)
	delete(b.grantsByTokenStore(region), grant.GrantToken)
	if rm := b.grantsByKey[region]; rm != nil {
		delete(rm[grant.KeyID], input.GrantID)
	}

	return nil
}

// ListRetirableGrants returns all grants for which the given principal is the retiring principal.
func (b *InMemoryBackend) ListRetirableGrants(
	ctx context.Context,
	input *ListRetirableGrantsInput,
) (*ListGrantsOutput, error) {
	b.mu.RLock("ListRetirableGrants")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	grants := make([]Grant, 0)
	for _, g := range b.grantsStore(region) {
		if g.RetiringPrincipal == input.RetiringPrincipal {
			grants = append(grants, *g)
		}
	}

	sort.Slice(grants, func(i, j int) bool { return grants[i].GrantID < grants[j].GrantID })

	startIdx := parseMarker(input.Marker)
	limit := int32(defaultListLimit)

	if input.Limit != nil && *input.Limit > 0 {
		limit = *input.Limit
	}

	if startIdx >= len(grants) {
		return &ListGrantsOutput{Grants: []Grant{}}, nil
	}

	end := startIdx + int(limit)

	var nextMarker string
	if end < len(grants) {
		nextMarker = strconv.Itoa(end)
	} else {
		end = len(grants)
	}

	return &ListGrantsOutput{
		Grants:     grants[startIdx:end],
		NextMarker: nextMarker,
		Truncated:  nextMarker != "",
	}, nil
}

// GenerateDataKeyWithoutPlaintext generates a data key but returns only the encrypted copy.
func (b *InMemoryBackend) GenerateDataKeyWithoutPlaintext(
	ctx context.Context, input *GenerateDataKeyWithoutPlaintextInput,
) (*GenerateDataKeyWithoutPlaintextOutput, error) {
	out, err := b.GenerateDataKey(ctx, &GenerateDataKeyInput{
		KeyID:             input.KeyID,
		KeySpec:           input.KeySpec,
		NumberOfBytes:     input.NumberOfBytes,
		EncryptionContext: input.EncryptionContext,
		GrantTokens:       input.GrantTokens,
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
// Only the "default" policy name is supported.
func (b *InMemoryBackend) PutKeyPolicy(ctx context.Context, input *PutKeyPolicyInput) error {
	policyName := input.PolicyName
	if policyName == "" {
		policyName = defaultKeyPolicyName
	}

	if policyName != defaultKeyPolicyName {
		return fmt.Errorf(
			"%w: PolicyName must be %q; got %q",
			ErrValidation, defaultKeyPolicyName, policyName,
		)
	}

	b.mu.Lock("PutKeyPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	keyID, _, err := b.resolveKeyID(ctx, input.KeyID)
	if err != nil {
		return err
	}

	if _, ok := b.keysStore(region)[keyID]; !ok {
		return ErrKeyNotFound
	}

	b.policiesStore(region)[keyID] = input.Policy

	return nil
}

// GetKeyPolicy retrieves the key policy for a KMS key.
func (b *InMemoryBackend) GetKeyPolicy(ctx context.Context, input *GetKeyPolicyInput) (*GetKeyPolicyOutput, error) {
	b.mu.RLock("GetKeyPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	keyID, _, err := b.resolveKeyID(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if _, ok := b.keysStore(region)[keyID]; !ok {
		return nil, ErrKeyNotFound
	}

	policy, ok := b.policiesStore(region)[keyID]
	if !ok {
		// Return default policy
		rootARN := gopherarn.Build("iam", "", b.accountID, "root")
		policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":"` + rootARN + `"},"Action":"kms:*","Resource":"*"}]}`
	}

	policyName := input.PolicyName
	if policyName == "" {
		policyName = defaultKeyPolicyName
	}

	return &GetKeyPolicyOutput{Policy: policy, PolicyName: policyName}, nil
}

// GetParametersForImport returns mock wrapping parameters for EXTERNAL-origin key material import.
func (b *InMemoryBackend) GetParametersForImport(
	ctx context.Context, input *GetParametersForImportInput,
) (*GetParametersForImportOutput, error) {
	// Validate WrappingAlgorithm if provided.
	validWrappingAlgorithms := map[string]struct{}{
		"RSAES_PKCS1_V1_5":         {},
		"RSAES_OAEP_SHA_1":         {},
		encryptionAlgorithmRSAOAEP: {},
		"RSA_AES_KEY_WRAP_SHA_1":   {},
		"RSA_AES_KEY_WRAP_SHA_256": {},
	}

	if input.WrappingAlgorithm != "" {
		if _, ok := validWrappingAlgorithms[input.WrappingAlgorithm]; !ok {
			return nil, fmt.Errorf(
				"%w: WrappingAlgorithm %q is not valid; must be one of RSAES_PKCS1_V1_5, "+
					"RSAES_OAEP_SHA_1, RSAES_OAEP_SHA_256, RSA_AES_KEY_WRAP_SHA_1, or RSA_AES_KEY_WRAP_SHA_256",
				ErrValidation,
				input.WrappingAlgorithm,
			)
		}
	}

	validWrappingKeySpecs := map[string]struct{}{
		"RSA_2048": {},
		"RSA_3072": {},
		"RSA_4096": {},
	}

	if input.WrappingKeySpec != "" {
		if _, ok := validWrappingKeySpecs[input.WrappingKeySpec]; !ok {
			return nil, fmt.Errorf(
				"%w: WrappingKeySpec %q is not valid; must be RSA_2048, RSA_3072, or RSA_4096",
				ErrValidation, input.WrappingKeySpec,
			)
		}
	}

	b.mu.RLock("GetParametersForImport")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.Origin != KeyOriginExternal {
		return nil, fmt.Errorf(
			"%w: GetParametersForImport is only valid for keys with Origin=%s",
			ErrUnsupportedOrigin,
			KeyOriginExternal,
		)
	}

	importToken := make([]byte, aes256Bytes)
	if _, readErr := io.ReadFull(rand.Reader, importToken); readErr != nil {
		return nil, fmt.Errorf("generating import token: %w", readErr)
	}

	publicKey := make([]byte, getParametersImportPublicKeyBytes)
	if _, readErr := io.ReadFull(rand.Reader, publicKey); readErr != nil {
		return nil, fmt.Errorf("generating wrapping public key: %w", readErr)
	}

	return &GetParametersForImportOutput{
		KeyID:             key.KeyID,
		ImportToken:       importToken,
		PublicKey:         publicKey,
		ParametersValidTo: UnixTimeFloat(time.Now().Add(getParametersValidityWindow)),
	}, nil
}

// ListKeyPolicies returns policy names available for a key.
func (b *InMemoryBackend) ListKeyPolicies(
	ctx context.Context,
	input *ListKeyPoliciesInput,
) (*ListKeyPoliciesOutput, error) {
	b.mu.RLock("ListKeyPolicies")
	defer b.mu.RUnlock()

	if _, err := b.lookupKey(ctx, input.KeyID); err != nil {
		return nil, err
	}

	names := []string{defaultKeyPolicyName}
	startIdx := parseMarker(input.Marker)
	limit := int32(defaultListLimit)

	if input.Limit != nil && *input.Limit > 0 {
		limit = *input.Limit
	}

	if startIdx >= len(names) {
		return &ListKeyPoliciesOutput{PolicyNames: []string{}, Truncated: false}, nil
	}

	end := startIdx + int(limit)
	nextMarker := ""
	if end < len(names) {
		nextMarker = strconv.Itoa(end)
	} else {
		end = len(names)
	}

	return &ListKeyPoliciesOutput{
		PolicyNames: names[startIdx:end],
		NextMarker:  nextMarker,
		Truncated:   nextMarker != "",
	}, nil
}

// ListKeyRotations returns observed key material rotation timestamps for a key.
func (b *InMemoryBackend) ListKeyRotations(
	ctx context.Context,
	input *ListKeyRotationsInput,
) (*ListKeyRotationsOutput, error) {
	b.mu.RLock("ListKeyRotations")
	defer b.mu.RUnlock()

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	// Build rotation entries from the typed Rotations slice. Legacy keys loaded
	// from older snapshots that only have RotationDates (no Rotations) will show
	// empty history; this is acceptable since type information cannot be recovered.
	rotations := make([]KeyRotationEntry, 0, len(key.Rotations))
	for _, r := range key.Rotations {
		rotations = append(rotations, KeyRotationEntry{
			KeyID:        key.KeyID,
			RotationDate: r.Date,
			RotationType: r.RotationType,
		})
	}

	startIdx := parseMarker(input.Marker)
	limit := int32(defaultListLimit)

	if input.Limit != nil && *input.Limit > 0 {
		limit = *input.Limit
	}

	if startIdx >= len(rotations) {
		return &ListKeyRotationsOutput{Rotations: []KeyRotationEntry{}, Truncated: false}, nil
	}

	end := startIdx + int(limit)
	nextMarker := ""
	if end < len(rotations) {
		nextMarker = strconv.Itoa(end)
	} else {
		end = len(rotations)
	}

	return &ListKeyRotationsOutput{
		Rotations:  rotations[startIdx:end],
		NextMarker: nextMarker,
		Truncated:  nextMarker != "",
	}, nil
}

// ImportKeyMaterial imports externally supplied key material into a key created with
// Origin=EXTERNAL. The key must be in PendingImport state. On success the key transitions
// to Enabled. Only SYMMETRIC_DEFAULT keys are supported; asymmetric EXTERNAL keys are
// not modeled by this mock.
func (b *InMemoryBackend) ImportKeyMaterial(ctx context.Context, input *ImportKeyMaterialInput) error {
	b.mu.Lock("ImportKeyMaterial")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return err
	}

	if key.Origin != KeyOriginExternal {
		return fmt.Errorf(
			"%w: ImportKeyMaterial is only valid for keys with Origin=%s",
			ErrUnsupportedOrigin,
			KeyOriginExternal,
		)
	}

	// Only allow import when the key is awaiting material.
	if key.KeyState != KeyStatePendingImport {
		return keyStateError(key)
	}

	// Only symmetric (AES-256) key material is supported for external import.
	if key.KeySpec != keySpecSymmetric {
		return fmt.Errorf(
			"%w: ImportKeyMaterial only supports SYMMETRIC_DEFAULT keys; got %s",
			ErrInvalidKeyUsage, key.KeySpec,
		)
	}

	if len(input.KeyMaterial) == 0 {
		return fmt.Errorf("%w: KeyMaterial must not be empty", ErrInvalidKeyUsage)
	}

	if len(input.KeyMaterial) != aes256Bytes {
		return fmt.Errorf(
			"%w: symmetric key material must be exactly %d bytes, got %d",
			ErrInvalidKeyUsage, aes256Bytes, len(input.KeyMaterial),
		)
	}

	// Copy the material bytes so the caller cannot mutate the key's internal state.
	mat := make([]byte, aes256Bytes)
	copy(mat, input.KeyMaterial)

	km, kmErr := newSymmetricKeyMaterial(mat)
	if kmErr != nil {
		return fmt.Errorf("creating imported symmetric key material: %w", kmErr)
	}

	b.keyMaterialsStore(region)[key.KeyID] = km
	key.KeyState = KeyStateEnabled
	key.Enabled = true

	// Store expiration model and ValidTo for metadata and janitor enforcement.
	expModel := input.ExpirationModel

	// Infer default expiration model from context when not explicitly set:
	// - if ValidTo is set but ExpirationModel is absent, default to KEY_MATERIAL_EXPIRES
	// - if both are absent, default to KEY_MATERIAL_DOES_NOT_EXPIRE
	if expModel == "" {
		if input.ValidTo > 0 {
			expModel = expirationModelExpires
		} else {
			expModel = expirationModelNoExpiry
		}
	}

	// KEY_MATERIAL_EXPIRES requires a ValidTo timestamp.
	if expModel == expirationModelExpires && input.ValidTo == 0 {
		return fmt.Errorf(
			"%w: ExpirationModel=%s requires ValidTo to be set",
			ErrValidation, expirationModelExpires,
		)
	}

	// KEY_MATERIAL_DOES_NOT_EXPIRE must not include a ValidTo timestamp.
	if expModel == expirationModelNoExpiry && input.ValidTo > 0 {
		return fmt.Errorf(
			"%w: ExpirationModel=%s must not include ValidTo",
			ErrValidation, expirationModelNoExpiry,
		)
	}

	if input.ValidTo > 0 {
		key.ValidTo = input.ValidTo
	} else {
		key.ValidTo = 0
	}

	key.ExpirationModel = expModel

	return nil
}

// DeleteImportedKeyMaterial removes the imported key material from an EXTERNAL-origin key.
// The key transitions to PendingImport; it can receive new material via ImportKeyMaterial.
func (b *InMemoryBackend) DeleteImportedKeyMaterial(ctx context.Context, input *DeleteImportedKeyMaterialInput) error {
	b.mu.Lock("DeleteImportedKeyMaterial")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return err
	}

	if key.Origin != KeyOriginExternal {
		return fmt.Errorf(
			"%w: DeleteImportedKeyMaterial is only valid for keys with Origin=%s",
			ErrUnsupportedOrigin,
			KeyOriginExternal,
		)
	}

	delete(b.keyMaterialsStore(region), key.KeyID)
	delete(b.keyMaterialHistoryStore(region), key.KeyID)
	key.KeyState = KeyStatePendingImport
	key.Enabled = false
	key.ValidTo = 0
	key.ExpirationModel = ""

	return nil
}

// ReplicateKey creates a multi-region replica for an existing key in the target region.
func (b *InMemoryBackend) ReplicateKey(ctx context.Context, input *ReplicateKeyInput) (*ReplicateKeyOutput, error) {
	if strings.TrimSpace(input.ReplicaRegion) == "" {
		return nil, fmt.Errorf("%w: ReplicaRegion must not be empty", ErrValidation)
	}

	b.mu.Lock("ReplicateKey")
	defer b.mu.Unlock()

	sourceRegion := getRegion(ctx, b.defaultRegion)

	sourceKey, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	// Only Enabled keys can be replicated; PendingDeletion / PendingImport / Disabled are rejected.
	if sourceKey.KeyState != KeyStateEnabled {
		return nil, fmt.Errorf(
			"%w: only Enabled keys can be replicated; key %q is in state %s",
			ErrKeyInvalidState, sourceKey.KeyID, sourceKey.KeyState,
		)
	}

	// Only keys created with MultiRegion=true can be replicated.
	if !sourceKey.MultiRegion {
		return nil, fmt.Errorf(
			"%w: only multi-region keys can be replicated; key %q was not created with MultiRegion=true",
			ErrUnsupportedOrigin, sourceKey.KeyID,
		)
	}

	newKeyID := uuid.New().String()
	description := sourceKey.Description
	if input.Description != "" {
		description = input.Description
	}

	replicaARN := gopherarn.Build("kms", input.ReplicaRegion, b.accountID, "key/"+newKeyID)
	replica := &Key{
		KeyID:                newKeyID,
		Arn:                  replicaARN,
		Description:          description,
		KeyState:             sourceKey.KeyState,
		KeyUsage:             sourceKey.KeyUsage,
		KeySpec:              sourceKey.KeySpec,
		Origin:               sourceKey.Origin,
		CreationDate:         UnixTimeFloat(time.Now()),
		RotationEnabled:      sourceKey.RotationEnabled,
		RotationPeriodInDays: sourceKey.RotationPeriodInDays,
		MultiRegion:          true,
		PrimaryRegion:        b.keyRegion(sourceKey.Arn),
		Enabled:              sourceKey.Enabled,
	}

	sourceKey.MultiRegion = true
	if sourceKey.PrimaryRegion == "" {
		sourceKey.PrimaryRegion = b.keyRegion(sourceKey.Arn)
	}

	if km := b.keyMaterialsStore(sourceRegion)[sourceKey.KeyID]; km != nil {
		serialized, serErr := marshalKeyMaterial(km)
		if serErr != nil {
			return nil, fmt.Errorf("serializing key material for replication: %w", serErr)
		}

		cloned, cloneErr := unmarshalKeyMaterial(serialized)
		if cloneErr != nil {
			return nil, fmt.Errorf("deserializing replicated key material: %w", cloneErr)
		}

		// Store replica key material in the target region's store.
		b.keyMaterialsStore(input.ReplicaRegion)[replica.KeyID] = cloned
	}

	// Store replica key in the target region's store.
	b.keysStore(input.ReplicaRegion)[replica.KeyID] = replica

	// Record the replica key ID on the source (primary) key so DescribeKey can
	// return the full MultiRegionConfiguration.
	sourceKey.ReplicaKeyIDs = append(sourceKey.ReplicaKeyIDs, replica.KeyID)

	return &ReplicateKeyOutput{ReplicaKeyMetadata: keyToMetadata(replica)}, nil
}

// UpdateKeyDescription updates a key's description field.
func (b *InMemoryBackend) UpdateKeyDescription(ctx context.Context, input *UpdateKeyDescriptionInput) error {
	if len(input.Description) > maxDescriptionLength {
		return fmt.Errorf(
			"%w: Description exceeds maximum length of %d characters",
			ErrValidation, maxDescriptionLength,
		)
	}

	b.mu.Lock("UpdateKeyDescription")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return err
	}

	key.Description = input.Description

	return nil
}

// UpdatePrimaryRegion updates the primary region marker for a multi-region key.
func (b *InMemoryBackend) UpdatePrimaryRegion(ctx context.Context, input *UpdatePrimaryRegionInput) error {
	if strings.TrimSpace(input.PrimaryRegion) == "" {
		return fmt.Errorf("%w: PrimaryRegion must not be empty", ErrValidation)
	}

	b.mu.Lock("UpdatePrimaryRegion")
	defer b.mu.Unlock()

	key, err := b.lookupKeyWrite(ctx, input.KeyID)
	if err != nil {
		return err
	}

	key.MultiRegion = true
	key.PrimaryRegion = input.PrimaryRegion

	return nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.keys = make(map[string]map[string]*Key)
	b.aliases = make(map[string]map[string]*Alias)
	b.grants = make(map[string]map[string]*Grant)
	b.grantsByToken = make(map[string]map[string]*Grant)
	b.grantsByKey = make(map[string]map[string]map[string]*Grant)
	b.policies = make(map[string]map[string]string)
	b.keyMaterials = make(map[string]map[string]*keyMaterial)
	b.keyMaterialHistory = make(map[string]map[string][]*keyMaterial)
	b.customKeyStores = make(map[string]map[string]*CustomKeyStore)
	b.clearResolutionCache()
}

// CreateCustomKeyStore creates a new in-memory custom key store entry in DISCONNECTED state.
func (b *InMemoryBackend) CreateCustomKeyStore(
	ctx context.Context, input *CreateCustomKeyStoreInput,
) (*CreateCustomKeyStoreOutput, error) {
	if strings.TrimSpace(input.CustomKeyStoreName) == "" {
		return nil, fmt.Errorf("%w: CustomKeyStoreName must not be empty", ErrValidation)
	}

	storeType := input.CustomKeyStoreType
	if storeType == "" {
		storeType = "AWS_CLOUDHSM"
	}

	if storeType != "AWS_CLOUDHSM" && storeType != "EXTERNAL_KEY_STORE" {
		return nil, fmt.Errorf("%w: CustomKeyStoreType must be AWS_CLOUDHSM or EXTERNAL_KEY_STORE", ErrValidation)
	}

	b.mu.Lock("CreateCustomKeyStore")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	// Ensure name is unique.
	for _, ks := range b.customKeyStoresStore(region) {
		if ks.CustomKeyStoreName == input.CustomKeyStoreName {
			return nil, fmt.Errorf(
				"%w: custom key store with name %q already exists",
				ErrCustomKeyStoreAlreadyExists, input.CustomKeyStoreName,
			)
		}
	}

	storeID := uuid.New().String()

	b.customKeyStoresStore(region)[storeID] = &CustomKeyStore{
		CustomKeyStoreID:   storeID,
		CustomKeyStoreName: input.CustomKeyStoreName,
		ConnectionState:    ConnectionStateDisconnected,
		CreationDate:       UnixTimeFloat(time.Now()),
		CustomKeyStoreType: storeType,
	}

	return &CreateCustomKeyStoreOutput{CustomKeyStoreID: storeID}, nil
}

// DeleteCustomKeyStore removes an existing custom key store. It must be in DISCONNECTED state.
func (b *InMemoryBackend) DeleteCustomKeyStore(ctx context.Context, input *DeleteCustomKeyStoreInput) error {
	if input.CustomKeyStoreID == "" {
		return fmt.Errorf("%w: CustomKeyStoreId must not be empty", ErrValidation)
	}

	b.mu.Lock("DeleteCustomKeyStore")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ks, ok := b.customKeyStoresStore(region)[input.CustomKeyStoreID]
	if !ok {
		return fmt.Errorf("%w: custom key store %q not found", ErrCustomKeyStoreNotFound, input.CustomKeyStoreID)
	}

	if ks.ConnectionState != ConnectionStateDisconnected {
		return fmt.Errorf(
			"%w: custom key store must be DISCONNECTED before deletion; current state: %s",
			ErrKeyInvalidState, ks.ConnectionState,
		)
	}

	delete(b.customKeyStoresStore(region), input.CustomKeyStoreID)

	return nil
}

// DescribeCustomKeyStores returns a list of custom key stores matching optional filters.
func (b *InMemoryBackend) DescribeCustomKeyStores(
	ctx context.Context, input *DescribeCustomKeyStoresInput,
) (*DescribeCustomKeyStoresOutput, error) {
	b.mu.RLock("DescribeCustomKeyStores")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	stores := make([]CustomKeyStore, 0, len(b.customKeyStoresStore(region)))

	for _, ks := range b.customKeyStoresStore(region) {
		if input.CustomKeyStoreID != "" && ks.CustomKeyStoreID != input.CustomKeyStoreID {
			continue
		}

		if input.CustomKeyStoreName != "" && ks.CustomKeyStoreName != input.CustomKeyStoreName {
			continue
		}

		stores = append(stores, *ks)
	}

	sort.Slice(stores, func(i, j int) bool {
		return stores[i].CustomKeyStoreID < stores[j].CustomKeyStoreID
	})

	startIdx := parseMarker(input.Marker)
	limit := int32(defaultListLimit)

	if input.Limit != nil && *input.Limit > 0 {
		limit = *input.Limit
	}

	if startIdx >= len(stores) {
		return &DescribeCustomKeyStoresOutput{CustomKeyStores: []CustomKeyStore{}}, nil
	}

	end := startIdx + int(limit)

	var nextMarker string
	if end < len(stores) {
		nextMarker = strconv.Itoa(end)
	} else {
		end = len(stores)
	}

	return &DescribeCustomKeyStoresOutput{
		CustomKeyStores: stores[startIdx:end],
		NextMarker:      nextMarker,
		Truncated:       nextMarker != "",
	}, nil
}

// ConnectCustomKeyStore transitions a custom key store from DISCONNECTED to CONNECTED.
func (b *InMemoryBackend) ConnectCustomKeyStore(ctx context.Context, input *ConnectCustomKeyStoreInput) error {
	if input.CustomKeyStoreID == "" {
		return fmt.Errorf("%w: CustomKeyStoreId must not be empty", ErrValidation)
	}

	b.mu.Lock("ConnectCustomKeyStore")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ks, ok := b.customKeyStoresStore(region)[input.CustomKeyStoreID]
	if !ok {
		return fmt.Errorf("%w: custom key store %q not found", ErrCustomKeyStoreNotFound, input.CustomKeyStoreID)
	}

	if ks.ConnectionState == ConnectionStateConnected {
		return fmt.Errorf(
			"%w: custom key store %q is already connected",
			ErrKeyInvalidState, input.CustomKeyStoreID,
		)
	}

	ks.ConnectionState = ConnectionStateConnected

	return nil
}

// DisconnectCustomKeyStore transitions a custom key store from CONNECTED to DISCONNECTED.
func (b *InMemoryBackend) DisconnectCustomKeyStore(ctx context.Context, input *DisconnectCustomKeyStoreInput) error {
	if input.CustomKeyStoreID == "" {
		return fmt.Errorf("%w: CustomKeyStoreId must not be empty", ErrValidation)
	}

	b.mu.Lock("DisconnectCustomKeyStore")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ks, ok := b.customKeyStoresStore(region)[input.CustomKeyStoreID]
	if !ok {
		return fmt.Errorf("%w: custom key store %q not found", ErrCustomKeyStoreNotFound, input.CustomKeyStoreID)
	}

	if ks.ConnectionState == ConnectionStateDisconnected {
		return fmt.Errorf(
			"%w: custom key store %q is already disconnected",
			ErrKeyInvalidState, input.CustomKeyStoreID,
		)
	}

	ks.ConnectionState = ConnectionStateDisconnected

	return nil
}

// UpdateCustomKeyStore updates mutable properties for a custom key store.
func (b *InMemoryBackend) UpdateCustomKeyStore(ctx context.Context, input *UpdateCustomKeyStoreInput) error {
	if strings.TrimSpace(input.CustomKeyStoreID) == "" {
		return fmt.Errorf("%w: CustomKeyStoreId must not be empty", ErrValidation)
	}

	b.mu.Lock("UpdateCustomKeyStore")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ks, ok := b.customKeyStoresStore(region)[input.CustomKeyStoreID]
	if !ok {
		return fmt.Errorf("%w: custom key store %q not found", ErrCustomKeyStoreNotFound, input.CustomKeyStoreID)
	}

	if input.NewCustomKeyStoreName != "" && input.NewCustomKeyStoreName != ks.CustomKeyStoreName {
		for _, existing := range b.customKeyStoresStore(region) {
			if existing.CustomKeyStoreName == input.NewCustomKeyStoreName {
				return fmt.Errorf(
					"%w: custom key store with name %q already exists",
					ErrCustomKeyStoreAlreadyExists,
					input.NewCustomKeyStoreName,
				)
			}
		}

		ks.CustomKeyStoreName = input.NewCustomKeyStoreName
	}

	return nil
}

// DeriveSharedSecret computes an ECDH shared secret using an ECC KEY_AGREEMENT KMS key
// and the provided DER-encoded peer public key.
func (b *InMemoryBackend) DeriveSharedSecret(
	ctx context.Context, input *DeriveSharedSecretInput,
) (*DeriveSharedSecretOutput, error) {
	if input.KeyAgreementAlgorithm != "" && input.KeyAgreementAlgorithm != algoECDH {
		return nil, fmt.Errorf(
			"%w: KeyAgreementAlgorithm must be ECDH, got %q",
			ErrValidation, input.KeyAgreementAlgorithm,
		)
	}

	if len(input.PublicKey) == 0 {
		return nil, fmt.Errorf("%w: PublicKey must not be empty", ErrValidation)
	}

	b.mu.RLock("DeriveSharedSecret")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageKeyAgreement {
		return nil, fmt.Errorf(
			"%w: key %q must have KeyUsage=%s for DeriveSharedSecret",
			ErrInvalidKeyUsage, key.KeyID, KeyUsageKeyAgreement,
		)
	}

	km, err := b.requireKeyMaterial(region, key.KeyID)
	if err != nil {
		return nil, err
	}

	sharedSecret, err := deriveECDH(input.PublicKey, km)
	if err != nil {
		return nil, err
	}

	algo := input.KeyAgreementAlgorithm
	if algo == "" {
		algo = algoECDH
	}

	return &DeriveSharedSecretOutput{
		KeyID:                 key.Arn,
		SharedSecret:          sharedSecret,
		KeyAgreementAlgorithm: algo,
	}, nil
}

// GenerateDataKeyPair generates a new ephemeral asymmetric key pair, returning the public key,
// plaintext private key (DER-encoded PKCS#8), and the private key encrypted under the specified
// KMS wrapping key.
func (b *InMemoryBackend) GenerateDataKeyPair(
	ctx context.Context, input *GenerateDataKeyPairInput,
) (*GenerateDataKeyPairOutput, error) {
	if input.KeyPairSpec == "" {
		return nil, fmt.Errorf("%w: KeyPairSpec must not be empty", ErrValidation)
	}

	b.mu.RLock("GenerateDataKeyPair")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	wrapKey, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if wrapKey.KeyState != KeyStateEnabled {
		return nil, keyStateError(wrapKey)
	}

	if wrapKey.KeyUsage != KeyUsageEncryptDecrypt {
		return nil, fmt.Errorf("%w: wrapping key %q must have ENCRYPT_DECRYPT usage", ErrInvalidKeyUsage, wrapKey.KeyID)
	}

	wrapKM, err := b.requireKeyMaterial(region, wrapKey.KeyID)
	if err != nil {
		return nil, err
	}

	pairKM, err := generateKeyMaterial(input.KeyPairSpec)
	if err != nil {
		return nil, fmt.Errorf("generating key pair for spec %q: %w", input.KeyPairSpec, err)
	}

	privDER, err := privateKeyPKCS8DER(pairKM)
	if err != nil {
		return nil, err
	}

	pubDER, err := publicKeyDER(pairKM)
	if err != nil {
		return nil, err
	}

	encPriv, err := encryptData(privDER, wrapKey.KeyID, input.EncryptionContext, wrapKM)
	if err != nil {
		return nil, fmt.Errorf("encrypting private key: %w", err)
	}

	return &GenerateDataKeyPairOutput{
		KeyID:                    wrapKey.Arn,
		KeyPairSpec:              input.KeyPairSpec,
		PrivateKeyCiphertextBlob: encPriv,
		PrivateKeyPlaintext:      privDER,
		PublicKey:                pubDER,
	}, nil
}

// GenerateDataKeyPairWithoutPlaintext generates an asymmetric key pair but omits the plaintext
// private key from the response.
func (b *InMemoryBackend) GenerateDataKeyPairWithoutPlaintext(
	ctx context.Context, input *GenerateDataKeyPairWithoutPlaintextInput,
) (*GenerateDataKeyPairWithoutPlaintextOutput, error) {
	out, err := b.GenerateDataKeyPair(ctx, &GenerateDataKeyPairInput{
		KeyID:             input.KeyID,
		KeyPairSpec:       input.KeyPairSpec,
		EncryptionContext: input.EncryptionContext,
	})
	if err != nil {
		return nil, err
	}

	return &GenerateDataKeyPairWithoutPlaintextOutput{
		KeyID:                    out.KeyID,
		KeyPairSpec:              out.KeyPairSpec,
		PrivateKeyCiphertextBlob: out.PrivateKeyCiphertextBlob,
		PublicKey:                out.PublicKey,
	}, nil
}

// GenerateMac computes an HMAC tag over the provided message using an HMAC KMS key.
func (b *InMemoryBackend) GenerateMac(ctx context.Context, input *GenerateMacInput) (*GenerateMacOutput, error) {
	if input.MacAlgorithm == "" {
		return nil, fmt.Errorf("%w: MacAlgorithm must not be empty", ErrValidation)
	}

	b.mu.RLock("GenerateMac")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageGenerateMac {
		return nil, fmt.Errorf(
			"%w: key %q must have KeyUsage=%s for GenerateMac",
			ErrInvalidKeyUsage, key.KeyID, KeyUsageGenerateMac,
		)
	}

	if algErr := validateMacAlgorithm(input.MacAlgorithm, key.KeySpec); algErr != nil {
		return nil, algErr
	}

	km, err := b.requireKeyMaterial(region, key.KeyID)
	if err != nil {
		return nil, err
	}

	mac, err := computeHMAC(input.Message, input.MacAlgorithm, km)
	if err != nil {
		return nil, err
	}

	return &GenerateMacOutput{
		KeyID:        key.Arn,
		Mac:          mac,
		MacAlgorithm: input.MacAlgorithm,
	}, nil
}

// GenerateRandom returns the requested number of cryptographically secure random bytes.
// NumberOfBytes defaults to 32 when not specified; maximum is 1024.
func (b *InMemoryBackend) GenerateRandom(_ context.Context, input *GenerateRandomInput) (*GenerateRandomOutput, error) {
	n := int32(aes256Bytes)

	if input.NumberOfBytes != nil {
		n = *input.NumberOfBytes
	}

	if n <= 0 || n > maxDataKeyBytes {
		return nil, fmt.Errorf(
			"%w: NumberOfBytes must be between 1 and %d, got %d",
			ErrValidation, maxDataKeyBytes, n,
		)
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return nil, fmt.Errorf("generating random bytes: %w", err)
	}

	return &GenerateRandomOutput{Plaintext: buf}, nil
}

// VerifyMac verifies an HMAC tag over the provided message using an HMAC KMS key.
// Returns an error if the MAC does not match; on success returns the key ARN and algorithm.
func (b *InMemoryBackend) VerifyMac(ctx context.Context, input *VerifyMacInput) (*VerifyMacOutput, error) {
	if input.MacAlgorithm == "" {
		return nil, fmt.Errorf("%w: MacAlgorithm must not be empty", ErrValidation)
	}

	b.mu.RLock("VerifyMac")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	key, err := b.lookupKey(ctx, input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, keyStateError(key)
	}

	if key.KeyUsage != KeyUsageGenerateMac {
		return nil, fmt.Errorf(
			"%w: key %q must have KeyUsage=%s for VerifyMac",
			ErrInvalidKeyUsage, key.KeyID, KeyUsageGenerateMac,
		)
	}

	if algErr := validateMacAlgorithm(input.MacAlgorithm, key.KeySpec); algErr != nil {
		return nil, algErr
	}

	km, err := b.requireKeyMaterial(region, key.KeyID)
	if err != nil {
		return nil, err
	}

	expected, err := computeHMAC(input.Message, input.MacAlgorithm, km)
	if err != nil {
		return nil, err
	}

	if !hmacEqual(expected, input.Mac) {
		return nil, fmt.Errorf("%w: MAC verification failed", ErrInvalidSignature)
	}

	return &VerifyMacOutput{
		KeyID:        key.Arn,
		MacAlgorithm: input.MacAlgorithm,
		MacValid:     true,
	}, nil
}

// AddKeyInternal inserts a key directly into the backend without going through CreateKey.
// It also inserts the provided key material if non-nil. This is intended for test seeding only.
// The region is derived from the key ARN, falling back to defaultRegion.
func (b *InMemoryBackend) AddKeyInternal(key *Key, km *keyMaterial) {
	b.mu.Lock("AddKeyInternal")
	defer b.mu.Unlock()

	region := b.keyRegion(key.Arn)
	if region == "" {
		region = b.defaultRegion
	}

	b.keysStore(region)[key.KeyID] = key

	if km != nil {
		b.keyMaterialsStore(region)[key.KeyID] = km
	}
}

// AddCustomKeyStoreInternal inserts a custom key store directly into the backend.
// This is intended for test seeding only.
func (b *InMemoryBackend) AddCustomKeyStoreInternal(ks *CustomKeyStore) {
	b.mu.Lock("AddCustomKeyStoreInternal")
	defer b.mu.Unlock()

	b.customKeyStoresStore(b.defaultRegion)[ks.CustomKeyStoreID] = ks
}
