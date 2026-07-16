package kms

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	// minRSAWrappedMaterialBytes is the minimum size of RSA-wrapped key material (RSA-2048 output).
	minRSAWrappedMaterialBytes = 256
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

// StorageBackend defines the interface for the KMS in-memory backend.
type StorageBackend interface {
	CreateKey(ctx context.Context, input *CreateKeyInput) (*CreateKeyOutput, error)
	DescribeKey(ctx context.Context, input *DescribeKeyInput) (*DescribeKeyOutput, error)
	ListKeys(ctx context.Context, input *ListKeysInput) (*ListKeysOutput, error)
	Encrypt(ctx context.Context, input *EncryptInput) (*EncryptOutput, error)
	Decrypt(ctx context.Context, input *DecryptInput) (*DecryptOutput, error)
	GenerateDataKey(
		ctx context.Context,
		input *GenerateDataKeyInput,
	) (*GenerateDataKeyOutput, error)
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
	GetKeyRotationStatus(
		ctx context.Context,
		input *GetKeyRotationStatusInput,
	) (*GetKeyRotationStatusOutput, error)
	DisableKey(ctx context.Context, input *DisableKeyInput) error
	EnableKey(ctx context.Context, input *EnableKeyInput) error
	ScheduleKeyDeletion(
		ctx context.Context,
		input *ScheduleKeyDeletionInput,
	) (*ScheduleKeyDeletionOutput, error)
	CancelKeyDeletion(
		ctx context.Context,
		input *CancelKeyDeletionInput,
	) (*CancelKeyDeletionOutput, error)
	CreateGrant(ctx context.Context, input *CreateGrantInput) (*CreateGrantOutput, error)
	ListGrants(ctx context.Context, input *ListGrantsInput) (*ListGrantsOutput, error)
	RevokeGrant(ctx context.Context, input *RevokeGrantInput) error
	RetireGrant(ctx context.Context, input *RetireGrantInput) error
	ListRetirableGrants(
		ctx context.Context,
		input *ListRetirableGrantsInput,
	) (*ListGrantsOutput, error)
	PutKeyPolicy(ctx context.Context, input *PutKeyPolicyInput) error
	GetKeyPolicy(ctx context.Context, input *GetKeyPolicyInput) (*GetKeyPolicyOutput, error)
	GetParametersForImport(
		ctx context.Context,
		input *GetParametersForImportInput,
	) (*GetParametersForImportOutput, error)
	ListKeyPolicies(
		ctx context.Context,
		input *ListKeyPoliciesInput,
	) (*ListKeyPoliciesOutput, error)
	ListKeyRotations(
		ctx context.Context,
		input *ListKeyRotationsInput,
	) (*ListKeyRotationsOutput, error)
	ImportKeyMaterial(ctx context.Context, input *ImportKeyMaterialInput) error
	DeleteImportedKeyMaterial(ctx context.Context, input *DeleteImportedKeyMaterialInput) error
	ReplicateKey(ctx context.Context, input *ReplicateKeyInput) (*ReplicateKeyOutput, error)
	RotateKeyOnDemand(
		ctx context.Context,
		input *RotateKeyOnDemandInput,
	) (*RotateKeyOnDemandOutput, error)
	ConnectCustomKeyStore(ctx context.Context, input *ConnectCustomKeyStoreInput) error
	CreateCustomKeyStore(
		ctx context.Context,
		input *CreateCustomKeyStoreInput,
	) (*CreateCustomKeyStoreOutput, error)
	DeleteCustomKeyStore(ctx context.Context, input *DeleteCustomKeyStoreInput) error
	DeriveSharedSecret(
		ctx context.Context,
		input *DeriveSharedSecretInput,
	) (*DeriveSharedSecretOutput, error)
	DescribeCustomKeyStores(
		ctx context.Context,
		input *DescribeCustomKeyStoresInput,
	) (*DescribeCustomKeyStoresOutput, error)
	DisconnectCustomKeyStore(ctx context.Context, input *DisconnectCustomKeyStoreInput) error
	UpdateCustomKeyStore(ctx context.Context, input *UpdateCustomKeyStoreInput) error
	UpdateKeyDescription(ctx context.Context, input *UpdateKeyDescriptionInput) error
	UpdatePrimaryRegion(ctx context.Context, input *UpdatePrimaryRegionInput) error
	GenerateDataKeyPair(
		ctx context.Context,
		input *GenerateDataKeyPairInput,
	) (*GenerateDataKeyPairOutput, error)
	GenerateDataKeyPairWithoutPlaintext(
		ctx context.Context, input *GenerateDataKeyPairWithoutPlaintextInput,
	) (*GenerateDataKeyPairWithoutPlaintextOutput, error)
	GenerateMac(ctx context.Context, input *GenerateMacInput) (*GenerateMacOutput, error)
	GenerateRandom(ctx context.Context, input *GenerateRandomInput) (*GenerateRandomOutput, error)
	VerifyMac(ctx context.Context, input *VerifyMacInput) (*VerifyMacOutput, error)
	GetKeyLastUsage(
		ctx context.Context,
		input *GetKeyLastUsageInput,
	) (*GetKeyLastUsageOutput, error)
}

// ensure InMemoryBackend satisfies StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)

// grantRegionStore bundles a region's canonical grants [store.Table] with its
// two secondary indexes. Table.Put/Delete keep both indexes consistent
// automatically, which is what lets RevokeGrant/RetireGrant/janitor purge drop
// a grant with a single table.Delete call instead of the three-map manual
// bookkeeping (grants/grantsByToken/grantsByKey) this replaces.
type grantRegionStore struct {
	table *store.Table[Grant]
	// byToken indexes grants by GrantToken for O(1) lookup on the
	// encrypt/decrypt grant-validation hot path (findGrantByToken).
	byToken *store.Index[Grant]
	// byKey indexes grants by KeyID for O(1) ListGrants and grant-count checks
	// on the CreateGrant hot path.
	byKey *store.Index[Grant]
}

// InMemoryBackend is a concurrency-safe in-memory KMS backend.
type InMemoryBackend struct {
	// keys, aliases, and customKeyStores hold one [store.Table] per AWS
	// region, created lazily by keysStore/aliasesStore/customKeyStoresStore
	// exactly as the plain per-region maps they replace were. grants holds one
	// [grantRegionStore] per region instead, bundling the canonical grants
	// table with its two secondary indexes -- see grantRegionStore.
	keys               map[string]*store.Table[Key]
	aliases            map[string]*store.Table[Alias]
	grants             map[string]*grantRegionStore
	policies           map[string]map[string]string
	keyMaterials       map[string]map[string]*keyMaterial
	keyMaterialHistory map[string]map[string][]*keyMaterial
	customKeyStores    map[string]*store.Table[CustomKeyStore]
	// registry accumulates every per-region table registered by
	// keysStore/aliasesStore/grantsRegion/customKeyStoresStore over the
	// backend's lifetime, so Snapshot/Restore/Reset collapse to one
	// store.Registry call each regardless of how many regions are in use.
	registry      *store.Registry
	mu            *lockmetrics.RWMutex
	accountID     string
	defaultRegion string
	// keyIDResolutionCache maps alias names and ARNs to resolved key UUIDs to avoid
	// repeated aliasesStore lookups on hot paths. Stored as a pointer so clearResolutionCache
	// can swap it in O(1) instead of iterating all entries.
	keyIDResolutionCache *sync.Map
	// lastUsage tracks the last successful cryptographic operation per key.
	// Key format: "region:keyID" → *KeyLastUsageData.
	lastUsage sync.Map
	// importWrappingKeys stores ephemeral RSA private keys generated by GetParametersForImport.
	// Keyed by keyID. Used by ImportKeyMaterial to unwrap RSA-OAEP-encrypted key material.
	// Concurrency-safe via sync.Map; no backend write lock needed.
	importWrappingKeys sync.Map // keyID → *rsa.PrivateKey
}

// NewInMemoryBackend creates and returns a new empty KMS backend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion)
}

// NewInMemoryBackendWithConfig creates a new KMS backend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		keys:                 make(map[string]*store.Table[Key]),
		aliases:              make(map[string]*store.Table[Alias]),
		grants:               make(map[string]*grantRegionStore),
		policies:             make(map[string]map[string]string),
		keyMaterials:         make(map[string]map[string]*keyMaterial),
		keyMaterialHistory:   make(map[string]map[string][]*keyMaterial),
		customKeyStores:      make(map[string]*store.Table[CustomKeyStore]),
		registry:             store.NewRegistry(),
		accountID:            accountID,
		defaultRegion:        region,
		mu:                   lockmetrics.New("kms"),
		keyIDResolutionCache: new(sync.Map),
	}
}

// keysStore returns (registering lazily) the per-region keys table.
func (b *InMemoryBackend) keysStore(region string) *store.Table[Key] {
	if t, ok := b.keys[region]; ok {
		return t
	}

	t := store.Register(b.registry, "keys:"+region, store.New(func(k *Key) string { return k.KeyID }))
	b.keys[region] = t

	return t
}

// aliasesStore returns (registering lazily) the per-region aliases table.
func (b *InMemoryBackend) aliasesStore(region string) *store.Table[Alias] {
	if t, ok := b.aliases[region]; ok {
		return t
	}

	t := store.Register(b.registry, "aliases:"+region, store.New(func(a *Alias) string { return a.AliasName }))
	b.aliases[region] = t

	return t
}

// grantsRegion returns (registering lazily) the per-region grant store, which
// bundles the canonical grants table with its byToken/byKey indexes.
func (b *InMemoryBackend) grantsRegion(region string) *grantRegionStore {
	if g, ok := b.grants[region]; ok {
		return g
	}

	t := store.Register(b.registry, "grants:"+region, store.New(func(g *Grant) string { return g.GrantID }))
	g := &grantRegionStore{
		table:   t,
		byToken: t.AddIndex("byToken", func(g *Grant) string { return g.GrantToken }),
		byKey:   t.AddIndex("byKey", func(g *Grant) string { return g.KeyID }),
	}
	b.grants[region] = g

	return g
}

// grantsStore returns (registering lazily) the per-region canonical grants table.
func (b *InMemoryBackend) grantsStore(region string) *store.Table[Grant] {
	return b.grantsRegion(region).table
}

// policiesStore returns (creating lazily) the per-region policies map.
// Policies remain a plain map: a policy is a bare JSON string with no identity
// field of its own to derive a store.Table key function from.
func (b *InMemoryBackend) policiesStore(region string) map[string]string {
	if m, ok := b.policies[region]; ok {
		return m
	}

	m := make(map[string]string)
	b.policies[region] = m

	return m
}

// keyMaterialsStore returns (creating lazily) the per-region keyMaterials map.
// keyMaterial carries no identity field of its own (it is pure crypto
// material -- see crypto.go), so it remains a plain map keyed externally by
// keyID, exactly as before.
func (b *InMemoryBackend) keyMaterialsStore(region string) map[string]*keyMaterial {
	if m, ok := b.keyMaterials[region]; ok {
		return m
	}

	m := make(map[string]*keyMaterial)
	b.keyMaterials[region] = m

	return m
}

// keyMaterialHistoryStore returns (creating lazily) the per-region
// keyMaterialHistory map. The value is a slice of history entries rather than
// a single identity-bearing struct, so it remains a plain map.
func (b *InMemoryBackend) keyMaterialHistoryStore(region string) map[string][]*keyMaterial {
	if m, ok := b.keyMaterialHistory[region]; ok {
		return m
	}

	m := make(map[string][]*keyMaterial)
	b.keyMaterialHistory[region] = m

	return m
}

// customKeyStoresStore returns (registering lazily) the per-region customKeyStores table.
func (b *InMemoryBackend) customKeyStoresStore(region string) *store.Table[CustomKeyStore] {
	if t, ok := b.customKeyStores[region]; ok {
		return t
	}

	t := store.Register(
		b.registry,
		"customKeyStores:"+region,
		store.New(func(cs *CustomKeyStore) string { return cs.CustomKeyStoreID }),
	)
	b.customKeyStores[region] = t

	return t
}

// cachedResolution is the value stored in keyIDResolutionCache: the resolved
// canonical key UUID plus the region it lives in. A region of "" means "derive
// the region from the request context at load time" -- used for alias inputs,
// whose region is the caller's request region (aliases are region-scoped and the
// bare "alias/name" cache key carries no region of its own). An ARN input stores
// its own embedded region, which is part of the ARN cache key and therefore
// stable across requests regardless of the caller's request region.
type cachedResolution struct {
	keyID  string
	region string
}

// resolveKeyID resolves an alias name or ARN to a plain key UUID and region.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) resolveKeyID(
	ctx context.Context,
	keyID string,
) (string, string, error) {
	ctxRegion := getRegion(ctx, b.defaultRegion)

	if cached, ok := b.keyIDResolutionCache.Load(keyID); ok {
		resolved, resolvedOK := cached.(cachedResolution)
		if !resolvedOK {
			return "", "", fmt.Errorf("%w: invalid key resolution cache entry", ErrValidation)
		}

		// An empty cached region means the resolution is request-region-scoped
		// (alias inputs); fall back to the caller's region. An ARN input caches
		// its own embedded region so it stays correct no matter which region the
		// caller's request targets -- the bug this replaces returned ctxRegion
		// unconditionally, so a cached cross-region ARN resolved into the wrong
		// region's stores on every call after the first.
		region := resolved.region
		if region == "" {
			region = ctxRegion
		}

		return resolved.keyID, region, nil
	}

	if strings.HasPrefix(keyID, "alias/") {
		alias, ok := b.aliasesStore(ctxRegion).Get(keyID)
		if !ok {
			return "", "", ErrAliasNotFound
		}

		b.keyIDResolutionCache.Store(keyID, cachedResolution{keyID: alias.TargetKeyID, region: ""})

		return alias.TargetKeyID, ctxRegion, nil
	}

	if strings.HasPrefix(keyID, "arn:") {
		resolved, arnRegion, arnErr := b.resolveARNKeyID(keyID)
		if arnErr != nil {
			return "", "", arnErr
		}

		b.keyIDResolutionCache.Store(keyID, cachedResolution{keyID: resolved, region: arnRegion})

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
		alias, ok := b.aliasesStore(parsed.Region).Get(parsed.Resource)
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

// clearResolutionCache discards all cached alias/ARN→keyID mappings in O(1) by swapping
// to a fresh map. Only use this when the entire cache must be invalidated (e.g. Reset).
// For targeted invalidation prefer evictAliasesFromCache or a single Delete call.
func (b *InMemoryBackend) clearResolutionCache() {
	b.keyIDResolutionCache = new(sync.Map)
}

// evictAliasesFromCache removes resolution-cache entries for all aliases in region
// that target keyID. Called when a key's state changes so that the next lookup
// re-validates the alias against the live store instead of serving a stale hit.
// Must be called with the write lock held.
func (b *InMemoryBackend) evictAliasesFromCache(region, keyID string) {
	for _, alias := range b.aliasesStore(region).All() {
		if alias.TargetKeyID == keyID {
			b.keyIDResolutionCache.Delete(alias.AliasName)
		}
	}
}

func (b *InMemoryBackend) keyRegion(keyARN string) string {
	parsed, err := awsarn.Parse(keyARN)
	if err != nil {
		return b.defaultRegion
	}

	return parsed.Region
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
		return fmt.Errorf(
			"%w: key %q imported material has expired",
			ErrExpiredKeyMaterial,
			key.KeyID,
		)
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

// resolveKeyAndRegion resolves a key ID, alias, or ARN to the live *Key, the
// canonical key UUID, and the region the key actually lives in. Caller must hold
// at least a read lock.
//
// The returned region is authoritative: for an ARN input it is the ARN's own
// embedded region, for an alias it is the alias's region, and for a bare UUID
// that is not present in the request region it is the region discovered by the
// all-region fallback below. Region-partitioned ops (GetKeyPolicy, PutKeyPolicy,
// CreateGrant, ListGrants, RevokeGrant, RetireGrant) MUST index their secondary
// stores (policiesStore/grantsRegion) with THIS region rather than the request
// region, so that a KeyId ARN carrying a different embedded region than the
// request resolves consistently — the same region-awareness DescribeKey/Encrypt/
// Decrypt already get for free by routing through this helper (via lookupKey).
func (b *InMemoryBackend) resolveKeyAndRegion(
	ctx context.Context,
	keyID string,
) (*Key, string, error) {
	resolved, region, err := b.resolveKeyID(ctx, keyID)
	if err != nil {
		return nil, "", err
	}

	if key, ok := b.keysStore(region).Get(resolved); ok {
		return key, region, nil
	}

	// For plain UUID lookups (not ARN or alias), fall back to searching all regions.
	// This preserves mock compatibility: multi-region tests create replicas in a target
	// region and look them up without specifying the target region in ctx. The region
	// reported back is the key's actual ARN region so region-partitioned callers index
	// the right store.
	if !strings.HasPrefix(keyID, "arn:") && !strings.HasPrefix(keyID, "alias/") {
		if key := b.findKeyInAnyRegion(resolved); key != nil {
			return key, extractRegionFromARN(key.Arn), nil
		}
	}

	return nil, "", ErrKeyNotFound
}

// lookupKey finds a key by ID, alias, or ARN. Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupKey(ctx context.Context, keyID string) (*Key, error) {
	key, _, err := b.resolveKeyAndRegion(ctx, keyID)

	return key, err
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

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// registry.ResetAll empties every already-registered per-region keys/
	// aliases/grants/customKeyStores table in place. It deliberately does NOT
	// unregister them: b.keys/b.aliases/b.grants/b.customKeyStores keep
	// pointing at the same *store.Table/*grantRegionStore instances, so a
	// region touched again after Reset reuses its existing registration
	// instead of re-registering under an already-used name (which would
	// panic -- see store.Register).
	b.registry.ResetAll()
	b.policies = make(map[string]map[string]string)
	b.keyMaterials = make(map[string]map[string]*keyMaterial)
	b.keyMaterialHistory = make(map[string]map[string][]*keyMaterial)
	b.clearResolutionCache()
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

	b.keysStore(region).Put(key)

	if km != nil {
		b.keyMaterialsStore(region)[key.KeyID] = km
	}
}

// AddCustomKeyStoreInternal inserts a custom key store directly into the backend.
// This is intended for test seeding only.
func (b *InMemoryBackend) AddCustomKeyStoreInternal(ks *CustomKeyStore) {
	b.mu.Lock("AddCustomKeyStoreInternal")
	defer b.mu.Unlock()

	b.customKeyStoresStore(b.defaultRegion).Put(ks)
}
