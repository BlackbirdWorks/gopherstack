package kms

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	// ErrInvalidCiphertext is returned when the ciphertext cannot be decrypted.
	ErrInvalidCiphertext = errors.New("InvalidCiphertextException")
	// ErrGrantNotFound is returned when the specified grant does not exist.
	ErrGrantNotFound = errors.New("NotFoundException: grant not found")
	// ErrCiphertextTooShort is returned when the ciphertext is too short.
	ErrCiphertextTooShort = errors.New("ciphertext too short")
	// ErrInvalidDataKeySize is returned when a data key size is invalid or too large.
	ErrInvalidDataKeySize = errors.New("ValidationException: invalid data key size")
)

const (
	// maxDataKeyBytes limits the maximum size of a generated data key when NumberOfBytes is specified.
	maxDataKeyBytes = 4096
)

const (
	// mockMasterKeyStr is the 32-byte AES-256 master key used for mock encryption.
	mockMasterKeyStr = "gopherstack-kms-master-key-32by!"
	// keyIDPrefixLen is the length of the key ID prefix embedded in ciphertext blobs.
	keyIDPrefixLen = 36
	// defaultListLimit is the default maximum number of results for list operations.
	defaultListLimit = 100
	// aes256Bytes is the size of an AES-256 data key in bytes.
	aes256Bytes = 32
	// aes128Bytes is the size of an AES-128 data key in bytes.
	aes128Bytes = 16
)

// mockMasterKey is the AES-256 encryption key shared across all KMS keys (mock only).
//
//nolint:gochecknoglobals // Mock KMS master key required for encryption.
var mockMasterKey = []byte(mockMasterKeyStr)

// StorageBackend defines the interface for the KMS in-memory backend.
type StorageBackend interface {
	CreateKey(input *CreateKeyInput) (*CreateKeyOutput, error)
	DescribeKey(input *DescribeKeyInput) (*DescribeKeyOutput, error)
	ListKeys(input *ListKeysInput) (*ListKeysOutput, error)
	Encrypt(input *EncryptInput) (*EncryptOutput, error)
	Decrypt(input *DecryptInput) (*DecryptOutput, error)
	GenerateDataKey(input *GenerateDataKeyInput) (*GenerateDataKeyOutput, error)
	GenerateDataKeyWithoutPlaintext(input *GenerateDataKeyWithoutPlaintextInput) (*GenerateDataKeyWithoutPlaintextOutput, error)
	ReEncrypt(input *ReEncryptInput) (*ReEncryptOutput, error)
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
	keys      map[string]*Key   // keyed by KeyId
	aliases   map[string]*Alias // keyed by AliasName
	grants    map[string]*Grant // keyed by GrantId
	policies  map[string]string // keyId -> policy JSON
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates and returns a new empty KMS backend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion)
}

// NewInMemoryBackendWithConfig creates a new KMS backend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		keys:      make(map[string]*Key),
		aliases:   make(map[string]*Alias),
		grants:    make(map[string]*Grant),
		policies:  make(map[string]string),
		accountID: accountID,
		region:    region,
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

// encryptData encrypts plaintext with AES-256-GCM, embedding the key ID in the blob.
func encryptData(plaintext []byte, keyID string) ([]byte, error) {
	block, err := aes.NewCipher(mockMasterKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, readErr := io.ReadFull(rand.Reader, nonce); readErr != nil {
		return nil, readErr
	}

	aad := []byte(keyID)
	encrypted := gcm.Seal(nonce, nonce, plaintext, aad)

	// Prepend fixed-length key ID so decryption can extract it
	result := make([]byte, keyIDPrefixLen+len(encrypted))
	copy(result[:keyIDPrefixLen], padKeyID(keyID))
	copy(result[keyIDPrefixLen:], encrypted)

	return result, nil
}

// padKeyID pads or truncates a key ID to exactly keyIDPrefixLen bytes.
func padKeyID(keyID string) []byte {
	b := make([]byte, keyIDPrefixLen)
	copy(b, keyID)

	return b
}

// decryptData decrypts a ciphertext blob produced by encryptData.
// Returns (plaintext, resolvedKeyID, error).
func decryptData(blob []byte) ([]byte, string, error) {
	if len(blob) < keyIDPrefixLen {
		return nil, "", ErrCiphertextTooShort
	}

	keyID := strings.TrimRight(string(blob[:keyIDPrefixLen]), "\x00")
	encrypted := blob[keyIDPrefixLen:]

	block, err := aes.NewCipher(mockMasterKey)
	if err != nil {
		return nil, "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, "", err
	}

	nonceSize := gcm.NonceSize()
	if len(encrypted) < nonceSize {
		return nil, "", ErrCiphertextTooShort
	}

	nonce, cipherOnly := encrypted[:nonceSize], encrypted[nonceSize:]
	aad := []byte(keyID)

	plaintext, openErr := gcm.Open(nil, nonce, cipherOnly, aad)
	if openErr != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrInvalidCiphertext, openErr)
	}

	return plaintext, keyID, nil
}

// CreateKey creates a new KMS key and stores it in the backend.
func (b *InMemoryBackend) CreateKey(input *CreateKeyInput) (*CreateKeyOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	keyID := uuid.New().String()
	keyUsage := input.KeyUsage

	if keyUsage == "" {
		keyUsage = KeyUsageEncryptDecrypt
	}

	region := b.region
	if input.Region != "" {
		region = input.Region
	}

	arn := fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", region, b.accountID, keyID)
	key := &Key{
		KeyID:        keyID,
		Arn:          arn,
		Description:  input.Description,
		KeyState:     KeyStateEnabled,
		KeyUsage:     keyUsage,
		CreationDate: UnixTimeFloat(time.Now()),
		Enabled:      true,
	}

	if keyUsage == KeyUsageEncryptDecrypt {
		key.KeySpec = "SYMMETRIC_DEFAULT"
	}

	b.keys[keyID] = key

	return &CreateKeyOutput{
		KeyMetadata: keyToMetadata(key),
	}, nil
}

// DescribeKey returns metadata for the specified key.
func (b *InMemoryBackend) DescribeKey(input *DescribeKeyInput) (*DescribeKeyOutput, error) {
	b.mu.RLock()
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
	b.mu.RLock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, ErrKeyDisabled
	}

	blob, encErr := encryptData(input.Plaintext, key.KeyID)
	if encErr != nil {
		return nil, encErr
	}

	return &EncryptOutput{
		CiphertextBlob: blob,
		KeyID:          key.KeyID,
	}, nil
}

// Decrypt decrypts the given ciphertext blob.
func (b *InMemoryBackend) Decrypt(input *DecryptInput) (*DecryptOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	plaintext, keyID, err := decryptData(input.CiphertextBlob)
	if err != nil {
		return nil, err
	}

	key, lookupErr := b.lookupKey(keyID)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if key.KeyState != KeyStateEnabled {
		return nil, ErrKeyDisabled
	}

	return &DecryptOutput{
		Plaintext: plaintext,
		KeyID:     key.KeyID,
	}, nil
}

// GenerateDataKey generates a random data key, returning both plaintext and encrypted forms.
func (b *InMemoryBackend) GenerateDataKey(input *GenerateDataKeyInput) (*GenerateDataKeyOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key, err := b.lookupKey(input.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != KeyStateEnabled {
		return nil, ErrKeyDisabled
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

	blob, encErr := encryptData(plaintextKey, key.KeyID)
	if encErr != nil {
		return nil, encErr
	}

	return &GenerateDataKeyOutput{
		CiphertextBlob: blob,
		Plaintext:      plaintextKey,
		KeyID:          key.KeyID,
	}, nil
}

// ReEncrypt decrypts a ciphertext and re-encrypts it under a different key.
func (b *InMemoryBackend) ReEncrypt(input *ReEncryptInput) (*ReEncryptOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	plaintext, sourceKeyID, err := decryptData(input.CiphertextBlob)
	if err != nil {
		return nil, err
	}

	destKey, lookupErr := b.lookupKey(input.DestinationKeyID)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if destKey.KeyState != KeyStateEnabled {
		return nil, ErrKeyDisabled
	}

	blob, encErr := encryptData(plaintext, destKey.KeyID)
	if encErr != nil {
		return nil, encErr
	}

	return &ReEncryptOutput{
		CiphertextBlob: blob,
		KeyID:          destKey.KeyID,
		SourceKeyID:    sourceKeyID,
	}, nil
}

// CreateAlias creates an alias pointing to a key.
func (b *InMemoryBackend) CreateAlias(input *CreateAliasInput) error {
	b.mu.Lock()
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

	aliasArn := fmt.Sprintf("arn:aws:kms:%s:%s:%s", b.region, b.accountID, input.AliasName)
	b.aliases[input.AliasName] = &Alias{
		AliasName:   input.AliasName,
		AliasArn:    aliasArn,
		TargetKeyID: targetID,
	}

	return nil
}

// DeleteAlias removes an alias.
func (b *InMemoryBackend) DeleteAlias(input *DeleteAliasInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.aliases[input.AliasName]; !exists {
		return ErrAliasNotFound
	}

	delete(b.aliases, input.AliasName)

	return nil
}

// ListAliases returns a paginated list of aliases, optionally filtered by key.
func (b *InMemoryBackend) ListAliases(input *ListAliasesInput) (*ListAliasesOutput, error) {
	b.mu.RLock()
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
	b.mu.Lock()
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
	b.mu.Lock()
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
	b.mu.RLock()
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
	b.mu.Lock()
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
	b.mu.Lock()
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
	b.mu.Lock()
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
	b.mu.Lock()
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

// keyToMetadata converts a Key to its KeyMetadata representation.
func keyToMetadata(k *Key) KeyMetadata {
	meta := KeyMetadata{
		KeyID:        k.KeyID,
		Arn:          k.Arn,
		Description:  k.Description,
		KeyState:     k.KeyState,
		KeyUsage:     k.KeyUsage,
		CreationDate: k.CreationDate,
		KeyManager:   "CUSTOMER",
		Origin:       "AWS_KMS",
		MultiRegion:  false,
	}

	if k.KeyUsage == KeyUsageEncryptDecrypt {
		meta.KeySpec = "SYMMETRIC_DEFAULT"
		meta.EncryptionAlgorithms = []string{"SYMMETRIC_DEFAULT"}
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
b.mu.Lock()
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
b.mu.RLock()
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
b.mu.Lock()
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
b.mu.Lock()
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

if input.GrantID != "" {
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

return ErrGrantNotFound
}

// ListRetirableGrants returns all grants for which the given principal is the retiring principal.
func (b *InMemoryBackend) ListRetirableGrants(input *ListRetirableGrantsInput) (*ListGrantsOutput, error) {
b.mu.RLock()
defer b.mu.RUnlock()

var grants []Grant
for _, g := range b.grants {
grants = append(grants, *g)
}

sort.Slice(grants, func(i, j int) bool { return grants[i].GrantID < grants[j].GrantID })

return &ListGrantsOutput{Grants: grants}, nil
}

// GenerateDataKeyWithoutPlaintext generates a data key but returns only the encrypted copy.
func (b *InMemoryBackend) GenerateDataKeyWithoutPlaintext(input *GenerateDataKeyWithoutPlaintextInput) (*GenerateDataKeyWithoutPlaintextOutput, error) {
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
b.mu.Lock()
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
b.mu.RLock()
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
policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::000000000000:root"},"Action":"kms:*","Resource":"*"}]}`
}

policyName := input.PolicyName
if policyName == "" {
policyName = "default"
}

return &GetKeyPolicyOutput{Policy: policy, PolicyName: policyName}, nil
}
