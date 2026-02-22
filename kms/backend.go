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
	// ErrCiphertextTooShort is returned when the ciphertext is too short.
	ErrCiphertextTooShort = errors.New("ciphertext too short")
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
	ReEncrypt(input *ReEncryptInput) (*ReEncryptOutput, error)
	CreateAlias(input *CreateAliasInput) error
	DeleteAlias(input *DeleteAliasInput) error
	ListAliases(input *ListAliasesInput) (*ListAliasesOutput, error)
	EnableKeyRotation(input *EnableKeyRotationInput) error
	DisableKeyRotation(input *DisableKeyRotationInput) error
	GetKeyRotationStatus(input *GetKeyRotationStatusInput) (*GetKeyRotationStatusOutput, error)
}

// InMemoryBackend is a concurrency-safe in-memory KMS backend.
type InMemoryBackend struct {
	keys    map[string]*Key   // keyed by KeyId
	aliases map[string]*Alias // keyed by AliasName
	mu      sync.RWMutex
}

// NewInMemoryBackend creates and returns a new empty KMS backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		keys:    make(map[string]*Key),
		aliases: make(map[string]*Alias),
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

	arn := fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", MockRegion, MockAccountID, keyID)
	key := &Key{
		KeyID:        keyID,
		Arn:          arn,
		Description:  input.Description,
		KeyState:     KeyStateEnabled,
		KeyUsage:     keyUsage,
		CreationDate: UnixTimeFloat(time.Now()),
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
		entries = append(entries, KeyListEntry{KeyID: k.KeyID, KeyArn: k.Arn})
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

	keyBytes := dataKeySize(input.KeySpec, input.NumberOfBytes)

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

	aliasArn := fmt.Sprintf("arn:aws:kms:%s:%s:%s", MockRegion, MockAccountID, input.AliasName)
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
	return KeyMetadata{
		KeyID:        k.KeyID,
		Arn:          k.Arn,
		Description:  k.Description,
		KeyState:     k.KeyState,
		KeyUsage:     k.KeyUsage,
		CreationDate: k.CreationDate,
	}
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
