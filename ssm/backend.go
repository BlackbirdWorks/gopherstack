package ssm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrParameterNotFound      = errors.New("ParameterNotFound")
	ErrParameterAlreadyExists = errors.New("ParameterAlreadyExists")
	ErrInvalidKeyID           = errors.New("InvalidKeyId")
	ErrCiphertextTooShort     = errors.New("ciphertext too short")
)

const (
	SecureStringType  = "SecureString"
	mockKMSKeyStr     = "gopherstack-mock-kms-key-32byte!"
	maxHistoryResults = 50
)

// mockKMSKey is a 32-byte root key for AES-256 encryption (mock KMS).
//
//nolint:gochecknoglobals // Mock KMS key needed for encryption.
var mockKMSKey = []byte(mockKMSKeyStr)

// encryptValue encrypts a value using AES-256 (mock KMS encryption).
func encryptValue(plaintext string) (string, error) {
	block, err := aes.NewCipher(mockKMSKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, nonceErr := io.ReadFull(rand.Reader, nonce); nonceErr != nil {
		return "", nonceErr
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptValue decrypts a value encrypted with encryptValue.
func decryptValue(ciphertext string) (string, error) {
	block, err := aes.NewCipher(mockKMSKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	ciphertextBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertextBytes) < nonceSize {
		return "", ErrCiphertextTooShort
	}

	nonce, ciphertextOnly := ciphertextBytes[:nonceSize], ciphertextBytes[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertextOnly, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// StorageBackend defines the interface for an SSM Parameter Store backend.
type StorageBackend interface {
	PutParameter(input *PutParameterInput) (*PutParameterOutput, error)
	GetParameter(input *GetParameterInput) (*GetParameterOutput, error)
	GetParameters(input *GetParametersInput) (*GetParametersOutput, error)
	DeleteParameter(input *DeleteParameterInput) (*DeleteParameterOutput, error)
	DeleteParameters(input *DeleteParametersInput) (*DeleteParametersOutput, error)
	GetParameterHistory(input *GetParameterHistoryInput) (*GetParameterHistoryOutput, error)
	ListAll() []Parameter
}

// InMemoryBackend implements StorageBackend using a concurrency-safe map.
type InMemoryBackend struct {
	parameters map[string]Parameter
	history    map[string][]ParameterHistory // Stores all versions of each parameter
	mu         sync.RWMutex
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		parameters: make(map[string]Parameter),
		history:    make(map[string][]ParameterHistory),
	}
}

// PutParameter creates or updates a parameter.
func (b *InMemoryBackend) PutParameter(input *PutParameterInput) (*PutParameterOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing, exists := b.parameters[input.Name]
	if exists && !input.Overwrite {
		return nil, ErrParameterAlreadyExists
	}

	version := int64(1)
	if exists {
		version = existing.Version + 1
	}

	// Encrypt if SecureString type
	value := input.Value
	if input.Type == SecureStringType {
		encrypted, err := encryptValue(input.Value)
		if err != nil {
			return nil, err
		}
		value = encrypted
	}

	param := Parameter{
		Name:             input.Name,
		Type:             input.Type,
		Value:            value,
		Description:      input.Description,
		Version:          version,
		LastModifiedDate: UnixTimeFloat(time.Now()),
	}

	b.parameters[input.Name] = param

	// Store in history (store encrypted value for SecureString)
	paramHistory := ParameterHistory{
		Name:             input.Name,
		Type:             input.Type,
		Value:            value,
		Version:          version,
		LastModifiedDate: param.LastModifiedDate,
		Labels:           []string{}, // Placeholder for labels support in future
	}
	b.history[input.Name] = append(b.history[input.Name], paramHistory)

	return &PutParameterOutput{Version: version}, nil
}

// GetParameter retrieves a single parameter.
func (b *InMemoryBackend) GetParameter(input *GetParameterInput) (*GetParameterOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	param, exists := b.parameters[input.Name]
	if !exists {
		return nil, ErrParameterNotFound
	}

	// Decrypt SecureString if WithDecryption is true
	if input.WithDecryption && param.Type == SecureStringType {
		decrypted, err := decryptValue(param.Value)
		if err != nil {
			// If decryption fails, return the parameter with encrypted value
			return &GetParameterOutput{Parameter: param}, nil
		}
		param.Value = decrypted
	}

	return &GetParameterOutput{Parameter: param}, nil
}

// GetParameters retrieves multiple parameters. Missing names are returned as InvalidParameters.
func (b *InMemoryBackend) GetParameters(input *GetParametersInput) (*GetParametersOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	output := &GetParametersOutput{
		Parameters:        make([]Parameter, 0),
		InvalidParameters: make([]string, 0),
	}

	for _, name := range input.Names {
		if param, exists := b.parameters[name]; exists {
			// Decrypt SecureString if WithDecryption is true
			if input.WithDecryption && param.Type == SecureStringType {
				decrypted, err := decryptValue(param.Value)
				if err != nil {
					// If decryption fails, add to invalid parameters
					output.InvalidParameters = append(output.InvalidParameters, name)

					continue
				}
				param.Value = decrypted
			}
			output.Parameters = append(output.Parameters, param)
		} else {
			output.InvalidParameters = append(output.InvalidParameters, name)
		}
	}

	return output, nil
}

// DeleteParameter deletes a single parameter.
func (b *InMemoryBackend) DeleteParameter(input *DeleteParameterInput) (*DeleteParameterOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.parameters[input.Name]; !exists {
		return nil, ErrParameterNotFound
	}

	delete(b.parameters, input.Name)

	return &DeleteParameterOutput{}, nil
}

// DeleteParameters deletes multiple parameters.
func (b *InMemoryBackend) DeleteParameters(input *DeleteParametersInput) (*DeleteParametersOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	output := &DeleteParametersOutput{
		DeletedParameters: make([]string, 0),
		InvalidParameters: make([]string, 0),
	}

	for _, name := range input.Names {
		if _, exists := b.parameters[name]; exists {
			delete(b.parameters, name)
			output.DeletedParameters = append(output.DeletedParameters, name)
		} else {
			output.InvalidParameters = append(output.InvalidParameters, name)
		}
	}

	return output, nil
}

// GetParameterHistory retrieves all versions of a parameter.
func (b *InMemoryBackend) GetParameterHistory(input *GetParameterHistoryInput) (*GetParameterHistoryOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	historyList, exists := b.history[input.Name]
	if !exists {
		return nil, ErrParameterNotFound
	}

	// Default max results to 50
	maxResults := int64(maxHistoryResults)
	if input.MaxResults != nil && *input.MaxResults > 0 && *input.MaxResults < 50 {
		maxResults = *input.MaxResults
	}

	// For simplicity, we'll return results in reverse order (latest first)
	// In a real implementation, NextToken would handle pagination properly
	output := &GetParameterHistoryOutput{
		Parameters: make([]ParameterHistory, 0),
	}

	// Return in reverse order (newest first)
	for i := len(historyList) - 1; i >= 0 && int64(len(output.Parameters)) < maxResults; i-- {
		output.Parameters = append(output.Parameters, historyList[i])
	}

	return output, nil
}

// ListAll returns all parameters sorted by name (useful for Dashboard UI).
func (b *InMemoryBackend) ListAll() []Parameter {
	b.mu.RLock()
	defer b.mu.RUnlock()

	params := make([]Parameter, 0, len(b.parameters))
	for _, p := range b.parameters {
		params = append(params, p)
	}

	sort.Slice(params, func(i, j int) bool {
		return strings.Compare(params[i].Name, params[j].Name) < 0
	})

	return params
}
