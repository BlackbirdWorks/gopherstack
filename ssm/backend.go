package ssm

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrParameterNotFound      = errors.New("ParameterNotFound")
	ErrParameterAlreadyExists = errors.New("ParameterAlreadyExists")
	ErrInvalidKeyID           = errors.New("InvalidKeyId")
)

// StorageBackend defines the interface for an SSM Parameter Store backend.
type StorageBackend interface {
	PutParameter(input *PutParameterInput) (*PutParameterOutput, error)
	GetParameter(input *GetParameterInput) (*GetParameterOutput, error)
	GetParameters(input *GetParametersInput) (*GetParametersOutput, error)
	DeleteParameter(input *DeleteParameterInput) (*DeleteParameterOutput, error)
	DeleteParameters(input *DeleteParametersInput) (*DeleteParametersOutput, error)
	ListAll() []Parameter
}

// InMemoryBackend implements StorageBackend using a concurrency-safe map.
type InMemoryBackend struct {
	parameters map[string]Parameter
	mu         sync.RWMutex
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		parameters: make(map[string]Parameter),
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

	param := Parameter{
		Name:             input.Name,
		Type:             input.Type,
		Value:            input.Value,
		Description:      input.Description,
		Version:          version,
		LastModifiedDate: UnixTimeFloat(time.Now()),
	}

	b.parameters[input.Name] = param

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
