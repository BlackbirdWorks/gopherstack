package lambda

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Code signing configs ---

// CreateCodeSigningConfig creates a new Lambda code signing configuration.
func (b *InMemoryBackend) CreateCodeSigningConfig(
	input *CreateCodeSigningConfigInput,
) (*CodeSigningConfig, error) {
	b.mu.Lock("CreateCodeSigningConfig")
	defer b.mu.Unlock()

	b.cscIDCounter++
	cscID := fmt.Sprintf("csc-%08d", b.cscIDCounter)
	cscARN := buildCodeSigningConfigARN(b.region, b.accountID, cscID)
	now := time.Now().UTC().Format(time.RFC3339)

	cfg := &CodeSigningConfig{
		CodeSigningConfigID:  cscID,
		CodeSigningConfigArn: cscARN,
		AllowedPublishers:    input.AllowedPublishers,
		CodeSigningPolicies:  input.CodeSigningPolicies,
		Description:          input.Description,
		LastModified:         now,
	}

	if cfg.AllowedPublishers == nil {
		cfg.AllowedPublishers = &AllowedPublishers{SigningProfileVersionArns: []string{}}
	}

	if cfg.CodeSigningPolicies == nil {
		cfg.CodeSigningPolicies = &CodeSigningPolicies{UntrustedArtifactOnDeployment: "Warn"}
	}

	b.codeSigningConfigs.Put(cfg)

	return cfg, nil
}

// GetCodeSigningConfig retrieves a code signing config by ARN.
func (b *InMemoryBackend) GetCodeSigningConfig(cscARN string) (*CodeSigningConfig, error) {
	b.mu.RLock("GetCodeSigningConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.codeSigningConfigs.Get(cscARN)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	return cfg, nil
}

// DeleteCodeSigningConfig removes a code signing config by ARN.
func (b *InMemoryBackend) DeleteCodeSigningConfig(cscARN string) error {
	b.mu.Lock("DeleteCodeSigningConfig")
	defer b.mu.Unlock()

	if _, ok := b.codeSigningConfigs.Get(cscARN); !ok {
		return ErrFunctionNotFound
	}

	b.codeSigningConfigs.Delete(cscARN)

	return nil
}

// UpdateCodeSigningConfig updates an existing code signing config.
func (b *InMemoryBackend) UpdateCodeSigningConfig(
	cscARN string,
	input *UpdateCodeSigningConfigInput,
) (*CodeSigningConfig, error) {
	b.mu.Lock("UpdateCodeSigningConfig")
	defer b.mu.Unlock()

	cfg, ok := b.codeSigningConfigs.Get(cscARN)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if input.AllowedPublishers != nil {
		cfg.AllowedPublishers = input.AllowedPublishers
	}

	if input.CodeSigningPolicies != nil {
		cfg.CodeSigningPolicies = input.CodeSigningPolicies
	}

	if input.Description != "" {
		cfg.Description = input.Description
	}

	cfg.LastModified = time.Now().UTC().Format(time.RFC3339)
	b.codeSigningConfigs.Put(cfg)

	return cfg, nil
}

// ListCodeSigningConfigs returns all code signing configs.
func (b *InMemoryBackend) ListCodeSigningConfigs(marker string, maxItems int) page.Page[*CodeSigningConfig] {
	b.mu.RLock("ListCodeSigningConfigs")
	defer b.mu.RUnlock()

	cfgs := b.codeSigningConfigs.All()

	sort.Slice(cfgs, func(i, j int) bool {
		return cfgs[i].CodeSigningConfigID < cfgs[j].CodeSigningConfigID
	})

	return page.New(cfgs, marker, maxItems, lambdaDefaultMaxItems)
}

// PutFunctionCodeSigningConfig associates a code signing config with a function.
func (b *InMemoryBackend) PutFunctionCodeSigningConfig(functionName, cscARN string) error {
	b.mu.Lock("PutFunctionCodeSigningConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions.Get(functionName); !ok {
		return ErrFunctionNotFound
	}

	if _, ok := b.codeSigningConfigs.Get(cscARN); !ok {
		return ErrCodeSigningConfigNotFound
	}

	b.fnCodeSigningConfigs[functionName] = cscARN

	return nil
}

// GetFunctionCodeSigningConfig returns the code signing config ARN associated with a function.
func (b *InMemoryBackend) GetFunctionCodeSigningConfig(functionName string) (string, error) {
	b.mu.RLock("GetFunctionCodeSigningConfig")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(functionName); !ok {
		return "", ErrFunctionNotFound
	}

	cscARN, ok := b.fnCodeSigningConfigs[functionName]
	if !ok {
		return "", ErrCodeSigningConfigNotFound
	}

	return cscARN, nil
}

// DeleteFunctionCodeSigningConfig removes the code signing config association from a function.
func (b *InMemoryBackend) DeleteFunctionCodeSigningConfig(functionName string) error {
	b.mu.Lock("DeleteFunctionCodeSigningConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions.Get(functionName); !ok {
		return ErrFunctionNotFound
	}

	delete(b.fnCodeSigningConfigs, functionName)

	return nil
}

// ListFunctionsByCodeSigningConfig returns a page of function ARNs associated with a
// code signing config.
func (b *InMemoryBackend) ListFunctionsByCodeSigningConfig(
	cscARN, marker string, maxItems int,
) (page.Page[string], error) {
	b.mu.RLock("ListFunctionsByCodeSigningConfig")
	defer b.mu.RUnlock()

	if _, ok := b.codeSigningConfigs.Get(cscARN); !ok {
		return page.Page[string]{}, ErrFunctionNotFound
	}

	var arns []string

	for fnName, arn := range b.fnCodeSigningConfigs {
		if arn == cscARN {
			fn, ok := b.functions.Get(fnName)
			if ok {
				arns = append(arns, fn.FunctionArn)
			}
		}
	}

	sort.Strings(arns)

	return page.New(arns, marker, maxItems, lambdaDefaultMaxItems), nil
}
