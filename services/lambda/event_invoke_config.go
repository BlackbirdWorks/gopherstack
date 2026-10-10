package lambda

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// maxRetryAttempts is the maximum allowed value for MaximumRetryAttempts.
const maxRetryAttempts = 2

// minEventAgeInSeconds is the minimum allowed value for MaximumEventAgeInSeconds.
const minEventAgeInSeconds = 60

// maxEventAgeInSeconds is the maximum allowed value for MaximumEventAgeInSeconds.
const maxEventAgeInSeconds = 21600

// eventInvokeConfigKey scopes a config to its function, or to a version/alias
// when qualifier is set; the unqualified key stays the bare function name.
func eventInvokeConfigKey(name, qualifier string) string {
	if qualifier == "" {
		return name
	}

	return name + ":" + qualifier
}

// eventInvokeConfigForLocked returns the config scoped to qualifier, falling
// back to the function-level one. Caller holds b.mu.
func (b *InMemoryBackend) eventInvokeConfigForLocked(name, qualifier string) *FunctionEventInvokeConfig {
	if qualifier != "" {
		if cfg, ok := b.eventInvokeConfigs[eventInvokeConfigKey(name, qualifier)]; ok {
			return cfg
		}
	}

	return b.eventInvokeConfigs[name]
}

// eventInvokeConfigArn is the function ARN, suffixed with the qualifier when scoped.
func eventInvokeConfigArn(fn *FunctionConfiguration, qualifier string) string {
	if qualifier == "" {
		return fn.FunctionArn
	}

	return fn.FunctionArn + ":" + qualifier
}

// eventInvokeFunctionLocked resolves name and checks qualifier exists. Caller holds b.mu.
func (b *InMemoryBackend) eventInvokeFunctionLocked(name, qualifier string) (*FunctionConfiguration, error) {
	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if !b.qualifierExistsLocked(name, qualifier) {
		return nil, ErrFunctionNotFound
	}

	return fn, nil
}

// PutFunctionEventInvokeConfig creates or replaces the event invoke configuration for a function.
func (b *InMemoryBackend) PutFunctionEventInvokeConfig(
	name string,
	input *PutFunctionEventInvokeConfigInput,
) (*FunctionEventInvokeConfig, error) {
	return b.PutFunctionEventInvokeConfigQualified(name, "", input)
}

// PutFunctionEventInvokeConfigQualified is PutFunctionEventInvokeConfig scoped to a version or alias.
func (b *InMemoryBackend) PutFunctionEventInvokeConfigQualified(
	name, qualifier string,
	input *PutFunctionEventInvokeConfigInput,
) (*FunctionEventInvokeConfig, error) {
	b.mu.Lock("PutFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	fn, err := b.eventInvokeFunctionLocked(name, qualifier)
	if err != nil {
		return nil, err
	}

	if err = validateEventInvokeConfigInput(input); err != nil {
		return nil, err
	}

	cfg := &FunctionEventInvokeConfig{
		FunctionArn:              eventInvokeConfigArn(fn, qualifier),
		LastModified:             awstime.Epoch(time.Now().UTC()),
		MaximumRetryAttempts:     input.MaximumRetryAttempts,
		MaximumEventAgeInSeconds: input.MaximumEventAgeInSeconds,
		DestinationConfig:        input.DestinationConfig,
	}

	b.eventInvokeConfigs[eventInvokeConfigKey(name, qualifier)] = cfg

	return cfg, nil
}

// GetFunctionEventInvokeConfig returns the event invoke configuration for a function.
func (b *InMemoryBackend) GetFunctionEventInvokeConfig(
	name string,
) (*FunctionEventInvokeConfig, error) {
	return b.GetFunctionEventInvokeConfigQualified(name, "")
}

// GetFunctionEventInvokeConfigQualified is GetFunctionEventInvokeConfig scoped to a version or alias.
func (b *InMemoryBackend) GetFunctionEventInvokeConfigQualified(
	name, qualifier string,
) (*FunctionEventInvokeConfig, error) {
	b.mu.RLock("GetFunctionEventInvokeConfig")
	defer b.mu.RUnlock()

	if _, err := b.eventInvokeFunctionLocked(name, qualifier); err != nil {
		return nil, err
	}

	cfg, ok := b.eventInvokeConfigs[eventInvokeConfigKey(name, qualifier)]
	if !ok {
		return nil, ErrEventInvokeConfigNotFound
	}

	return cfg, nil
}

// UpdateFunctionEventInvokeConfig updates the event invoke configuration for a function.
// It returns ErrEventInvokeConfigNotFound if no config exists yet.
func (b *InMemoryBackend) UpdateFunctionEventInvokeConfig(
	name string,
	input *PutFunctionEventInvokeConfigInput,
) (*FunctionEventInvokeConfig, error) {
	return b.UpdateFunctionEventInvokeConfigQualified(name, "", input)
}

// UpdateFunctionEventInvokeConfigQualified is UpdateFunctionEventInvokeConfig scoped to a version or alias.
func (b *InMemoryBackend) UpdateFunctionEventInvokeConfigQualified(
	name, qualifier string,
	input *PutFunctionEventInvokeConfigInput,
) (*FunctionEventInvokeConfig, error) {
	b.mu.Lock("UpdateFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	fn, err := b.eventInvokeFunctionLocked(name, qualifier)
	if err != nil {
		return nil, err
	}

	cfg, ok := b.eventInvokeConfigs[eventInvokeConfigKey(name, qualifier)]
	if !ok {
		return nil, ErrEventInvokeConfigNotFound
	}

	if err = validateEventInvokeConfigInput(input); err != nil {
		return nil, err
	}

	if input.MaximumRetryAttempts != nil {
		cfg.MaximumRetryAttempts = input.MaximumRetryAttempts
	}

	if input.MaximumEventAgeInSeconds != nil {
		cfg.MaximumEventAgeInSeconds = input.MaximumEventAgeInSeconds
	}

	if input.DestinationConfig != nil {
		cfg.DestinationConfig = input.DestinationConfig
	}

	cfg.FunctionArn = eventInvokeConfigArn(fn, qualifier)
	cfg.LastModified = awstime.Epoch(time.Now().UTC())

	return cfg, nil
}

// DeleteFunctionEventInvokeConfig removes the event invoke configuration for a function.
func (b *InMemoryBackend) DeleteFunctionEventInvokeConfig(name string) error {
	return b.DeleteFunctionEventInvokeConfigQualified(name, "")
}

// DeleteFunctionEventInvokeConfigQualified is DeleteFunctionEventInvokeConfig scoped to a version or alias.
func (b *InMemoryBackend) DeleteFunctionEventInvokeConfigQualified(name, qualifier string) error {
	b.mu.Lock("DeleteFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	if _, err := b.eventInvokeFunctionLocked(name, qualifier); err != nil {
		return err
	}

	key := eventInvokeConfigKey(name, qualifier)
	if _, ok := b.eventInvokeConfigs[key]; !ok {
		return ErrEventInvokeConfigNotFound
	}

	delete(b.eventInvokeConfigs, key)

	return nil
}

// ListFunctionEventInvokeConfigs returns a page of every event invoke configuration of a function,
// the unqualified one first, then each version/alias scoped one by qualifier.
func (b *InMemoryBackend) ListFunctionEventInvokeConfigs(
	name, marker string,
	maxItems int,
) ([]*FunctionEventInvokeConfig, string, error) {
	b.mu.RLock("ListFunctionEventInvokeConfigs")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, "", ErrFunctionNotFound
	}

	var result []*FunctionEventInvokeConfig

	if cfg, ok := b.eventInvokeConfigs[name]; ok {
		result = append(result, cfg)
	}

	prefix := name + ":"
	for _, key := range slices.Sorted(maps.Keys(b.eventInvokeConfigs)) {
		if strings.HasPrefix(key, prefix) {
			result = append(result, b.eventInvokeConfigs[key])
		}
	}

	p := page.New(result, marker, maxItems, lambdaDefaultMaxItems)

	return p.Data, p.Next, nil
}

// validateEventInvokeConfigInput validates MaximumRetryAttempts and MaximumEventAgeInSeconds.
func validateEventInvokeConfigInput(input *PutFunctionEventInvokeConfigInput) error {
	if input.MaximumRetryAttempts != nil {
		v := *input.MaximumRetryAttempts
		if v < 0 || v > maxRetryAttempts {
			return fmt.Errorf(
				"%w: MaximumRetryAttempts must be between 0 and %d",
				ErrInvalidParameterValue,
				maxRetryAttempts,
			)
		}
	}

	if input.MaximumEventAgeInSeconds != nil {
		v := *input.MaximumEventAgeInSeconds
		if v < minEventAgeInSeconds || v > maxEventAgeInSeconds {
			return fmt.Errorf(
				"%w: MaximumEventAgeInSeconds must be between %d and %d",
				ErrInvalidParameterValue, minEventAgeInSeconds, maxEventAgeInSeconds,
			)
		}
	}

	return nil
}
