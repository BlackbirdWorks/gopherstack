package lambda

import (
	"fmt"
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

// PutFunctionEventInvokeConfig creates or replaces the event invoke configuration for a function.
func (b *InMemoryBackend) PutFunctionEventInvokeConfig(
	name string,
	input *PutFunctionEventInvokeConfigInput,
) (*FunctionEventInvokeConfig, error) {
	b.mu.Lock("PutFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if err := validateEventInvokeConfigInput(input); err != nil {
		return nil, err
	}

	cfg := &FunctionEventInvokeConfig{
		FunctionArn:              fn.FunctionArn,
		LastModified:             awstime.Epoch(time.Now().UTC()),
		MaximumRetryAttempts:     input.MaximumRetryAttempts,
		MaximumEventAgeInSeconds: input.MaximumEventAgeInSeconds,
		DestinationConfig:        input.DestinationConfig,
	}

	b.eventInvokeConfigs[name] = cfg

	return cfg, nil
}

// GetFunctionEventInvokeConfig returns the event invoke configuration for a function.
func (b *InMemoryBackend) GetFunctionEventInvokeConfig(
	name string,
) (*FunctionEventInvokeConfig, error) {
	b.mu.RLock("GetFunctionEventInvokeConfig")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.eventInvokeConfigs[name]
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
	b.mu.Lock("UpdateFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.eventInvokeConfigs[name]
	if !ok {
		return nil, ErrEventInvokeConfigNotFound
	}

	if err := validateEventInvokeConfigInput(input); err != nil {
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

	cfg.FunctionArn = fn.FunctionArn
	cfg.LastModified = awstime.Epoch(time.Now().UTC())

	return cfg, nil
}

// DeleteFunctionEventInvokeConfig removes the event invoke configuration for a function.
func (b *InMemoryBackend) DeleteFunctionEventInvokeConfig(name string) error {
	b.mu.Lock("DeleteFunctionEventInvokeConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions.Get(name); !ok {
		return ErrFunctionNotFound
	}

	if _, ok := b.eventInvokeConfigs[name]; !ok {
		return ErrEventInvokeConfigNotFound
	}

	delete(b.eventInvokeConfigs, name)

	return nil
}

// ListFunctionEventInvokeConfigs returns a page of event invoke configurations for a function.
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
		result = []*FunctionEventInvokeConfig{cfg}
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
