package kinesisanalytics

import (
	"context"
	"fmt"
)

// AddApplicationInput adds an input configuration to an application.
func (b *InMemoryBackend) AddApplicationInput(
	ctx context.Context, name string, versionID int64, input InputDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationInput")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// AddApplicationInput's modeled error set has no LimitExceededException -- the hard
	// architectural cap of one input per SQL application surfaces as InvalidArgumentException.
	if len(app.Inputs) >= maxInputs {
		return fmt.Errorf("%w: maximum of %d input(s) per application", ErrValidation, maxInputs)
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	input.InputID = b.newResourceID("input")
	app.Inputs = append(app.Inputs, input)

	return nil
}

// AddApplicationInputProcessingConfiguration sets a processing configuration on an existing input.
func (b *InMemoryBackend) AddApplicationInputProcessingConfiguration(
	ctx context.Context, name string, versionID int64, inputID string, config *InputProcessingConfigurationDesc,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationInputProcessingConfiguration")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// Find the input before bumping the version to avoid side-effects on NotFound.
	idx := -1

	for i := range app.Inputs {
		if app.Inputs[i].InputID == inputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	app.Inputs[idx].InputProcessingConfigurationDescription = config

	return nil
}

// DeleteApplicationInputProcessingConfiguration removes the processing config from an input.
func (b *InMemoryBackend) DeleteApplicationInputProcessingConfiguration(
	ctx context.Context, name string, versionID int64, inputID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationInputProcessingConfiguration")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i := range app.Inputs {
		if app.Inputs[i].InputID == inputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	app.Inputs[idx].InputProcessingConfigurationDescription = nil

	return nil
}
