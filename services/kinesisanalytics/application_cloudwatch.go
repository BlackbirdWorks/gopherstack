package kinesisanalytics

import (
	"context"
	"fmt"
)

// AddApplicationCloudWatchLoggingOption adds a CloudWatch logging option to an application.
func (b *InMemoryBackend) AddApplicationCloudWatchLoggingOption(
	ctx context.Context, name string, versionID int64, option CloudWatchLoggingOptionDesc,
) error {
	if option.LogStreamARN == "" {
		return fmt.Errorf("%w: CloudWatchLoggingOption.LogStreamARN is required", ErrValidation)
	}

	if option.RoleARN == "" {
		return fmt.Errorf("%w: CloudWatchLoggingOption.RoleARN is required", ErrValidation)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// AddApplicationCloudWatchLoggingOption's modeled error set (verified against
	// aws-sdk-go-v2/service/kinesisanalytics deserializers.go) has no LimitExceededException --
	// this per-application cap violation surfaces as InvalidArgumentException instead.
	if len(app.CloudWatchLoggingOptions) >= maxCWLOptions {
		return fmt.Errorf(
			"%w: maximum of %d CloudWatch logging options per application",
			ErrValidation,
			maxCWLOptions,
		)
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	option.CloudWatchLoggingOptionID = b.newResourceID("cwl")
	app.CloudWatchLoggingOptions = append(app.CloudWatchLoggingOptions, option)

	return nil
}

// DeleteApplicationCloudWatchLoggingOption removes a CloudWatch logging option from an application.
func (b *InMemoryBackend) DeleteApplicationCloudWatchLoggingOption(
	ctx context.Context, name string, versionID int64, loggingOptionID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// Find before bumping to avoid a phantom version increment on NotFound.
	idx := -1

	for i, opt := range app.CloudWatchLoggingOptions {
		if opt.CloudWatchLoggingOptionID == loggingOptionID {
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

	app.CloudWatchLoggingOptions = append(
		app.CloudWatchLoggingOptions[:idx],
		app.CloudWatchLoggingOptions[idx+1:]...,
	)

	return nil
}
