package kinesisanalytics

// This file groups AddApplicationOutput/DeleteApplicationOutput together with
// AddApplicationReferenceDataSource/DeleteApplicationReferenceDataSource: with only a per-app
// cap check plus a find-then-append/remove body, the two op pairs are structurally
// near-identical (they previously lived in separate application_outputs.go /
// application_reference_data.go files and tripped golangci-lint's dupl check) -- keeping them
// in one file avoids adding a dupl nolint per the project's parity-principles.

import (
	"context"
	"fmt"
)

// AddApplicationOutput adds an output configuration to an application.
func (b *InMemoryBackend) AddApplicationOutput(
	ctx context.Context, name string, versionID int64, output OutputDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationOutput")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// AddApplicationOutput's modeled error set has no LimitExceededException -- the cap of
	// three outputs per application surfaces as InvalidArgumentException.
	if len(app.Outputs) >= maxOutputs {
		return fmt.Errorf("%w: maximum of %d outputs per application", ErrValidation, maxOutputs)
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	output.OutputID = b.newResourceID("output")
	app.Outputs = append(app.Outputs, output)

	return nil
}

// DeleteApplicationOutput removes an output configuration from an application.
func (b *InMemoryBackend) DeleteApplicationOutput(
	ctx context.Context, name string, versionID int64, outputID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationOutput")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i, out := range app.Outputs {
		if out.OutputID == outputID {
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

	app.Outputs = append(app.Outputs[:idx], app.Outputs[idx+1:]...)

	return nil
}

// AddApplicationReferenceDataSource adds a reference data source to an application.
func (b *InMemoryBackend) AddApplicationReferenceDataSource(
	ctx context.Context, name string, versionID int64, ref ReferenceDataSourceDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationReferenceDataSource")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// AddApplicationReferenceDataSource's modeled error set has no LimitExceededException --
	// the cap of one reference data source per application surfaces as InvalidArgumentException.
	if len(app.ReferenceDataSources) >= maxRefSources {
		return fmt.Errorf("%w: maximum of %d reference data source(s) per application", ErrValidation, maxRefSources)
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	ref.ReferenceID = b.newResourceID("ref")
	app.ReferenceDataSources = append(app.ReferenceDataSources, ref)

	return nil
}

// DeleteApplicationReferenceDataSource removes a reference data source from an application.
func (b *InMemoryBackend) DeleteApplicationReferenceDataSource(
	ctx context.Context, name string, versionID int64, referenceID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationReferenceDataSource")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i, ref := range app.ReferenceDataSources {
		if ref.ReferenceID == referenceID {
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

	app.ReferenceDataSources = append(app.ReferenceDataSources[:idx], app.ReferenceDataSources[idx+1:]...)

	return nil
}
