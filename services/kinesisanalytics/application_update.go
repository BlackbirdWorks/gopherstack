package kinesisanalytics

import (
	"context"
	"fmt"
	"time"
)

// keepIfEmpty returns v, or old when the update left the member out.
func keepIfEmpty(v, old string) string {
	if v == "" {
		return old
	}

	return v
}

// applyInputUpdates applies input update operations to the application.
func applyInputUpdates(app *Application, updates []inputUpdate) error {
	for _, iu := range updates {
		idx := findInputIndex(app.Inputs, iu.InputID)
		if idx < 0 {
			return fmt.Errorf("%w: InputId %q not found", ErrNotFound, iu.InputID)
		}

		if err := applyOneInputUpdate(&app.Inputs[idx], &iu); err != nil {
			return err
		}
	}

	return nil
}

// applyOneInputUpdate applies a single input update to an InputDescription.
func applyOneInputUpdate(inp *InputDescription, iu *inputUpdate) error {
	if iu.NamePrefixUpdate != "" {
		inp.NamePrefix = iu.NamePrefixUpdate
	}

	if iu.KinesisStreamsInputUpdate != nil {
		var old KinesisStreamsInputDesc
		if inp.KinesisStreamsInputDescription != nil {
			old = *inp.KinesisStreamsInputDescription
		}

		inp.KinesisStreamsInputDescription = &KinesisStreamsInputDesc{
			ResourceARN: keepIfEmpty(iu.KinesisStreamsInputUpdate.ResourceARN, old.ResourceARN),
			RoleARN:     keepIfEmpty(iu.KinesisStreamsInputUpdate.RoleARN, old.RoleARN),
		}
		inp.KinesisFirehoseInputDescription = nil
	}

	if iu.KinesisFirehoseInputUpdate != nil {
		var old KinesisFirehoseInputDesc
		if inp.KinesisFirehoseInputDescription != nil {
			old = *inp.KinesisFirehoseInputDescription
		}

		inp.KinesisFirehoseInputDescription = &KinesisFirehoseInputDesc{
			ResourceARN: keepIfEmpty(iu.KinesisFirehoseInputUpdate.ResourceARN, old.ResourceARN),
			RoleARN:     keepIfEmpty(iu.KinesisFirehoseInputUpdate.RoleARN, old.RoleARN),
		}
		inp.KinesisStreamsInputDescription = nil
	}

	applyInputSchemaUpdate(inp, iu.InputSchemaUpdate)

	if iu.InputProcessingConfigurationUpdate != nil &&
		iu.InputProcessingConfigurationUpdate.InputLambdaProcessor != nil {
		var old LambdaProcessorDesc
		if cur := inp.InputProcessingConfigurationDescription; cur != nil &&
			cur.InputLambdaProcessorDescription != nil {
			old = *cur.InputLambdaProcessorDescription
		}

		upd := iu.InputProcessingConfigurationUpdate.InputLambdaProcessor
		inp.InputProcessingConfigurationDescription = &InputProcessingConfigurationDesc{
			InputLambdaProcessorDescription: &LambdaProcessorDesc{
				ResourceARN: keepIfEmpty(upd.ResourceARN, old.ResourceARN),
				RoleARN:     keepIfEmpty(upd.RoleARN, old.RoleARN),
			},
		}
	}

	if iu.InputParallelismUpdate != nil {
		count := iu.InputParallelismUpdate.Count
		if count < minInputParallelism || count > maxInputParallelism {
			return fmt.Errorf("%w: InputParallelismUpdate.CountUpdate must be %d-%d",
				ErrValidation, minInputParallelism, maxInputParallelism)
		}

		inp.InputParallelism = &InputParallelism{Count: count}
	}

	if inp.InputParallelism != nil {
		inp.InAppStreamNames = inAppStreamNames(inp.NamePrefix, inp.InputParallelism.Count)
	}

	return nil
}

// applyInputSchemaUpdate merges an InputSchemaUpdate payload into an input's schema.
// Unlike ReferenceSchemaUpdate (a whole-object replace using the full SourceSchema shape),
// InputSchemaUpdate carries its own "Update"-suffixed sub-fields and AWS applies it as a
// partial patch: only the sub-fields the caller supplied are overwritten.
func applyInputSchemaUpdate(inp *InputDescription, update *inputSchemaUpdateInput) {
	if update == nil {
		return
	}

	if inp.InputSchema == nil {
		inp.InputSchema = &SourceSchema{}
	}

	if update.RecordFormat != nil {
		inp.InputSchema.RecordFormat = RecordFormat{
			RecordFormatType:  update.RecordFormat.RecordFormatType,
			MappingParameters: update.RecordFormat.MappingParameters,
		}
	}

	if update.RecordEncoding != "" {
		inp.InputSchema.RecordEncoding = update.RecordEncoding
	}

	if update.RecordColumns != nil {
		inp.InputSchema.RecordColumns = update.RecordColumns
	}
}

// applyOutputUpdates applies output update operations to the application.
func applyOutputUpdates(app *Application, updates []outputUpdate) error {
	for _, ou := range updates {
		idx := findOutputIndex(app.Outputs, ou.OutputID)
		if idx < 0 {
			return fmt.Errorf("%w: OutputId %q not found", ErrNotFound, ou.OutputID)
		}

		if err := applyOneOutputUpdate(&app.Outputs[idx], &ou); err != nil {
			return err
		}
	}

	return nil
}

// findOutputIndex returns the index of the output with the given ID, or -1 if not found.
func findOutputIndex(outputs []OutputDescription, outputID string) int {
	for i := range outputs {
		if outputs[i].OutputID == outputID {
			return i
		}
	}

	return -1
}

// applyOneOutputUpdate applies a single output update to an OutputDescription.
func applyOneOutputUpdate(out *OutputDescription, ou *outputUpdate) error {
	if ou.NameUpdate != "" {
		out.Name = ou.NameUpdate
	}

	if ou.KinesisStreamsOutputUpdate != nil {
		var old KinesisStreamsOutputDesc
		if out.KinesisStreamsOutputDescription != nil {
			old = *out.KinesisStreamsOutputDescription
		}

		out.KinesisStreamsOutputDescription = &KinesisStreamsOutputDesc{
			ResourceARN: keepIfEmpty(ou.KinesisStreamsOutputUpdate.ResourceARN, old.ResourceARN),
			RoleARN:     keepIfEmpty(ou.KinesisStreamsOutputUpdate.RoleARN, old.RoleARN),
		}
		out.KinesisFirehoseOutputDescription = nil
		out.LambdaOutputDescription = nil
	}

	if ou.KinesisFirehoseOutputUpdate != nil {
		var old KinesisFirehoseOutputDesc
		if out.KinesisFirehoseOutputDescription != nil {
			old = *out.KinesisFirehoseOutputDescription
		}

		out.KinesisFirehoseOutputDescription = &KinesisFirehoseOutputDesc{
			ResourceARN: keepIfEmpty(ou.KinesisFirehoseOutputUpdate.ResourceARN, old.ResourceARN),
			RoleARN:     keepIfEmpty(ou.KinesisFirehoseOutputUpdate.RoleARN, old.RoleARN),
		}
		out.KinesisStreamsOutputDescription = nil
		out.LambdaOutputDescription = nil
	}

	if ou.LambdaOutputUpdate != nil {
		var old LambdaOutputDesc
		if out.LambdaOutputDescription != nil {
			old = *out.LambdaOutputDescription
		}

		out.LambdaOutputDescription = &LambdaOutputDesc{
			ResourceARN: keepIfEmpty(ou.LambdaOutputUpdate.ResourceARN, old.ResourceARN),
			RoleARN:     keepIfEmpty(ou.LambdaOutputUpdate.RoleARN, old.RoleARN),
		}
		out.KinesisStreamsOutputDescription = nil
		out.KinesisFirehoseOutputDescription = nil
	}

	if ou.DestinationSchemaUpdate != nil {
		ft := ou.DestinationSchemaUpdate.RecordFormatType
		if ft != recordFormatJSON && ft != "CSV" {
			return fmt.Errorf(
				"%w: DestinationSchema.RecordFormatType must be JSON or CSV",
				ErrValidation,
			)
		}

		out.DestinationSchema = &DestinationSchemaDesc{RecordFormatType: ft}
	}

	return nil
}

// applyReferenceDataSourceUpdates applies reference data source updates to the application.
func applyReferenceDataSourceUpdates(
	app *Application,
	updates []referenceDataSourceUpdate,
) error {
	for _, ru := range updates {
		idx := findReferenceIndex(app.ReferenceDataSources, ru.ReferenceID)
		if idx < 0 {
			return fmt.Errorf("%w: ReferenceId %q not found", ErrNotFound, ru.ReferenceID)
		}

		ref := &app.ReferenceDataSources[idx]

		if ru.TableNameUpdate != "" {
			ref.TableName = ru.TableNameUpdate
		}

		if ru.S3ReferenceDataSourceUpdate != nil {
			var old S3ReferenceDataSourceDesc
			if ref.S3ReferenceDataSourceDescription != nil {
				old = *ref.S3ReferenceDataSourceDescription
			}

			upd := ru.S3ReferenceDataSourceUpdate
			ref.S3ReferenceDataSourceDescription = &S3ReferenceDataSourceDesc{
				BucketARN:        keepIfEmpty(upd.BucketARN, old.BucketARN),
				FileKey:          keepIfEmpty(upd.FileKey, old.FileKey),
				ReferenceRoleARN: keepIfEmpty(upd.ReferenceRoleARN, old.ReferenceRoleARN),
			}
		}

		if ru.ReferenceSchemaUpdate != nil {
			schema, err := convertSourceSchema(ru.ReferenceSchemaUpdate)
			if err != nil {
				return err
			}

			ref.ReferenceSchema = &schema
		}
	}

	return nil
}

// findReferenceIndex returns the index of the reference with the given ID, or -1.
func findReferenceIndex(refs []ReferenceDataSourceDescription, refID string) int {
	for i := range refs {
		if refs[i].ReferenceID == refID {
			return i
		}
	}

	return -1
}

// applyCWLOptionUpdates applies CloudWatch logging option updates to the application.
func applyCWLOptionUpdates(
	app *Application,
	updates []cwlOptionUpdate,
) error {
	for _, cu := range updates {
		idx := findCWLOptionIndex(
			app.CloudWatchLoggingOptions,
			cu.CloudWatchLoggingOptionID,
		)
		if idx < 0 {
			return fmt.Errorf(
				"%w: CloudWatchLoggingOptionId %q not found",
				ErrNotFound, cu.CloudWatchLoggingOptionID,
			)
		}

		opt := &app.CloudWatchLoggingOptions[idx]

		if cu.LogStreamARNUpdate != "" {
			opt.LogStreamARN = cu.LogStreamARNUpdate
		}

		if cu.RoleARNUpdate != "" {
			opt.RoleARN = cu.RoleARNUpdate
		}
	}

	return nil
}

// findCWLOptionIndex returns the index of the CWL option with the given ID, or -1.
func findCWLOptionIndex(opts []CloudWatchLoggingOptionDesc, optID string) int {
	for i := range opts {
		if opts[i].CloudWatchLoggingOptionID == optID {
			return i
		}
	}

	return -1
}

// applyUpdate applies the full application update payload. Must be called under b.mu.
func applyUpdate(app *Application, update *applicationUpdate) error {
	if update == nil {
		return nil
	}

	if update.ApplicationCodeUpdate != "" {
		// AWS docs (kinesisanalytics limits page): "The SQL code in an application is
		// limited to 100 KB" -- CreateApplication enforces this via validateApplicationCode
		// but UpdateApplication previously let ApplicationCodeUpdate bypass it entirely.
		if err := validateApplicationCode(update.ApplicationCodeUpdate); err != nil {
			return err
		}

		app.ApplicationCode = update.ApplicationCodeUpdate
	}

	if err := applyInputUpdates(app, update.InputUpdates); err != nil {
		return err
	}

	if err := applyOutputUpdates(app, update.OutputUpdates); err != nil {
		return err
	}

	if err := applyReferenceDataSourceUpdates(app, update.ReferenceDataSourceUpdates); err != nil {
		return err
	}

	return applyCWLOptionUpdates(app, update.CloudWatchLoggingOptionUpdates)
}

// UpdateApplication updates the application with the full update payload and bumps the version.
func (b *InMemoryBackend) UpdateApplication(
	ctx context.Context,
	name string,
	currentVersionID int64,
	update *applicationUpdate,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return nil, ErrNotFound
	}

	if app.ApplicationStatus != statusReady && app.ApplicationStatus != statusRunning {
		return nil, fmt.Errorf(
			"%w: application must be in READY or RUNNING state to update (current: %s)",
			ErrResourceInUse, app.ApplicationStatus,
		)
	}

	if app.ApplicationVersionID != currentVersionID {
		return nil, ErrConcurrentUpdate
	}

	if err := applyUpdate(app, update); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	app.ApplicationVersionID++
	app.LastUpdateTimestamp = &now

	return appCopy(app), nil
}
