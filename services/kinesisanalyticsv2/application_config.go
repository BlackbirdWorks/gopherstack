package kinesisanalyticsv2

import "context"

// AddApplicationCloudWatchLoggingOption adds a CloudWatch logging option to an application.
func (b *InMemoryBackend) AddApplicationCloudWatchLoggingOption(
	ctx context.Context,
	name string, currentVersionID int64, logStreamARN, roleARN string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationCloudWatchLoggingOption")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	app.CloudWatchLoggingOptionDescs = append(
		app.CloudWatchLoggingOptionDescs,
		CloudWatchLoggingOptionDesc{
			CloudWatchLoggingOptionID: b.newResourceID("cwl"),
			LogStreamARN:              logStreamARN,
			RoleARN:                   roleARN,
		},
	)

	return nil
}

// AddApplicationInput adds an input configuration to an application.
func (b *InMemoryBackend) AddApplicationInput(
	ctx context.Context,
	name string, currentVersionID int64, input InputDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationInput")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	input.InputID = b.newResourceID("input")
	app.InputDescriptions = append(app.InputDescriptions, input)

	return nil
}

// AddApplicationInputProcessingConfiguration sets a processing config on an existing input.
func (b *InMemoryBackend) AddApplicationInputProcessingConfiguration(
	ctx context.Context,
	name string,
	currentVersionID int64,
	inputID string,
	config *InputProcessingConfigurationDesc,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationInputProcessingConfiguration")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	// Find input before bumping version to avoid phantom increments on NotFound.
	idx := -1

	for i := range app.InputDescriptions {
		if app.InputDescriptions[i].InputID == inputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	app.InputDescriptions[idx].InputProcessingConfigurationDescription = config

	return nil
}

// AddApplicationOutput adds an output configuration to an application.
func (b *InMemoryBackend) AddApplicationOutput(
	ctx context.Context,
	name string, currentVersionID int64, output OutputDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationOutput")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	output.OutputID = b.newResourceID("output")
	app.OutputDescriptions = append(app.OutputDescriptions, output)

	return nil
}

// AddApplicationReferenceDataSource adds a reference data source to an application.
func (b *InMemoryBackend) AddApplicationReferenceDataSource(
	ctx context.Context,
	name string, currentVersionID int64, ref ReferenceDataSourceDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationReferenceDataSource")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	ref.ReferenceID = b.newResourceID("ref")
	app.ReferenceDataSourceDescriptions = append(app.ReferenceDataSourceDescriptions, ref)

	return nil
}

// AddApplicationVpcConfiguration adds a VPC configuration to an application.
func (b *InMemoryBackend) AddApplicationVpcConfiguration(
	ctx context.Context,
	name string, currentVersionID int64, vpc VpcConfigurationDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationVpcConfiguration")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	vpc.VpcConfigurationID = b.newResourceID("vpc")

	if vpc.SubnetIDs == nil {
		vpc.SubnetIDs = []string{}
	}

	if vpc.SecurityGroupIDs == nil {
		vpc.SecurityGroupIDs = []string{}
	}

	app.VpcConfigurationDescriptions = append(app.VpcConfigurationDescriptions, vpc)

	return nil
}

// DeleteApplicationCloudWatchLoggingOption removes a CloudWatch logging option from an application.
func (b *InMemoryBackend) DeleteApplicationCloudWatchLoggingOption(
	ctx context.Context,
	name string, currentVersionID int64, loggingOptionID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationCloudWatchLoggingOption")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	// Find before bumping to avoid a phantom version increment on NotFound.
	idx := -1

	for i, opt := range app.CloudWatchLoggingOptionDescs {
		if opt.CloudWatchLoggingOptionID == loggingOptionID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	app.CloudWatchLoggingOptionDescs = append(
		app.CloudWatchLoggingOptionDescs[:idx],
		app.CloudWatchLoggingOptionDescs[idx+1:]...,
	)

	return nil
}

// DeleteApplicationInputProcessingConfiguration removes the processing config from an input.
func (b *InMemoryBackend) DeleteApplicationInputProcessingConfiguration(
	ctx context.Context,
	name string, currentVersionID int64, inputID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationInputProcessingConfiguration")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i := range app.InputDescriptions {
		if app.InputDescriptions[i].InputID == inputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	app.InputDescriptions[idx].InputProcessingConfigurationDescription = nil

	return nil
}

// DeleteApplicationOutput removes an output configuration from an application.
func (b *InMemoryBackend) DeleteApplicationOutput(
	ctx context.Context,
	name string, currentVersionID int64, outputID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationOutput")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i, out := range app.OutputDescriptions {
		if out.OutputID == outputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	app.OutputDescriptions = append(
		app.OutputDescriptions[:idx],
		app.OutputDescriptions[idx+1:]...,
	)

	return nil
}

// DeleteApplicationReferenceDataSource removes a reference data source from an application.
func (b *InMemoryBackend) DeleteApplicationReferenceDataSource(
	ctx context.Context,
	name string, currentVersionID int64, referenceID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationReferenceDataSource")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	idx := -1

	for i, ref := range app.ReferenceDataSourceDescriptions {
		if ref.ReferenceID == referenceID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	app.ReferenceDataSourceDescriptions = append(
		app.ReferenceDataSourceDescriptions[:idx],
		app.ReferenceDataSourceDescriptions[idx+1:]...,
	)

	return nil
}

// DeleteApplicationVpcConfiguration removes a VPC configuration from an application.
func (b *InMemoryBackend) DeleteApplicationVpcConfiguration(
	ctx context.Context,
	name string, currentVersionID int64, vpcConfigurationID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationVpcConfiguration")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	idx := -1

	for i, vpc := range app.VpcConfigurationDescriptions {
		if vpc.VpcConfigurationID == vpcConfigurationID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

	app.VpcConfigurationDescriptions = append(
		app.VpcConfigurationDescriptions[:idx],
		app.VpcConfigurationDescriptions[idx+1:]...,
	)

	return nil
}
