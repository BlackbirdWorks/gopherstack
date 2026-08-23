package kinesisanalytics

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// applicationARN builds the ARN for a Kinesis Analytics application.
func applicationARN(region, accountID, name string) string {
	return arn.Build("kinesisanalytics", region, accountID, "application/"+name)
}

// validateApplicationName returns an error if the name violates AWS rules.
func validateApplicationName(name string) error {
	if len(name) == 0 || len(name) > maxAppNameLen {
		return fmt.Errorf("%w: ApplicationName must be 1-%d characters", ErrValidation, maxAppNameLen)
	}

	if !appNameRegexp.MatchString(name) {
		return fmt.Errorf("%w: ApplicationName must match [a-zA-Z0-9_.\\-]+", ErrValidation)
	}

	return nil
}

// validateApplicationDescription returns an error if the description violates AWS rules.
func validateApplicationDescription(desc string) error {
	if len(desc) > maxAppDescLen {
		return fmt.Errorf("%w: ApplicationDescription must be 0-%d characters", ErrValidation, maxAppDescLen)
	}

	return nil
}

// validateApplicationCode returns an error if the code violates AWS rules.
func validateApplicationCode(code string) error {
	if len(code) > maxAppCodeLen {
		return fmt.Errorf("%w: ApplicationCode must be 0-%d characters", ErrValidation, maxAppCodeLen)
	}

	return nil
}

// inAppStreamNames derives the in-application stream names from a NamePrefix and parallelism count.
func inAppStreamNames(namePrefix string, count int) []string {
	if namePrefix == "" || count <= 0 {
		return nil
	}

	names := make([]string, count)

	for i := range count {
		names[i] = fmt.Sprintf("%s_%03d", namePrefix, i+1)
	}

	return names
}

// convertInputConfig converts a request input config to an InputDescription.
//
// InputSchema is a required member of the real Input shape (verified against
// aws-sdk-go-v2/service/kinesisanalytics/validators.go's validateInput, which -- unlike its
// doc comment alone -- is the authoritative source: it unconditionally requires InputSchema),
// as are ResourceARN/RoleARN on whichever of KinesisStreamsInput/KinesisFirehoseInput is
// supplied (validateKinesisStreamsInput/validateKinesisFirehoseInput).
func convertInputConfig(cfg *applicationInputConfig) (InputDescription, error) {
	var desc InputDescription

	if cfg == nil {
		return desc, nil
	}

	if cfg.NamePrefix == "" {
		return desc, fmt.Errorf("%w: Input.NamePrefix is required", ErrValidation)
	}

	desc.NamePrefix = cfg.NamePrefix

	if err := applyInputSource(&desc, cfg); err != nil {
		return desc, err
	}

	parallelism, err := resolveInputParallelism(cfg.InputParallelism)
	if err != nil {
		return desc, err
	}

	desc.InputParallelism = &InputParallelism{Count: parallelism}
	desc.InAppStreamNames = inAppStreamNames(cfg.NamePrefix, parallelism)

	if cfg.InputSchema == nil {
		return desc, fmt.Errorf("%w: Input.InputSchema is required", ErrValidation)
	}

	schema, err := convertSourceSchema(cfg.InputSchema)
	if err != nil {
		return desc, err
	}

	desc.InputSchema = &schema

	procCfg, err := convertInputProcessingConfig(cfg.InputProcessingConfiguration)
	if err != nil {
		return desc, err
	}

	desc.InputProcessingConfigurationDescription = procCfg

	return desc, nil
}

// applyInputSource enforces mutual exclusion of KinesisStreamsInput/KinesisFirehoseInput and,
// once at most one is confirmed present, validates its required ResourceARN/RoleARN and
// populates the matching *Description field on desc.
func applyInputSource(desc *InputDescription, cfg *applicationInputConfig) error {
	sourceCount := 0
	if cfg.KinesisStreamsInput != nil {
		sourceCount++
	}

	if cfg.KinesisFirehoseInput != nil {
		sourceCount++
	}

	if sourceCount > 1 {
		return fmt.Errorf(
			"%w: exactly one of KinesisStreamsInput or KinesisFirehoseInput must be specified", ErrValidation,
		)
	}

	if cfg.KinesisStreamsInput != nil {
		if err := validateResourceRoleARN(
			"KinesisStreamsInput", cfg.KinesisStreamsInput.ResourceARN, cfg.KinesisStreamsInput.RoleARN,
		); err != nil {
			return err
		}

		desc.KinesisStreamsInputDescription = &KinesisStreamsInputDesc{
			ResourceARN: cfg.KinesisStreamsInput.ResourceARN,
			RoleARN:     cfg.KinesisStreamsInput.RoleARN,
		}
	}

	if cfg.KinesisFirehoseInput != nil {
		if err := validateResourceRoleARN(
			"KinesisFirehoseInput", cfg.KinesisFirehoseInput.ResourceARN, cfg.KinesisFirehoseInput.RoleARN,
		); err != nil {
			return err
		}

		desc.KinesisFirehoseInputDescription = &KinesisFirehoseInputDesc{
			ResourceARN: cfg.KinesisFirehoseInput.ResourceARN,
			RoleARN:     cfg.KinesisFirehoseInput.RoleARN,
		}
	}

	return nil
}

// resolveInputParallelism returns the effective InputParallelism.Count (defaulting to 1 when
// unspecified, matching real AWS behavior) after validating it falls within the allowed range.
func resolveInputParallelism(p *inputParallelismConfig) (int, error) {
	parallelism := 1
	if p != nil {
		parallelism = p.Count
	}

	if parallelism < minInputParallelism || parallelism > maxInputParallelism {
		return 0, fmt.Errorf("%w: InputParallelism.Count must be %d-%d",
			ErrValidation, minInputParallelism, maxInputParallelism)
	}

	return parallelism, nil
}

// validateResourceRoleARN validates the ResourceARN/RoleARN pair required on every
// Kinesis-backed input/output sub-shape (KinesisStreamsInput, KinesisFirehoseInput,
// KinesisStreamsOutput, KinesisFirehoseOutput, LambdaOutput) whenever that sub-shape is
// supplied at all -- verified against the corresponding validate* functions in
// aws-sdk-go-v2/service/kinesisanalytics/validators.go, which all share this exact shape.
func validateResourceRoleARN(shapeName, resourceARN, roleARN string) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: %s.ResourceARN is required", ErrValidation, shapeName)
	}

	if roleARN == "" {
		return fmt.Errorf("%w: %s.RoleARN is required", ErrValidation, shapeName)
	}

	return nil
}

// convertInputProcessingConfig converts a request input-processing-configuration to an
// InputProcessingConfigurationDesc. Real AWS requires InputLambdaProcessor whenever
// InputProcessingConfiguration itself is supplied, and ResourceARN/RoleARN whenever
// InputLambdaProcessor is supplied (aws-sdk-go-v2/service/kinesisanalytics/validators.go's
// validateInputProcessingConfiguration/validateInputLambdaProcessor) -- a request that sends an
// empty InputProcessingConfiguration is rejected, not silently dropped.
func convertInputProcessingConfig(cfg *inputProcessingConfigInput) (*InputProcessingConfigurationDesc, error) {
	if cfg == nil {
		//nolint:nilnil // nil cfg is a valid "no processing configuration" case, not an error.
		return nil, nil
	}

	if cfg.InputLambdaProcessor == nil {
		return nil, fmt.Errorf("%w: InputProcessingConfiguration.InputLambdaProcessor is required", ErrValidation)
	}

	if err := validateResourceRoleARN(
		"InputLambdaProcessor", cfg.InputLambdaProcessor.ResourceARN, cfg.InputLambdaProcessor.RoleARN,
	); err != nil {
		return nil, err
	}

	return &InputProcessingConfigurationDesc{
		InputLambdaProcessorDescription: &LambdaProcessorDesc{
			ResourceARN: cfg.InputLambdaProcessor.ResourceARN,
			RoleARN:     cfg.InputLambdaProcessor.RoleARN,
		},
	}, nil
}

// convertOutputConfig converts a request output config to an OutputDescription.
func convertOutputConfig(cfg *applicationOutputConfig) (OutputDescription, error) {
	var desc OutputDescription

	if cfg == nil {
		return desc, fmt.Errorf("%w: Output is required", ErrValidation)
	}

	if cfg.Name == "" {
		return desc, fmt.Errorf("%w: Output.Name is required", ErrValidation)
	}

	desc.Name = cfg.Name

	// Enforce mutual exclusion of output destination types.
	destCount := 0
	if cfg.KinesisStreamsOutput != nil {
		destCount++
	}

	if cfg.KinesisFirehoseOutput != nil {
		destCount++
	}

	if cfg.LambdaOutput != nil {
		destCount++
	}

	if destCount > 1 {
		return desc, fmt.Errorf(
			"%w: exactly one of KinesisStreamsOutput, KinesisFirehoseOutput, or LambdaOutput must be specified",
			ErrValidation,
		)
	}

	if cfg.KinesisStreamsOutput != nil {
		if err := validateResourceRoleARN(
			"KinesisStreamsOutput", cfg.KinesisStreamsOutput.ResourceARN, cfg.KinesisStreamsOutput.RoleARN,
		); err != nil {
			return desc, err
		}

		desc.KinesisStreamsOutputDescription = &KinesisStreamsOutputDesc{
			ResourceARN: cfg.KinesisStreamsOutput.ResourceARN,
			RoleARN:     cfg.KinesisStreamsOutput.RoleARN,
		}
	}

	if cfg.KinesisFirehoseOutput != nil {
		if err := validateResourceRoleARN(
			"KinesisFirehoseOutput", cfg.KinesisFirehoseOutput.ResourceARN, cfg.KinesisFirehoseOutput.RoleARN,
		); err != nil {
			return desc, err
		}

		desc.KinesisFirehoseOutputDescription = &KinesisFirehoseOutputDesc{
			ResourceARN: cfg.KinesisFirehoseOutput.ResourceARN,
			RoleARN:     cfg.KinesisFirehoseOutput.RoleARN,
		}
	}

	if cfg.LambdaOutput != nil {
		if err := validateResourceRoleARN(
			"LambdaOutput", cfg.LambdaOutput.ResourceARN, cfg.LambdaOutput.RoleARN,
		); err != nil {
			return desc, err
		}

		desc.LambdaOutputDescription = &LambdaOutputDesc{
			ResourceARN: cfg.LambdaOutput.ResourceARN,
			RoleARN:     cfg.LambdaOutput.RoleARN,
		}
	}

	if cfg.DestinationSchema == nil {
		return desc, fmt.Errorf("%w: DestinationSchema is required", ErrValidation)
	}

	if err := validateRecordFormatType(cfg.DestinationSchema.RecordFormatType); err != nil {
		return desc, err
	}

	desc.DestinationSchema = &DestinationSchemaDesc{RecordFormatType: cfg.DestinationSchema.RecordFormatType}

	return desc, nil
}

// validateRecordFormatType returns an error unless ft is a real RecordFormatType enum value.
// aws-sdk-go-v2/service/kinesisanalytics/types.RecordFormatType has exactly two values (JSON,
// CSV) -- shared by both DestinationSchema.RecordFormatType (output) and
// RecordFormat.RecordFormatType (source/reference schema).
func validateRecordFormatType(ft string) error {
	if ft != recordFormatJSON && ft != "CSV" {
		return fmt.Errorf("%w: RecordFormatType must be JSON or CSV", ErrValidation)
	}

	return nil
}

// convertSourceSchema converts a request source schema input to a SourceSchema, validating
// SourceSchema's required members (RecordFormat.RecordFormatType, RecordColumns) and their own
// nested required members, matching aws-sdk-go-v2/service/kinesisanalytics/validators.go's
// validateSourceSchema/validateRecordFormat/validateRecordColumns/validateRecordColumn. Used for
// both Input.InputSchema and ReferenceDataSource.ReferenceSchema (Add path), and for
// ReferenceDataSourceUpdate.ReferenceSchemaUpdate (Update path) -- which, unlike
// InputSchemaUpdate, reuses the full SourceSchema shape verbatim as a whole-object replace, so
// the same required-field contract applies there too.
func convertSourceSchema(s *sourceSchemaInput) (SourceSchema, error) {
	var schema SourceSchema

	if s == nil {
		return schema, fmt.Errorf("%w: SourceSchema is required", ErrValidation)
	}

	if err := validateRecordFormatType(s.RecordFormat.RecordFormatType); err != nil {
		return schema, err
	}

	if err := validateMappingParameters(s.RecordFormat.MappingParameters); err != nil {
		return schema, err
	}

	if s.RecordColumns == nil {
		return schema, fmt.Errorf("%w: SourceSchema.RecordColumns is required", ErrValidation)
	}

	if err := validateRecordColumns(s.RecordColumns); err != nil {
		return schema, err
	}

	schema = SourceSchema{
		RecordEncoding: s.RecordEncoding,
		RecordColumns:  s.RecordColumns,
		RecordFormat: RecordFormat{
			RecordFormatType:  s.RecordFormat.RecordFormatType,
			MappingParameters: s.RecordFormat.MappingParameters,
		},
	}

	return schema, nil
}

// validateMappingParameters validates the optional mapping-parameters sub-object of a
// RecordFormat. JSONMappingParameters.RecordRowPath and CSVMappingParameters.
// RecordRowDelimiter/RecordColumnDelimiter are all required members of their respective shapes
// whenever that shape is supplied at all (validateJSONMappingParameters/
// validateCSVMappingParameters).
func validateMappingParameters(mp *MappingParameters) error {
	if mp == nil {
		return nil
	}

	if mp.JSONMappingParameters != nil && mp.JSONMappingParameters.RecordRowPath == "" {
		return fmt.Errorf("%w: JSONMappingParameters.RecordRowPath is required", ErrValidation)
	}

	if mp.CSVMappingParameters != nil {
		if mp.CSVMappingParameters.RecordRowDelimiter == "" {
			return fmt.Errorf("%w: CSVMappingParameters.RecordRowDelimiter is required", ErrValidation)
		}

		if mp.CSVMappingParameters.RecordColumnDelimiter == "" {
			return fmt.Errorf("%w: CSVMappingParameters.RecordColumnDelimiter is required", ErrValidation)
		}
	}

	return nil
}

// validateRecordColumns validates each RecordColumn's required Name/SqlType members
// (validateRecordColumn).
func validateRecordColumns(cols []RecordColumn) error {
	for i, c := range cols {
		if c.Name == "" {
			return fmt.Errorf("%w: RecordColumns[%d].Name is required", ErrValidation, i)
		}

		if c.SQLType == "" {
			return fmt.Errorf("%w: RecordColumns[%d].SqlType is required", ErrValidation, i)
		}
	}

	return nil
}

// CreateApplication creates a new Kinesis Analytics application.
func (b *InMemoryBackend) CreateApplication(
	ctx context.Context,
	name, description, code string,
	inputs []InputDescription,
	outputs []OutputDescription,
	cwlOptions []CloudWatchLoggingOptionDesc,
	tags map[string]string,
) (*Application, error) {
	if err := validateApplicationName(name); err != nil {
		return nil, err
	}

	if err := validateApplicationDescription(description); err != nil {
		return nil, err
	}

	if err := validateApplicationCode(code); err != nil {
		return nil, err
	}

	// CreateApplication is a modeled source of TooManyTagsException / tag validation errors
	// (same as TagResource), so initial Tags must go through the same validation as a later
	// TagResource call -- previously this was skipped entirely, letting CreateApplication
	// silently accept invalid keys/values and an unbounded tag count.
	if err := validateAndMergeTags(nil, tags); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if len(b.appsByRegion.Get(region)) >= maxApplicationsPerRegion {
		return nil, fmt.Errorf(
			"%w: maximum of %d applications per account/region",
			ErrLimitExceeded, maxApplicationsPerRegion,
		)
	}

	if b.apps.Has(applicationKey(region, name)) {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	t := make(map[string]string)
	maps.Copy(t, tags)

	app := &Application{
		ApplicationName:          name,
		ApplicationARN:           applicationARN(region, b.accountID, name),
		ApplicationDescription:   description,
		ApplicationCode:          code,
		ApplicationStatus:        statusReady,
		ApplicationVersionID:     1,
		CreateTimestamp:          &now,
		LastUpdateTimestamp:      &now,
		Region:                   region,
		Tags:                     t,
		CloudWatchLoggingOptions: make([]CloudWatchLoggingOptionDesc, 0),
		Inputs:                   make([]InputDescription, 0),
		Outputs:                  make([]OutputDescription, 0),
		ReferenceDataSources:     make([]ReferenceDataSourceDescription, 0),
	}

	for _, cwl := range cwlOptions {
		cwl.CloudWatchLoggingOptionID = b.newResourceID("cwl")
		app.CloudWatchLoggingOptions = append(app.CloudWatchLoggingOptions, cwl)
	}

	for _, inp := range inputs {
		inp.InputID = b.newResourceID("input")
		app.Inputs = append(app.Inputs, inp)
	}

	for _, out := range outputs {
		out.OutputID = b.newResourceID("output")
		app.Outputs = append(app.Outputs, out)
	}

	b.apps.Put(app)

	return appCopy(app), nil
}

// DeleteApplication marks an application for deletion (DELETING state) then removes it after a delay.
func (b *InMemoryBackend) DeleteApplication(ctx context.Context, name string, createTimestamp *time.Time) error {
	if createTimestamp == nil {
		return fmt.Errorf("%w: CreateTimestamp is required", ErrValidation)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	appKey := applicationKey(region, name)

	app, exists := b.apps.Get(appKey)
	if !exists {
		return ErrNotFound
	}

	if app.CreateTimestamp != nil && createTimestamp.Unix() != app.CreateTimestamp.Unix() {
		return fmt.Errorf("%w: CreateTimestamp does not match stored value", ErrConcurrentUpdate)
	}

	// Cancel any in-flight lifecycle goroutine.
	key := cancelKey(region, name)
	if cancel, ok := b.cancelFuncs[key]; ok {
		cancel()
		delete(b.cancelFuncs, key)
	}

	now := time.Now().UTC()
	app.ApplicationStatus = statusDeleting
	app.LastUpdateTimestamp = &now

	cancelCtx, cancel := context.WithCancel(b.svcCtx)
	b.cancelFuncs[key] = cancel

	go func() {
		defer cancel()

		select {
		case <-cancelCtx.Done():
			return
		case <-time.After(transitionDelay):
		}

		b.mu.Lock("DeleteApplication.async")
		defer b.mu.Unlock()

		delete(b.cancelFuncs, key)
		b.apps.Delete(appKey)
	}()

	return nil
}

// DescribeApplication returns a copy of the details for a Kinesis Analytics application.
func (b *InMemoryBackend) DescribeApplication(ctx context.Context, name string) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplication")
	defer b.mu.RUnlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return nil, ErrNotFound
	}

	return appCopy(app), nil
}

// ListApplications returns paginated applications.
func (b *InMemoryBackend) ListApplications(
	ctx context.Context,
	exclusiveStart string,
	limit int,
) ([]*Application, bool, error) {
	if limit < 0 {
		return nil, false, fmt.Errorf("%w: Limit must be >= 0", ErrValidation)
	}

	if limit > maxApplicationsPerRegion {
		return nil, false, fmt.Errorf("%w: Limit must not exceed %d", ErrValidation, maxApplicationsPerRegion)
	}

	if limit == 0 {
		limit = maxApplicationsPerRegion
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	regionApps := b.appsByRegion.Get(region)

	all := make([]*Application, 0, len(regionApps))
	all = append(all, regionApps...)

	sort.Slice(all, func(i, j int) bool {
		return all[i].ApplicationName < all[j].ApplicationName
	})

	if exclusiveStart != "" {
		idx := -1

		for i, a := range all {
			if a.ApplicationName == exclusiveStart {
				idx = i

				break
			}
		}

		if idx >= 0 {
			all = all[idx+1:]
		}
	}

	if len(all) > limit {
		return all[:limit], true, nil
	}

	return all, false, nil
}

// validateAndApplyInputConfigs checks that all input configurations reference known input IDs
// and applies starting position configurations. Must be called under b.mu.
func validateAndApplyInputConfigs(app *Application, inputConfigs []inputConfiguration) error {
	for _, ic := range inputConfigs {
		if ic.ID == "" {
			return fmt.Errorf("%w: InputConfigurations[].Id is required", ErrValidation)
		}

		idx := findInputIndex(app.Inputs, ic.ID)
		if idx < 0 {
			return fmt.Errorf("%w: input ID %q not found on application", ErrNotFound, ic.ID)
		}

		if ic.InputStartingPositionConfiguration != nil {
			app.Inputs[idx].InputStartingPositionConfiguration = ic.InputStartingPositionConfiguration
		}
	}

	return nil
}

// findInputIndex returns the index of the input with the given ID, or -1 if not found.
func findInputIndex(inputs []InputDescription, inputID string) int {
	for i := range inputs {
		if inputs[i].InputID == inputID {
			return i
		}
	}

	return -1
}

// StartApplication transitions an application to RUNNING via a STARTING transient state.
func (b *InMemoryBackend) StartApplication(ctx context.Context, name string, inputConfigs []inputConfiguration) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("StartApplication")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	if app.ApplicationStatus != statusReady {
		return fmt.Errorf("%w: application must be in %s state to start (current: %s)",
			ErrResourceInUse, statusReady, app.ApplicationStatus)
	}

	if err := validateAndApplyInputConfigs(app, inputConfigs); err != nil {
		return err
	}

	now := time.Now().UTC()
	app.ApplicationStatus = statusStarting
	app.LastUpdateTimestamp = &now

	b.launchTransition(region, name, statusRunning)

	return nil
}

// StopApplication transitions an application to READY via a STOPPING transient state.
func (b *InMemoryBackend) StopApplication(ctx context.Context, name string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("StopApplication")
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	if app.ApplicationStatus != statusRunning {
		return fmt.Errorf("%w: application must be in %s state to stop (current: %s)",
			ErrResourceInUse, statusRunning, app.ApplicationStatus)
	}

	now := time.Now().UTC()
	app.ApplicationStatus = statusStopping
	app.LastUpdateTimestamp = &now

	b.launchTransition(region, name, statusReady)

	return nil
}

// launchTransition starts a goroutine that moves app to targetStatus after transitionDelay.
// Must be called under b.mu.
func (b *InMemoryBackend) launchTransition(region, name, targetStatus string) {
	key := cancelKey(region, name)

	if cancel, ok := b.cancelFuncs[key]; ok {
		cancel()
	}

	ctx, cancel := context.WithCancel(b.svcCtx)
	b.cancelFuncs[key] = cancel

	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
			return
		case <-time.After(transitionDelay):
		}

		b.mu.Lock("launchTransition.async")
		defer b.mu.Unlock()

		app, exists := b.apps.Get(applicationKey(region, name))
		if !exists {
			return
		}

		now := time.Now().UTC()
		app.ApplicationStatus = targetStatus
		app.LastUpdateTimestamp = &now
		delete(b.cancelFuncs, key)
	}()
}

// appCopy returns a deep copy of the Application, safe to hand to callers.
func appCopy(src *Application) *Application {
	cp := *src

	cp.Tags = maps.Clone(src.Tags)
	cp.CloudWatchLoggingOptions = copyCloudWatchOptions(src.CloudWatchLoggingOptions)
	cp.Inputs = copyInputDescs(src.Inputs)
	cp.Outputs = copyOutputDescs(src.Outputs)
	cp.ReferenceDataSources = copyRefDataSources(src.ReferenceDataSources)

	return &cp
}

func copyCloudWatchOptions(src []CloudWatchLoggingOptionDesc) []CloudWatchLoggingOptionDesc {
	dst := make([]CloudWatchLoggingOptionDesc, len(src))
	copy(dst, src)

	return dst
}

func copyInputParallelism(p *InputParallelism) *InputParallelism {
	if p == nil {
		return nil
	}

	cp := *p

	return &cp
}

func copyInputStartingPosition(s *InputStartingPositionConfiguration) *InputStartingPositionConfiguration {
	if s == nil {
		return nil
	}

	cp := *s

	return &cp
}

func copySourceSchema(s *SourceSchema) *SourceSchema {
	if s == nil {
		return nil
	}

	cp := *s
	cp.RecordColumns = make([]RecordColumn, len(s.RecordColumns))
	copy(cp.RecordColumns, s.RecordColumns)

	if s.RecordFormat.MappingParameters != nil {
		mp := *s.RecordFormat.MappingParameters
		cp.RecordFormat.MappingParameters = &mp
	}

	return &cp
}

func copyInputDescs(src []InputDescription) []InputDescription {
	dst := make([]InputDescription, len(src))

	for i, inp := range src {
		dup := inp

		if inp.InputProcessingConfigurationDescription != nil {
			c := *inp.InputProcessingConfigurationDescription
			if c.InputLambdaProcessorDescription != nil {
				lp := *c.InputLambdaProcessorDescription
				c.InputLambdaProcessorDescription = &lp
			}

			dup.InputProcessingConfigurationDescription = &c
		}

		dup.InputParallelism = copyInputParallelism(inp.InputParallelism)
		dup.InputStartingPositionConfiguration = copyInputStartingPosition(inp.InputStartingPositionConfiguration)
		dup.InputSchema = copySourceSchema(inp.InputSchema)

		if inp.InAppStreamNames != nil {
			dup.InAppStreamNames = make([]string, len(inp.InAppStreamNames))
			copy(dup.InAppStreamNames, inp.InAppStreamNames)
		}

		dst[i] = dup
	}

	return dst
}

func copyOutputDescs(src []OutputDescription) []OutputDescription {
	dst := make([]OutputDescription, len(src))

	for i, out := range src {
		dup := out

		if out.KinesisStreamsOutputDescription != nil {
			s := *out.KinesisStreamsOutputDescription
			dup.KinesisStreamsOutputDescription = &s
		}

		if out.KinesisFirehoseOutputDescription != nil {
			f := *out.KinesisFirehoseOutputDescription
			dup.KinesisFirehoseOutputDescription = &f
		}

		if out.LambdaOutputDescription != nil {
			l := *out.LambdaOutputDescription
			dup.LambdaOutputDescription = &l
		}

		if out.DestinationSchema != nil {
			ds := *out.DestinationSchema
			dup.DestinationSchema = &ds
		}

		dst[i] = dup
	}

	return dst
}

func copyRefDataSources(src []ReferenceDataSourceDescription) []ReferenceDataSourceDescription {
	dst := make([]ReferenceDataSourceDescription, len(src))

	for i, ref := range src {
		dup := ref

		if ref.S3ReferenceDataSourceDescription != nil {
			s3 := *ref.S3ReferenceDataSourceDescription
			dup.S3ReferenceDataSourceDescription = &s3
		}

		dup.ReferenceSchema = copySourceSchema(ref.ReferenceSchema)
		dst[i] = dup
	}

	return dst
}

// AddApplicationInternal is a test-only seed helper that stores an application directly.
// The region is extracted from the application ARN.
func (b *InMemoryBackend) AddApplicationInternal(app *Application) {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	cp := appCopy(app)

	// Extract region from ARN (format: arn:partition:service:region:account:resource).
	region := b.defaultRegion
	const arnFieldCount = 6

	parts := strings.SplitN(cp.ApplicationARN, ":", arnFieldCount)

	if len(parts) == arnFieldCount && parts[3] != "" {
		region = parts[3]
	}

	cp.Region = region

	b.apps.Put(cp)
}
