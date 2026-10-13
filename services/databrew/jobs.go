package databrew

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

func (b *InMemoryBackend) jobARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "job/"+name)
}

func (b *InMemoryBackend) CreateJob(
	ctx context.Context,
	name, jobType, datasetName, projectName, recipeName, roleArn string,
	outputs []Output,
	tags map[string]string,
	extra JobExtras,
) (*Job, error) {
	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	t := b.jobsTable(region)
	if t.Has(name) {
		return nil, existsErr("Job", name)
	}
	if err := b.validateJobResourceRefs(region, datasetName, projectName, recipeName, extra.RecipeVersion); err != nil {
		return nil, err
	}
	if err := validateJobExtras(extra); err != nil {
		return nil, err
	}
	j := &Job{
		Name: name, Arn: b.jobARN(region, name), Type: jobType,
		DatasetName: datasetName, ProjectName: projectName,
		RecipeName: recipeName, RoleArn: roleArn, Outputs: outputs,
		AccountID: b.accountID,
		Tags:      maps.Clone(tags), CreateDate: float64(time.Now().Unix()),
		LastModifiedDate:         float64(time.Now().Unix()),
		CreatedBy:                awsmeta.CallerArn(ctx),
		LastModifiedBy:           awsmeta.CallerArn(ctx),
		ProfileConfiguration:     cloneProfileConfiguration(extra.ProfileConfiguration),
		JobSample:                extra.JobSample,
		EncryptionMode:           extra.EncryptionMode,
		EncryptionKeyArn:         extra.EncryptionKeyArn,
		LogSubscription:          extra.LogSubscription,
		DataCatalogOutputs:       extra.DataCatalogOutputs,
		DatabaseOutputs:          extra.DatabaseOutputs,
		ValidationConfigurations: slices.Clone(extra.ValidationConfigurations),
		MaxCapacity:              extra.MaxCapacity,
		MaxRetries:               extra.MaxRetries,
		Timeout:                  extra.Timeout,
	}
	if recipeName != "" {
		version := extra.RecipeVersion
		if version == "" {
			version = recipeVersionLatestWorking
		}
		j.RecipeReference = &RecipeRef{Name: recipeName, RecipeVersion: version}
	}
	t.Put(j)

	return j, nil
}

func (b *InMemoryBackend) DescribeJob(ctx context.Context, name string) (*Job, error) {
	b.mu.RLock("DescribeJob")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	j, ok := b.jobsTable(region).Get(name)
	if !ok {
		return nil, notFoundErr("Job", name)
	}

	return jobCopy(j), nil
}

func (b *InMemoryBackend) ListJobs(
	ctx context.Context,
	maxResults int,
	nextToken,
	datasetName,
	projectName string,
) ([]*Job, string) {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.jobsTable(region)
	keys := snapshotKeys(t, jobKeyFn)
	var filtered []string
	for _, k := range keys {
		j, _ := t.Get(k)
		if datasetName != "" && j.DatasetName != datasetName {
			continue
		}
		if projectName != "" && j.ProjectName != projectName {
			continue
		}
		filtered = append(filtered, k)
	}
	pageKeys, next := paginateKeys(filtered, maxResults, nextToken)
	out := make([]*Job, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		out = append(out, jobCopy(v))
	}

	return out, next
}

func (b *InMemoryBackend) UpdateJob(
	ctx context.Context,
	name, roleArn string,
	outputs []Output,
	maxCapacity, maxRetries, timeout int,
	extra JobExtras,
) error {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	j, ok := b.jobsTable(region).Get(name)
	if !ok {
		return notFoundErr("Job", name)
	}
	if err := validateJobExtras(extra); err != nil {
		return err
	}
	if roleArn != "" {
		j.RoleArn = roleArn
	}
	if len(outputs) > 0 {
		j.Outputs = outputs
	}
	if maxCapacity > 0 {
		j.MaxCapacity = maxCapacity
	}
	if maxRetries > 0 {
		j.MaxRetries = maxRetries
	}
	if timeout > 0 {
		j.Timeout = timeout
	}
	applyJobExtras(j, extra)
	j.LastModifiedDate = float64(time.Now().Unix())
	stampModifiedBy(ctx, &j.LastModifiedBy)

	return nil
}

// validateJobResourceRefs rejects a CreateJob call that names a dataset,
// project, or recipe that doesn't exist, before any state is mutated.
// CreateProfileJob and CreateRecipeJob both document ResourceNotFoundException
// (aws-sdk-go-v2/service/databrew's deserializers.go:465/960, in
// awsRestjson1_deserializeOpErrorCreateProfileJob/CreateRecipeJob);
// CreateProject's error switch (deserializers.go:626-638) has no
// ResourceNotFoundException case, so ProjectName/RecipeName there are
// deliberately left unvalidated by CreateProject itself. Each name here is
// checked only when non-empty: CreateRecipeJobInput accepts ProjectName as an
// alternative to DatasetName+RecipeReference, so an unset reference is not an
// error.
func (b *InMemoryBackend) validateJobResourceRefs(
	region, datasetName, projectName, recipeName, recipeVersion string,
) error {
	if datasetName != "" && !b.datasetsTable(region).Has(datasetName) {
		return notFoundErr("Dataset", datasetName)
	}
	if projectName != "" && !b.projectsTable(region).Has(projectName) {
		return notFoundErr("Project", projectName)
	}
	if recipeName != "" && !b.recipesTable(region).Has(recipeName) {
		return notFoundErr("Recipe", recipeName)
	}
	if recipeName != "" && !b.recipeVersionExists(region, recipeName, recipeVersion) {
		return notFoundErr("Recipe", recipeName+" version "+recipeVersion)
	}

	return nil
}

func (b *InMemoryBackend) recipeVersionExists(region, name, version string) bool {
	switch version {
	case "", recipeVersionLatestWorking:
		return true
	case recipeVersionLatestPublished:
		return len(b.recipeVersionsStoreRO(region)[name]) > 0
	}

	return recipeVersionIndex(b.recipeVersionsStoreRO(region)[name], version) != -1
}

// validateJobExtras rejects extras values the real service would reject,
// before any caller mutates stored Job state. Enum values and DataCatalog/
// Database output required members are confirmed against botocore
// databrew/2017-07-25 service-2.json (EncryptionMode, LogSubscription,
// SampleMode, DatabaseOutputMode, DataCatalogOutput/DatabaseOutput
// "required" lists).
func validateJobExtras(extra JobExtras) error {
	if err := validateProfileConfiguration(extra.ProfileConfiguration); err != nil {
		return err
	}
	if err := validateValidationConfigurations(extra.ValidationConfigurations); err != nil {
		return err
	}

	if err := validateJobScalarEnums(extra); err != nil {
		return err
	}
	if err := validateDataCatalogOutputs(extra.DataCatalogOutputs); err != nil {
		return err
	}

	return validateDatabaseOutputs(extra.DatabaseOutputs)
}

func validateJobScalarEnums(extra JobExtras) error {
	if extra.EncryptionMode != "" && extra.EncryptionMode != "SSE-KMS" && extra.EncryptionMode != "SSE-S3" {
		return fmt.Errorf("%w: invalid EncryptionMode %q", ErrValidation, extra.EncryptionMode)
	}
	if extra.LogSubscription != "" && extra.LogSubscription != "ENABLE" && extra.LogSubscription != "DISABLE" {
		return fmt.Errorf("%w: invalid LogSubscription %q", ErrValidation, extra.LogSubscription)
	}
	if extra.JobSample != nil && extra.JobSample.Mode != "" &&
		extra.JobSample.Mode != "FULL_DATASET" && extra.JobSample.Mode != "CUSTOM_ROWS" {
		return fmt.Errorf("%w: invalid JobSample.Mode %q", ErrValidation, extra.JobSample.Mode)
	}
	if js := extra.JobSample; js != nil && js.Mode == "CUSTOM_ROWS" && js.Size < 1 {
		return fmt.Errorf("%w: JobSample.Size must be a positive row count when Mode is CUSTOM_ROWS", ErrValidation)
	}

	return nil
}

func validateDataCatalogOutputs(outs []DataCatalogOutput) error {
	for i, o := range outs {
		if o.DatabaseName == "" || o.TableName == "" {
			return fmt.Errorf("%w: DataCatalogOutputs[%d] requires DatabaseName and TableName", ErrValidation, i)
		}
		if o.DatabaseOptions != nil && o.DatabaseOptions.TableName == "" {
			return fmt.Errorf("%w: DataCatalogOutputs[%d].DatabaseOptions requires TableName", ErrValidation, i)
		}
		if o.S3Options != nil && o.S3Options.Location.Bucket == "" {
			return fmt.Errorf("%w: DataCatalogOutputs[%d].S3Options requires Location.Bucket", ErrValidation, i)
		}
		// Overwrite is "Not supported with DatabaseOptions" per
		// aws-sdk-go-v2/service/databrew/types.DataCatalogOutput's doc comment.
		if o.Overwrite && o.DatabaseOptions != nil {
			return fmt.Errorf(
				"%w: DataCatalogOutputs[%d].Overwrite is not supported with DatabaseOptions", ErrValidation, i,
			)
		}
	}

	return nil
}

func validateDatabaseOutputs(outs []DatabaseOutput) error {
	for i, o := range outs {
		if o.GlueConnectionName == "" || o.DatabaseOptions == nil {
			return fmt.Errorf(
				"%w: DatabaseOutputs[%d] requires GlueConnectionName and DatabaseOptions", ErrValidation, i,
			)
		}
		if o.DatabaseOptions.TableName == "" {
			return fmt.Errorf("%w: DatabaseOutputs[%d].DatabaseOptions requires TableName", ErrValidation, i)
		}
		if o.DatabaseOutputMode != "" && o.DatabaseOutputMode != "NEW_TABLE" {
			return fmt.Errorf(
				"%w: invalid DatabaseOutputs[%d].DatabaseOutputMode %q",
				ErrValidation,
				i,
				o.DatabaseOutputMode,
			)
		}
	}

	return nil
}

// applyJobExtras overwrites j's optional fields with any non-zero values in
// extra, leaving fields extra didn't set unchanged. Callers must hold b.mu.
func applyJobExtras(j *Job, extra JobExtras) {
	if extra.ProfileConfiguration != nil {
		j.ProfileConfiguration = cloneProfileConfiguration(extra.ProfileConfiguration)
	}
	if extra.JobSample != nil {
		j.JobSample = extra.JobSample
	}
	if extra.EncryptionMode != "" {
		j.EncryptionMode = extra.EncryptionMode
	}
	if extra.EncryptionKeyArn != "" {
		j.EncryptionKeyArn = extra.EncryptionKeyArn
	}
	if extra.LogSubscription != "" {
		j.LogSubscription = extra.LogSubscription
	}
	if extra.DataCatalogOutputs != nil {
		j.DataCatalogOutputs = extra.DataCatalogOutputs
	}
	if extra.DatabaseOutputs != nil {
		j.DatabaseOutputs = extra.DatabaseOutputs
	}
	if extra.ValidationConfigurations != nil {
		j.ValidationConfigurations = slices.Clone(extra.ValidationConfigurations)
	}
}

func (b *InMemoryBackend) DeleteJob(ctx context.Context, name string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.jobsTable(region).Delete(name) {
		return notFoundErr("Job", name)
	}
	runStore := b.jobRunsStore(region)
	delete(runStore, name)

	return nil
}

const (
	// jobRunTransitionDelay is how long to wait before transitioning a job run to SUCCEEDED.
	jobRunTransitionDelay = 100 * time.Millisecond
	// jobRunDefaultExecTime is the simulated execution time returned in a completed job run.
	jobRunDefaultExecTime = 30
	// jobRunHops is the number of equal-length hops STARTING->RUNNING->SUCCEEDED.
	jobRunHops     = 2
	jobRunStarting = "STARTING"
	jobRunRunning  = "RUNNING"
)

// StartJobRun creates a new job run with STARTING state, transitioning to SUCCEEDED asynchronously.
func (b *InMemoryBackend) StartJobRun(ctx context.Context, jobName string) (*JobRun, error) {
	b.mu.Lock("StartJobRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	j, ok := b.jobsTable(region).Get(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, jobName)
	}

	// Attempt/DataCatalogOutputs/DatabaseOutputs/DatasetName/JobSample/
	// LogSubscription/Outputs/RecipeReference/ValidationConfigurations are
	// real types.JobRun members (deserializers.go's
	// awsRestjson1_deserializeDocumentJobRun) this backend only has one
	// source for: the parent Job's own configuration at the moment the run
	// starts. Attempt is always 1: this backend never retries a run
	// (StartJobRun always transitions STARTING->SUCCEEDED, see
	// jobRunTransitionDelay), so there is never a second attempt to count.
	run := &JobRun{
		JobName:                  jobName,
		RunID:                    uuid.New().String(),
		State:                    jobRunStarting,
		StartedOn:                float64(time.Now().Unix()),
		StartedBy:                awsmeta.CallerArn(ctx),
		Attempt:                  1,
		DatasetName:              j.DatasetName,
		DataCatalogOutputs:       append([]DataCatalogOutput(nil), j.DataCatalogOutputs...),
		DatabaseOutputs:          append([]DatabaseOutput(nil), j.DatabaseOutputs...),
		JobSample:                j.JobSample,
		LogSubscription:          j.LogSubscription,
		Outputs:                  append([]Output(nil), j.Outputs...),
		RecipeReference:          j.RecipeReference,
		ValidationConfigurations: slices.Clone(j.ValidationConfigurations),
	}

	runStore := b.jobRunsStore(region)
	runStore[jobName] = append(runStore[jobName], run)

	half := b.jobRunDelay() / jobRunHops
	b.runDelayed(half, func() {
		b.mu.Lock("StartJobRun.running")
		defer b.mu.Unlock()

		if b.jobRunExists(region, jobName, run.RunID) && run.State == jobRunStarting {
			run.State = jobRunRunning
		}
	})
	b.runDelayed(b.jobRunDelay(), func() {
		b.mu.Lock("StartJobRun.transition")
		defer b.mu.Unlock()
		// Reset may have cleared jobRuns while the transition was pending.
		if !b.jobRunExists(region, jobName, run.RunID) {
			return
		}
		if run.State != jobRunStarting && run.State != jobRunRunning {
			return
		}
		run.State = "SUCCEEDED"
		run.CompletedOn = float64(time.Now().Unix())
		run.ExecutionTime = jobRunDefaultExecTime
	})

	cp := *run

	return &cp, nil
}

// jobRunExists reports whether a run with runID still exists for jobName in the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) jobRunExists(region, jobName, runID string) bool {
	regionRuns := b.jobRuns[region]
	if regionRuns == nil {
		return false
	}
	for _, r := range regionRuns[jobName] {
		if r.RunID == runID {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) ListJobRuns(
	ctx context.Context,
	jobName string,
	maxResults int,
	nextToken string,
) ([]*JobRun, string, error) {
	b.mu.RLock("ListJobRuns")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	if !b.jobsTable(region).Has(jobName) {
		return nil, "", notFoundErr("Job", jobName)
	}

	runStore := b.jobRunsStoreRO(region)
	runs := runStore[jobName]

	// runs are stored in chronological order, ListJobRuns expects reverse chronological
	var reversed []*JobRun
	for _, run := range slices.Backward(runs) {
		cp := *run
		reversed = append(reversed, &cp)
	}

	if maxResults <= 0 {
		maxResults = 100
	}
	startIdx := 0
	if nextToken != "" {
		startIdx = len(reversed)
		for i, r := range reversed {
			// nextToken for runs is RunID (or we can just compare RunID)
			// Wait, the test might rely on RunID for token.
			// Let's assume nextToken is RunID and find the run *after* the token
			if r.RunID == nextToken {
				startIdx = i + 1

				break
			}
		}
	}

	endIdx := startIdx + maxResults
	endIdx = min(endIdx, len(reversed))

	var next string
	if endIdx < len(reversed) {
		next = reversed[endIdx-1].RunID
	}

	if startIdx < len(reversed) {
		return reversed[startIdx:endIdx], next, nil
	}

	return nil, "", nil
}

func (b *InMemoryBackend) StopJobRun(ctx context.Context, name, runID string) (*JobRun, error) {
	b.mu.Lock("StopJobRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	runStore := b.jobRunsStore(region)
	runs, ok := runStore[name]
	if !ok {
		return nil, notFoundErr("Job", name)
	}

	for _, run := range runs {
		if run.RunID == runID {
			if run.State != "SUCCEEDED" && run.State != "FAILED" && run.State != "STOPPED" {
				run.State = "STOPPED"
				run.CompletedOn = float64(time.Now().Unix())
			}
			cp := *run

			return &cp, nil
		}
	}

	return nil, notFoundErr("Job run", runID)
}

func (b *InMemoryBackend) DescribeJobRun(ctx context.Context, name, runID string) (*JobRun, error) {
	b.mu.RLock("DescribeJobRun")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	runStore := b.jobRunsStoreRO(region)
	runs, ok := runStore[name]
	if !ok {
		return nil, notFoundErr("Job", name)
	}

	for _, run := range runs {
		if run.RunID == runID {
			cp := *run

			return &cp, nil
		}
	}

	return nil, notFoundErr("Job run", runID)
}

func jobCopy(j *Job) *Job {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.Outputs = append([]Output(nil), j.Outputs...)
	cp.ProfileConfiguration = cloneProfileConfiguration(j.ProfileConfiguration)
	cp.ValidationConfigurations = slices.Clone(j.ValidationConfigurations)

	return &cp
}

func cloneStatisticsConfiguration(s *StatisticsConfiguration) *StatisticsConfiguration {
	if s == nil {
		return nil
	}
	out := &StatisticsConfiguration{IncludedStatistics: slices.Clone(s.IncludedStatistics)}
	for _, o := range s.Overrides {
		out.Overrides = append(
			out.Overrides,
			StatisticOverride{Statistic: o.Statistic, Parameters: maps.Clone(o.Parameters)},
		)
	}

	return out
}

func cloneProfileConfiguration(p *ProfileConfiguration) *ProfileConfiguration {
	if p == nil {
		return nil
	}
	out := &ProfileConfiguration{
		DatasetStatisticsConfiguration: cloneStatisticsConfiguration(p.DatasetStatisticsConfiguration),
		ProfileColumns:                 slices.Clone(p.ProfileColumns),
	}
	for _, c := range p.ColumnStatisticsConfigurations {
		out.ColumnStatisticsConfigurations = append(out.ColumnStatisticsConfigurations, ColumnStatisticsConfiguration{
			Statistics: cloneStatisticsConfiguration(c.Statistics), Selectors: slices.Clone(c.Selectors),
		})
	}
	if e := p.EntityDetectorConfiguration; e != nil {
		out.EntityDetectorConfiguration = &EntityDetectorConfiguration{EntityTypes: slices.Clone(e.EntityTypes)}
		for _, a := range e.AllowedStatistics {
			out.EntityDetectorConfiguration.AllowedStatistics = append(
				out.EntityDetectorConfiguration.AllowedStatistics,
				AllowedStatistics{Statistics: slices.Clone(a.Statistics)},
			)
		}
	}

	return out
}

// validateProfileConfiguration enforces the SDK's "required" members (databrew@v1.42.4 types.go).
func validateProfileConfiguration(p *ProfileConfiguration) error {
	if p == nil {
		return nil
	}
	for i, c := range p.ColumnStatisticsConfigurations {
		if c.Statistics == nil {
			return fmt.Errorf("%w: ColumnStatisticsConfigurations[%d] requires Statistics", ErrValidation, i)
		}
		if err := validateStatisticOverrides(c.Statistics.Overrides); err != nil {
			return err
		}
	}
	if p.DatasetStatisticsConfiguration != nil {
		if err := validateStatisticOverrides(p.DatasetStatisticsConfiguration.Overrides); err != nil {
			return err
		}
	}
	if e := p.EntityDetectorConfiguration; e != nil {
		if len(e.EntityTypes) == 0 {
			return fmt.Errorf("%w: EntityDetectorConfiguration requires EntityTypes", ErrValidation)
		}
		for i, a := range e.AllowedStatistics {
			if len(a.Statistics) == 0 {
				return fmt.Errorf("%w: AllowedStatistics[%d] requires Statistics", ErrValidation, i)
			}
		}
	}

	return nil
}

func validateStatisticOverrides(os []StatisticOverride) error {
	for i, o := range os {
		if o.Statistic == "" || o.Parameters == nil {
			return fmt.Errorf("%w: Overrides[%d] requires Statistic and Parameters", ErrValidation, i)
		}
	}

	return nil
}

func validateValidationConfigurations(vcs []ValidationConfiguration) error {
	for i, v := range vcs {
		if v.RulesetArn == "" || (v.ValidationMode != "" && v.ValidationMode != "CHECK_ALL") {
			return fmt.Errorf("%w: ValidationConfigurations[%d] needs RulesetArn and mode CHECK_ALL", ErrValidation, i)
		}
	}

	return nil
}

// SetJobRunDelay overrides how long a job run takes to reach SUCCEEDED; d <= 0 restores the built-in default.
func (b *InMemoryBackend) SetJobRunDelay(d time.Duration) { b.runDelayNanos.Store(int64(d)) }

func (b *InMemoryBackend) jobRunDelay() time.Duration {
	if d := time.Duration(b.runDelayNanos.Load()); d > 0 {
		return d
	}

	return jobRunTransitionDelay
}
