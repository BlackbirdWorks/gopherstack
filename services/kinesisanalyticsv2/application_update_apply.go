package kinesisanalyticsv2

import (
	"crypto/md5" //nolint:gosec // CodeMD5 mirrors AWS's own field: a content checksum, not a security control
	"encoding/hex"
)

// configurationTypeDefault is the Flink CheckpointConfiguration.ConfigurationType
// value under which real AWS forces CheckpointingEnabled/CheckpointInterval/
// MinPauseBetweenCheckpoints to fixed values regardless of what the caller
// requests (see applyCheckpointDefaults). MonitoringConfiguration and
// ParallelismConfiguration also accept a DEFAULT ConfigurationType, but AWS's
// public API documentation does not specify literal forced values for
// those two the way it does for CheckpointConfiguration, so gopherstack
// leaves them as provided rather than fabricating undocumented defaults.
const configurationTypeDefault = "DEFAULT"

// Real AWS's documented DEFAULT values for CheckpointConfiguration (see
// applyCheckpointDefaults).
const (
	defaultCheckpointIntervalMillis         = 60000
	defaultMinPauseBetweenCheckpointsMillis = 5000
)

// --- validation (must run before any version bump; see UpdateApplication) ---

// validateUpdateReferences checks that every sub-resource ID referenced by
// params exists on app, returning ErrNotFound if any is missing. Must run
// before checkAndBumpVersionOrToken so a rejected request never bumps
// ApplicationVersionId (matching the Add*/Delete* config ops' "find before
// bumping" convention elsewhere in this package).
func validateUpdateReferences(app *Application, params UpdateApplicationParams) error {
	for _, u := range params.CloudWatchLoggingOptionUpdates {
		if !hasCWLOption(app, u.CloudWatchLoggingOptionID) {
			return ErrNotFound
		}
	}

	acu := params.ApplicationConfigurationUpdate
	if acu == nil {
		return nil
	}

	for _, u := range acu.VpcConfigurationUpdates {
		if findVpcIndex(app, u.VpcConfigurationID) < 0 {
			return ErrNotFound
		}
	}

	return validateSQLConfigReferences(app, acu.SQLApplicationConfigurationUpdate)
}

func validateSQLConfigReferences(app *Application, upd *SQLApplicationConfigUpdate) error {
	if upd == nil {
		return nil
	}

	for _, u := range upd.InputUpdates {
		if findInputIndex(app, u.InputID) < 0 {
			return ErrNotFound
		}
	}

	for _, u := range upd.OutputUpdates {
		if findOutputIndex(app, u.OutputID) < 0 {
			return ErrNotFound
		}
	}

	for _, u := range upd.ReferenceDataSourceUpdates {
		if findRefIndex(app, u.ReferenceID) < 0 {
			return ErrNotFound
		}
	}

	return nil
}

func hasCWLOption(app *Application, id string) bool {
	for _, o := range app.CloudWatchLoggingOptionDescs {
		if o.CloudWatchLoggingOptionID == id {
			return true
		}
	}

	return false
}

func findInputIndex(app *Application, id string) int {
	for i := range app.InputDescriptions {
		if app.InputDescriptions[i].InputID == id {
			return i
		}
	}

	return -1
}

func findOutputIndex(app *Application, id string) int {
	for i := range app.OutputDescriptions {
		if app.OutputDescriptions[i].OutputID == id {
			return i
		}
	}

	return -1
}

func findRefIndex(app *Application, id string) int {
	for i := range app.ReferenceDataSourceDescriptions {
		if app.ReferenceDataSourceDescriptions[i].ReferenceID == id {
			return i
		}
	}

	return -1
}

func findVpcIndex(app *Application, id string) int {
	for i := range app.VpcConfigurationDescriptions {
		if app.VpcConfigurationDescriptions[i].VpcConfigurationID == id {
			return i
		}
	}

	return -1
}

// --- mutation (only reached after validateUpdateReferences+version bump succeed) ---

// applyApplicationUpdate mutates app in place to reflect every field set in
// params. Every sub-resource ID it dereferences was already confirmed to
// exist by validateUpdateReferences, so none of the helpers below need their
// own not-found handling.
func applyApplicationUpdate(app *Application, params UpdateApplicationParams) {
	applyBasicFields(app, params)
	applyCWLOptionUpdates(app, params.CloudWatchLoggingOptionUpdates)

	if params.ApplicationConfigurationUpdate != nil {
		applyApplicationConfigurationUpdate(app, params.ApplicationConfigurationUpdate)
	}

	applyRunConfigInput(app, params.RunConfigurationUpdate)
}

func applyBasicFields(app *Application, params UpdateApplicationParams) {
	if params.ServiceExecutionRoleUpdate != "" {
		app.ServiceExecutionRole = params.ServiceExecutionRoleUpdate
	}

	if params.RuntimeEnvironmentUpdate != "" {
		app.RuntimeEnvironment = params.RuntimeEnvironmentUpdate
	}
}

func applyCWLOptionUpdates(app *Application, updates []CloudWatchLoggingOptionUpdate) {
	for _, u := range updates {
		for i := range app.CloudWatchLoggingOptionDescs {
			if app.CloudWatchLoggingOptionDescs[i].CloudWatchLoggingOptionID != u.CloudWatchLoggingOptionID {
				continue
			}

			if u.LogStreamARNUpdate != "" {
				app.CloudWatchLoggingOptionDescs[i].LogStreamARN = u.LogStreamARNUpdate
			}

			break
		}
	}
}

func applyApplicationConfigurationUpdate(app *Application, upd *ApplicationConfigurationUpdate) {
	if upd.ApplicationCodeConfigurationUpdate != nil {
		applyCodeConfigUpdate(app, upd.ApplicationCodeConfigurationUpdate)
	}

	if upd.FlinkApplicationConfigurationUpdate != nil {
		applyFlinkConfigUpdate(app, upd.FlinkApplicationConfigurationUpdate)
	}

	if upd.ZeppelinApplicationConfigurationUpdate != nil {
		applyZeppelinConfigUpdate(app, upd.ZeppelinApplicationConfigurationUpdate)
	}

	if upd.HasEnvironmentPropertyUpdates {
		app.EnvironmentPropertyGroups = upd.EnvironmentPropertyUpdates
	}

	if upd.ApplicationSnapshotConfigurationUpdate != nil {
		app.SnapshotsEnabled = upd.ApplicationSnapshotConfigurationUpdate
	}

	if upd.ApplicationSystemRollbackConfigurationUpdate != nil {
		app.RollbackEnabled = upd.ApplicationSystemRollbackConfigurationUpdate
	}

	if upd.ApplicationEncryptionConfigurationUpdate != nil {
		app.EncryptionConfig = upd.ApplicationEncryptionConfigurationUpdate
	}

	applyVpcConfigUpdates(app, upd.VpcConfigurationUpdates)
	applySQLConfigUpdate(app, upd.SQLApplicationConfigurationUpdate)
}

func applyCodeConfigUpdate(app *Application, upd *ApplicationCodeConfigUpdate) {
	if app.CodeConfig == nil {
		app.CodeConfig = &ApplicationCodeConfigDesc{}
	}

	if upd.CodeContentTypeUpdate != "" {
		app.CodeConfig.CodeContentType = upd.CodeContentTypeUpdate
	}

	if upd.CodeContentUpdate == nil {
		return
	}

	cu := upd.CodeContentUpdate
	app.CodeConfig.CodeContentDescription = buildCodeContentDescription(
		ptrString(cu.TextContentUpdate),
		cu.ZipFileContentUpdate,
		ptrString(cu.S3BucketARNUpdate),
		ptrString(cu.S3FileKeyUpdate),
		ptrString(cu.S3ObjectVersionUpdate),
	)
}

func ptrString(v *string) string {
	if v == nil {
		return ""
	}

	return *v
}

// buildCodeContentDescription derives a CodeContentDescription from raw code
// content fields, computing CodeMD5/CodeSize for zip-format code the same
// way real AWS does (an MD5 checksum and byte length of the zip payload).
// Real AWS's CodeContent/CodeContentUpdate expects exactly one of
// text/zip/s3Bucket+s3Key to be populated; the first non-empty one wins.
func buildCodeContentDescription(text string, zip []byte, s3Bucket, s3Key, s3Version string) *CodeContentDescription {
	desc := &CodeContentDescription{}

	switch {
	case text != "":
		desc.TextContent = text
	case len(zip) > 0:
		sum := md5.Sum(zip) //nolint:gosec // see file-level nolint rationale
		desc.CodeMD5 = hex.EncodeToString(sum[:])
		desc.CodeSize = int64(len(zip))
	case s3Bucket != "" || s3Key != "":
		desc.S3ApplicationCodeLocationDescription = &S3CodeLocationDesc{
			BucketARN:     s3Bucket,
			FileKey:       s3Key,
			ObjectVersion: s3Version,
		}
	}

	return desc
}

func applyFlinkConfigUpdate(app *Application, upd *FlinkApplicationConfigUpdate) {
	if app.FlinkConfig == nil {
		app.FlinkConfig = &FlinkApplicationConfigDesc{}
	}

	if upd.CheckpointConfigurationUpdate != nil {
		app.FlinkConfig.CheckpointConfigurationDescription = applyCheckpointConfigUpdate(
			app.FlinkConfig.CheckpointConfigurationDescription, upd.CheckpointConfigurationUpdate)
	}

	if upd.MonitoringConfigurationUpdate != nil {
		app.FlinkConfig.MonitoringConfigurationDescription = applyMonitoringConfigUpdate(
			app.FlinkConfig.MonitoringConfigurationDescription, upd.MonitoringConfigurationUpdate)
	}

	if upd.ParallelismConfigurationUpdate != nil {
		app.FlinkConfig.ParallelismConfigurationDescription = applyParallelismConfigUpdate(
			app.FlinkConfig.ParallelismConfigurationDescription, upd.ParallelismConfigurationUpdate)
	}
}

func applyCheckpointConfigUpdate(cur *CheckpointConfigDesc, upd *CheckpointConfigUpdate) *CheckpointConfigDesc {
	if cur == nil {
		cur = &CheckpointConfigDesc{}
	}

	if upd.ConfigurationTypeUpdate != "" {
		cur.ConfigurationType = upd.ConfigurationTypeUpdate
	}

	if upd.CheckpointingEnabledUpdate != nil {
		cur.CheckpointingEnabled = upd.CheckpointingEnabledUpdate
	}

	if upd.CheckpointIntervalUpdate != nil {
		cur.CheckpointInterval = upd.CheckpointIntervalUpdate
	}

	if upd.MinPauseBetweenCheckpointsUpdate != nil {
		cur.MinPauseBetweenCheckpoints = upd.MinPauseBetweenCheckpointsUpdate
	}

	return applyCheckpointDefaults(cur)
}

// applyCheckpointDefaults forces CheckpointingEnabled/CheckpointInterval/
// MinPauseBetweenCheckpoints to real AWS's documented DEFAULT values
// whenever ConfigurationType is DEFAULT, "even if they are set to other
// values using APIs or application code" (verbatim from the real API's
// CheckpointConfiguration.ConfigurationType documentation).
func applyCheckpointDefaults(c *CheckpointConfigDesc) *CheckpointConfigDesc {
	if c.ConfigurationType != configurationTypeDefault {
		return c
	}

	enabled, interval, pause := true, int64(
		defaultCheckpointIntervalMillis,
	), int64(
		defaultMinPauseBetweenCheckpointsMillis,
	)
	c.CheckpointingEnabled = &enabled
	c.CheckpointInterval = &interval
	c.MinPauseBetweenCheckpoints = &pause

	return c
}

func applyMonitoringConfigUpdate(cur *MonitoringConfigDesc, upd *MonitoringConfigUpdate) *MonitoringConfigDesc {
	if cur == nil {
		cur = &MonitoringConfigDesc{}
	}

	if upd.ConfigurationTypeUpdate != "" {
		cur.ConfigurationType = upd.ConfigurationTypeUpdate
	}

	if upd.LogLevelUpdate != "" {
		cur.LogLevel = upd.LogLevelUpdate
	}

	if upd.MetricsLevelUpdate != "" {
		cur.MetricsLevel = upd.MetricsLevelUpdate
	}

	return cur
}

func applyParallelismConfigUpdate(cur *ParallelismConfigDesc, upd *ParallelismConfigUpdate) *ParallelismConfigDesc {
	if cur == nil {
		cur = &ParallelismConfigDesc{}
	}

	if upd.ConfigurationTypeUpdate != "" {
		cur.ConfigurationType = upd.ConfigurationTypeUpdate
	}

	if upd.AutoScalingEnabledUpdate != nil {
		cur.AutoScalingEnabled = upd.AutoScalingEnabledUpdate
	}

	if upd.ParallelismUpdate != nil {
		cur.Parallelism = upd.ParallelismUpdate
		cur.CurrentParallelism = upd.ParallelismUpdate
	}

	if upd.ParallelismPerKPUUpdate != nil {
		cur.ParallelismPerKPU = upd.ParallelismPerKPUUpdate
	}

	return cur
}

// applyZeppelinConfigUpdate mirrors applyFlinkConfigUpdate's merge-not-replace
// pattern for ZeppelinApplicationConfigurationUpdate's four sub-updates.
// MonitoringConfigurationDescription is a required member of the real
// ZeppelinApplicationConfigurationDescription (botocore
// kinesisanalyticsv2/2018-05-23/service-2.json.gz), so ZeppelinConfig is
// only initialized once a monitoring/catalog/deploy/artifact update actually
// arrives -- never left as an empty non-nil struct with a nil monitoring field.
func applyZeppelinConfigUpdate(app *Application, upd *ZeppelinApplicationConfigUpdate) {
	if app.ZeppelinConfig == nil {
		app.ZeppelinConfig = &ZeppelinApplicationConfigDescription{}
	}

	if upd.MonitoringConfigurationUpdate != nil {
		if app.ZeppelinConfig.MonitoringConfigurationDescription == nil {
			app.ZeppelinConfig.MonitoringConfigurationDescription = &ZeppelinMonitoringConfigDesc{}
		}

		app.ZeppelinConfig.MonitoringConfigurationDescription.LogLevel = upd.MonitoringConfigurationUpdate.LogLevelUpdate
	}

	if upd.CatalogConfigurationUpdate != nil {
		app.ZeppelinConfig.CatalogConfigurationDescription = &CatalogConfigDescription{
			GlueDataCatalogConfigurationDescription: &GlueDataCatalogConfigDesc{
				DatabaseARN: upd.CatalogConfigurationUpdate.DatabaseARNUpdate,
			},
		}
	}

	if upd.DeployAsApplicationConfigurationUpdate != nil {
		applyDeployAsApplicationConfigUpdate(app.ZeppelinConfig, upd.DeployAsApplicationConfigurationUpdate)
	}

	if upd.HasCustomArtifactsConfigurationUpdate {
		app.ZeppelinConfig.CustomArtifactsConfigurationDescription = upd.CustomArtifactsConfigurationUpdate
	}
}

func applyDeployAsApplicationConfigUpdate(
	zc *ZeppelinApplicationConfigDescription, upd *DeployAsApplicationConfigUpdate,
) {
	cur := zc.DeployAsApplicationConfigurationDescription
	if cur == nil {
		cur = &DeployAsApplicationConfigDescription{}
	}

	if cur.S3ContentLocationDescription == nil {
		cur.S3ContentLocationDescription = &S3ContentBaseLocationDesc{}
	}

	if upd.BucketARNUpdate != "" {
		cur.S3ContentLocationDescription.BucketARN = upd.BucketARNUpdate
	}

	if upd.BasePathUpdate != "" {
		cur.S3ContentLocationDescription.BasePath = upd.BasePathUpdate
	}

	zc.DeployAsApplicationConfigurationDescription = cur
}

func applySQLConfigUpdate(app *Application, upd *SQLApplicationConfigUpdate) {
	if upd == nil {
		return
	}

	for _, u := range upd.InputUpdates {
		applyInputUpdate(app, u)
	}

	for _, u := range upd.OutputUpdates {
		applyOutputUpdate(app, u)
	}

	for _, u := range upd.ReferenceDataSourceUpdates {
		applyReferenceDataSourceUpdate(app, u)
	}
}

func applyInputUpdate(app *Application, u InputUpdate) {
	idx := findInputIndex(app, u.InputID)
	if idx < 0 {
		return
	}

	in := &app.InputDescriptions[idx]

	streamNamesDirty := u.NamePrefixUpdate != "" || u.InputParallelismUpdate != nil

	if u.NamePrefixUpdate != "" {
		in.NamePrefix = u.NamePrefixUpdate
	}

	if u.KinesisStreamsInputUpdate != nil {
		in.KinesisStreamsInputDescription = u.KinesisStreamsInputUpdate
	}

	if u.KinesisFirehoseInputUpdate != nil {
		in.KinesisFirehoseInputDescription = u.KinesisFirehoseInputUpdate
	}

	if u.InputProcessingConfigurationUpdate != nil {
		in.InputProcessingConfigurationDescription = u.InputProcessingConfigurationUpdate
	}

	if su := u.InputSchemaUpdate; su != nil {
		in.InputSchema = &SourceSchemaDesc{
			RecordFormat:   su.RecordFormatUpdate,
			RecordEncoding: su.RecordEncodingUpdate,
			RecordColumns:  su.RecordColumnUpdates,
		}
	}

	if u.InputParallelismUpdate != nil {
		in.InputParallelism = &InputParallelismDesc{Count: u.InputParallelismUpdate.CountUpdate}
	}

	if streamNamesDirty {
		in.InAppStreamNames = inAppStreamNames(in.NamePrefix, in.InputParallelism)
	}
}

func applyOutputUpdate(app *Application, u OutputUpdate) {
	idx := findOutputIndex(app, u.OutputID)
	if idx < 0 {
		return
	}

	out := &app.OutputDescriptions[idx]

	if u.NameUpdate != "" {
		out.Name = u.NameUpdate
	}

	if u.KinesisStreamsOutputUpdate != nil {
		out.KinesisStreamsOutputDescription = u.KinesisStreamsOutputUpdate
	}

	if u.KinesisFirehoseOutputUpdate != nil {
		out.KinesisFirehoseOutputDescription = u.KinesisFirehoseOutputUpdate
	}

	if u.LambdaOutputUpdate != nil {
		out.LambdaOutputDescription = u.LambdaOutputUpdate
	}

	if u.DestinationSchemaUpdate != nil {
		out.DestinationSchema = u.DestinationSchemaUpdate
	}
}

func applyReferenceDataSourceUpdate(app *Application, u ReferenceDataSourceUpdate) {
	idx := findRefIndex(app, u.ReferenceID)
	if idx < 0 {
		return
	}

	ref := &app.ReferenceDataSourceDescriptions[idx]

	if u.TableNameUpdate != "" {
		ref.TableName = u.TableNameUpdate
	}

	if u.S3ReferenceDataSourceUpdate != nil {
		ref.S3ReferenceDataSourceDescription = u.S3ReferenceDataSourceUpdate
	}

	if u.ReferenceSchemaUpdate != nil {
		ref.ReferenceSchema = u.ReferenceSchemaUpdate
	}
}

func applyVpcConfigUpdates(app *Application, updates []VpcConfigUpdate) {
	for _, u := range updates {
		idx := findVpcIndex(app, u.VpcConfigurationID)
		if idx < 0 {
			continue
		}

		vpc := &app.VpcConfigurationDescriptions[idx]

		if len(u.SubnetIDUpdates) > 0 {
			vpc.SubnetIDs = u.SubnetIDUpdates
		}

		if len(u.SecurityGroupIDUpdates) > 0 {
			vpc.SecurityGroupIDs = u.SecurityGroupIDUpdates
		}
	}
}

// applyRunConfigInput stores rc as the application's RunConfigurationDescription,
// merging (not replacing) the two independent sub-fields -- shared by
// StartApplication's RunConfiguration and UpdateApplication's
// RunConfigurationUpdate, both of which use the identical shape in real AWS.
func applyRunConfigInput(app *Application, rc *RunConfigInput) {
	if rc == nil {
		return
	}

	if app.RunConfig == nil {
		app.RunConfig = &RunConfigDesc{}
	}

	if rc.ApplicationRestoreConfiguration != nil {
		app.RunConfig.ApplicationRestoreConfigurationDescription = rc.ApplicationRestoreConfiguration
	}

	if rc.FlinkRunConfiguration != nil {
		app.RunConfig.FlinkRunConfigurationDescription = rc.FlinkRunConfiguration
	}
}
