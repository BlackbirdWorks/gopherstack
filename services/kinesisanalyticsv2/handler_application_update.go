package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// runConfigurationInput mirrors real AWS's RunConfiguration (StartApplication)
// and RunConfigurationUpdate (UpdateApplication) request shapes -- both use
// the identical ApplicationRestoreConfiguration/FlinkRunConfiguration field
// shapes in the real SDK (see models.go's ApplicationRestoreConfig/
// FlinkRunConfig doc comments), so gopherstack reuses one wire type for both
// call sites. SqlRunConfigurations is accepted-but-ignored: no state
// anywhere in this backend models per-input starting position for real
// stream consumption (same root cause as DiscoverInputSchema's documented
// synthetic-schema limitation -- see PARITY.md).
type runConfigurationInput struct {
	ApplicationRestoreConfiguration *ApplicationRestoreConfig `json:"ApplicationRestoreConfiguration,omitempty"` //nolint:lll // AWS API name
	FlinkRunConfiguration           *FlinkRunConfig           `json:"FlinkRunConfiguration,omitempty"`
}

// toRunConfigInput converts a runConfigurationInput to the backend-facing RunConfigInput.
func toRunConfigInput(in *runConfigurationInput) *RunConfigInput {
	if in == nil {
		return nil
	}

	return &RunConfigInput{
		ApplicationRestoreConfiguration: in.ApplicationRestoreConfiguration,
		FlinkRunConfiguration:           in.FlinkRunConfiguration,
	}
}

type cwlOptionUpdateInput struct {
	CloudWatchLoggingOptionID string `json:"CloudWatchLoggingOptionId"`
	LogStreamARNUpdate        string `json:"LogStreamARNUpdate,omitempty"`
}

type s3ContentLocationUpdateInput struct {
	BucketARNUpdate     string `json:"BucketARNUpdate,omitempty"`
	FileKeyUpdate       string `json:"FileKeyUpdate,omitempty"`
	ObjectVersionUpdate string `json:"ObjectVersionUpdate,omitempty"`
}

type codeContentUpdateInput struct {
	S3ContentLocationUpdate *s3ContentLocationUpdateInput `json:"S3ContentLocationUpdate,omitempty"`
	TextContentUpdate       string                        `json:"TextContentUpdate,omitempty"`
	ZipFileContentUpdate    []byte                        `json:"ZipFileContentUpdate,omitempty"`
}

type applicationCodeConfigUpdateInput struct {
	CodeContentUpdate     *codeContentUpdateInput `json:"CodeContentUpdate,omitempty"`
	CodeContentTypeUpdate string                  `json:"CodeContentTypeUpdate,omitempty"`
}

type checkpointConfigUpdateInput struct {
	CheckpointingEnabledUpdate       *bool  `json:"CheckpointingEnabledUpdate,omitempty"`
	CheckpointIntervalUpdate         *int64 `json:"CheckpointIntervalUpdate,omitempty"`
	MinPauseBetweenCheckpointsUpdate *int64 `json:"MinPauseBetweenCheckpointsUpdate,omitempty"`
	ConfigurationTypeUpdate          string `json:"ConfigurationTypeUpdate,omitempty"`
}

type monitoringConfigUpdateInput struct {
	ConfigurationTypeUpdate string `json:"ConfigurationTypeUpdate,omitempty"`
	LogLevelUpdate          string `json:"LogLevelUpdate,omitempty"`
	MetricsLevelUpdate      string `json:"MetricsLevelUpdate,omitempty"`
}

type parallelismConfigUpdateInput struct {
	AutoScalingEnabledUpdate *bool  `json:"AutoScalingEnabledUpdate,omitempty"`
	ParallelismUpdate        *int32 `json:"ParallelismUpdate,omitempty"`
	ParallelismPerKPUUpdate  *int32 `json:"ParallelismPerKPUUpdate,omitempty"`
	ConfigurationTypeUpdate  string `json:"ConfigurationTypeUpdate,omitempty"`
}

type flinkApplicationConfigUpdateInput struct {
	CheckpointConfigurationUpdate  *checkpointConfigUpdateInput  `json:"CheckpointConfigurationUpdate,omitempty"`  //nolint:lll // AWS API name
	MonitoringConfigurationUpdate  *monitoringConfigUpdateInput  `json:"MonitoringConfigurationUpdate,omitempty"`  //nolint:lll // AWS API name
	ParallelismConfigurationUpdate *parallelismConfigUpdateInput `json:"ParallelismConfigurationUpdate,omitempty"` //nolint:lll // AWS API name
}

type environmentPropertyUpdatesInput struct {
	PropertyGroups []PropertyGroup `json:"PropertyGroups"`
}

type snapshotConfigUpdateInput struct {
	SnapshotsEnabledUpdate bool `json:"SnapshotsEnabledUpdate"`
}

type systemRollbackConfigUpdateInput struct {
	RollbackEnabledUpdate bool `json:"RollbackEnabledUpdate"`
}

type encryptionConfigUpdateInput struct {
	KeyTypeUpdate string `json:"KeyTypeUpdate"`
	KeyIDUpdate   string `json:"KeyIdUpdate,omitempty"`
}

type inputUpdateInput struct {
	KinesisStreamsInputUpdate          *kinesisStreamsInputConfig  `json:"KinesisStreamsInputUpdate,omitempty"`
	KinesisFirehoseInputUpdate         *kinesisFirehoseInputConfig `json:"KinesisFirehoseInputUpdate,omitempty"`
	InputProcessingConfigurationUpdate *inputProcessingConfigInput `json:"InputProcessingConfigurationUpdate,omitempty"` //nolint:lll // AWS API name
	InputID                            string                      `json:"InputId"`
	NamePrefixUpdate                   string                      `json:"NamePrefixUpdate,omitempty"`
}

type outputUpdateInput struct {
	KinesisStreamsOutputUpdate  *kinesisStreamsOutputConfig  `json:"KinesisStreamsOutputUpdate,omitempty"`
	KinesisFirehoseOutputUpdate *kinesisFirehoseOutputConfig `json:"KinesisFirehoseOutputUpdate,omitempty"`
	LambdaOutputUpdate          *lambdaOutputConfig          `json:"LambdaOutputUpdate,omitempty"`
	DestinationSchemaUpdate     *destinationSchemaInput      `json:"DestinationSchemaUpdate,omitempty"`
	OutputID                    string                       `json:"OutputId"`
	NameUpdate                  string                       `json:"NameUpdate,omitempty"`
}

type referenceDataSourceUpdateInput struct {
	S3ReferenceDataSourceUpdate *s3ReferenceDataSourceConfig `json:"S3ReferenceDataSourceUpdate,omitempty"`
	ReferenceID                 string                       `json:"ReferenceId"`
	TableNameUpdate             string                       `json:"TableNameUpdate,omitempty"`
}

type sqlConfigUpdateInput struct {
	InputUpdates               []inputUpdateInput               `json:"InputUpdates,omitempty"`
	OutputUpdates              []outputUpdateInput              `json:"OutputUpdates,omitempty"`
	ReferenceDataSourceUpdates []referenceDataSourceUpdateInput `json:"ReferenceDataSourceUpdates,omitempty"` //nolint:lll // AWS API name
}

type vpcConfigUpdateInput struct {
	VpcConfigurationID     string   `json:"VpcConfigurationId"`
	SubnetIDUpdates        []string `json:"SubnetIdUpdates,omitempty"`
	SecurityGroupIDUpdates []string `json:"SecurityGroupIdUpdates,omitempty"`
}

// applicationConfigurationUpdateInput mirrors real AWS's
// ApplicationConfigurationUpdate request shape. ZeppelinApplicationConfigurationUpdate
// is accepted-but-ignored for the same reason ZeppelinApplicationConfiguration is
// at Create time -- see appConfigDesc's doc comment.
type applicationConfigurationUpdateInput struct {
	ApplicationCodeConfigurationUpdate           *applicationCodeConfigUpdateInput  `json:"ApplicationCodeConfigurationUpdate,omitempty"`           //nolint:lll // AWS API name
	FlinkApplicationConfigurationUpdate          *flinkApplicationConfigUpdateInput `json:"FlinkApplicationConfigurationUpdate,omitempty"`          //nolint:lll // AWS API name
	EnvironmentPropertyUpdates                   *environmentPropertyUpdatesInput   `json:"EnvironmentPropertyUpdates,omitempty"`                   //nolint:lll // AWS API name
	ApplicationSnapshotConfigurationUpdate       *snapshotConfigUpdateInput         `json:"ApplicationSnapshotConfigurationUpdate,omitempty"`       //nolint:lll // AWS API name
	ApplicationSystemRollbackConfigurationUpdate *systemRollbackConfigUpdateInput   `json:"ApplicationSystemRollbackConfigurationUpdate,omitempty"` //nolint:lll // AWS API name
	ApplicationEncryptionConfigurationUpdate     *encryptionConfigUpdateInput       `json:"ApplicationEncryptionConfigurationUpdate,omitempty"`     //nolint:lll // AWS API name
	SQLApplicationConfigurationUpdate            *sqlConfigUpdateInput              `json:"SqlApplicationConfigurationUpdate,omitempty"`            //nolint:lll,tagliatelle // AWS API name
	VpcConfigurationUpdates                      []vpcConfigUpdateInput             `json:"VpcConfigurationUpdates,omitempty"`                      //nolint:lll // AWS API name
}

type updateApplicationInput struct {
	ApplicationConfigurationUpdate *applicationConfigurationUpdateInput `json:"ApplicationConfigurationUpdate,omitempty"` //nolint:lll // AWS API name
	RunConfigurationUpdate         *runConfigurationInput               `json:"RunConfigurationUpdate,omitempty"`
	ApplicationName                string                               `json:"ApplicationName"`
	ConditionalToken               string                               `json:"ConditionalToken,omitempty"`
	ServiceExecutionRoleUpdate     string                               `json:"ServiceExecutionRoleUpdate,omitempty"`
	ApplicationDescription         string                               `json:"ApplicationDescription,omitempty"`
	RuntimeEnvironmentUpdate       string                               `json:"RuntimeEnvironmentUpdate,omitempty"`
	CloudWatchLoggingOptionUpdates []cwlOptionUpdateInput               `json:"CloudWatchLoggingOptionUpdates,omitempty"` //nolint:lll // AWS API name
	CurrentApplicationVersionID    int64                                `json:"CurrentApplicationVersionId,omitempty"`
}

type updateApplicationOutput struct {
	OperationID       string                  `json:"OperationId,omitempty"`
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

func (h *Handler) handleUpdateApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in updateApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, opID, err := h.Backend.UpdateApplication(ctx, buildUpdateApplicationParams(&in))
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
		OperationID:       opID,
	})
}

// buildUpdateApplicationParams converts the wire request into the
// backend-facing UpdateApplicationParams.
func buildUpdateApplicationParams(in *updateApplicationInput) UpdateApplicationParams {
	cwlUpdates := make([]CloudWatchLoggingOptionUpdate, 0, len(in.CloudWatchLoggingOptionUpdates))
	for _, u := range in.CloudWatchLoggingOptionUpdates {
		// cwlOptionUpdateInput and CloudWatchLoggingOptionUpdate share an
		// identical field sequence (name+type+order), so a direct conversion
		// is safe and avoids repeating the field list.
		cwlUpdates = append(cwlUpdates, CloudWatchLoggingOptionUpdate(u))
	}

	return UpdateApplicationParams{
		Name:                           in.ApplicationName,
		ConditionalToken:               in.ConditionalToken,
		CurrentApplicationVersionID:    in.CurrentApplicationVersionID,
		ServiceExecutionRoleUpdate:     in.ServiceExecutionRoleUpdate,
		ApplicationDescription:         in.ApplicationDescription,
		RuntimeEnvironmentUpdate:       in.RuntimeEnvironmentUpdate,
		ApplicationConfigurationUpdate: buildApplicationConfigurationUpdate(in.ApplicationConfigurationUpdate),
		CloudWatchLoggingOptionUpdates: cwlUpdates,
		RunConfigurationUpdate:         toRunConfigInput(in.RunConfigurationUpdate),
	}
}

func buildApplicationConfigurationUpdate(in *applicationConfigurationUpdateInput) *ApplicationConfigurationUpdate {
	if in == nil {
		return nil
	}

	out := &ApplicationConfigurationUpdate{
		VpcConfigurationUpdates: buildVpcConfigUpdates(in.VpcConfigurationUpdates),
	}

	if in.ApplicationCodeConfigurationUpdate != nil {
		out.ApplicationCodeConfigurationUpdate = buildCodeConfigUpdate(in.ApplicationCodeConfigurationUpdate)
	}

	if in.FlinkApplicationConfigurationUpdate != nil {
		out.FlinkApplicationConfigurationUpdate = buildFlinkConfigUpdate(in.FlinkApplicationConfigurationUpdate)
	}

	if in.EnvironmentPropertyUpdates != nil {
		out.HasEnvironmentPropertyUpdates = true
		out.EnvironmentPropertyUpdates = in.EnvironmentPropertyUpdates.PropertyGroups
	}

	applyResourceConfigUpdates(in, out)

	return out
}

// applyResourceConfigUpdates fills in the snapshot/rollback/encryption/SQL
// sub-fields of out -- split out of buildApplicationConfigurationUpdate
// purely to keep each function's branch count low; both together perform one
// linear field-by-field wire-to-backend conversion.
func applyResourceConfigUpdates(in *applicationConfigurationUpdateInput, out *ApplicationConfigurationUpdate) {
	if in.ApplicationSnapshotConfigurationUpdate != nil {
		v := in.ApplicationSnapshotConfigurationUpdate.SnapshotsEnabledUpdate
		out.ApplicationSnapshotConfigurationUpdate = &v
	}

	if in.ApplicationSystemRollbackConfigurationUpdate != nil {
		v := in.ApplicationSystemRollbackConfigurationUpdate.RollbackEnabledUpdate
		out.ApplicationSystemRollbackConfigurationUpdate = &v
	}

	if in.ApplicationEncryptionConfigurationUpdate != nil {
		out.ApplicationEncryptionConfigurationUpdate = &ApplicationEncryptionConfigDesc{
			KeyType: in.ApplicationEncryptionConfigurationUpdate.KeyTypeUpdate,
			KeyID:   in.ApplicationEncryptionConfigurationUpdate.KeyIDUpdate,
		}
	}

	if in.SQLApplicationConfigurationUpdate != nil {
		out.SQLApplicationConfigurationUpdate = buildSQLConfigUpdate(in.SQLApplicationConfigurationUpdate)
	}
}

func buildCodeConfigUpdate(in *applicationCodeConfigUpdateInput) *ApplicationCodeConfigUpdate {
	out := &ApplicationCodeConfigUpdate{CodeContentTypeUpdate: in.CodeContentTypeUpdate}

	if cu := in.CodeContentUpdate; cu != nil {
		update := &CodeContentUpdate{ZipFileContentUpdate: cu.ZipFileContentUpdate}
		if cu.TextContentUpdate != "" {
			update.TextContentUpdate = &cu.TextContentUpdate
		}

		if cu.S3ContentLocationUpdate != nil {
			s3 := cu.S3ContentLocationUpdate
			update.S3BucketARNUpdate = &s3.BucketARNUpdate
			update.S3FileKeyUpdate = &s3.FileKeyUpdate
			update.S3ObjectVersionUpdate = &s3.ObjectVersionUpdate
		}

		out.CodeContentUpdate = update
	}

	return out
}

func buildFlinkConfigUpdate(in *flinkApplicationConfigUpdateInput) *FlinkApplicationConfigUpdate {
	out := &FlinkApplicationConfigUpdate{}

	if c := in.CheckpointConfigurationUpdate; c != nil {
		out.CheckpointConfigurationUpdate = &CheckpointConfigUpdate{
			ConfigurationTypeUpdate:          c.ConfigurationTypeUpdate,
			CheckpointingEnabledUpdate:       c.CheckpointingEnabledUpdate,
			CheckpointIntervalUpdate:         c.CheckpointIntervalUpdate,
			MinPauseBetweenCheckpointsUpdate: c.MinPauseBetweenCheckpointsUpdate,
		}
	}

	if m := in.MonitoringConfigurationUpdate; m != nil {
		out.MonitoringConfigurationUpdate = &MonitoringConfigUpdate{
			ConfigurationTypeUpdate: m.ConfigurationTypeUpdate,
			LogLevelUpdate:          m.LogLevelUpdate,
			MetricsLevelUpdate:      m.MetricsLevelUpdate,
		}
	}

	if p := in.ParallelismConfigurationUpdate; p != nil {
		out.ParallelismConfigurationUpdate = &ParallelismConfigUpdate{
			ConfigurationTypeUpdate:  p.ConfigurationTypeUpdate,
			AutoScalingEnabledUpdate: p.AutoScalingEnabledUpdate,
			ParallelismUpdate:        p.ParallelismUpdate,
			ParallelismPerKPUUpdate:  p.ParallelismPerKPUUpdate,
		}
	}

	return out
}

func buildSQLConfigUpdate(in *sqlConfigUpdateInput) *SQLApplicationConfigUpdate {
	out := &SQLApplicationConfigUpdate{
		InputUpdates:               make([]InputUpdate, 0, len(in.InputUpdates)),
		OutputUpdates:              make([]OutputUpdate, 0, len(in.OutputUpdates)),
		ReferenceDataSourceUpdates: make([]ReferenceDataSourceUpdate, 0, len(in.ReferenceDataSourceUpdates)),
	}

	for _, u := range in.InputUpdates {
		out.InputUpdates = append(out.InputUpdates, InputUpdate{
			InputID:                            u.InputID,
			NamePrefixUpdate:                   u.NamePrefixUpdate,
			KinesisStreamsInputUpdate:          buildKinesisStreamsInputDesc(u.KinesisStreamsInputUpdate),
			KinesisFirehoseInputUpdate:         buildKinesisFirehoseInputDesc(u.KinesisFirehoseInputUpdate),
			InputProcessingConfigurationUpdate: buildInputProcessingConfigDesc(u.InputProcessingConfigurationUpdate),
		})
	}

	for _, u := range in.OutputUpdates {
		out.OutputUpdates = append(out.OutputUpdates, OutputUpdate{
			OutputID:                    u.OutputID,
			NameUpdate:                  u.NameUpdate,
			KinesisStreamsOutputUpdate:  buildKinesisStreamsOutputDesc(u.KinesisStreamsOutputUpdate),
			KinesisFirehoseOutputUpdate: buildKinesisFirehoseOutputDesc(u.KinesisFirehoseOutputUpdate),
			LambdaOutputUpdate:          buildLambdaOutputDesc(u.LambdaOutputUpdate),
			DestinationSchemaUpdate:     buildDestinationSchemaDesc(u.DestinationSchemaUpdate),
		})
	}

	for _, u := range in.ReferenceDataSourceUpdates {
		out.ReferenceDataSourceUpdates = append(out.ReferenceDataSourceUpdates, ReferenceDataSourceUpdate{
			ReferenceID:                 u.ReferenceID,
			TableNameUpdate:             u.TableNameUpdate,
			S3ReferenceDataSourceUpdate: buildS3RefDataSourceDesc(u.S3ReferenceDataSourceUpdate),
		})
	}

	return out
}

func buildVpcConfigUpdates(in []vpcConfigUpdateInput) []VpcConfigUpdate {
	out := make([]VpcConfigUpdate, 0, len(in))
	for _, u := range in {
		// vpcConfigUpdateInput and VpcConfigUpdate share an identical field
		// sequence (name+type+order), so a direct conversion is safe.
		out = append(out, VpcConfigUpdate(u))
	}

	return out
}

func buildKinesisStreamsInputDesc(in *kinesisStreamsInputConfig) *KinesisStreamsInputDesc {
	if in == nil {
		return nil
	}

	return &KinesisStreamsInputDesc{ResourceARN: in.ResourceARN}
}

func buildKinesisFirehoseInputDesc(in *kinesisFirehoseInputConfig) *KinesisFirehoseInputDesc {
	if in == nil {
		return nil
	}

	return &KinesisFirehoseInputDesc{ResourceARN: in.ResourceARN}
}

func buildInputProcessingConfigDesc(in *inputProcessingConfigInput) *InputProcessingConfigurationDesc {
	if in == nil || in.InputLambdaProcessor == nil {
		return nil
	}

	return &InputProcessingConfigurationDesc{
		InputLambdaProcessor: &LambdaProcessorDesc{ResourceARN: in.InputLambdaProcessor.ResourceARN},
	}
}

func buildKinesisStreamsOutputDesc(in *kinesisStreamsOutputConfig) *KinesisStreamsOutputDesc {
	if in == nil {
		return nil
	}

	return &KinesisStreamsOutputDesc{ResourceARN: in.ResourceARN}
}

func buildKinesisFirehoseOutputDesc(in *kinesisFirehoseOutputConfig) *KinesisFirehoseOutputDesc {
	if in == nil {
		return nil
	}

	return &KinesisFirehoseOutputDesc{ResourceARN: in.ResourceARN}
}

func buildLambdaOutputDesc(in *lambdaOutputConfig) *LambdaOutputDesc {
	if in == nil {
		return nil
	}

	return &LambdaOutputDesc{ResourceARN: in.ResourceARN}
}

func buildDestinationSchemaDesc(in *destinationSchemaInput) *DestinationSchemaDesc {
	if in == nil {
		return nil
	}

	return &DestinationSchemaDesc{RecordFormatType: in.RecordFormatType}
}

func buildS3RefDataSourceDesc(in *s3ReferenceDataSourceConfig) *S3ReferenceDataSourceDesc {
	if in == nil {
		return nil
	}

	return &S3ReferenceDataSourceDesc{BucketARN: in.BucketARN, FileKey: in.FileKey}
}
