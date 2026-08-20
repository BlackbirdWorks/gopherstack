package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// sqlApplicationConfigInput mirrors real AWS's SqlApplicationConfiguration
// request shape: the SQL-based inputs/outputs/reference-data-sources a
// client can specify inline at CreateApplication time, instead of via the
// separate AddApplicationInput/AddApplicationOutput/
// AddApplicationReferenceDataSource calls.
type sqlApplicationConfigInput struct {
	Inputs               []*inputConfig         `json:"Inputs,omitempty"`
	Outputs              []*outputConfig        `json:"Outputs,omitempty"`
	ReferenceDataSources []*refDataSourceConfig `json:"ReferenceDataSources,omitempty"`
}

// s3ContentLocationInput mirrors real AWS's S3ContentLocation request shape.
type s3ContentLocationInput struct {
	BucketARN     string `json:"BucketARN"`
	FileKey       string `json:"FileKey"`
	ObjectVersion string `json:"ObjectVersion,omitempty"`
}

// codeContentInput mirrors real AWS's CodeContent request shape. Exactly one
// of TextContent/ZipFileContent/S3ContentLocation is expected to be set.
type codeContentInput struct {
	S3ContentLocation *s3ContentLocationInput `json:"S3ContentLocation,omitempty"`
	TextContent       string                  `json:"TextContent,omitempty"`
	ZipFileContent    []byte                  `json:"ZipFileContent,omitempty"`
}

// applicationCodeConfigInput mirrors real AWS's ApplicationCodeConfiguration request shape.
type applicationCodeConfigInput struct {
	CodeContent     *codeContentInput `json:"CodeContent,omitempty"`
	CodeContentType string            `json:"CodeContentType"`
}

// checkpointConfigInput mirrors real AWS's CheckpointConfiguration request shape.
type checkpointConfigInput struct {
	CheckpointingEnabled       *bool  `json:"CheckpointingEnabled,omitempty"`
	CheckpointInterval         *int64 `json:"CheckpointInterval,omitempty"`
	MinPauseBetweenCheckpoints *int64 `json:"MinPauseBetweenCheckpoints,omitempty"`
	ConfigurationType          string `json:"ConfigurationType"`
}

// monitoringConfigInput mirrors real AWS's MonitoringConfiguration request shape.
type monitoringConfigInput struct {
	ConfigurationType string `json:"ConfigurationType"`
	LogLevel          string `json:"LogLevel,omitempty"`
	MetricsLevel      string `json:"MetricsLevel,omitempty"`
}

// parallelismConfigInput mirrors real AWS's ParallelismConfiguration request shape.
type parallelismConfigInput struct {
	AutoScalingEnabled *bool  `json:"AutoScalingEnabled,omitempty"`
	Parallelism        *int32 `json:"Parallelism,omitempty"`
	ParallelismPerKPU  *int32 `json:"ParallelismPerKPU,omitempty"`
	ConfigurationType  string `json:"ConfigurationType"`
}

// flinkApplicationConfigInput mirrors real AWS's FlinkApplicationConfiguration request shape.
type flinkApplicationConfigInput struct {
	CheckpointConfiguration  *checkpointConfigInput  `json:"CheckpointConfiguration,omitempty"`
	MonitoringConfiguration  *monitoringConfigInput  `json:"MonitoringConfiguration,omitempty"`
	ParallelismConfiguration *parallelismConfigInput `json:"ParallelismConfiguration,omitempty"`
}

// environmentPropertiesInput mirrors real AWS's EnvironmentProperties request shape.
type environmentPropertiesInput struct {
	PropertyGroups []PropertyGroup `json:"PropertyGroups"`
}

// snapshotConfigInput mirrors real AWS's ApplicationSnapshotConfiguration request shape.
type snapshotConfigInput struct {
	SnapshotsEnabled bool `json:"SnapshotsEnabled"`
}

// systemRollbackConfigInput mirrors real AWS's ApplicationSystemRollbackConfiguration request shape.
type systemRollbackConfigInput struct {
	RollbackEnabled bool `json:"RollbackEnabled"`
}

// encryptionConfigInput mirrors real AWS's ApplicationEncryptionConfiguration request shape.
type encryptionConfigInput struct {
	KeyType string `json:"KeyType"`
	KeyID   string `json:"KeyId,omitempty"`
}

// zeppelinMonitoringConfigInput mirrors real AWS's ZeppelinMonitoringConfiguration.
type zeppelinMonitoringConfigInput struct {
	LogLevel string `json:"LogLevel"`
}

// catalogConfigInput mirrors real AWS's CatalogConfiguration request shape.
type catalogConfigInput struct {
	GlueDataCatalogConfiguration *GlueDataCatalogConfigDesc `json:"GlueDataCatalogConfiguration,omitempty"` //nolint:lll // AWS API name
}

// deployAsApplicationConfigInput mirrors real AWS's
// DeployAsApplicationConfiguration request shape.
type deployAsApplicationConfigInput struct {
	S3ContentLocation *S3ContentBaseLocationDesc `json:"S3ContentLocation,omitempty"`
}

// customArtifactConfigInput mirrors real AWS's CustomArtifactConfiguration
// request shape (one entry of CustomArtifactsConfiguration).
type customArtifactConfigInput struct {
	S3ContentLocation *S3CodeLocationDesc `json:"S3ContentLocation,omitempty"`
	MavenReference    *MavenReferenceDesc `json:"MavenReference,omitempty"`
	ArtifactType      string              `json:"ArtifactType"`
}

// zeppelinApplicationConfigInput mirrors real AWS's ZeppelinApplicationConfiguration
// request shape (Studio-notebook-only: Glue Data Catalog + Maven/S3 custom
// artifacts + deploy-as-application).
type zeppelinApplicationConfigInput struct {
	MonitoringConfiguration          *zeppelinMonitoringConfigInput  `json:"MonitoringConfiguration,omitempty"`          //nolint:lll // AWS API name
	CatalogConfiguration             *catalogConfigInput             `json:"CatalogConfiguration,omitempty"`             //nolint:lll // AWS API name
	DeployAsApplicationConfiguration *deployAsApplicationConfigInput `json:"DeployAsApplicationConfiguration,omitempty"` //nolint:lll // AWS API name
	CustomArtifactsConfiguration     []customArtifactConfigInput     `json:"CustomArtifactsConfiguration,omitempty"`     //nolint:lll // AWS API name
}

// applicationConfigurationInput mirrors real AWS's ApplicationConfiguration request shape.
type applicationConfigurationInput struct {
	SQLApplicationConfiguration            *sqlApplicationConfigInput      `json:"SqlApplicationConfiguration,omitempty"`            //nolint:lll,tagliatelle // AWS API name
	ApplicationCodeConfiguration           *applicationCodeConfigInput     `json:"ApplicationCodeConfiguration,omitempty"`           //nolint:lll // AWS API name
	FlinkApplicationConfiguration          *flinkApplicationConfigInput    `json:"FlinkApplicationConfiguration,omitempty"`          //nolint:lll // AWS API name
	ZeppelinApplicationConfiguration       *zeppelinApplicationConfigInput `json:"ZeppelinApplicationConfiguration,omitempty"`       //nolint:lll // AWS API name
	EnvironmentProperties                  *environmentPropertiesInput     `json:"EnvironmentProperties,omitempty"`                  //nolint:lll // AWS API name
	ApplicationSnapshotConfiguration       *snapshotConfigInput            `json:"ApplicationSnapshotConfiguration,omitempty"`       //nolint:lll // AWS API name
	ApplicationSystemRollbackConfiguration *systemRollbackConfigInput      `json:"ApplicationSystemRollbackConfiguration,omitempty"` //nolint:lll // AWS API name
	ApplicationEncryptionConfiguration     *encryptionConfigInput          `json:"ApplicationEncryptionConfiguration,omitempty"`     //nolint:lll // AWS API name
	VpcConfigurations                      []*vpcConfigInput               `json:"VpcConfigurations,omitempty"`
}

type createApplicationInput struct {
	ApplicationName          string                         `json:"ApplicationName"`
	RuntimeEnvironment       string                         `json:"RuntimeEnvironment"`
	ServiceExecutionRole     string                         `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription   string                         `json:"ApplicationDescription,omitempty"`
	ApplicationMode          string                         `json:"ApplicationMode,omitempty"`
	ApplicationConfiguration *applicationConfigurationInput `json:"ApplicationConfiguration,omitempty"` //nolint:lll // AWS API name
	CloudWatchLoggingOptions []*cwlOptionInput              `json:"CloudWatchLoggingOptions,omitempty"`
	Tags                     []Tag                          `json:"Tags,omitempty"`
}

// applicationDetailOutput mirrors real AWS's ApplicationDetail (types/types.go:179).
// It deliberately carries no Tags field -- ApplicationDetail has none; a
// real client retrieves tags via the separate ListTagsForResource operation.
type applicationDetailOutput struct {
	ApplicationConfigurationDescription            *appConfigDesc                `json:"ApplicationConfigurationDescription,omitempty"`            //nolint:lll // AWS API name
	ApplicationMaintenanceConfigurationDescription *maintenanceConfigDescription `json:"ApplicationMaintenanceConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	ApplicationVersionUpdatedFrom                  *int64                        `json:"ApplicationVersionUpdatedFrom,omitempty"`                  //nolint:lll // AWS API name
	ApplicationVersionRolledBackTo                 *int64                        `json:"ApplicationVersionRolledBackTo,omitempty"`                 //nolint:lll // AWS API name
	ApplicationVersionRolledBackFrom               *int64                        `json:"ApplicationVersionRolledBackFrom,omitempty"`               //nolint:lll // AWS API name
	ApplicationName                                string                        `json:"ApplicationName"`
	RuntimeEnvironment                             string                        `json:"RuntimeEnvironment"`
	ApplicationARN                                 string                        `json:"ApplicationARN"`
	ServiceExecutionRole                           string                        `json:"ServiceExecutionRole,omitempty"`
	ConditionalToken                               string                        `json:"ConditionalToken,omitempty"`
	ApplicationDescription                         string                        `json:"ApplicationDescription,omitempty"`
	ApplicationMode                                string                        `json:"ApplicationMode,omitempty"`
	ApplicationStatus                              string                        `json:"ApplicationStatus"`
	CloudWatchLoggingOptionDescriptions            []CloudWatchLoggingOptionDesc `json:"CloudWatchLoggingOptionDescriptions,omitempty"` //nolint:lll // AWS API name
	ApplicationVersionID                           int64                         `json:"ApplicationVersionId"`
	ApplicationVersionCreateTimestamp              float64                       `json:"ApplicationVersionCreateTimestamp,omitempty"` //nolint:lll // AWS API name
	LastUpdateTimestamp                            float64                       `json:"LastUpdateTimestamp,omitempty"`
	CreateTimestamp                                float64                       `json:"CreateTimestamp"`
}

// appConfigDesc mirrors real AWS's ApplicationConfigurationDescription.
type appConfigDesc struct {
	SQLApplicationConfigurationDescription            *sqlAppConfigDesc                     `json:"SqlApplicationConfigurationDescription,omitempty"`            //nolint:lll,tagliatelle // AWS API name
	ApplicationCodeConfigurationDescription           *ApplicationCodeConfigDesc            `json:"ApplicationCodeConfigurationDescription,omitempty"`           //nolint:lll // AWS API name
	FlinkApplicationConfigurationDescription          *FlinkApplicationConfigDesc           `json:"FlinkApplicationConfigurationDescription,omitempty"`          //nolint:lll // AWS API name
	ZeppelinApplicationConfigurationDescription       *ZeppelinApplicationConfigDescription `json:"ZeppelinApplicationConfigurationDescription,omitempty"`       //nolint:lll // AWS API name
	EnvironmentPropertyDescriptions                   *environmentPropertyDescOutput        `json:"EnvironmentPropertyDescriptions,omitempty"`                   //nolint:lll // AWS API name
	ApplicationSnapshotConfigurationDescription       *ApplicationSnapshotConfigDesc        `json:"ApplicationSnapshotConfigurationDescription,omitempty"`       //nolint:lll // AWS API name
	ApplicationSystemRollbackConfigurationDescription *ApplicationSystemRollbackConfigDesc  `json:"ApplicationSystemRollbackConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	ApplicationEncryptionConfigurationDescription     *ApplicationEncryptionConfigDesc      `json:"ApplicationEncryptionConfigurationDescription,omitempty"`     //nolint:lll // AWS API name
	RunConfigurationDescription                       *RunConfigDesc                        `json:"RunConfigurationDescription,omitempty"`                       //nolint:lll // AWS API name
	VpcConfigurationDescriptions                      []VpcConfigurationDescription         `json:"VpcConfigurationDescriptions,omitempty"`                      //nolint:lll // AWS API name
}

// environmentPropertyDescOutput mirrors real AWS's EnvironmentPropertyDescriptions.
type environmentPropertyDescOutput struct {
	PropertyGroupDescriptions []PropertyGroup `json:"PropertyGroupDescriptions"`
}

// sqlAppConfigDesc holds inputs, outputs, and reference data sources.
type sqlAppConfigDesc struct {
	InputDescriptions               []InputDescription               `json:"InputDescriptions"`
	OutputDescriptions              []OutputDescription              `json:"OutputDescriptions"`
	ReferenceDataSourceDescriptions []ReferenceDataSourceDescription `json:"ReferenceDataSourceDescriptions"` //nolint:lll // AWS API name
}

type createApplicationOutput struct {
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

type describeApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

type describeApplicationOutput struct {
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

type listApplicationsInput struct {
	NextToken string `json:"NextToken,omitempty"`
}

type listApplicationsOutput struct {
	NextToken            string               `json:"NextToken,omitempty"`
	ApplicationSummaries []applicationSummary `json:"ApplicationSummaries"`
}

type deleteApplicationInput struct {
	ApplicationName string      `json:"ApplicationName"`
	CreateTimestamp json.Number `json:"CreateTimestamp,omitempty"`
}

type startApplicationInput struct {
	RunConfiguration *runConfigurationInput `json:"RunConfiguration,omitempty"`
	ApplicationName  string                 `json:"ApplicationName"`
}

// startStopApplicationInput is StopApplication's request shape (the shared
// name is misleading: real AWS's StartApplicationInput has no Force field,
// only StopApplicationInput does -- botocore kinesisanalyticsv2/2018-05-23/
// service-2.json.gz shapes "StartApplicationRequest"/"StopApplicationRequest").
type startStopApplicationInput struct {
	Force           *bool  `json:"Force,omitempty"`
	ApplicationName string `json:"ApplicationName"`
}

// startStopApplicationOutput is the shared response shape for
// StartApplication and StopApplication: both real AWS outputs carry only an
// optional OperationId used to track the request via
// DescribeApplicationOperation/ListApplicationOperations.
type startStopApplicationOutput struct {
	OperationID string `json:"OperationId,omitempty"`
}

type updateMaintenanceConfigInput struct {
	ApplicationName                    string `json:"ApplicationName"`
	ApplicationMaintenanceConfigUpdate struct {
		ApplicationMaintenanceWindowStartTimeUpdate string `json:"ApplicationMaintenanceWindowStartTimeUpdate"`
	} `json:"ApplicationMaintenanceConfigurationUpdate"`
}

type maintenanceConfigDescription struct {
	ApplicationMaintenanceWindowStartTime string `json:"ApplicationMaintenanceWindowStartTime"`
	ApplicationMaintenanceWindowEndTime   string `json:"ApplicationMaintenanceWindowEndTime,omitempty"`
}

type updateMaintenanceConfigOutput struct {
	ApplicationARN                                 string                       `json:"ApplicationARN"`
	ApplicationMaintenanceConfigurationDescription maintenanceConfigDescription `json:"ApplicationMaintenanceConfigurationDescription"` //nolint:lll // AWS API name
}

// discoverInputSchemaInput mirrors real AWS's DiscoverInputSchemaRequest.
// The wire key is ServiceExecutionRole, not RoleARN ("RoleARN" is only the
// shape name of ServiceExecutionRole's string value) -- verified against
// botocore kinesisanalyticsv2/2018-05-23/service-2.json.gz shape
// "DiscoverInputSchemaRequest"; the previous "RoleARN" wire key never
// matched any real client's request body, so the field was always empty
// here. InputStartingPositionConfiguration is a nested object, not a
// top-level string (same shape "InputStartingPositionConfiguration" used by
// StartApplication's SqlRunConfigurations).
type discoverInputSchemaInput struct {
	InputStartingPositionConfiguration *InputStartingPositionConfig `json:"InputStartingPositionConfiguration,omitempty"` //nolint:lll // AWS API name
	ResourceARN                        string                       `json:"ResourceARN"`
	ServiceExecutionRole               string                       `json:"ServiceExecutionRole,omitempty"` //nolint:lll // AWS API name
}

type discoverInputSchemaRecordFormat struct {
	RecordFormatType string `json:"RecordFormatType"`
}

// discoverInputSchemaInner mirrors real AWS's SourceSchema. RecordColumns is
// a required member (botocore shape "SourceSchema") -- previously omitted
// entirely, so a real client using this response to configure its
// application's input schema (the operation's whole purpose) received no
// columns at all.
type discoverInputSchemaInner struct {
	RecordEncoding string                          `json:"RecordEncoding,omitempty"`
	RecordFormat   discoverInputSchemaRecordFormat `json:"RecordFormat"`
	RecordColumns  []RecordColumnDesc              `json:"RecordColumns"`
}

type discoverInputSchemaOutput struct {
	InputSchema        discoverInputSchemaInner `json:"InputSchema"`
	ParsedInputRecords [][]string               `json:"ParsedInputRecords,omitempty"`
}

func (h *Handler) handleCreateApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in createApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.ApplicationName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "ApplicationName is required")
	}

	if in.RuntimeEnvironment == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "RuntimeEnvironment is required")
	}

	app, err := h.Backend.CreateApplication(
		ctx,
		in.ApplicationName,
		in.RuntimeEnvironment,
		in.ServiceExecutionRole,
		in.ApplicationDescription,
		in.ApplicationMode,
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	// Real AWS clients (Terraform, CloudFormation, the console) overwhelmingly
	// create fully-configured applications in one CreateApplication call
	// rather than following up with AddApplicationInput/AddApplicationOutput/
	// etc. -- seed that inline configuration now so it isn't silently
	// dropped. This intentionally does not go through the Add* backend
	// methods: those each bump ApplicationVersionId, but real AWS keeps a
	// freshly created application (even with inline config) at version 1.
	cfg := buildInitialConfig(&in)
	if !cfg.IsEmpty() {
		if seedErr := h.Backend.SeedApplicationConfiguration(ctx, in.ApplicationName, cfg); seedErr != nil {
			return h.handleError(c, seedErr)
		}

		app, err = h.Backend.DescribeApplication(ctx, in.ApplicationName)
		if err != nil {
			return h.handleError(c, err)
		}
	}

	return c.JSON(http.StatusOK, createApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
	})
}

// buildInitialConfig extracts the inline configuration from a
// CreateApplicationInput, converting each entry with the same
// buildInputDescription/buildOutputDescription/buildRefDataSourceDescription/
// buildVpcConfigDescription helpers the Add* handlers use, so the two paths
// always produce identical shapes.
func buildInitialConfig(in *createApplicationInput) SeedConfig {
	var cfg SeedConfig

	if ac := in.ApplicationConfiguration; ac != nil {
		if sql := ac.SQLApplicationConfiguration; sql != nil {
			cfg.Inputs, cfg.Outputs, cfg.ReferenceDataSources = buildSQLSeedConfig(sql)
		}

		for _, v := range ac.VpcConfigurations {
			cfg.VpcConfigs = append(cfg.VpcConfigs, buildVpcConfigDescription(v))
		}

		cfg.CodeConfig = buildCodeConfigDesc(ac.ApplicationCodeConfiguration)
		cfg.FlinkConfig = buildFlinkConfigDesc(ac.FlinkApplicationConfiguration)
		cfg.ZeppelinConfig = buildZeppelinConfigDesc(ac.ZeppelinApplicationConfiguration)
		cfg.EnvironmentPropertyGroups = buildEnvironmentPropertyGroups(ac.EnvironmentProperties)
		cfg.SnapshotsEnabled = buildSnapshotsEnabled(ac.ApplicationSnapshotConfiguration)
		cfg.RollbackEnabled = buildRollbackEnabled(ac.ApplicationSystemRollbackConfiguration)
		cfg.EncryptionConfig = buildEncryptionConfigDesc(ac.ApplicationEncryptionConfiguration)
	}

	cfg.CWLOptions = buildCWLOptions(in.CloudWatchLoggingOptions)

	return cfg
}

// buildSQLSeedConfig converts SqlApplicationConfiguration's
// Inputs/Outputs/ReferenceDataSources to the corresponding Description types.
func buildSQLSeedConfig(
	sql *sqlApplicationConfigInput,
) ([]InputDescription, []OutputDescription, []ReferenceDataSourceDescription) {
	inputs := make([]InputDescription, 0, len(sql.Inputs))
	outputs := make([]OutputDescription, 0, len(sql.Outputs))
	refs := make([]ReferenceDataSourceDescription, 0, len(sql.ReferenceDataSources))

	for _, i := range sql.Inputs {
		inputs = append(inputs, buildInputDescription(i))
	}

	for _, o := range sql.Outputs {
		outputs = append(outputs, buildOutputDescription(o))
	}

	for _, r := range sql.ReferenceDataSources {
		refs = append(refs, buildRefDataSourceDescription(r))
	}

	return inputs, outputs, refs
}

func buildCWLOptions(in []*cwlOptionInput) []CloudWatchLoggingOptionDesc {
	if len(in) == 0 {
		return nil
	}

	out := make([]CloudWatchLoggingOptionDesc, 0, len(in))
	for _, c := range in {
		out = append(out, CloudWatchLoggingOptionDesc{LogStreamARN: c.LogStreamARN, RoleARN: c.RoleARN})
	}

	return out
}

func buildCodeConfigDesc(in *applicationCodeConfigInput) *ApplicationCodeConfigDesc {
	if in == nil {
		return nil
	}

	desc := &ApplicationCodeConfigDesc{CodeContentType: in.CodeContentType}

	if cc := in.CodeContent; cc != nil {
		var bucket, key, ver string
		if cc.S3ContentLocation != nil {
			bucket = cc.S3ContentLocation.BucketARN
			key = cc.S3ContentLocation.FileKey
			ver = cc.S3ContentLocation.ObjectVersion
		}

		desc.CodeContentDescription = buildCodeContentDescription(cc.TextContent, cc.ZipFileContent, bucket, key, ver)
	}

	return desc
}

func buildFlinkConfigDesc(in *flinkApplicationConfigInput) *FlinkApplicationConfigDesc {
	if in == nil {
		return nil
	}

	desc := &FlinkApplicationConfigDesc{}

	if c := in.CheckpointConfiguration; c != nil {
		desc.CheckpointConfigurationDescription = applyCheckpointDefaults(&CheckpointConfigDesc{
			ConfigurationType:          c.ConfigurationType,
			CheckpointingEnabled:       c.CheckpointingEnabled,
			CheckpointInterval:         c.CheckpointInterval,
			MinPauseBetweenCheckpoints: c.MinPauseBetweenCheckpoints,
		})
	}

	if m := in.MonitoringConfiguration; m != nil {
		desc.MonitoringConfigurationDescription = &MonitoringConfigDesc{
			ConfigurationType: m.ConfigurationType,
			LogLevel:          m.LogLevel,
			MetricsLevel:      m.MetricsLevel,
		}
	}

	if p := in.ParallelismConfiguration; p != nil {
		desc.ParallelismConfigurationDescription = &ParallelismConfigDesc{
			ConfigurationType:  p.ConfigurationType,
			AutoScalingEnabled: p.AutoScalingEnabled,
			Parallelism:        p.Parallelism,
			ParallelismPerKPU:  p.ParallelismPerKPU,
			CurrentParallelism: p.Parallelism,
		}
	}

	return desc
}

func buildEnvironmentPropertyGroups(in *environmentPropertiesInput) []PropertyGroup {
	if in == nil {
		return nil
	}

	return in.PropertyGroups
}

func buildSnapshotsEnabled(in *snapshotConfigInput) *bool {
	if in == nil {
		return nil
	}

	v := in.SnapshotsEnabled

	return &v
}

func buildRollbackEnabled(in *systemRollbackConfigInput) *bool {
	if in == nil {
		return nil
	}

	v := in.RollbackEnabled

	return &v
}

func buildEncryptionConfigDesc(in *encryptionConfigInput) *ApplicationEncryptionConfigDesc {
	if in == nil {
		return nil
	}

	return &ApplicationEncryptionConfigDesc{KeyType: in.KeyType, KeyID: in.KeyID}
}

// buildZeppelinConfigDesc converts the inline ZeppelinApplicationConfiguration
// request into its stored/echoed description form. MonitoringConfigurationDescription
// is a required member of the real response shape, but real AWS's public
// documentation does not specify a default LogLevel when the request omits
// MonitoringConfiguration -- so, matching the existing DEFAULT-value
// convention for Flink's MonitoringConfiguration/ParallelismConfiguration
// (see applyCheckpointDefaults' doc comment), gopherstack leaves it unset
// rather than fabricating an undocumented default.
func buildZeppelinConfigDesc(in *zeppelinApplicationConfigInput) *ZeppelinApplicationConfigDescription {
	if in == nil {
		return nil
	}

	desc := &ZeppelinApplicationConfigDescription{}

	if in.MonitoringConfiguration != nil {
		desc.MonitoringConfigurationDescription = &ZeppelinMonitoringConfigDesc{
			LogLevel: in.MonitoringConfiguration.LogLevel,
		}
	}

	if cc := in.CatalogConfiguration; cc != nil && cc.GlueDataCatalogConfiguration != nil {
		desc.CatalogConfigurationDescription = &CatalogConfigDescription{
			GlueDataCatalogConfigurationDescription: cc.GlueDataCatalogConfiguration,
		}
	}

	if dc := in.DeployAsApplicationConfiguration; dc != nil && dc.S3ContentLocation != nil {
		desc.DeployAsApplicationConfigurationDescription = &DeployAsApplicationConfigDescription{
			S3ContentLocationDescription: dc.S3ContentLocation,
		}
	}

	if len(in.CustomArtifactsConfiguration) > 0 {
		artifacts := make([]CustomArtifactConfigDescription, 0, len(in.CustomArtifactsConfiguration))
		for _, a := range in.CustomArtifactsConfiguration {
			artifacts = append(artifacts, CustomArtifactConfigDescription{
				ArtifactType:                 a.ArtifactType,
				S3ContentLocationDescription: a.S3ContentLocation,
				MavenReferenceDescription:    a.MavenReference,
			})
		}

		desc.CustomArtifactsConfigurationDescription = artifacts
	}

	return desc
}

func (h *Handler) handleDescribeApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in describeApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleListApplications(ctx context.Context, c *echo.Context, body []byte) error {
	var in listApplicationsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	apps, outToken := h.Backend.ListApplications(ctx, in.NextToken)
	summaries := make([]applicationSummary, 0, len(apps))

	for _, app := range apps {
		summaries = append(summaries, toSummary(app))
	}

	return c.JSON(http.StatusOK, listApplicationsOutput{ApplicationSummaries: summaries, NextToken: outToken})
}

// deleteTimestampSeconds converts deleteApplicationInput's CreateTimestamp
// json.Number (present only when the caller supplies it -- real AWS's
// DeleteApplicationInput.CreateTimestamp is a required safety check retrieved
// from a prior DescribeApplication) into the epoch-seconds *float64
// DeleteApplication validates against. Returns nil (skip the check) when the
// field is absent or unparseable, matching the pre-existing "leniency, never
// causes a false accept/reject" note in PARITY.md.
func deleteTimestampSeconds(n json.Number) *float64 {
	if n == "" {
		return nil
	}

	f, err := n.Float64()
	if err != nil {
		return nil
	}

	return &f
}

func (h *Handler) handleDeleteApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	ts := deleteTimestampSeconds(in.CreateTimestamp)
	if err := h.Backend.DeleteApplication(ctx, in.ApplicationName, ts); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStartApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in startApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	var sqlRunConfigs []SQLRunConfigInput
	if in.RunConfiguration != nil {
		sqlRunConfigs = in.RunConfiguration.SQLRunConfigurations
	}

	opID, err := h.Backend.StartApplication(
		ctx, in.ApplicationName, toRunConfigInput(in.RunConfiguration), sqlRunConfigs,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, startStopApplicationOutput{OperationID: opID})
}

func (h *Handler) handleStopApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in startStopApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	force := in.Force != nil && *in.Force

	opID, err := h.Backend.StopApplication(ctx, in.ApplicationName, force)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, startStopApplicationOutput{OperationID: opID})
}

func (h *Handler) handleUpdateApplicationMaintenanceConfiguration(
	ctx context.Context, c *echo.Context, body []byte,
) error {
	var in updateMaintenanceConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	startTime := in.ApplicationMaintenanceConfigUpdate.ApplicationMaintenanceWindowStartTimeUpdate
	app, err := h.Backend.UpdateApplicationMaintenanceConfiguration(ctx, in.ApplicationName, startTime)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateMaintenanceConfigOutput{
		ApplicationARN: app.ApplicationARN,
		ApplicationMaintenanceConfigurationDescription: maintenanceConfigDescription{
			ApplicationMaintenanceWindowStartTime: app.MaintenanceWindowStartTime,
		},
	})
}

func (h *Handler) handleDiscoverInputSchema(ctx context.Context, c *echo.Context, body []byte) error {
	var in discoverInputSchemaInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	var startingPosition string
	if in.InputStartingPositionConfiguration != nil {
		startingPosition = in.InputStartingPositionConfiguration.InputStartingPosition
	}

	schema, err := h.Backend.DiscoverInputSchema(ctx, in.ResourceARN, in.ServiceExecutionRole, startingPosition)
	if err != nil {
		return h.handleError(c, err)
	}

	var out discoverInputSchemaOutput
	out.InputSchema.RecordFormat.RecordFormatType = schema.RecordFormat
	out.InputSchema.RecordEncoding = schema.RecordEncoding
	out.InputSchema.RecordColumns = schema.RecordColumns
	out.ParsedInputRecords = schema.ParsedInputRecords

	return c.JSON(http.StatusOK, out)
}

// toDetailOutput converts an Application to an API detail output.
func toDetailOutput(app *Application) applicationDetailOutput {
	out := applicationDetailOutput{
		ApplicationARN:                      app.ApplicationARN,
		ApplicationName:                     app.ApplicationName,
		ApplicationStatus:                   app.ApplicationStatus,
		RuntimeEnvironment:                  app.RuntimeEnvironment,
		ServiceExecutionRole:                app.ServiceExecutionRole,
		ApplicationDescription:              app.ApplicationDescription,
		ApplicationMode:                     app.ApplicationMode,
		ApplicationVersionID:                app.ApplicationVersionID,
		CreateTimestamp:                     awstime.Epoch(app.CreatedAt),
		LastUpdateTimestamp:                 awstime.Epoch(app.LastUpdateTimestamp),
		ApplicationVersionCreateTimestamp:   awstime.Epoch(app.ApplicationVersionCreateTimestamp),
		ApplicationVersionRolledBackFrom:    app.ApplicationVersionRolledBackFrom,
		ApplicationVersionRolledBackTo:      app.ApplicationVersionRolledBackTo,
		ApplicationVersionUpdatedFrom:       app.ApplicationVersionUpdatedFrom,
		ConditionalToken:                    conditionalToken(app),
		ApplicationConfigurationDescription: buildAppConfigDescOutput(app),
	}

	if len(app.CloudWatchLoggingOptionDescs) > 0 {
		out.CloudWatchLoggingOptionDescriptions = app.CloudWatchLoggingOptionDescs
	}

	if app.MaintenanceWindowStartTime != "" {
		out.ApplicationMaintenanceConfigurationDescription = &maintenanceConfigDescription{
			ApplicationMaintenanceWindowStartTime: app.MaintenanceWindowStartTime,
		}
	}

	return out
}

// buildAppConfigDescOutput builds ApplicationConfigurationDescription,
// returning nil when the application carries none of its sub-fields --
// matches the pre-existing convention of omitting the whole envelope for a
// bare application with no configuration at all.
func buildAppConfigDescOutput(app *Application) *appConfigDesc {
	hasSQL := appHasSQLConfig(app)
	if !hasSQL && !appHasNonSQLConfig(app) {
		return nil
	}

	desc := &appConfigDesc{
		ApplicationCodeConfigurationDescription:           app.CodeConfig,
		FlinkApplicationConfigurationDescription:          app.FlinkConfig,
		ZeppelinApplicationConfigurationDescription:       app.ZeppelinConfig,
		ApplicationSnapshotConfigurationDescription:       buildSnapshotConfigDescOutput(app.SnapshotsEnabled),
		ApplicationSystemRollbackConfigurationDescription: buildRollbackConfigDescOutput(app.RollbackEnabled),
		ApplicationEncryptionConfigurationDescription:     app.EncryptionConfig,
		RunConfigurationDescription:                       app.RunConfig,
	}

	if hasSQL {
		desc.SQLApplicationConfigurationDescription = &sqlAppConfigDesc{
			InputDescriptions:               app.InputDescriptions,
			OutputDescriptions:              app.OutputDescriptions,
			ReferenceDataSourceDescriptions: app.ReferenceDataSourceDescriptions,
		}
	}

	if len(app.VpcConfigurationDescriptions) > 0 {
		desc.VpcConfigurationDescriptions = app.VpcConfigurationDescriptions
	}

	if len(app.EnvironmentPropertyGroups) > 0 {
		desc.EnvironmentPropertyDescriptions = &environmentPropertyDescOutput{
			PropertyGroupDescriptions: app.EnvironmentPropertyGroups,
		}
	}

	return desc
}

func appHasSQLConfig(app *Application) bool {
	return len(app.InputDescriptions) > 0 || len(app.OutputDescriptions) > 0 ||
		len(app.ReferenceDataSourceDescriptions) > 0
}

func appHasNonSQLConfig(app *Application) bool {
	return len(app.VpcConfigurationDescriptions) > 0 || app.CodeConfig != nil ||
		app.FlinkConfig != nil || len(app.EnvironmentPropertyGroups) > 0 ||
		app.SnapshotsEnabled != nil || app.RollbackEnabled != nil || app.EncryptionConfig != nil ||
		app.RunConfig != nil || app.ZeppelinConfig != nil
}

func buildSnapshotConfigDescOutput(v *bool) *ApplicationSnapshotConfigDesc {
	if v == nil {
		return nil
	}

	return &ApplicationSnapshotConfigDesc{SnapshotsEnabled: *v}
}

func buildRollbackConfigDescOutput(v *bool) *ApplicationSystemRollbackConfigDesc {
	if v == nil {
		return nil
	}

	return &ApplicationSystemRollbackConfigDesc{RollbackEnabled: *v}
}
