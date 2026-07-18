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

// applicationConfigurationInput mirrors real AWS's ApplicationConfiguration
// request shape. Only the SQL-application and VPC portions are modeled --
// gopherstack's Application has no state for application code artifacts or
// Flink runtime settings, so ApplicationCodeConfiguration,
// FlinkApplicationConfiguration, EnvironmentProperties,
// ApplicationSnapshotConfiguration, ApplicationSystemRollbackConfiguration,
// ApplicationEncryptionConfiguration, and ZeppelinApplicationConfiguration
// are accepted (to avoid rejecting well-formed requests) but not modeled.
type applicationConfigurationInput struct {
	SQLApplicationConfiguration *sqlApplicationConfigInput `json:"SqlApplicationConfiguration,omitempty"` //nolint:lll,tagliatelle // AWS API name
	VpcConfigurations           []*vpcConfigInput          `json:"VpcConfigurations,omitempty"`
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

type applicationDetailOutput struct {
	ApplicationConfigurationDescription *appConfigDesc                `json:"ApplicationConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	ApplicationMode                     string                        `json:"ApplicationMode,omitempty"`
	ApplicationStatus                   string                        `json:"ApplicationStatus"`
	RuntimeEnvironment                  string                        `json:"RuntimeEnvironment"`
	ServiceExecutionRole                string                        `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription              string                        `json:"ApplicationDescription,omitempty"`
	ApplicationARN                      string                        `json:"ApplicationARN"`
	ApplicationName                     string                        `json:"ApplicationName"`
	Tags                                []Tag                         `json:"Tags,omitempty"`
	CloudWatchLoggingOptionDescriptions []CloudWatchLoggingOptionDesc `json:"CloudWatchLoggingOptionDescriptions,omitempty"` //nolint:lll // AWS API name
	VpcConfigurationDescriptions        []VpcConfigurationDescription `json:"VpcConfigurationDescriptions,omitempty"`
	ApplicationVersionID                int64                         `json:"ApplicationVersionId"`
	CreateTimestamp                     float64                       `json:"CreateTimestamp"`
}

// appConfigDesc holds the SQL-based application configuration.
type appConfigDesc struct {
	SQLApplicationConfigurationDescription *sqlAppConfigDesc `json:"SqlApplicationConfigurationDescription,omitempty"` //nolint:lll,tagliatelle // AWS API name
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

type updateApplicationInput struct {
	ApplicationName             string `json:"ApplicationName"`
	ServiceExecutionRoleUpdate  string `json:"ServiceExecutionRoleUpdate,omitempty"`
	ApplicationDescription      string `json:"ApplicationDescription,omitempty"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId"`
}

type updateApplicationOutput struct {
	OperationID       string                  `json:"OperationId,omitempty"`
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

type deleteApplicationInput struct {
	ApplicationName string      `json:"ApplicationName"`
	CreateTimestamp json.Number `json:"CreateTimestamp,omitempty"`
}

type startStopApplicationInput struct {
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

type discoverInputSchemaInput struct {
	ResourceARN           string `json:"ResourceARN"`
	RoleARN               string `json:"RoleARN,omitempty"`
	InputStartingPosition string `json:"InputStartingPosition,omitempty"`
}

type discoverInputSchemaRecordFormat struct {
	RecordFormatType string `json:"RecordFormatType"`
}

type discoverInputSchemaInner struct {
	RecordEncoding string                          `json:"RecordEncoding,omitempty"`
	RecordFormat   discoverInputSchemaRecordFormat `json:"RecordFormat"`
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
	inputs, outputs, refs, vpcs, cwlOpts := buildInitialConfig(&in)
	if len(inputs) > 0 || len(outputs) > 0 || len(refs) > 0 || len(vpcs) > 0 || len(cwlOpts) > 0 {
		if seedErr := h.Backend.SeedApplicationConfiguration(
			ctx, in.ApplicationName, inputs, outputs, refs, vpcs, cwlOpts,
		); seedErr != nil {
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

// buildInitialConfig extracts the inline SQL/VPC/CloudWatch-logging
// configuration from a CreateApplicationInput, converting each entry with
// the same buildInputDescription/buildOutputDescription/
// buildRefDataSourceDescription/buildVpcConfigDescription helpers the
// Add* handlers use, so the two paths always produce identical shapes.
func buildInitialConfig(in *createApplicationInput) (
	[]InputDescription,
	[]OutputDescription,
	[]ReferenceDataSourceDescription,
	[]VpcConfigurationDescription,
	[]CloudWatchLoggingOptionDesc,
) {
	var (
		inputs  []InputDescription
		outputs []OutputDescription
		refs    []ReferenceDataSourceDescription
		vpcs    []VpcConfigurationDescription
		cwlOpts []CloudWatchLoggingOptionDesc
	)

	if in.ApplicationConfiguration != nil {
		if sql := in.ApplicationConfiguration.SQLApplicationConfiguration; sql != nil {
			for _, i := range sql.Inputs {
				inputs = append(inputs, buildInputDescription(i))
			}

			for _, o := range sql.Outputs {
				outputs = append(outputs, buildOutputDescription(o))
			}

			for _, r := range sql.ReferenceDataSources {
				refs = append(refs, buildRefDataSourceDescription(r))
			}
		}

		for _, v := range in.ApplicationConfiguration.VpcConfigurations {
			vpcs = append(vpcs, buildVpcConfigDescription(v))
		}
	}

	if len(in.CloudWatchLoggingOptions) > 0 {
		cwlOpts = make([]CloudWatchLoggingOptionDesc, 0, len(in.CloudWatchLoggingOptions))
	}

	for _, c := range in.CloudWatchLoggingOptions {
		cwlOpts = append(cwlOpts, CloudWatchLoggingOptionDesc{
			LogStreamARN: c.LogStreamARN,
			RoleARN:      c.RoleARN,
		})
	}

	return inputs, outputs, refs, vpcs, cwlOpts
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

func (h *Handler) handleUpdateApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in updateApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, opID, err := h.Backend.UpdateApplication(
		ctx,
		in.ApplicationName,
		in.CurrentApplicationVersionID,
		in.ServiceExecutionRoleUpdate,
		in.ApplicationDescription,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
		OperationID:       opID,
	})
}

func (h *Handler) handleDeleteApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplication(ctx, in.ApplicationName); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStartApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in startStopApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	opID, err := h.Backend.StartApplication(ctx, in.ApplicationName)
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

	opID, err := h.Backend.StopApplication(ctx, in.ApplicationName)
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

	schema, err := h.Backend.DiscoverInputSchema(ctx, in.ResourceARN, in.RoleARN, in.InputStartingPosition)
	if err != nil {
		return h.handleError(c, err)
	}

	var out discoverInputSchemaOutput
	out.InputSchema.RecordFormat.RecordFormatType = schema.RecordFormat
	out.InputSchema.RecordEncoding = schema.RecordEncoding
	out.ParsedInputRecords = schema.ParsedInputRecords

	return c.JSON(http.StatusOK, out)
}

// toDetailOutput converts an Application to an API detail output.
func toDetailOutput(app *Application) applicationDetailOutput {
	out := applicationDetailOutput{
		ApplicationARN:         app.ApplicationARN,
		ApplicationName:        app.ApplicationName,
		ApplicationStatus:      app.ApplicationStatus,
		RuntimeEnvironment:     app.RuntimeEnvironment,
		ServiceExecutionRole:   app.ServiceExecutionRole,
		ApplicationDescription: app.ApplicationDescription,
		ApplicationMode:        app.ApplicationMode,
		ApplicationVersionID:   app.ApplicationVersionID,
		Tags:                   app.Tags,
		CreateTimestamp:        awstime.Epoch(app.CreatedAt),
	}

	if len(app.CloudWatchLoggingOptionDescs) > 0 {
		out.CloudWatchLoggingOptionDescriptions = app.CloudWatchLoggingOptionDescs
	}

	if len(app.VpcConfigurationDescriptions) > 0 {
		out.VpcConfigurationDescriptions = app.VpcConfigurationDescriptions
	}

	if len(app.InputDescriptions) > 0 || len(app.OutputDescriptions) > 0 ||
		len(app.ReferenceDataSourceDescriptions) > 0 {
		out.ApplicationConfigurationDescription = &appConfigDesc{
			SQLApplicationConfigurationDescription: &sqlAppConfigDesc{
				InputDescriptions:               app.InputDescriptions,
				OutputDescriptions:              app.OutputDescriptions,
				ReferenceDataSourceDescriptions: app.ReferenceDataSourceDescriptions,
			},
		}
	}

	return out
}
