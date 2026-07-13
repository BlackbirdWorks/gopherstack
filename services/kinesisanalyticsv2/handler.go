package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// targetPrefix is the X-Amz-Target prefix for the Kinesis Analytics v2 JSON protocol.
	targetPrefix = "KinesisAnalytics_20180523."
)

// Handler is the HTTP handler for the Kinesis Data Analytics v2 JSON API.
type Handler struct {
	Backend StorageBackend
	ops     map[string]func(context.Context, *echo.Context, []byte) error
}

// NewHandler creates a new Kinesis Data Analytics v2 handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears handler state by delegating to the backend if it supports it.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "KinesisAnalyticsV2" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"AddApplicationReferenceDataSource",
		"AddApplicationVpcConfiguration",
		"CreateApplication",
		"CreateApplicationPresignedUrl",
		"DeleteApplication",
		"DeleteApplicationCloudWatchLoggingOption",
		"DeleteApplicationInputProcessingConfiguration",
		"DeleteApplicationOutput",
		"DeleteApplicationSnapshot",
		"DescribeApplication",
		"DescribeApplicationSnapshot",
		"ListApplications",
		"ListApplicationSnapshots",
		"ListTagsForResource",
		"StartApplication",
		"StopApplication",
		"TagResource",
		"UntagResource",
		"UpdateApplication",
		"CreateApplicationSnapshot",
		"DeleteApplicationReferenceDataSource",
		"DeleteApplicationVpcConfiguration",
		"DescribeApplicationOperation",
		"DescribeApplicationVersion",
		"DiscoverInputSchema",
		"ListApplicationOperations",
		"ListApplicationVersions",
		"RollbackApplication",
		"UpdateApplicationMaintenanceConfiguration",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "kinesisanalyticsv2" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Kinesis Data Analytics v2 requests.
// The SDK uses X-Amz-Target: KinesisAnalytics_20180523.{Operation} with POST to /.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	if !strings.HasPrefix(target, targetPrefix) {
		return ""
	}

	return strings.TrimPrefix(target, targetPrefix)
}

// ExtractResource extracts the application name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ApplicationName string `json:"ApplicationName"`
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ApplicationName
}

// Handler returns the Echo handler function for Kinesis Data Analytics v2 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := h.contextWithRegion(c)
		log := logger.Load(ctx)

		op := h.ExtractOperation(c)
		if op == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "missing X-Amz-Target header")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "kinesisanalyticsv2: failed to read request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "kinesisanalyticsv2 request", "op", op)

		fn, ok := h.ops[op]
		if !ok {
			return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "unknown operation: "+op)
		}

		return fn(ctx, c, body)
	}
}

// contextWithRegion returns the request context with the resolved AWS region attached
// under regionContextKey so that backend operations are routed to the correct region.
// The SigV4 credential-scope region in the Authorization header (extracted by
// httputils.ExtractRegionFromRequest) takes precedence over the backend default.
func (h *Handler) contextWithRegion(c *echo.Context) context.Context {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

	return context.WithValue(c.Request().Context(), regionContextKey{}, region)
}

// buildOps constructs the dispatch map once at handler-creation time.
func (h *Handler) buildOps() map[string]func(context.Context, *echo.Context, []byte) error {
	return map[string]func(context.Context, *echo.Context, []byte) error{
		// Add operations
		"AddApplicationCloudWatchLoggingOption":      h.handleAddApplicationCloudWatchLoggingOption,
		"AddApplicationInput":                        h.handleAddApplicationInput,
		"AddApplicationInputProcessingConfiguration": h.handleAddApplicationInputProcessingConfiguration,
		"AddApplicationOutput":                       h.handleAddApplicationOutput,
		"AddApplicationReferenceDataSource":          h.handleAddApplicationReferenceDataSource,
		"AddApplicationVpcConfiguration":             h.handleAddApplicationVpcConfiguration,
		// Delete operations
		"DeleteApplicationCloudWatchLoggingOption":      h.handleDeleteApplicationCloudWatchLoggingOption,
		"DeleteApplicationInputProcessingConfiguration": h.handleDeleteApplicationInputProcessingConfiguration,
		"DeleteApplicationOutput":                       h.handleDeleteApplicationOutput,
		"DeleteApplication":                             h.handleDeleteApplication,
		"DeleteApplicationSnapshot":                     h.handleDeleteApplicationSnapshot,
		// Core operations
		"CreateApplication":             h.handleCreateApplication,
		"CreateApplicationPresignedUrl": h.handleCreateApplicationPresignedURL,
		"CreateApplicationSnapshot":     h.handleCreateApplicationSnapshot,
		"DescribeApplication":           h.handleDescribeApplication,
		"DescribeApplicationSnapshot":   h.handleDescribeApplicationSnapshot,
		"ListApplications":              h.handleListApplications,
		"ListApplicationSnapshots":      h.handleListApplicationSnapshots,
		"ListTagsForResource":           h.handleListTagsForResource,
		"StartApplication":              h.handleStartApplication,
		"StopApplication":               h.handleStopApplication,
		"TagResource":                   h.handleTagResource,
		"UntagResource":                 h.handleUntagResource,
		"UpdateApplication":             h.handleUpdateApplication,
		// New operations
		"DeleteApplicationReferenceDataSource":      h.handleDeleteApplicationReferenceDataSource,
		"DeleteApplicationVpcConfiguration":         h.handleDeleteApplicationVpcConfiguration,
		"DescribeApplicationOperation":              h.handleDescribeApplicationOperation,
		"DescribeApplicationVersion":                h.handleDescribeApplicationVersion,
		"DiscoverInputSchema":                       h.handleDiscoverInputSchema,
		"ListApplicationOperations":                 h.handleListApplicationOperations,
		"ListApplicationVersions":                   h.handleListApplicationVersions,
		"RollbackApplication":                       h.handleRollbackApplication,
		"UpdateApplicationMaintenanceConfiguration": h.handleUpdateApplicationMaintenanceConfiguration,
	}
}

// ----------------------------------------
// Request / response types
// ----------------------------------------

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

type createSnapshotInput struct {
	ApplicationName string `json:"ApplicationName"`
	SnapshotName    string `json:"SnapshotName"`
}

type describeSnapshotInput struct {
	ApplicationName string `json:"ApplicationName"`
	SnapshotName    string `json:"SnapshotName"`
}

type describeSnapshotOutput struct {
	SnapshotDetails snapshotDetail `json:"SnapshotDetails"`
}

type listSnapshotsInput struct {
	ApplicationName string `json:"ApplicationName"`
	NextToken       string `json:"NextToken,omitempty"`
}

type listSnapshotsOutput struct {
	NextToken         string           `json:"NextToken,omitempty"`
	SnapshotSummaries []snapshotDetail `json:"SnapshotSummaries"`
}

type deleteSnapshotInput struct {
	ApplicationName           string      `json:"ApplicationName"`
	SnapshotName              string      `json:"SnapshotName"`
	SnapshotCreationTimestamp json.Number `json:"SnapshotCreationTimestamp,omitempty"`
}

type tagResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []Tag  `json:"Tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsOutput struct {
	Tags []Tag `json:"Tags"`
}

type errorResponse struct {
	Message string `json:"message"`
	Code    string `json:"__type"`
}

// ----------------------------------------
// New operation request / response types
// ----------------------------------------

type cwlOptionInput struct {
	LogStreamARN string `json:"LogStreamARN"`
	RoleARN      string `json:"RoleARN,omitempty"`
}

type addApplicationCWLOptionInput struct {
	CloudWatchLoggingOption     *cwlOptionInput `json:"CloudWatchLoggingOption"`
	ApplicationName             string          `json:"ApplicationName"`
	CurrentApplicationVersionID int64           `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationCWLOptionOutput struct {
	ApplicationARN                      string                        `json:"ApplicationARN"`
	CloudWatchLoggingOptionDescriptions []CloudWatchLoggingOptionDesc `json:"CloudWatchLoggingOptionDescriptions"`
	ApplicationVersionID                int64                         `json:"ApplicationVersionId"`
}

type kinesisStreamsInputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type kinesisFirehoseInputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type lambdaProcessorInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type inputProcessingConfigInput struct {
	InputLambdaProcessor *lambdaProcessorInput `json:"InputLambdaProcessor,omitempty"`
}

type inputConfig struct {
	KinesisStreamsInput          *kinesisStreamsInputConfig  `json:"KinesisStreamsInput,omitempty"`
	KinesisFirehoseInput         *kinesisFirehoseInputConfig `json:"KinesisFirehoseInput,omitempty"`
	InputProcessingConfiguration *inputProcessingConfigInput `json:"InputProcessingConfiguration,omitempty"`
	NamePrefix                   string                      `json:"NamePrefix,omitempty"`
}

type addApplicationInputInput struct {
	Input                       *inputConfig `json:"Input"`
	ApplicationName             string       `json:"ApplicationName"`
	CurrentApplicationVersionID int64        `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationInputOutput struct {
	ApplicationARN       string             `json:"ApplicationARN"`
	InputDescriptions    []InputDescription `json:"InputDescriptions"`
	ApplicationVersionID int64              `json:"ApplicationVersionId"`
}

type addInputProcessingConfigInput struct {
	InputProcessingConfiguration *inputProcessingConfigInput `json:"InputProcessingConfiguration"`
	ApplicationName              string                      `json:"ApplicationName"`
	InputID                      string                      `json:"InputId"`
	CurrentApplicationVersionID  int64                       `json:"CurrentApplicationVersionId,omitempty"`
}

type addInputProcessingConfigOutput struct {
	InputProcessingConfigurationDescription *InputProcessingConfigurationDesc `json:"InputProcessingConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	ApplicationARN                          string                            `json:"ApplicationARN"`
	InputID                                 string                            `json:"InputId"`
	ApplicationVersionID                    int64                             `json:"ApplicationVersionId"`
}

type kinesisStreamsOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type kinesisFirehoseOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type lambdaOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type destinationSchemaInput struct {
	RecordFormatType string `json:"RecordFormatType"`
}

type outputConfig struct {
	KinesisStreamsOutput  *kinesisStreamsOutputConfig  `json:"KinesisStreamsOutput,omitempty"`
	KinesisFirehoseOutput *kinesisFirehoseOutputConfig `json:"KinesisFirehoseOutput,omitempty"`
	LambdaOutput          *lambdaOutputConfig          `json:"LambdaOutput,omitempty"`
	DestinationSchema     *destinationSchemaInput      `json:"DestinationSchema,omitempty"`
	Name                  string                       `json:"Name,omitempty"`
}

type addApplicationOutputInput struct {
	Output                      *outputConfig `json:"Output"`
	ApplicationName             string        `json:"ApplicationName"`
	CurrentApplicationVersionID int64         `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationOutputOutput struct {
	ApplicationARN       string              `json:"ApplicationARN"`
	OutputDescriptions   []OutputDescription `json:"OutputDescriptions"`
	ApplicationVersionID int64               `json:"ApplicationVersionId"`
}

type s3ReferenceDataSourceConfig struct {
	BucketARN string `json:"BucketARN"`
	FileKey   string `json:"FileKey"`
}

type refDataSourceConfig struct {
	S3ReferenceDataSource *s3ReferenceDataSourceConfig `json:"S3ReferenceDataSource,omitempty"`
	TableName             string                       `json:"TableName,omitempty"`
}

type addApplicationRefDataSourceInput struct {
	ReferenceDataSource         *refDataSourceConfig `json:"ReferenceDataSource"`
	ApplicationName             string               `json:"ApplicationName"`
	CurrentApplicationVersionID int64                `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationRefDataSourceOutput struct {
	ApplicationARN                  string                           `json:"ApplicationARN"`
	ReferenceDataSourceDescriptions []ReferenceDataSourceDescription `json:"ReferenceDataSourceDescriptions"`
	ApplicationVersionID            int64                            `json:"ApplicationVersionId"`
}

type vpcConfigInput struct {
	SubnetIDs        []string `json:"SubnetIds"`
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
}

type addApplicationVpcConfigInput struct {
	VpcConfiguration            *vpcConfigInput `json:"VpcConfiguration"`
	ApplicationName             string          `json:"ApplicationName"`
	CurrentApplicationVersionID int64           `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationVpcConfigOutput struct {
	VpcConfigurationDescription *VpcConfigurationDescription `json:"VpcConfigurationDescription,omitempty"`
	ApplicationARN              string                       `json:"ApplicationARN"`
	ApplicationVersionID        int64                        `json:"ApplicationVersionId"`
}

type createPresignedURLInput struct {
	ApplicationName                    string `json:"ApplicationName"`
	URLType                            string `json:"URLType"`
	SessionExpirationDurationInSeconds int64  `json:"SessionExpirationDurationInSeconds,omitempty"`
}

type createPresignedURLOutput struct {
	AuthorizedURL string `json:"AuthorizedUrl,omitempty"` //nolint:tagliatelle // AWS API field is AuthorizedUrl
}

type deleteApplicationCWLOptionInput struct {
	CloudWatchLoggingOptionID   string `json:"CloudWatchLoggingOptionId"`
	ApplicationName             string `json:"ApplicationName"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteApplicationCWLOptionOutput struct {
	ApplicationARN                      string                        `json:"ApplicationARN"`
	CloudWatchLoggingOptionDescriptions []CloudWatchLoggingOptionDesc `json:"CloudWatchLoggingOptionDescriptions"`
	ApplicationVersionID                int64                         `json:"ApplicationVersionId"`
}

type deleteInputProcessingConfigInput struct {
	ApplicationName             string `json:"ApplicationName"`
	InputID                     string `json:"InputId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteInputProcessingConfigOutput struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type deleteApplicationOutputInput struct {
	ApplicationName             string `json:"ApplicationName"`
	OutputID                    string `json:"OutputId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteApplicationOutputOutput struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

// ----------------------------------------
// Application handlers
// ----------------------------------------

func (h *Handler) handleAddApplicationCloudWatchLoggingOption(ctx context.Context, c *echo.Context, body []byte) error {
	var in addApplicationCWLOptionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.CloudWatchLoggingOption == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "CloudWatchLoggingOption is required")
	}

	if err := h.Backend.AddApplicationCloudWatchLoggingOption(
		ctx,
		in.ApplicationName,
		in.CurrentApplicationVersionID,
		in.CloudWatchLoggingOption.LogStreamARN,
		in.CloudWatchLoggingOption.RoleARN,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationCWLOptionOutput{
		ApplicationARN:                      app.ApplicationARN,
		ApplicationVersionID:                app.ApplicationVersionID,
		CloudWatchLoggingOptionDescriptions: app.CloudWatchLoggingOptionDescs,
	})
}

//nolint:dupl // add input/output handlers share structure but are semantically distinct operations
func (h *Handler) handleAddApplicationInput(
	ctx context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in addApplicationInputInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.Input == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "Input is required")
	}

	desc := buildInputDescription(in.Input)

	if err := h.Backend.AddApplicationInput(ctx, in.ApplicationName, in.CurrentApplicationVersionID, desc); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationInputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
		InputDescriptions:    app.InputDescriptions,
	})
}

func (h *Handler) handleAddApplicationInputProcessingConfiguration(
	ctx context.Context, c *echo.Context, body []byte,
) error {
	var in addInputProcessingConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	var config *InputProcessingConfigurationDesc
	if in.InputProcessingConfiguration != nil && in.InputProcessingConfiguration.InputLambdaProcessor != nil {
		config = &InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: in.InputProcessingConfiguration.InputLambdaProcessor.ResourceARN,
			},
		}
	}

	if err := h.Backend.AddApplicationInputProcessingConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.InputID, config,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	var desc *InputProcessingConfigurationDesc

	for i := range app.InputDescriptions {
		if app.InputDescriptions[i].InputID == in.InputID {
			desc = app.InputDescriptions[i].InputProcessingConfigurationDescription

			break
		}
	}

	return c.JSON(http.StatusOK, addInputProcessingConfigOutput{
		ApplicationARN:                          app.ApplicationARN,
		ApplicationVersionID:                    app.ApplicationVersionID,
		InputID:                                 in.InputID,
		InputProcessingConfigurationDescription: desc,
	})
}

//nolint:dupl // add input/output handlers share structure but are semantically distinct operations
func (h *Handler) handleAddApplicationOutput(
	ctx context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in addApplicationOutputInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.Output == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "Output is required")
	}

	desc := buildOutputDescription(in.Output)

	if err := h.Backend.AddApplicationOutput(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, desc,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationOutputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
		OutputDescriptions:   app.OutputDescriptions,
	})
}

//nolint:dupl // add input/reference-data-source handlers share structure but are semantically distinct operations
func (h *Handler) handleAddApplicationReferenceDataSource(ctx context.Context, c *echo.Context, body []byte) error {
	var in addApplicationRefDataSourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.ReferenceDataSource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "ReferenceDataSource is required")
	}

	ref := buildRefDataSourceDescription(in.ReferenceDataSource)

	if err := h.Backend.AddApplicationReferenceDataSource(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, ref,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationRefDataSourceOutput{
		ApplicationARN:                  app.ApplicationARN,
		ApplicationVersionID:            app.ApplicationVersionID,
		ReferenceDataSourceDescriptions: app.ReferenceDataSourceDescriptions,
	})
}

func (h *Handler) handleAddApplicationVpcConfiguration(ctx context.Context, c *echo.Context, body []byte) error {
	var in addApplicationVpcConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.VpcConfiguration == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "VpcConfiguration is required")
	}

	vpc := buildVpcConfigDescription(in.VpcConfiguration)

	if err := h.Backend.AddApplicationVpcConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, vpc,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	var vpcDesc *VpcConfigurationDescription

	if len(app.VpcConfigurationDescriptions) > 0 {
		last := app.VpcConfigurationDescriptions[len(app.VpcConfigurationDescriptions)-1]
		vpcDesc = &last
	}

	return c.JSON(http.StatusOK, addApplicationVpcConfigOutput{
		ApplicationARN:              app.ApplicationARN,
		ApplicationVersionID:        app.ApplicationVersionID,
		VpcConfigurationDescription: vpcDesc,
	})
}

func (h *Handler) handleCreateApplicationPresignedURL(ctx context.Context, c *echo.Context, body []byte) error {
	var in createPresignedURLInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.URLType == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "URLType is required")
	}

	// Verify the application exists.
	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	// Return a synthetic presigned URL based on the application ARN.
	presignedURL := "https://flink.amazonaws.com/dashboard/" + app.ApplicationARN + "?type=" + in.URLType

	return c.JSON(http.StatusOK, createPresignedURLOutput{AuthorizedURL: presignedURL})
}

func (h *Handler) handleDeleteApplicationCloudWatchLoggingOption(
	ctx context.Context, c *echo.Context, body []byte,
) error {
	var in deleteApplicationCWLOptionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationCloudWatchLoggingOption(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.CloudWatchLoggingOptionID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationCWLOptionOutput{
		ApplicationARN:                      app.ApplicationARN,
		ApplicationVersionID:                app.ApplicationVersionID,
		CloudWatchLoggingOptionDescriptions: app.CloudWatchLoggingOptionDescs,
	})
}

func (h *Handler) handleDeleteApplicationInputProcessingConfiguration(
	ctx context.Context, c *echo.Context, body []byte,
) error {
	var in deleteInputProcessingConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationInputProcessingConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.InputID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteInputProcessingConfigOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

func (h *Handler) handleDeleteApplicationOutput(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteApplicationOutputInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationOutput(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.OutputID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationOutputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
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

// ----------------------------------------
// Snapshot handlers
// ----------------------------------------

func (h *Handler) handleCreateApplicationSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var in createSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snap, err := h.Backend.CreateApplicationSnapshot(ctx, in.ApplicationName, in.SnapshotName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct {
		SnapshotDetails snapshotDetail `json:"SnapshotDetails"`
	}{SnapshotDetails: toSnapshotDetail(snap)})
}

func (h *Handler) handleDescribeApplicationSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var in describeSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snap, err := h.Backend.DescribeApplicationSnapshot(ctx, in.ApplicationName, in.SnapshotName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeSnapshotOutput{SnapshotDetails: toSnapshotDetail(snap)})
}

func (h *Handler) handleListApplicationSnapshots(ctx context.Context, c *echo.Context, body []byte) error {
	var in listSnapshotsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snaps, outToken, err := h.Backend.ListApplicationSnapshots(ctx, in.ApplicationName, in.NextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	details := make([]snapshotDetail, 0, len(snaps))
	for _, s := range snaps {
		details = append(details, toSnapshotDetail(s))
	}

	return c.JSON(http.StatusOK, listSnapshotsOutput{SnapshotSummaries: details, NextToken: outToken})
}

func (h *Handler) handleDeleteApplicationSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationSnapshot(ctx, in.ApplicationName, in.SnapshotName); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleTagResource(ctx context.Context, c *echo.Context, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.TagResource(ctx, in.ResourceARN, in.Tags); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(ctx context.Context, c *echo.Context, body []byte) error {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.UntagResource(ctx, in.ResourceARN, in.TagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(ctx context.Context, c *echo.Context, body []byte) error {
	var in listTagsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsOutput{Tags: tags})
}

// ----------------------------------------
// New operation request/response types
// ----------------------------------------

type deleteApplicationRefDataSourceInput struct {
	ApplicationName             string `json:"ApplicationName"`
	ReferenceID                 string `json:"ReferenceId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteApplicationRefDataSourceOutput struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type deleteApplicationVpcConfigInput struct {
	ApplicationName             string `json:"ApplicationName"`
	VpcConfigurationID          string `json:"VpcConfigurationId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteApplicationVpcConfigOutput struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type describeApplicationOperationInput struct {
	ApplicationName string `json:"ApplicationName"`
	OperationID     string `json:"OperationId"`
}

// applicationOperationInfoDetails mirrors real AWS's
// ApplicationOperationInfoDetails shape (DescribeApplicationOperation): it
// does NOT carry OperationId (the caller already supplied it in the
// request) but does require StartTime/EndTime, both awsjson1.1 epoch-seconds
// numbers (see pkgs/awstime).
type applicationOperationInfoDetails struct {
	Operation       string  `json:"Operation"`
	OperationStatus string  `json:"OperationStatus"`
	StartTime       float64 `json:"StartTime"`
	EndTime         float64 `json:"EndTime"`
}

type describeApplicationOperationOutput struct {
	ApplicationOperationInfoDetails applicationOperationInfoDetails `json:"ApplicationOperationInfoDetails"`
}

type listApplicationOperationsInput struct {
	ApplicationName string `json:"ApplicationName"`
	NextToken       string `json:"NextToken,omitempty"`
}

// applicationOperationInfo mirrors real AWS's ApplicationOperationInfo shape
// (ListApplicationOperations): unlike applicationOperationInfoDetails it does
// carry OperationId, since list items need it to identify which operation to
// describe next.
type applicationOperationInfo struct {
	OperationID     string  `json:"OperationId,omitempty"`
	Operation       string  `json:"Operation,omitempty"`
	OperationStatus string  `json:"OperationStatus,omitempty"`
	StartTime       float64 `json:"StartTime,omitempty"`
	EndTime         float64 `json:"EndTime,omitempty"`
}

type listApplicationOperationsOutput struct {
	NextToken                    string                     `json:"NextToken,omitempty"`
	ApplicationOperationInfoList []applicationOperationInfo `json:"ApplicationOperationInfoList"`
}

type describeApplicationVersionInput struct {
	ApplicationName      string `json:"ApplicationName"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type describeApplicationVersionOutput struct {
	ApplicationVersionDetail applicationDetailOutput `json:"ApplicationVersionDetail"`
}

type listApplicationVersionsInput struct {
	ApplicationName string `json:"ApplicationName"`
	NextToken       string `json:"NextToken,omitempty"`
}

type applicationVersionSummaryOutput struct {
	ApplicationStatus    string `json:"ApplicationStatus"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type listApplicationVersionsOutput struct {
	NextToken                   string                            `json:"NextToken,omitempty"`
	ApplicationVersionSummaries []applicationVersionSummaryOutput `json:"ApplicationVersionSummaries"`
}

type rollbackApplicationInput struct {
	ApplicationName             string `json:"ApplicationName"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId"`
}

type rollbackApplicationOutput struct {
	OperationID       string                  `json:"OperationId,omitempty"`
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
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

// ----------------------------------------
// New operation handlers
// ----------------------------------------

func (h *Handler) handleDeleteApplicationReferenceDataSource(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteApplicationRefDataSourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationReferenceDataSource(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.ReferenceID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationRefDataSourceOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

func (h *Handler) handleDeleteApplicationVpcConfiguration(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteApplicationVpcConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationVpcConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.VpcConfigurationID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationVpcConfigOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

func (h *Handler) handleDescribeApplicationOperation(ctx context.Context, c *echo.Context, body []byte) error {
	var in describeApplicationOperationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	op, err := h.Backend.DescribeApplicationOperation(ctx, in.ApplicationName, in.OperationID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeApplicationOperationOutput{
		ApplicationOperationInfoDetails: applicationOperationInfoDetails{
			Operation:       op.Operation,
			OperationStatus: op.OperationStatus,
			StartTime:       awstime.Epoch(op.StartTimestamp),
			EndTime:         awstime.Epoch(op.EndTimestamp),
		},
	})
}

func (h *Handler) handleListApplicationOperations(ctx context.Context, c *echo.Context, body []byte) error {
	var in listApplicationOperationsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	ops, outToken, err := h.Backend.ListApplicationOperations(ctx, in.ApplicationName, in.NextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]applicationOperationInfo, 0, len(ops))
	for _, op := range ops {
		items = append(items, applicationOperationInfo{
			OperationID:     op.OperationID,
			Operation:       op.Operation,
			OperationStatus: op.OperationStatus,
			StartTime:       awstime.Epoch(op.StartTimestamp),
			EndTime:         awstime.Epoch(op.EndTimestamp),
		})
	}

	return c.JSON(http.StatusOK, listApplicationOperationsOutput{
		ApplicationOperationInfoList: items,
		NextToken:                    outToken,
	})
}

func (h *Handler) handleDescribeApplicationVersion(ctx context.Context, c *echo.Context, body []byte) error {
	var in describeApplicationVersionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, err := h.Backend.DescribeApplicationVersion(ctx, in.ApplicationName, in.ApplicationVersionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeApplicationVersionOutput{
		ApplicationVersionDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleListApplicationVersions(ctx context.Context, c *echo.Context, body []byte) error {
	var in listApplicationVersionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	vers, outToken, err := h.Backend.ListApplicationVersions(ctx, in.ApplicationName, in.NextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]applicationVersionSummaryOutput, 0, len(vers))
	for _, v := range vers {
		summaries = append(summaries, applicationVersionSummaryOutput{
			ApplicationVersionID: v.ApplicationVersionID,
			ApplicationStatus:    v.ApplicationStatus,
		})
	}

	return c.JSON(http.StatusOK, listApplicationVersionsOutput{
		ApplicationVersionSummaries: summaries,
		NextToken:                   outToken,
	})
}

func (h *Handler) handleRollbackApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in rollbackApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, opID, err := h.Backend.RollbackApplication(ctx, in.ApplicationName, in.CurrentApplicationVersionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, rollbackApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
		OperationID:       opID,
	})
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

// ----------------------------------------
// Helper functions
// ----------------------------------------

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

func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, errorResponse{
		Message: message,
		Code:    code,
	})
}

// handleError maps a backend error to the appropriate HTTP response.
//
// ErrConcurrentModification wraps awserr.ErrInvalidParameter (see backend.go),
// so its case must be checked before the generic ErrInvalidParameter case
// below -- otherwise every optimistic-concurrency conflict would be reported
// to the client as the generic "InvalidArgumentException" instead of
// "ConcurrentModificationException", which is the exception name aws-sdk-go-v2
// switches on (via the __type response field) to construct
// *types.ConcurrentModificationException for caller retry logic. Sibling
// service kinesisanalytics (v1) maps the same condition to
// (400, "ConcurrentModificationException"); kinesisanalyticsv2 follows suit.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "ResourceInUseException", err.Error())
	case errors.Is(err, ErrConcurrentModification):
		return h.writeError(c, http.StatusBadRequest, "ConcurrentModificationException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}

// buildInputDescription converts an inputConfig to an InputDescription.
func buildInputDescription(in *inputConfig) InputDescription {
	desc := InputDescription{
		NamePrefix: in.NamePrefix,
	}

	if in.KinesisStreamsInput != nil {
		desc.KinesisStreamsInputDescription = &KinesisStreamsInputDesc{
			ResourceARN: in.KinesisStreamsInput.ResourceARN,
		}
	}

	if in.KinesisFirehoseInput != nil {
		desc.KinesisFirehoseInputDescription = &KinesisFirehoseInputDesc{
			ResourceARN: in.KinesisFirehoseInput.ResourceARN,
		}
	}

	if in.InputProcessingConfiguration != nil && in.InputProcessingConfiguration.InputLambdaProcessor != nil {
		desc.InputProcessingConfigurationDescription = &InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: in.InputProcessingConfiguration.InputLambdaProcessor.ResourceARN,
			},
		}
	}

	return desc
}

// buildOutputDescription converts an outputConfig to an OutputDescription.
func buildOutputDescription(out *outputConfig) OutputDescription {
	desc := OutputDescription{
		Name: out.Name,
	}

	if out.KinesisStreamsOutput != nil {
		desc.KinesisStreamsOutputDescription = &KinesisStreamsOutputDesc{
			ResourceARN: out.KinesisStreamsOutput.ResourceARN,
		}
	}

	if out.KinesisFirehoseOutput != nil {
		desc.KinesisFirehoseOutputDescription = &KinesisFirehoseOutputDesc{
			ResourceARN: out.KinesisFirehoseOutput.ResourceARN,
		}
	}

	if out.LambdaOutput != nil {
		desc.LambdaOutputDescription = &LambdaOutputDesc{
			ResourceARN: out.LambdaOutput.ResourceARN,
		}
	}

	if out.DestinationSchema != nil {
		desc.DestinationSchema = &DestinationSchemaDesc{
			RecordFormatType: out.DestinationSchema.RecordFormatType,
		}
	}

	return desc
}

// buildRefDataSourceDescription converts a refDataSourceConfig to a
// ReferenceDataSourceDescription. Shared by handleAddApplicationReferenceDataSource
// and buildInitialConfig (CreateApplication's inline
// SqlApplicationConfiguration.ReferenceDataSources) so both paths produce
// identical shapes.
func buildRefDataSourceDescription(in *refDataSourceConfig) ReferenceDataSourceDescription {
	ref := ReferenceDataSourceDescription{
		TableName: in.TableName,
	}

	if in.S3ReferenceDataSource != nil {
		ref.S3ReferenceDataSourceDescription = &S3ReferenceDataSourceDesc{
			BucketARN: in.S3ReferenceDataSource.BucketARN,
			FileKey:   in.S3ReferenceDataSource.FileKey,
		}
	}

	return ref
}

// buildVpcConfigDescription converts a vpcConfigInput to a
// VpcConfigurationDescription. Shared by handleAddApplicationVpcConfiguration
// and buildInitialConfig (CreateApplication's inline VpcConfigurations) so
// both paths produce identical shapes.
func buildVpcConfigDescription(in *vpcConfigInput) VpcConfigurationDescription {
	return VpcConfigurationDescription{
		SubnetIDs:        in.SubnetIDs,
		SecurityGroupIDs: in.SecurityGroupIDs,
	}
}
