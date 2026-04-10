package kinesisanalyticsv2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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
	ops     map[string]func(*echo.Context, []byte) error
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
		ctx := c.Request().Context()
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

		return fn(c, body)
	}
}

// buildOps constructs the dispatch map once at handler-creation time.
func (h *Handler) buildOps() map[string]func(*echo.Context, []byte) error {
	return map[string]func(*echo.Context, []byte) error{
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
	}
}

// ----------------------------------------
// Request / response types
// ----------------------------------------

type createApplicationInput struct {
	ApplicationName        string `json:"ApplicationName"`
	RuntimeEnvironment     string `json:"RuntimeEnvironment"`
	ServiceExecutionRole   string `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription string `json:"ApplicationDescription,omitempty"`
	ApplicationMode        string `json:"ApplicationMode,omitempty"`
	Tags                   []Tag  `json:"Tags,omitempty"`
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
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

type deleteApplicationInput struct {
	ApplicationName string      `json:"ApplicationName"`
	CreateTimestamp json.Number `json:"CreateTimestamp,omitempty"`
}

type startStopApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
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

func (h *Handler) handleAddApplicationCloudWatchLoggingOption(c *echo.Context, body []byte) error {
	var in addApplicationCWLOptionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.CloudWatchLoggingOption == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "CloudWatchLoggingOption is required")
	}

	if err := h.Backend.AddApplicationCloudWatchLoggingOption(
		in.ApplicationName,
		in.CurrentApplicationVersionID,
		in.CloudWatchLoggingOption.LogStreamARN,
		in.CloudWatchLoggingOption.RoleARN,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
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

	if err := h.Backend.AddApplicationInput(in.ApplicationName, in.CurrentApplicationVersionID, desc); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationInputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
		InputDescriptions:    app.InputDescriptions,
	})
}

func (h *Handler) handleAddApplicationInputProcessingConfiguration(c *echo.Context, body []byte) error {
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
		in.ApplicationName, in.CurrentApplicationVersionID, in.InputID, config,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
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

	if err := h.Backend.AddApplicationOutput(in.ApplicationName, in.CurrentApplicationVersionID, desc); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationOutputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
		OutputDescriptions:   app.OutputDescriptions,
	})
}

func (h *Handler) handleAddApplicationReferenceDataSource(c *echo.Context, body []byte) error {
	var in addApplicationRefDataSourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.ReferenceDataSource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "ReferenceDataSource is required")
	}

	ref := ReferenceDataSourceDescription{
		TableName: in.ReferenceDataSource.TableName,
	}

	if in.ReferenceDataSource.S3ReferenceDataSource != nil {
		ref.S3ReferenceDataSourceDescription = &S3ReferenceDataSourceDesc{
			BucketARN: in.ReferenceDataSource.S3ReferenceDataSource.BucketARN,
			FileKey:   in.ReferenceDataSource.S3ReferenceDataSource.FileKey,
		}
	}

	if err := h.Backend.AddApplicationReferenceDataSource(
		in.ApplicationName, in.CurrentApplicationVersionID, ref,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationRefDataSourceOutput{
		ApplicationARN:                  app.ApplicationARN,
		ApplicationVersionID:            app.ApplicationVersionID,
		ReferenceDataSourceDescriptions: app.ReferenceDataSourceDescriptions,
	})
}

func (h *Handler) handleAddApplicationVpcConfiguration(c *echo.Context, body []byte) error {
	var in addApplicationVpcConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.VpcConfiguration == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "VpcConfiguration is required")
	}

	vpc := VpcConfigurationDescription{
		SubnetIDs:        in.VpcConfiguration.SubnetIDs,
		SecurityGroupIDs: in.VpcConfiguration.SecurityGroupIDs,
	}

	if err := h.Backend.AddApplicationVpcConfiguration(
		in.ApplicationName, in.CurrentApplicationVersionID, vpc,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
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

func (h *Handler) handleCreateApplicationPresignedURL(c *echo.Context, body []byte) error {
	var in createPresignedURLInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.URLType == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "URLType is required")
	}

	// Verify the application exists.
	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	// Return a synthetic presigned URL based on the application ARN.
	presignedURL := "https://flink.amazonaws.com/dashboard/" + app.ApplicationARN + "?type=" + in.URLType

	return c.JSON(http.StatusOK, createPresignedURLOutput{AuthorizedURL: presignedURL})
}

func (h *Handler) handleDeleteApplicationCloudWatchLoggingOption(c *echo.Context, body []byte) error {
	var in deleteApplicationCWLOptionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationCloudWatchLoggingOption(
		in.ApplicationName, in.CurrentApplicationVersionID, in.CloudWatchLoggingOptionID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationCWLOptionOutput{
		ApplicationARN:                      app.ApplicationARN,
		ApplicationVersionID:                app.ApplicationVersionID,
		CloudWatchLoggingOptionDescriptions: app.CloudWatchLoggingOptionDescs,
	})
}

func (h *Handler) handleDeleteApplicationInputProcessingConfiguration(c *echo.Context, body []byte) error {
	var in deleteInputProcessingConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationInputProcessingConfiguration(
		in.ApplicationName, in.CurrentApplicationVersionID, in.InputID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteInputProcessingConfigOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

func (h *Handler) handleDeleteApplicationOutput(c *echo.Context, body []byte) error {
	var in deleteApplicationOutputInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationOutput(
		in.ApplicationName, in.CurrentApplicationVersionID, in.OutputID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationOutputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

func (h *Handler) handleCreateApplication(c *echo.Context, body []byte) error {
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

	return c.JSON(http.StatusOK, createApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleDescribeApplication(c *echo.Context, body []byte) error {
	var in describeApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleListApplications(c *echo.Context, body []byte) error {
	var in listApplicationsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	apps, outToken := h.Backend.ListApplications(in.NextToken)
	summaries := make([]applicationSummary, 0, len(apps))

	for _, app := range apps {
		summaries = append(summaries, toSummary(app))
	}

	return c.JSON(http.StatusOK, listApplicationsOutput{ApplicationSummaries: summaries, NextToken: outToken})
}

func (h *Handler) handleUpdateApplication(c *echo.Context, body []byte) error {
	var in updateApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, err := h.Backend.UpdateApplication(
		in.ApplicationName,
		in.ServiceExecutionRoleUpdate,
		in.ApplicationDescription,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleDeleteApplication(c *echo.Context, body []byte) error {
	var in deleteApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplication(in.ApplicationName); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStartApplication(c *echo.Context, body []byte) error {
	var in startStopApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.StartApplication(in.ApplicationName); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStopApplication(c *echo.Context, body []byte) error {
	var in startStopApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.StopApplication(in.ApplicationName); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// ----------------------------------------
// Snapshot handlers
// ----------------------------------------

func (h *Handler) handleCreateApplicationSnapshot(c *echo.Context, body []byte) error {
	var in createSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snap, err := h.Backend.CreateApplicationSnapshot(in.ApplicationName, in.SnapshotName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct {
		SnapshotDetails snapshotDetail `json:"SnapshotDetails"`
	}{SnapshotDetails: toSnapshotDetail(snap)})
}

func (h *Handler) handleDescribeApplicationSnapshot(c *echo.Context, body []byte) error {
	var in describeSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snap, err := h.Backend.DescribeApplicationSnapshot(in.ApplicationName, in.SnapshotName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeSnapshotOutput{SnapshotDetails: toSnapshotDetail(snap)})
}

func (h *Handler) handleListApplicationSnapshots(c *echo.Context, body []byte) error {
	var in listSnapshotsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snaps, outToken, err := h.Backend.ListApplicationSnapshots(in.ApplicationName, in.NextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	details := make([]snapshotDetail, 0, len(snaps))
	for _, s := range snaps {
		details = append(details, toSnapshotDetail(s))
	}

	return c.JSON(http.StatusOK, listSnapshotsOutput{SnapshotSummaries: details, NextToken: outToken})
}

func (h *Handler) handleDeleteApplicationSnapshot(c *echo.Context, body []byte) error {
	var in deleteSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationSnapshot(in.ApplicationName, in.SnapshotName); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var in listTagsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsOutput{Tags: tags})
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
		CreateTimestamp:        float64(app.CreatedAt.Unix()),
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
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "ResourceInUseException", err.Error())
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
