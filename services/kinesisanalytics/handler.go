package kinesisanalytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	kinesisanalyticsTargetPrefix = "KinesisAnalytics_20150814."
	kinesisanalyticsService      = "kinesisanalytics"
)

var (
	errUnknownAction   = errors.New("unknown action")
	errApplicationName = errors.New("ApplicationName is required")
	errResourceARN     = errors.New("ResourceARN is required")
	errInputID         = errors.New("InputId is required")
	errOutputID        = errors.New("OutputId is required")
	errReferenceID     = errors.New("ReferenceId is required")
	errCWLOptionID     = errors.New("CloudWatchLoggingOptionId is required")
)

// Handler is the HTTP handler for the Kinesis Analytics v1 API.
type Handler struct {
	ops           map[string]service.JSONOpFunc
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Kinesis Analytics handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears handler state (delegates to backend if supported).
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// buildOps constructs the dispatch map once at handler creation time.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateApplication":                     service.WrapOp(h.handleCreateApplication),
		"DeleteApplication":                     service.WrapOp(h.handleDeleteApplication),
		"DescribeApplication":                   service.WrapOp(h.handleDescribeApplication),
		"ListApplications":                      service.WrapOp(h.handleListApplications),
		"StartApplication":                      service.WrapOp(h.handleStartApplication),
		"StopApplication":                       service.WrapOp(h.handleStopApplication),
		"UpdateApplication":                     service.WrapOp(h.handleUpdateApplication),
		"ListTagsForResource":                   service.WrapOp(h.handleListTagsForResource),
		"TagResource":                           service.WrapOp(h.handleTagResource),
		"UntagResource":                         service.WrapOp(h.handleUntagResource),
		"AddApplicationCloudWatchLoggingOption": service.WrapOp(h.handleAddApplicationCloudWatchLoggingOption),
		"AddApplicationInput":                   service.WrapOp(h.handleAddApplicationInput),
		"AddApplicationInputProcessingConfiguration": service.WrapOp(
			h.handleAddApplicationInputProcessingConfiguration,
		),
		"AddApplicationOutput":              service.WrapOp(h.handleAddApplicationOutput),
		"AddApplicationReferenceDataSource": service.WrapOp(h.handleAddApplicationReferenceDataSource),
		"DeleteApplicationCloudWatchLoggingOption": service.WrapOp(
			h.handleDeleteApplicationCloudWatchLoggingOption,
		),
		"DeleteApplicationInputProcessingConfiguration": service.WrapOp(
			h.handleDeleteApplicationInputProcessingConfiguration,
		),
		"DeleteApplicationOutput":              service.WrapOp(h.handleDeleteApplicationOutput),
		"DeleteApplicationReferenceDataSource": service.WrapOp(h.handleDeleteApplicationReferenceDataSource),
		"DiscoverInputSchema":                  service.WrapOp(h.handleDiscoverInputSchema),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "KinesisAnalytics" }

// GetSupportedOperations returns the list of supported Kinesis Analytics operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"DeleteApplication",
		"DescribeApplication",
		"ListApplications",
		"StartApplication",
		"StopApplication",
		"UpdateApplication",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"AddApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"AddApplicationReferenceDataSource",
		"DeleteApplicationCloudWatchLoggingOption",
		"DeleteApplicationInputProcessingConfiguration",
		"DeleteApplicationOutput",
		"DeleteApplicationReferenceDataSource",
		"DiscoverInputSchema",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return kinesisanalyticsService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Kinesis Analytics requests by X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), kinesisanalyticsTargetPrefix)
	}
}

// MatchPriority returns the routing priority for header-based matching.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Kinesis Analytics action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, kinesisanalyticsTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type applicationNameInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// ExtractResource extracts the application name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req applicationNameInput
	_ = json.Unmarshal(body, &req)

	return req.ApplicationName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"KinesisAnalytics", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound,
			errorResponse{Type: "ResourceNotFoundException", Message: err.Error()})
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest,
			errorResponse{Type: "ResourceInUseException", Message: err.Error()})
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest,
			errorResponse{Type: "InvalidArgumentException", Message: err.Error()})
	case errors.Is(err, ErrConcurrentUpdate):
		return c.JSON(http.StatusBadRequest,
			errorResponse{Type: "ConcurrentModificationException", Message: err.Error()})
	case errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest,
			errorResponse{Type: "InvalidArgumentException", Message: err.Error()})
	case errors.Is(err, errApplicationName),
		errors.Is(err, errResourceARN),
		errors.Is(err, errInputID),
		errors.Is(err, errOutputID),
		errors.Is(err, errReferenceID),
		errors.Is(err, errCWLOptionID):
		return c.JSON(http.StatusBadRequest,
			errorResponse{Type: "InvalidArgumentException", Message: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError,
			errorResponse{Type: "InternalServiceException", Message: err.Error()})
	}
}

func (h *Handler) handleCreateApplication(
	_ context.Context,
	in *createApplicationInput,
) (*createApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	tags := make(map[string]string, len(in.Tags))

	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}

	app, err := h.Backend.CreateApplication(
		h.DefaultRegion,
		h.AccountID,
		in.ApplicationName,
		in.ApplicationDescription,
		in.ApplicationCode,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createApplicationOutput{
		ApplicationSummary: applicationSummary{
			ApplicationARN:    app.ApplicationARN,
			ApplicationName:   app.ApplicationName,
			ApplicationStatus: app.ApplicationStatus,
		},
	}, nil
}

func (h *Handler) handleDeleteApplication(
	_ context.Context,
	in *deleteApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if err := h.Backend.DeleteApplication(in.ApplicationName, nil); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDescribeApplication(
	_ context.Context,
	in *describeApplicationInput,
) (*describeApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &describeApplicationOutput{
		ApplicationDetail: toApplicationDetail(app),
	}, nil
}

func (h *Handler) handleListApplications(
	_ context.Context,
	in *listApplicationsInput,
) (*listApplicationsOutput, error) {
	apps, hasMore := h.Backend.ListApplications(in.ExclusiveStartApplicationName, in.Limit)
	summaries := make([]applicationSummary, 0, len(apps))

	for _, app := range apps {
		summaries = append(summaries, applicationSummary{
			ApplicationARN:    app.ApplicationARN,
			ApplicationName:   app.ApplicationName,
			ApplicationStatus: app.ApplicationStatus,
		})
	}

	return &listApplicationsOutput{
		ApplicationSummaries: summaries,
		HasMoreApplications:  hasMore,
	}, nil
}

func (h *Handler) handleStartApplication(
	_ context.Context,
	in *startApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if err := h.Backend.StartApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleStopApplication(
	_ context.Context,
	in *stopApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if err := h.Backend.StopApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleUpdateApplication(
	_ context.Context,
	in *updateApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	var codeUpdate string
	if in.ApplicationUpdate != nil {
		codeUpdate = in.ApplicationUpdate.ApplicationCodeUpdate
	}

	if _, err := h.Backend.UpdateApplication(
		in.ApplicationName,
		in.CurrentApplicationVersionID,
		codeUpdate,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	tagMap, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(tagMap))

	for k := range tagMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	entries := make([]tagEntry, 0, len(keys))

	for _, k := range keys {
		entries = append(entries, tagEntry{Key: k, Value: tagMap[k]})
	}

	return &listTagsForResourceOutput{Tags: entries}, nil
}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*struct{}, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	tagMap := make(map[string]string, len(in.Tags))

	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(in.ResourceARN, tagMap); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*struct{}, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// toApplicationDetail converts an Application to the API detail struct.
func toApplicationDetail(app *Application) applicationDetail {
	detail := applicationDetail{
		ApplicationARN:                      app.ApplicationARN,
		ApplicationName:                     app.ApplicationName,
		ApplicationStatus:                   app.ApplicationStatus,
		ApplicationVersionID:                app.ApplicationVersionID,
		ApplicationCode:                     app.ApplicationCode,
		ApplicationDescription:              app.ApplicationDescription,
		CloudWatchLoggingOptionDescriptions: app.CloudWatchLoggingOptions,
		InputDescriptions:                   app.Inputs,
		OutputDescriptions:                  app.Outputs,
		ReferenceDataSourceDescriptions:     app.ReferenceDataSources,
	}

	if app.CreateTimestamp != nil {
		detail.CreateTimestamp = float64(app.CreateTimestamp.Unix())
	}

	if app.LastUpdateTimestamp != nil {
		detail.LastUpdateTimestamp = float64(app.LastUpdateTimestamp.Unix())
	}

	return detail
}

func (h *Handler) handleAddApplicationCloudWatchLoggingOption(
	_ context.Context,
	in *addApplicationCloudWatchLoggingOptionInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	var opt CloudWatchLoggingOptionDesc
	if in.CloudWatchLoggingOption != nil {
		opt.LogStreamARN = in.CloudWatchLoggingOption.LogStreamARN
		opt.RoleARN = in.CloudWatchLoggingOption.RoleARN
	}

	if err := h.Backend.AddApplicationCloudWatchLoggingOption(
		in.ApplicationName, in.CurrentApplicationVersionID, opt,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleAddApplicationInput(
	_ context.Context,
	in *addApplicationInputInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	var desc InputDescription
	if in.Input != nil {
		desc.NamePrefix = in.Input.NamePrefix

		if in.Input.KinesisStreamsInput != nil {
			desc.KinesisStreamsInputDescription = &KinesisStreamsInputDesc{
				ResourceARN: in.Input.KinesisStreamsInput.ResourceARN,
				RoleARN:     in.Input.KinesisStreamsInput.RoleARN,
			}
		}

		if in.Input.KinesisFirehoseInput != nil {
			desc.KinesisFirehoseInputDescription = &KinesisFirehoseInputDesc{
				ResourceARN: in.Input.KinesisFirehoseInput.ResourceARN,
				RoleARN:     in.Input.KinesisFirehoseInput.RoleARN,
			}
		}
	}

	if err := h.Backend.AddApplicationInput(
		in.ApplicationName, in.CurrentApplicationVersionID, desc,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleAddApplicationInputProcessingConfiguration(
	_ context.Context,
	in *addApplicationInputProcessingConfigurationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	var cfg *InputProcessingConfigurationDesc
	if in.InputProcessingConfiguration != nil && in.InputProcessingConfiguration.InputLambdaProcessor != nil {
		cfg = &InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: in.InputProcessingConfiguration.InputLambdaProcessor.ResourceARN,
				RoleARN:     in.InputProcessingConfiguration.InputLambdaProcessor.RoleARN,
			},
		}
	}

	if err := h.Backend.AddApplicationInputProcessingConfiguration(
		in.ApplicationName, in.CurrentApplicationVersionID, in.InputID, cfg,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleAddApplicationOutput(
	_ context.Context,
	in *addApplicationOutputInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	var desc OutputDescription
	if in.Output != nil {
		desc.Name = in.Output.Name

		if in.Output.KinesisStreamsOutput != nil {
			desc.KinesisStreamsOutputDescription = &KinesisStreamsOutputDesc{
				ResourceARN: in.Output.KinesisStreamsOutput.ResourceARN,
				RoleARN:     in.Output.KinesisStreamsOutput.RoleARN,
			}
		}

		if in.Output.KinesisFirehoseOutput != nil {
			desc.KinesisFirehoseOutputDescription = &KinesisFirehoseOutputDesc{
				ResourceARN: in.Output.KinesisFirehoseOutput.ResourceARN,
				RoleARN:     in.Output.KinesisFirehoseOutput.RoleARN,
			}
		}

		if in.Output.LambdaOutput != nil {
			desc.LambdaOutputDescription = &LambdaOutputDesc{
				ResourceARN: in.Output.LambdaOutput.ResourceARN,
				RoleARN:     in.Output.LambdaOutput.RoleARN,
			}
		}

		if in.Output.DestinationSchema != nil {
			desc.DestinationSchema = &DestinationSchemaDesc{
				RecordFormatType: in.Output.DestinationSchema.RecordFormatType,
			}
		}
	}

	if err := h.Backend.AddApplicationOutput(
		in.ApplicationName, in.CurrentApplicationVersionID, desc,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleAddApplicationReferenceDataSource(
	_ context.Context,
	in *addApplicationReferenceDataSourceInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	var ref ReferenceDataSourceDescription
	if in.ReferenceDataSource != nil {
		ref.TableName = in.ReferenceDataSource.TableName

		if in.ReferenceDataSource.S3ReferenceDataSource != nil {
			ref.S3ReferenceDataSourceDescription = &S3ReferenceDataSourceDesc{
				BucketARN: in.ReferenceDataSource.S3ReferenceDataSource.BucketARN,
				FileKey:   in.ReferenceDataSource.S3ReferenceDataSource.FileKey,
				RoleARN:   in.ReferenceDataSource.S3ReferenceDataSource.RoleARN,
			}
		}

		if in.ReferenceDataSource.ReferenceSchema != nil {
			schema := &SourceSchema{
				RecordEncoding: in.ReferenceDataSource.ReferenceSchema.RecordEncoding,
				RecordColumns:  in.ReferenceDataSource.ReferenceSchema.RecordColumns,
				RecordFormat: RecordFormat{
					RecordFormatType:  in.ReferenceDataSource.ReferenceSchema.RecordFormat.RecordFormatType,
					MappingParameters: in.ReferenceDataSource.ReferenceSchema.RecordFormat.MappingParameters,
				},
			}
			ref.ReferenceSchema = schema
		}
	}

	if err := h.Backend.AddApplicationReferenceDataSource(
		in.ApplicationName, in.CurrentApplicationVersionID, ref,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteApplicationCloudWatchLoggingOption(
	_ context.Context,
	in *deleteApplicationCloudWatchLoggingOptionInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.CloudWatchLoggingOptionID == "" {
		return nil, errCWLOptionID
	}

	if err := h.Backend.DeleteApplicationCloudWatchLoggingOption(
		in.ApplicationName, in.CurrentApplicationVersionID, in.CloudWatchLoggingOptionID,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteApplicationInputProcessingConfiguration(
	_ context.Context,
	in *deleteApplicationInputProcessingConfigurationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.InputID == "" {
		return nil, errInputID
	}

	if err := h.Backend.DeleteApplicationInputProcessingConfiguration(
		in.ApplicationName, in.CurrentApplicationVersionID, in.InputID,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteApplicationOutput(
	_ context.Context,
	in *deleteApplicationOutputInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.OutputID == "" {
		return nil, errOutputID
	}

	if err := h.Backend.DeleteApplicationOutput(
		in.ApplicationName, in.CurrentApplicationVersionID, in.OutputID,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteApplicationReferenceDataSource(
	_ context.Context,
	in *deleteApplicationReferenceDataSourceInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.ReferenceID == "" {
		return nil, errReferenceID
	}

	if err := h.Backend.DeleteApplicationReferenceDataSource(
		in.ApplicationName, in.CurrentApplicationVersionID, in.ReferenceID,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDiscoverInputSchema(
	_ context.Context,
	_ *discoverInputSchemaInput,
) (*discoverInputSchemaOutput, error) {
	return &discoverInputSchemaOutput{
		InputSchema: &SourceSchema{
			RecordFormat: RecordFormat{
				RecordFormatType: "JSON",
				MappingParameters: &MappingParameters{
					JSONMappingParameters: &JSONMappingParameters{RecordRowPath: "$"},
				},
			},
			RecordColumns: []RecordColumn{
				{Name: "COL_1", SQLType: "VARCHAR(4)"},
			},
		},
		ParsedInputRecords:    [][]string{{"value1"}},
		ProcessedInputRecords: []string{`{"COL_1":"value1"}`},
		RawInputRecords:       []string{`{"COL_1":"value1"}`},
	}, nil
}
