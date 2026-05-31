package datasync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	datasyncTargetPrefix = "FmrsService."
	matchPriority        = service.PriorityHeaderExact
	contentType          = "application/x-amz-json-1.1"

	keyType    = "__type"
	keyMessage = "message"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler handles DataSync HTTP requests.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "DataSync" }

// Reset resets the backend and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.ops = h.buildOps()
}

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateAgent",
		"DescribeAgent",
		"UpdateAgent",
		"DeleteAgent",
		"ListAgents",
		"CreateLocationS3",
		"DescribeLocationS3",
		"DeleteLocation",
		"ListLocations",
		"CreateTask",
		"DescribeTask",
		"UpdateTask",
		"DeleteTask",
		"ListTasks",
		"StartTaskExecution",
		"CancelTaskExecution",
		"DescribeTaskExecution",
		"ListTaskExecutions",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// RouteMatcher returns a function that matches DataSync API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), datasyncTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, datasyncTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function for DataSync requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"DataSync", contentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateAgent":           service.WrapOp(h.handleCreateAgent),
		"DescribeAgent":         service.WrapOp(h.handleDescribeAgent),
		"UpdateAgent":           service.WrapOp(h.handleUpdateAgent),
		"DeleteAgent":           service.WrapOp(h.handleDeleteAgent),
		"ListAgents":            service.WrapOp(h.handleListAgents),
		"CreateLocationS3":      service.WrapOp(h.handleCreateLocationS3),
		"DescribeLocationS3":    service.WrapOp(h.handleDescribeLocationS3),
		"DeleteLocation":        service.WrapOp(h.handleDeleteLocation),
		"ListLocations":         service.WrapOp(h.handleListLocations),
		"CreateTask":            service.WrapOp(h.handleCreateTask),
		"DescribeTask":          service.WrapOp(h.handleDescribeTask),
		"UpdateTask":            service.WrapOp(h.handleUpdateTask),
		"DeleteTask":            service.WrapOp(h.handleDeleteTask),
		"ListTasks":             service.WrapOp(h.handleListTasks),
		"StartTaskExecution":    service.WrapOp(h.handleStartTaskExecution),
		"CancelTaskExecution":   service.WrapOp(h.handleCancelTaskExecution),
		"DescribeTaskExecution": service.WrapOp(h.handleDescribeTaskExecution),
		"ListTaskExecutions":    service.WrapOp(h.handleListTaskExecutions),
		"TagResource":           service.WrapOp(h.handleTagResource),
		"UntagResource":         service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":   service.WrapOp(h.handleListTagsForResource),
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
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    "ResourceNotFoundException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    "ResourceExistsException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    "InvalidRequestException",
			keyMessage: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyType:    "InternalServiceError",
			keyMessage: err.Error(),
		})
	}
}

// --- Agent operations ---

type createAgentInput struct {
	ActivationKey string     `json:"ActivationKey"`
	AgentName     string     `json:"AgentName"`
	Tags          []tagInput `json:"Tags"`
}

type createAgentOutput struct {
	AgentArn string `json:"AgentArn"`
}

func (h *Handler) handleCreateAgent(_ context.Context, in *createAgentInput) (*createAgentOutput, error) {
	if in.ActivationKey == "" {
		return nil, fmt.Errorf("%w: ActivationKey is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	a, err := h.Backend.CreateAgent(in.AgentName, in.ActivationKey, tags)
	if err != nil {
		return nil, err
	}

	return &createAgentOutput{AgentArn: a.AgentArn}, nil
}

type describeAgentInput struct {
	AgentArn string `json:"AgentArn"`
}

type describeAgentOutput struct {
	AgentArn     string `json:"AgentArn"`
	Name         string `json:"Name"`
	Status       string `json:"Status"`
	EndpointType string `json:"EndpointType"`
	CreationTime int64  `json:"CreationTime"`
}

func (h *Handler) handleDescribeAgent(_ context.Context, in *describeAgentInput) (*describeAgentOutput, error) {
	if in.AgentArn == "" {
		return nil, fmt.Errorf("%w: AgentArn is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeAgent(in.AgentArn)
	if err != nil {
		return nil, err
	}

	return &describeAgentOutput{
		AgentArn:     a.AgentArn,
		Name:         a.Name,
		Status:       a.Status,
		EndpointType: a.EndpointType,
		CreationTime: a.CreationTime.Unix(),
	}, nil
}

type updateAgentInput struct {
	AgentArn string `json:"AgentArn"`
	Name     string `json:"Name"`
}

type updateAgentOutput struct{}

func (h *Handler) handleUpdateAgent(_ context.Context, in *updateAgentInput) (*updateAgentOutput, error) {
	if in.AgentArn == "" {
		return nil, fmt.Errorf("%w: AgentArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateAgent(in.AgentArn, in.Name); err != nil {
		return nil, err
	}

	return &updateAgentOutput{}, nil
}

type deleteAgentInput struct {
	AgentArn string `json:"AgentArn"`
}

type deleteAgentOutput struct{}

func (h *Handler) handleDeleteAgent(_ context.Context, in *deleteAgentInput) (*deleteAgentOutput, error) {
	if in.AgentArn == "" {
		return nil, fmt.Errorf("%w: AgentArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAgent(in.AgentArn); err != nil {
		return nil, err
	}

	return &deleteAgentOutput{}, nil
}

type listAgentsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type agentListEntryOutput struct {
	AgentArn string `json:"AgentArn"`
	Name     string `json:"Name"`
	Status   string `json:"Status"`
}

type listAgentsOutput struct {
	NextToken string                 `json:"NextToken,omitempty"`
	Agents    []agentListEntryOutput `json:"Agents"`
}

func (h *Handler) handleListAgents(_ context.Context, in *listAgentsInput) (*listAgentsOutput, error) {
	agents, nextToken, err := h.Backend.ListAgents(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]agentListEntryOutput, 0, len(agents))
	for _, a := range agents {
		out = append(out, agentListEntryOutput{
			AgentArn: a.AgentArn,
			Name:     a.Name,
			Status:   a.Status,
		})
	}

	return &listAgentsOutput{Agents: out, NextToken: nextToken}, nil
}

// --- Location (S3) operations ---

type s3ConfigInput struct {
	BucketAccessRoleArn string `json:"BucketAccessRoleArn"`
}

type createLocationS3Input struct {
	S3Config       *s3ConfigInput `json:"S3Config"`
	S3BucketArn    string         `json:"S3BucketArn"`
	Subdirectory   string         `json:"Subdirectory"`
	S3StorageClass string         `json:"S3StorageClass"`
	Tags           []tagInput     `json:"Tags"`
}

type createLocationS3Output struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationS3(
	_ context.Context,
	in *createLocationS3Input,
) (*createLocationS3Output, error) {
	if in.S3BucketArn == "" {
		return nil, fmt.Errorf("%w: S3BucketArn is required", errInvalidRequest)
	}

	if in.S3Config == nil {
		return nil, fmt.Errorf("%w: S3Config is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	cfg := S3Config{BucketAccessRoleArn: in.S3Config.BucketAccessRoleArn}

	l, err := h.Backend.CreateLocationS3(in.Subdirectory, in.S3BucketArn, in.S3StorageClass, cfg, tags)
	if err != nil {
		return nil, err
	}

	return &createLocationS3Output{LocationArn: l.LocationArn}, nil
}

type describeLocationS3Input struct {
	LocationArn string `json:"LocationArn"`
}

type s3ConfigOutput struct {
	BucketAccessRoleArn string `json:"BucketAccessRoleArn"`
}

type describeLocationS3Output struct {
	S3Config       *s3ConfigOutput `json:"S3Config,omitempty"`
	LocationArn    string          `json:"LocationArn"`
	LocationURI    string          `json:"LocationUri"`
	S3BucketArn    string          `json:"S3BucketArn"`
	Subdirectory   string          `json:"Subdirectory,omitempty"`
	S3StorageClass string          `json:"S3StorageClass,omitempty"`
	CreationTime   int64           `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationS3(
	_ context.Context,
	in *describeLocationS3Input,
) (*describeLocationS3Output, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationS3(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationS3Output{
		LocationArn:    l.LocationArn,
		LocationURI:    l.LocationURI,
		S3BucketArn:    l.S3BucketArn,
		Subdirectory:   l.Subdirectory,
		S3StorageClass: l.S3StorageClass,
		CreationTime:   l.CreationTime.Unix(),
	}

	if l.S3Config.BucketAccessRoleArn != "" {
		out.S3Config = &s3ConfigOutput{BucketAccessRoleArn: l.S3Config.BucketAccessRoleArn}
	}

	return out, nil
}

type deleteLocationInput struct {
	LocationArn string `json:"LocationArn"`
}

type deleteLocationOutput struct{}

func (h *Handler) handleDeleteLocation(_ context.Context, in *deleteLocationInput) (*deleteLocationOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteLocation(in.LocationArn); err != nil {
		return nil, err
	}

	return &deleteLocationOutput{}, nil
}

type listLocationsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type locationListEntryOutput struct {
	LocationArn  string `json:"LocationArn"`
	LocationURI  string `json:"LocationUri"`
	CreationTime int64  `json:"CreationTime"`
}

type listLocationsOutput struct {
	NextToken string                    `json:"NextToken,omitempty"`
	Locations []locationListEntryOutput `json:"Locations"`
}

func (h *Handler) handleListLocations(_ context.Context, in *listLocationsInput) (*listLocationsOutput, error) {
	locations, nextToken, err := h.Backend.ListLocations(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]locationListEntryOutput, 0, len(locations))
	for _, l := range locations {
		out = append(out, locationListEntryOutput{
			LocationArn:  l.LocationArn,
			LocationURI:  l.LocationURI,
			CreationTime: l.CreationTime.Unix(),
		})
	}

	return &listLocationsOutput{Locations: out, NextToken: nextToken}, nil
}

// --- Task operations ---

type createTaskInput struct {
	SourceLocationArn      string     `json:"SourceLocationArn"`
	DestinationLocationArn string     `json:"DestinationLocationArn"`
	Name                   string     `json:"Name"`
	CloudWatchLogGroupArn  string     `json:"CloudWatchLogGroupArn,omitempty"`
	Tags                   []tagInput `json:"Tags"`
}

type createTaskOutput struct {
	TaskArn string `json:"TaskArn"`
}

func (h *Handler) handleCreateTask(_ context.Context, in *createTaskInput) (*createTaskOutput, error) {
	if in.SourceLocationArn == "" {
		return nil, fmt.Errorf("%w: SourceLocationArn is required", errInvalidRequest)
	}

	if in.DestinationLocationArn == "" {
		return nil, fmt.Errorf("%w: DestinationLocationArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	t, err := h.Backend.CreateTask(
		in.SourceLocationArn,
		in.DestinationLocationArn,
		in.Name,
		in.CloudWatchLogGroupArn,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createTaskOutput{TaskArn: t.TaskArn}, nil
}

type describeTaskInput struct {
	TaskArn string `json:"TaskArn"`
}

type describeTaskOutput struct {
	TaskArn                 string `json:"TaskArn"`
	Name                    string `json:"Name"`
	Status                  string `json:"Status"`
	SourceLocationArn       string `json:"SourceLocationArn"`
	DestinationLocationArn  string `json:"DestinationLocationArn"`
	CloudWatchLogGroupArn   string `json:"CloudWatchLogGroupArn,omitempty"`
	CurrentTaskExecutionArn string `json:"CurrentTaskExecutionArn,omitempty"`
	CreationTime            int64  `json:"CreationTime"`
}

func (h *Handler) handleDescribeTask(_ context.Context, in *describeTaskInput) (*describeTaskOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", errInvalidRequest)
	}

	t, err := h.Backend.DescribeTask(in.TaskArn)
	if err != nil {
		return nil, err
	}

	return &describeTaskOutput{
		TaskArn:                 t.TaskArn,
		Name:                    t.Name,
		Status:                  t.Status,
		SourceLocationArn:       t.SourceLocationArn,
		DestinationLocationArn:  t.DestinationLocationArn,
		CloudWatchLogGroupArn:   t.CloudWatchLogGroupArn,
		CurrentTaskExecutionArn: t.CurrentTaskExecutionArn,
		CreationTime:            t.CreationTime.Unix(),
	}, nil
}

type updateTaskInput struct {
	TaskArn               string `json:"TaskArn"`
	Name                  string `json:"Name,omitempty"`
	CloudWatchLogGroupArn string `json:"CloudWatchLogGroupArn,omitempty"`
}

type updateTaskOutput struct{}

func (h *Handler) handleUpdateTask(_ context.Context, in *updateTaskInput) (*updateTaskOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateTask(in.TaskArn, in.Name, in.CloudWatchLogGroupArn); err != nil {
		return nil, err
	}

	return &updateTaskOutput{}, nil
}

type deleteTaskInput struct {
	TaskArn string `json:"TaskArn"`
}

type deleteTaskOutput struct{}

func (h *Handler) handleDeleteTask(_ context.Context, in *deleteTaskInput) (*deleteTaskOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTask(in.TaskArn); err != nil {
		return nil, err
	}

	return &deleteTaskOutput{}, nil
}

type listTasksInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type taskListEntryOutput struct {
	TaskArn string `json:"TaskArn"`
	Name    string `json:"Name"`
	Status  string `json:"Status"`
}

type listTasksOutput struct {
	NextToken string                `json:"NextToken,omitempty"`
	Tasks     []taskListEntryOutput `json:"Tasks"`
}

func (h *Handler) handleListTasks(_ context.Context, in *listTasksInput) (*listTasksOutput, error) {
	tasks, nextToken, err := h.Backend.ListTasks(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]taskListEntryOutput, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, taskListEntryOutput{
			TaskArn: t.TaskArn,
			Name:    t.Name,
			Status:  t.Status,
		})
	}

	return &listTasksOutput{Tasks: out, NextToken: nextToken}, nil
}

// --- Task execution operations ---

type startTaskExecutionInput struct {
	TaskArn string `json:"TaskArn"`
}

type startTaskExecutionOutput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
}

func (h *Handler) handleStartTaskExecution(
	_ context.Context,
	in *startTaskExecutionInput,
) (*startTaskExecutionOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", errInvalidRequest)
	}

	e, err := h.Backend.StartTaskExecution(in.TaskArn)
	if err != nil {
		return nil, err
	}

	return &startTaskExecutionOutput{TaskExecutionArn: e.TaskExecutionArn}, nil
}

type cancelTaskExecutionInput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
}

type cancelTaskExecutionOutput struct{}

func (h *Handler) handleCancelTaskExecution(
	_ context.Context,
	in *cancelTaskExecutionInput,
) (*cancelTaskExecutionOutput, error) {
	if in.TaskExecutionArn == "" {
		return nil, fmt.Errorf("%w: TaskExecutionArn is required", errInvalidRequest)
	}

	if err := h.Backend.CancelTaskExecution(in.TaskExecutionArn); err != nil {
		return nil, err
	}

	return &cancelTaskExecutionOutput{}, nil
}

type describeTaskExecutionInput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
}

type describeTaskExecutionOutput struct {
	TaskExecutionArn         string `json:"TaskExecutionArn"`
	Status                   string `json:"Status"`
	StartTime                int64  `json:"StartTime"`
	EstimatedFilesToTransfer int64  `json:"EstimatedFilesToTransfer"`
	EstimatedBytesToTransfer int64  `json:"EstimatedBytesToTransfer"`
	FilesTransferred         int64  `json:"FilesTransferred"`
	BytesTransferred         int64  `json:"BytesTransferred"`
}

func (h *Handler) handleDescribeTaskExecution(
	_ context.Context,
	in *describeTaskExecutionInput,
) (*describeTaskExecutionOutput, error) {
	if in.TaskExecutionArn == "" {
		return nil, fmt.Errorf("%w: TaskExecutionArn is required", errInvalidRequest)
	}

	e, err := h.Backend.DescribeTaskExecution(in.TaskExecutionArn)
	if err != nil {
		return nil, err
	}

	return &describeTaskExecutionOutput{
		TaskExecutionArn:         e.TaskExecutionArn,
		Status:                   e.Status,
		StartTime:                e.StartTime.Unix(),
		EstimatedFilesToTransfer: e.EstimatedFilesToTransfer,
		EstimatedBytesToTransfer: e.EstimatedBytesToTransfer,
		FilesTransferred:         e.FilesTransferred,
		BytesTransferred:         e.BytesTransferred,
	}, nil
}

type listTaskExecutionsInput struct {
	TaskArn    string `json:"TaskArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type taskExecutionListEntryOutput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
	Status           string `json:"Status"`
}

type listTaskExecutionsOutput struct {
	NextToken      string                         `json:"NextToken,omitempty"`
	TaskExecutions []taskExecutionListEntryOutput `json:"TaskExecutions"`
}

func (h *Handler) handleListTaskExecutions(
	_ context.Context,
	in *listTaskExecutionsInput,
) (*listTaskExecutionsOutput, error) {
	executions, nextToken, err := h.Backend.ListTaskExecutions(in.TaskArn, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]taskExecutionListEntryOutput, 0, len(executions))
	for _, e := range executions {
		out = append(out, taskExecutionListEntryOutput{
			TaskExecutionArn: e.TaskExecutionArn,
			Status:           e.Status,
		})
	}

	return &listTaskExecutionsOutput{TaskExecutions: out, NextToken: nextToken}, nil
}

// --- Tag operations ---

type tagInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type tagResourceInput struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagInput `json:"Tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	if err := h.Backend.TagResource(in.ResourceArn, tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	Keys        []string `json:"Keys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.Keys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
	MaxResults  int32  `json:"MaxResults"`
}

type listTagsForResourceOutput struct {
	NextToken string     `json:"NextToken,omitempty"`
	Tags      []tagInput `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags, nextToken, err := h.Backend.ListTagsForResource(in.ResourceArn, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]tagInput, 0, len(tags))
	for k, v := range tags {
		out = append(out, tagInput{Key: k, Value: v})
	}

	return &listTagsForResourceOutput{Tags: out, NextToken: nextToken}, nil
}

// tagsFromInput converts a slice of tagInput to a map.
func tagsFromInput(inputs []tagInput) map[string]string {
	if len(inputs) == 0 {
		return nil
	}

	m := make(map[string]string, len(inputs))
	for _, t := range inputs {
		m[t.Key] = t.Value
	}

	return m
}
