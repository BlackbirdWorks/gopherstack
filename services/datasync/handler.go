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
		"UpdateLocationS3",
		"UpdateTaskExecution",
		"CreateLocationAzureBlob",
		"DescribeLocationAzureBlob",
		"UpdateLocationAzureBlob",
		"CreateLocationEfs",
		"DescribeLocationEfs",
		"UpdateLocationEfs",
		"CreateLocationFsxLustre",
		"DescribeLocationFsxLustre",
		"UpdateLocationFsxLustre",
		"CreateLocationFsxOntap",
		"DescribeLocationFsxOntap",
		"UpdateLocationFsxOntap",
		"CreateLocationFsxOpenZfs",
		"DescribeLocationFsxOpenZfs",
		"UpdateLocationFsxOpenZfs",
		"CreateLocationFsxWindows",
		"DescribeLocationFsxWindows",
		"UpdateLocationFsxWindows",
		"CreateLocationHdfs",
		"DescribeLocationHdfs",
		"UpdateLocationHdfs",
		"CreateLocationNfs",
		"DescribeLocationNfs",
		"UpdateLocationNfs",
		"CreateLocationObjectStorage",
		"DescribeLocationObjectStorage",
		"UpdateLocationObjectStorage",
		"CreateLocationSmb",
		"DescribeLocationSmb",
		"UpdateLocationSmb",
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
		// Extended location operations
		"UpdateLocationS3":              service.WrapOp(h.handleUpdateLocationS3),
		"UpdateTaskExecution":           service.WrapOp(h.handleUpdateTaskExecution),
		"CreateLocationAzureBlob":       service.WrapOp(h.handleCreateLocationAzureBlob),
		"DescribeLocationAzureBlob":     service.WrapOp(h.handleDescribeLocationAzureBlob),
		"UpdateLocationAzureBlob":       service.WrapOp(h.handleUpdateLocationAzureBlob),
		"CreateLocationEfs":             service.WrapOp(h.handleCreateLocationEfs),
		"DescribeLocationEfs":           service.WrapOp(h.handleDescribeLocationEfs),
		"UpdateLocationEfs":             service.WrapOp(h.handleUpdateLocationEfs),
		"CreateLocationFsxLustre":       service.WrapOp(h.handleCreateLocationFsxLustre),
		"DescribeLocationFsxLustre":     service.WrapOp(h.handleDescribeLocationFsxLustre),
		"UpdateLocationFsxLustre":       service.WrapOp(h.handleUpdateLocationFsxLustre),
		"CreateLocationFsxOntap":        service.WrapOp(h.handleCreateLocationFsxOntap),
		"DescribeLocationFsxOntap":      service.WrapOp(h.handleDescribeLocationFsxOntap),
		"UpdateLocationFsxOntap":        service.WrapOp(h.handleUpdateLocationFsxOntap),
		"CreateLocationFsxOpenZfs":      service.WrapOp(h.handleCreateLocationFsxOpenZfs),
		"DescribeLocationFsxOpenZfs":    service.WrapOp(h.handleDescribeLocationFsxOpenZfs),
		"UpdateLocationFsxOpenZfs":      service.WrapOp(h.handleUpdateLocationFsxOpenZfs),
		"CreateLocationFsxWindows":      service.WrapOp(h.handleCreateLocationFsxWindows),
		"DescribeLocationFsxWindows":    service.WrapOp(h.handleDescribeLocationFsxWindows),
		"UpdateLocationFsxWindows":      service.WrapOp(h.handleUpdateLocationFsxWindows),
		"CreateLocationHdfs":            service.WrapOp(h.handleCreateLocationHdfs),
		"DescribeLocationHdfs":          service.WrapOp(h.handleDescribeLocationHdfs),
		"UpdateLocationHdfs":            service.WrapOp(h.handleUpdateLocationHdfs),
		"CreateLocationNfs":             service.WrapOp(h.handleCreateLocationNfs),
		"DescribeLocationNfs":           service.WrapOp(h.handleDescribeLocationNfs),
		"UpdateLocationNfs":             service.WrapOp(h.handleUpdateLocationNfs),
		"CreateLocationObjectStorage":   service.WrapOp(h.handleCreateLocationObjectStorage),
		"DescribeLocationObjectStorage": service.WrapOp(h.handleDescribeLocationObjectStorage),
		"UpdateLocationObjectStorage":   service.WrapOp(h.handleUpdateLocationObjectStorage),
		"CreateLocationSmb":             service.WrapOp(h.handleCreateLocationSmb),
		"DescribeLocationSmb":           service.WrapOp(h.handleDescribeLocationSmb),
		"UpdateLocationSmb":             service.WrapOp(h.handleUpdateLocationSmb),
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

// --- UpdateLocationS3 ---

type updateLocationS3Input struct {
	S3Config       *s3ConfigInput `json:"S3Config"`
	LocationArn    string         `json:"LocationArn"`
	Subdirectory   string         `json:"Subdirectory,omitempty"`
	S3StorageClass string         `json:"S3StorageClass,omitempty"`
}

type updateLocationS3Output struct{}

func (h *Handler) handleUpdateLocationS3(
	_ context.Context,
	in *updateLocationS3Input,
) (*updateLocationS3Output, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var cfg S3Config
	if in.S3Config != nil {
		cfg.BucketAccessRoleArn = in.S3Config.BucketAccessRoleArn
	}

	if err := h.Backend.UpdateLocationS3(in.LocationArn, in.Subdirectory, in.S3StorageClass, cfg); err != nil {
		return nil, err
	}

	return &updateLocationS3Output{}, nil
}

// --- UpdateTaskExecution ---

type updateTaskExecutionInput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
}

type updateTaskExecutionOutput struct{}

func (h *Handler) handleUpdateTaskExecution(
	_ context.Context,
	in *updateTaskExecutionInput,
) (*updateTaskExecutionOutput, error) {
	if in.TaskExecutionArn == "" {
		return nil, fmt.Errorf("%w: TaskExecutionArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateTaskExecution(in.TaskExecutionArn); err != nil {
		return nil, err
	}

	return &updateTaskExecutionOutput{}, nil
}

// --- AzureBlob location ---

type azureBlobSasConfigInput struct {
	Token string `json:"Token"`
}

type createLocationAzureBlobInput struct {
	SasConfiguration *azureBlobSasConfigInput `json:"SasConfiguration"`
	ContainerURL     string                   `json:"ContainerUrl"`
	Subdirectory     string                   `json:"Subdirectory,omitempty"`
	BlobType         string                   `json:"BlobType,omitempty"`
	AccessTier       string                   `json:"AccessTier,omitempty"`
	AgentArns        []string                 `json:"AgentArns"`
	Tags             []tagInput               `json:"Tags"`
}

type createLocationAzureBlobOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationAzureBlob(
	_ context.Context,
	in *createLocationAzureBlobInput,
) (*createLocationAzureBlobOutput, error) {
	if in.ContainerURL == "" {
		return nil, fmt.Errorf("%w: ContainerUrl is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var sasConfig *SasConfiguration
	if in.SasConfiguration != nil {
		sasConfig = &SasConfiguration{Token: in.SasConfiguration.Token}
	}

	l, err := h.Backend.CreateLocationAzureBlob(
		in.ContainerURL, in.Subdirectory, in.BlobType, in.AccessTier,
		sasConfig, in.AgentArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationAzureBlobOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationAzureBlobInput struct {
	LocationArn string `json:"LocationArn"`
}

type azureBlobSasConfigOutput struct {
	Token string `json:"Token"`
}

type describeLocationAzureBlobOutput struct {
	SasConfiguration *azureBlobSasConfigOutput `json:"SasConfiguration,omitempty"`
	LocationArn      string                    `json:"LocationArn"`
	LocationURI      string                    `json:"LocationUri"`
	ContainerURL     string                    `json:"ContainerUrl,omitempty"`
	Subdirectory     string                    `json:"Subdirectory,omitempty"`
	BlobType         string                    `json:"BlobType,omitempty"`
	AccessTier       string                    `json:"AccessTier,omitempty"`
	AgentArns        []string                  `json:"AgentArns,omitempty"`
	CreationTime     int64                     `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationAzureBlob(
	_ context.Context,
	in *describeLocationAzureBlobInput,
) (*describeLocationAzureBlobOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationAzureBlob(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationAzureBlobOutput{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		ContainerURL: l.ContainerURL,
		Subdirectory: l.Subdirectory,
		BlobType:     l.BlobType,
		AccessTier:   l.AccessTier,
		AgentArns:    l.AgentArns,
		CreationTime: l.CreationTime.Unix(),
	}

	if l.SasConfiguration != nil {
		out.SasConfiguration = &azureBlobSasConfigOutput{Token: l.SasConfiguration.Token}
	}

	return out, nil
}

type updateLocationAzureBlobInput struct {
	SasConfiguration *azureBlobSasConfigInput `json:"SasConfiguration"`
	LocationArn      string                   `json:"LocationArn"`
	ContainerURL     string                   `json:"ContainerUrl,omitempty"`
	Subdirectory     string                   `json:"Subdirectory,omitempty"`
	BlobType         string                   `json:"BlobType,omitempty"`
	AccessTier       string                   `json:"AccessTier,omitempty"`
	AgentArns        []string                 `json:"AgentArns"`
}

type updateLocationAzureBlobOutput struct{}

func (h *Handler) handleUpdateLocationAzureBlob(
	_ context.Context,
	in *updateLocationAzureBlobInput,
) (*updateLocationAzureBlobOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var sasConfig *SasConfiguration
	if in.SasConfiguration != nil {
		sasConfig = &SasConfiguration{Token: in.SasConfiguration.Token}
	}

	if err := h.Backend.UpdateLocationAzureBlob(
		in.LocationArn, in.ContainerURL, in.Subdirectory, in.BlobType, in.AccessTier,
		sasConfig, in.AgentArns,
	); err != nil {
		return nil, err
	}

	return &updateLocationAzureBlobOutput{}, nil
}

// --- EFS location ---

type ec2ConfigInput struct {
	SubnetArn         string   `json:"SubnetArn"`
	SecurityGroupArns []string `json:"SecurityGroupArns"`
}

type createLocationEfsInput struct {
	Ec2Config               *ec2ConfigInput `json:"Ec2Config"`
	EfsFilesystemArn        string          `json:"EfsFilesystemArn"`
	Subdirectory            string          `json:"Subdirectory,omitempty"`
	AccessPointArn          string          `json:"AccessPointArn,omitempty"`
	FileSystemAccessRoleArn string          `json:"FileSystemAccessRoleArn,omitempty"`
	InTransitEncryption     string          `json:"InTransitEncryption,omitempty"`
	Tags                    []tagInput      `json:"Tags"`
}

type createLocationEfsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationEfs(
	_ context.Context,
	in *createLocationEfsInput,
) (*createLocationEfsOutput, error) {
	if in.EfsFilesystemArn == "" {
		return nil, fmt.Errorf("%w: EfsFilesystemArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var ec2Cfg *Ec2Config
	if in.Ec2Config != nil {
		ec2Cfg = &Ec2Config{
			SubnetArn:         in.Ec2Config.SubnetArn,
			SecurityGroupArns: in.Ec2Config.SecurityGroupArns,
		}
	}

	l, err := h.Backend.CreateLocationEfs(
		in.EfsFilesystemArn, in.Subdirectory,
		in.AccessPointArn, in.FileSystemAccessRoleArn, in.InTransitEncryption,
		ec2Cfg, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationEfsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationEfsInput struct {
	LocationArn string `json:"LocationArn"`
}

type ec2ConfigOutput struct {
	SubnetArn         string   `json:"SubnetArn"`
	SecurityGroupArns []string `json:"SecurityGroupArns,omitempty"`
}

type describeLocationEfsOutput struct {
	Ec2Config               *ec2ConfigOutput `json:"Ec2Config,omitempty"`
	LocationArn             string           `json:"LocationArn"`
	LocationURI             string           `json:"LocationUri"`
	EfsFilesystemArn        string           `json:"EfsFilesystemArn,omitempty"`
	Subdirectory            string           `json:"Subdirectory,omitempty"`
	AccessPointArn          string           `json:"AccessPointArn,omitempty"`
	FileSystemAccessRoleArn string           `json:"FileSystemAccessRoleArn,omitempty"`
	InTransitEncryption     string           `json:"InTransitEncryption,omitempty"`
	CreationTime            int64            `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationEfs(
	_ context.Context,
	in *describeLocationEfsInput,
) (*describeLocationEfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationEfs(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationEfsOutput{
		LocationArn:             l.LocationArn,
		LocationURI:             l.LocationURI,
		EfsFilesystemArn:        l.EfsFilesystemArn,
		Subdirectory:            l.Subdirectory,
		AccessPointArn:          l.AccessPointArn,
		FileSystemAccessRoleArn: l.FileSystemAccessRoleArn,
		InTransitEncryption:     l.InTransitEncryption,
		CreationTime:            l.CreationTime.Unix(),
	}

	if l.Ec2Config != nil {
		out.Ec2Config = &ec2ConfigOutput{
			SubnetArn:         l.Ec2Config.SubnetArn,
			SecurityGroupArns: l.Ec2Config.SecurityGroupArns,
		}
	}

	return out, nil
}

type updateLocationEfsInput struct {
	Ec2Config               *ec2ConfigInput `json:"Ec2Config"`
	LocationArn             string          `json:"LocationArn"`
	Subdirectory            string          `json:"Subdirectory,omitempty"`
	AccessPointArn          string          `json:"AccessPointArn,omitempty"`
	FileSystemAccessRoleArn string          `json:"FileSystemAccessRoleArn,omitempty"`
	InTransitEncryption     string          `json:"InTransitEncryption,omitempty"`
}

type updateLocationEfsOutput struct{}

func (h *Handler) handleUpdateLocationEfs(
	_ context.Context,
	in *updateLocationEfsInput,
) (*updateLocationEfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var ec2Cfg *Ec2Config
	if in.Ec2Config != nil {
		ec2Cfg = &Ec2Config{
			SubnetArn:         in.Ec2Config.SubnetArn,
			SecurityGroupArns: in.Ec2Config.SecurityGroupArns,
		}
	}

	if err := h.Backend.UpdateLocationEfs(
		in.LocationArn, in.Subdirectory,
		in.AccessPointArn, in.FileSystemAccessRoleArn, in.InTransitEncryption,
		ec2Cfg,
	); err != nil {
		return nil, err
	}

	return &updateLocationEfsOutput{}, nil
}

// --- FSx Lustre location ---

type createLocationFsxLustreInput struct {
	FsxFilesystemArn  string     `json:"FsxFilesystemArn"`
	Subdirectory      string     `json:"Subdirectory,omitempty"`
	SecurityGroupArns []string   `json:"SecurityGroupArns"`
	Tags              []tagInput `json:"Tags"`
}

type createLocationFsxLustreOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationFsxLustre(
	_ context.Context,
	in *createLocationFsxLustreInput,
) (*createLocationFsxLustreOutput, error) {
	if in.FsxFilesystemArn == "" {
		return nil, fmt.Errorf("%w: FsxFilesystemArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationFsxLustre(in.FsxFilesystemArn, in.Subdirectory, in.SecurityGroupArns, tags)
	if err != nil {
		return nil, err
	}

	return &createLocationFsxLustreOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationFsxLustreInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationFsxLustreOutput struct {
	LocationArn       string   `json:"LocationArn"`
	LocationURI       string   `json:"LocationUri"`
	FsxFilesystemArn  string   `json:"FsxFilesystemArn,omitempty"`
	Subdirectory      string   `json:"Subdirectory,omitempty"`
	SecurityGroupArns []string `json:"SecurityGroupArns,omitempty"`
	CreationTime      int64    `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationFsxLustre(
	_ context.Context,
	in *describeLocationFsxLustreInput,
) (*describeLocationFsxLustreOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationFsxLustre(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationFsxLustreOutput{
		LocationArn:       l.LocationArn,
		LocationURI:       l.LocationURI,
		FsxFilesystemArn:  l.FsxFilesystemArn,
		Subdirectory:      l.Subdirectory,
		SecurityGroupArns: l.SecurityGroupArns,
		CreationTime:      l.CreationTime.Unix(),
	}, nil
}

type updateLocationFsxLustreInput struct {
	LocationArn  string `json:"LocationArn"`
	Subdirectory string `json:"Subdirectory,omitempty"`
}

type updateLocationFsxLustreOutput struct{}

func (h *Handler) handleUpdateLocationFsxLustre(
	_ context.Context,
	in *updateLocationFsxLustreInput,
) (*updateLocationFsxLustreOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationFsxLustre(in.LocationArn, in.Subdirectory); err != nil {
		return nil, err
	}

	return &updateLocationFsxLustreOutput{}, nil
}

// --- FSx ONTAP location ---

type fsxMountOptionsInput struct {
	Version string `json:"Version,omitempty"`
}

type fsxNfsProtocolInput struct {
	MountOptions *fsxMountOptionsInput `json:"MountOptions"`
}

type fsxSmbProtocolInput struct {
	MountOptions *fsxMountOptionsInput `json:"MountOptions"`
	Domain       string                `json:"Domain,omitempty"`
	Password     string                `json:"Password,omitempty"`
	User         string                `json:"User,omitempty"`
}

type fsxProtocolInput struct {
	NFS *fsxNfsProtocolInput `json:"NFS"`
	SMB *fsxSmbProtocolInput `json:"SMB"`
}

func fsxProtocolFromInput(p *fsxProtocolInput) *FsxProtocol {
	if p == nil {
		return nil
	}

	out := &FsxProtocol{}

	if p.NFS != nil {
		out.NFS = &FsxNfsProtocol{}
		if p.NFS.MountOptions != nil {
			out.NFS.MountOptions = &MountOptions{Version: p.NFS.MountOptions.Version}
		}
	}

	if p.SMB != nil {
		out.SMB = &FsxSmbProtocol{
			Domain:   p.SMB.Domain,
			Password: p.SMB.Password,
			User:     p.SMB.User,
		}
		if p.SMB.MountOptions != nil {
			out.SMB.MountOptions = &MountOptions{Version: p.SMB.MountOptions.Version}
		}
	}

	return out
}

type fsxMountOptionsOutput struct {
	Version string `json:"Version,omitempty"`
}

type fsxNfsProtocolOutput struct {
	MountOptions *fsxMountOptionsOutput `json:"MountOptions,omitempty"`
}

type fsxSmbProtocolOutput struct {
	MountOptions *fsxMountOptionsOutput `json:"MountOptions,omitempty"`
	Domain       string                 `json:"Domain,omitempty"`
	User         string                 `json:"User,omitempty"`
}

type fsxProtocolOutput struct {
	NFS *fsxNfsProtocolOutput `json:"NFS,omitempty"`
	SMB *fsxSmbProtocolOutput `json:"SMB,omitempty"`
}

func fsxProtocolToOutput(p *FsxProtocol) *fsxProtocolOutput {
	if p == nil {
		return nil
	}

	out := &fsxProtocolOutput{}

	if p.NFS != nil {
		out.NFS = &fsxNfsProtocolOutput{}
		if p.NFS.MountOptions != nil {
			out.NFS.MountOptions = &fsxMountOptionsOutput{Version: p.NFS.MountOptions.Version}
		}
	}

	if p.SMB != nil {
		out.SMB = &fsxSmbProtocolOutput{
			Domain: p.SMB.Domain,
			User:   p.SMB.User,
		}
		if p.SMB.MountOptions != nil {
			out.SMB.MountOptions = &fsxMountOptionsOutput{Version: p.SMB.MountOptions.Version}
		}
	}

	return out
}

type createLocationFsxOntapInput struct {
	Protocol                 *fsxProtocolInput `json:"Protocol"`
	StorageVirtualMachineArn string            `json:"StorageVirtualMachineArn"`
	Subdirectory             string            `json:"Subdirectory,omitempty"`
	SecurityGroupArns        []string          `json:"SecurityGroupArns"`
	Tags                     []tagInput        `json:"Tags"`
}

type createLocationFsxOntapOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationFsxOntap(
	_ context.Context,
	in *createLocationFsxOntapInput,
) (*createLocationFsxOntapOutput, error) {
	if in.StorageVirtualMachineArn == "" {
		return nil, fmt.Errorf("%w: StorageVirtualMachineArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationFsxOntap(
		in.StorageVirtualMachineArn, in.Subdirectory,
		fsxProtocolFromInput(in.Protocol), in.SecurityGroupArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationFsxOntapOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationFsxOntapInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationFsxOntapOutput struct {
	Protocol                 *fsxProtocolOutput `json:"Protocol,omitempty"`
	LocationArn              string             `json:"LocationArn"`
	LocationURI              string             `json:"LocationUri"`
	StorageVirtualMachineArn string             `json:"StorageVirtualMachineArn,omitempty"`
	Subdirectory             string             `json:"Subdirectory,omitempty"`
	SecurityGroupArns        []string           `json:"SecurityGroupArns,omitempty"`
	CreationTime             int64              `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationFsxOntap(
	_ context.Context,
	in *describeLocationFsxOntapInput,
) (*describeLocationFsxOntapOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationFsxOntap(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationFsxOntapOutput{
		LocationArn:              l.LocationArn,
		LocationURI:              l.LocationURI,
		StorageVirtualMachineArn: l.StorageVirtualMachineArn,
		Subdirectory:             l.Subdirectory,
		SecurityGroupArns:        l.SecurityGroupArns,
		Protocol:                 fsxProtocolToOutput(l.Protocol),
		CreationTime:             l.CreationTime.Unix(),
	}, nil
}

type updateLocationFsxOntapInput struct {
	Protocol     *fsxProtocolInput `json:"Protocol"`
	LocationArn  string            `json:"LocationArn"`
	Subdirectory string            `json:"Subdirectory,omitempty"`
}

type updateLocationFsxOntapOutput struct{}

func (h *Handler) handleUpdateLocationFsxOntap(
	_ context.Context,
	in *updateLocationFsxOntapInput,
) (*updateLocationFsxOntapOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationFsxOntap(
		in.LocationArn, in.Subdirectory, fsxProtocolFromInput(in.Protocol),
	); err != nil {
		return nil, err
	}

	return &updateLocationFsxOntapOutput{}, nil
}

// --- FSx OpenZFS location ---

type createLocationFsxOpenZfsInput struct {
	Protocol          *fsxProtocolInput `json:"Protocol"`
	FsxFilesystemArn  string            `json:"FsxFilesystemArn"`
	Subdirectory      string            `json:"Subdirectory,omitempty"`
	SecurityGroupArns []string          `json:"SecurityGroupArns"`
	Tags              []tagInput        `json:"Tags"`
}

type createLocationFsxOpenZfsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationFsxOpenZfs(
	_ context.Context,
	in *createLocationFsxOpenZfsInput,
) (*createLocationFsxOpenZfsOutput, error) {
	if in.FsxFilesystemArn == "" {
		return nil, fmt.Errorf("%w: FsxFilesystemArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationFsxOpenZfs(
		in.FsxFilesystemArn, in.Subdirectory,
		fsxProtocolFromInput(in.Protocol), in.SecurityGroupArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationFsxOpenZfsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationFsxOpenZfsInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationFsxOpenZfsOutput struct {
	Protocol          *fsxProtocolOutput `json:"Protocol,omitempty"`
	LocationArn       string             `json:"LocationArn"`
	LocationURI       string             `json:"LocationUri"`
	FsxFilesystemArn  string             `json:"FsxFilesystemArn,omitempty"`
	Subdirectory      string             `json:"Subdirectory,omitempty"`
	SecurityGroupArns []string           `json:"SecurityGroupArns,omitempty"`
	CreationTime      int64              `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationFsxOpenZfs(
	_ context.Context,
	in *describeLocationFsxOpenZfsInput,
) (*describeLocationFsxOpenZfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationFsxOpenZfs(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationFsxOpenZfsOutput{
		LocationArn:       l.LocationArn,
		LocationURI:       l.LocationURI,
		FsxFilesystemArn:  l.FsxFilesystemArn,
		Subdirectory:      l.Subdirectory,
		SecurityGroupArns: l.SecurityGroupArns,
		Protocol:          fsxProtocolToOutput(l.Protocol),
		CreationTime:      l.CreationTime.Unix(),
	}, nil
}

type updateLocationFsxOpenZfsInput struct {
	Protocol     *fsxProtocolInput `json:"Protocol"`
	LocationArn  string            `json:"LocationArn"`
	Subdirectory string            `json:"Subdirectory,omitempty"`
}

type updateLocationFsxOpenZfsOutput struct{}

func (h *Handler) handleUpdateLocationFsxOpenZfs(
	_ context.Context,
	in *updateLocationFsxOpenZfsInput,
) (*updateLocationFsxOpenZfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationFsxOpenZfs(
		in.LocationArn, in.Subdirectory, fsxProtocolFromInput(in.Protocol),
	); err != nil {
		return nil, err
	}

	return &updateLocationFsxOpenZfsOutput{}, nil
}

// --- FSx Windows location ---

type createLocationFsxWindowsInput struct {
	FsxFilesystemArn  string     `json:"FsxFilesystemArn"`
	Subdirectory      string     `json:"Subdirectory,omitempty"`
	Domain            string     `json:"Domain,omitempty"`
	User              string     `json:"User"`
	Password          string     `json:"Password"`
	SecurityGroupArns []string   `json:"SecurityGroupArns"`
	Tags              []tagInput `json:"Tags"`
}

type createLocationFsxWindowsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationFsxWindows(
	_ context.Context,
	in *createLocationFsxWindowsInput,
) (*createLocationFsxWindowsOutput, error) {
	if in.FsxFilesystemArn == "" {
		return nil, fmt.Errorf("%w: FsxFilesystemArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationFsxWindows(
		in.FsxFilesystemArn, in.Subdirectory, in.Domain, in.User, in.Password,
		in.SecurityGroupArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationFsxWindowsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationFsxWindowsInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationFsxWindowsOutput struct {
	LocationArn       string   `json:"LocationArn"`
	LocationURI       string   `json:"LocationUri"`
	FsxFilesystemArn  string   `json:"FsxFilesystemArn,omitempty"`
	Subdirectory      string   `json:"Subdirectory,omitempty"`
	Domain            string   `json:"Domain,omitempty"`
	User              string   `json:"User,omitempty"`
	SecurityGroupArns []string `json:"SecurityGroupArns,omitempty"`
	CreationTime      int64    `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationFsxWindows(
	_ context.Context,
	in *describeLocationFsxWindowsInput,
) (*describeLocationFsxWindowsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationFsxWindows(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationFsxWindowsOutput{
		LocationArn:       l.LocationArn,
		LocationURI:       l.LocationURI,
		FsxFilesystemArn:  l.FsxFilesystemArn,
		Subdirectory:      l.Subdirectory,
		Domain:            l.Domain,
		User:              l.User,
		SecurityGroupArns: l.SecurityGroupArns,
		CreationTime:      l.CreationTime.Unix(),
	}, nil
}

type updateLocationFsxWindowsInput struct {
	LocationArn  string `json:"LocationArn"`
	Subdirectory string `json:"Subdirectory,omitempty"`
	Domain       string `json:"Domain,omitempty"`
	User         string `json:"User,omitempty"`
	Password     string `json:"Password,omitempty"`
}

type updateLocationFsxWindowsOutput struct{}

func (h *Handler) handleUpdateLocationFsxWindows(
	_ context.Context,
	in *updateLocationFsxWindowsInput,
) (*updateLocationFsxWindowsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationFsxWindows(
		in.LocationArn, in.Subdirectory, in.Domain, in.User, in.Password,
	); err != nil {
		return nil, err
	}

	return &updateLocationFsxWindowsOutput{}, nil
}

// --- HDFS location ---

type hdfsNameNodeInput struct {
	Hostname string `json:"Hostname"`
	Port     int32  `json:"Port"`
}

type hdfsQopConfigInput struct {
	DataTransferProtection string `json:"DataTransferProtection,omitempty"`
	RPCProtection          string `json:"RpcProtection,omitempty"`
}

type createLocationHdfsInput struct {
	QopConfiguration   *hdfsQopConfigInput `json:"QopConfiguration"`
	SimpleUser         string              `json:"SimpleUser,omitempty"`
	KerberosPrincipal  string              `json:"KerberosPrincipal,omitempty"`
	KerberosKeytab     string              `json:"KerberosKeytab,omitempty"`
	KerberosKrb5Conf   string              `json:"KerberosKrb5Conf,omitempty"`
	KmsKeyProviderURI  string              `json:"KmsKeyProviderUri,omitempty"`
	AuthenticationType string              `json:"AuthenticationType,omitempty"`
	Subdirectory       string              `json:"Subdirectory,omitempty"`
	AgentArns          []string            `json:"AgentArns"`
	Tags               []tagInput          `json:"Tags"`
	NameNodes          []hdfsNameNodeInput `json:"NameNodes"`
	BlockSize          int64               `json:"BlockSize,omitempty"`
	ReplicationFactor  int32               `json:"ReplicationFactor,omitempty"`
}

type createLocationHdfsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationHdfs(
	_ context.Context,
	in *createLocationHdfsInput,
) (*createLocationHdfsOutput, error) {
	if len(in.NameNodes) == 0 {
		return nil, fmt.Errorf("%w: NameNodes is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	nameNodes := make([]HdfsNameNode, len(in.NameNodes))
	for i, n := range in.NameNodes {
		nameNodes[i] = HdfsNameNode{Hostname: n.Hostname, Port: n.Port}
	}

	var qopCfg *QopConfiguration
	if in.QopConfiguration != nil {
		qopCfg = &QopConfiguration{
			DataTransferProtection: in.QopConfiguration.DataTransferProtection,
			RPCProtection:          in.QopConfiguration.RPCProtection,
		}
	}

	l, err := h.Backend.CreateLocationHdfs(
		in.Subdirectory, in.AuthenticationType, in.SimpleUser,
		in.KerberosPrincipal, in.KerberosKeytab, in.KerberosKrb5Conf, in.KmsKeyProviderURI,
		nameNodes, in.BlockSize, in.ReplicationFactor, qopCfg, in.AgentArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationHdfsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationHdfsInput struct {
	LocationArn string `json:"LocationArn"`
}

type hdfsNameNodeOutput struct {
	Hostname string `json:"Hostname"`
	Port     int32  `json:"Port"`
}

type hdfsQopConfigOutput struct {
	DataTransferProtection string `json:"DataTransferProtection,omitempty"`
	RPCProtection          string `json:"RpcProtection,omitempty"`
}

type describeLocationHdfsOutput struct {
	QopConfiguration   *hdfsQopConfigOutput `json:"QopConfiguration,omitempty"`
	KmsKeyProviderURI  string               `json:"KmsKeyProviderUri,omitempty"`
	LocationArn        string               `json:"LocationArn"`
	LocationURI        string               `json:"LocationUri"`
	KerberosPrincipal  string               `json:"KerberosPrincipal,omitempty"`
	AuthenticationType string               `json:"AuthenticationType,omitempty"`
	SimpleUser         string               `json:"SimpleUser,omitempty"`
	Subdirectory       string               `json:"Subdirectory,omitempty"`
	AgentArns          []string             `json:"AgentArns,omitempty"`
	NameNodes          []hdfsNameNodeOutput `json:"NameNodes,omitempty"`
	CreationTime       int64                `json:"CreationTime"`
	BlockSize          int64                `json:"BlockSize,omitempty"`
	ReplicationFactor  int32                `json:"ReplicationFactor,omitempty"`
}

func (h *Handler) handleDescribeLocationHdfs(
	_ context.Context,
	in *describeLocationHdfsInput,
) (*describeLocationHdfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationHdfs(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationHdfsOutput{
		LocationArn:        l.LocationArn,
		LocationURI:        l.LocationURI,
		Subdirectory:       l.Subdirectory,
		AuthenticationType: l.AuthenticationType,
		SimpleUser:         l.SimpleUser,
		KerberosPrincipal:  l.KerberosPrincipal,
		KmsKeyProviderURI:  l.KmsKeyProviderURI,
		BlockSize:          l.BlockSize,
		ReplicationFactor:  l.ReplicationFactor,
		AgentArns:          l.AgentArns,
		CreationTime:       l.CreationTime.Unix(),
	}

	nodes := make([]hdfsNameNodeOutput, len(l.NameNodes))
	for i, n := range l.NameNodes {
		nodes[i] = hdfsNameNodeOutput{Hostname: n.Hostname, Port: n.Port}
	}

	out.NameNodes = nodes

	if l.QopConfiguration != nil {
		out.QopConfiguration = &hdfsQopConfigOutput{
			DataTransferProtection: l.QopConfiguration.DataTransferProtection,
			RPCProtection:          l.QopConfiguration.RPCProtection,
		}
	}

	return out, nil
}

type updateLocationHdfsInput struct {
	QopConfiguration   *hdfsQopConfigInput `json:"QopConfiguration"`
	KerberosKrb5Conf   string              `json:"KerberosKrb5Conf,omitempty"`
	LocationArn        string              `json:"LocationArn"`
	KerberosPrincipal  string              `json:"KerberosPrincipal,omitempty"`
	KerberosKeytab     string              `json:"KerberosKeytab,omitempty"`
	KmsKeyProviderURI  string              `json:"KmsKeyProviderUri,omitempty"`
	AuthenticationType string              `json:"AuthenticationType,omitempty"`
	SimpleUser         string              `json:"SimpleUser,omitempty"`
	Subdirectory       string              `json:"Subdirectory,omitempty"`
	AgentArns          []string            `json:"AgentArns"`
	NameNodes          []hdfsNameNodeInput `json:"NameNodes"`
	BlockSize          int64               `json:"BlockSize,omitempty"`
	ReplicationFactor  int32               `json:"ReplicationFactor,omitempty"`
}

type updateLocationHdfsOutput struct{}

func (h *Handler) handleUpdateLocationHdfs(
	_ context.Context,
	in *updateLocationHdfsInput,
) (*updateLocationHdfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	nameNodes := make([]HdfsNameNode, len(in.NameNodes))
	for i, n := range in.NameNodes {
		nameNodes[i] = HdfsNameNode{Hostname: n.Hostname, Port: n.Port}
	}

	var qopCfg *QopConfiguration
	if in.QopConfiguration != nil {
		qopCfg = &QopConfiguration{
			DataTransferProtection: in.QopConfiguration.DataTransferProtection,
			RPCProtection:          in.QopConfiguration.RPCProtection,
		}
	}

	if err := h.Backend.UpdateLocationHdfs(
		in.LocationArn, in.Subdirectory, in.AuthenticationType, in.SimpleUser,
		in.KerberosPrincipal, in.KerberosKeytab, in.KerberosKrb5Conf, in.KmsKeyProviderURI,
		nameNodes, in.BlockSize, in.ReplicationFactor, qopCfg, in.AgentArns,
	); err != nil {
		return nil, err
	}

	return &updateLocationHdfsOutput{}, nil
}

// --- NFS location ---

type mountOptionsInput struct {
	Version string `json:"Version,omitempty"`
}

type nfsOnPremConfigInput struct {
	AgentArns []string `json:"AgentArns"`
}

type createLocationNfsInput struct {
	MountOptions   *mountOptionsInput    `json:"MountOptions"`
	OnPremConfig   *nfsOnPremConfigInput `json:"OnPremConfig"`
	ServerHostname string                `json:"ServerHostname"`
	Subdirectory   string                `json:"Subdirectory,omitempty"`
	Tags           []tagInput            `json:"Tags"`
}

type createLocationNfsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationNfs(
	_ context.Context,
	in *createLocationNfsInput,
) (*createLocationNfsOutput, error) {
	if in.ServerHostname == "" {
		return nil, fmt.Errorf("%w: ServerHostname is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	var agentArns []string
	if in.OnPremConfig != nil {
		agentArns = in.OnPremConfig.AgentArns
	}

	l, err := h.Backend.CreateLocationNfs(in.ServerHostname, in.Subdirectory, mo, agentArns, tags)
	if err != nil {
		return nil, err
	}

	return &createLocationNfsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationNfsInput struct {
	LocationArn string `json:"LocationArn"`
}

type mountOptionsOutput struct {
	Version string `json:"Version,omitempty"`
}

type nfsOnPremConfigOutput struct {
	AgentArns []string `json:"AgentArns,omitempty"`
}

type describeLocationNfsOutput struct {
	MountOptions   *mountOptionsOutput    `json:"MountOptions,omitempty"`
	OnPremConfig   *nfsOnPremConfigOutput `json:"OnPremConfig,omitempty"`
	LocationArn    string                 `json:"LocationArn"`
	LocationURI    string                 `json:"LocationUri"`
	ServerHostname string                 `json:"ServerHostname,omitempty"`
	Subdirectory   string                 `json:"Subdirectory,omitempty"`
	CreationTime   int64                  `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationNfs(
	_ context.Context,
	in *describeLocationNfsInput,
) (*describeLocationNfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationNfs(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationNfsOutput{
		LocationArn:    l.LocationArn,
		LocationURI:    l.LocationURI,
		ServerHostname: l.ServerHostname,
		Subdirectory:   l.Subdirectory,
		CreationTime:   l.CreationTime.Unix(),
	}

	if l.MountOptions != nil {
		out.MountOptions = &mountOptionsOutput{Version: l.MountOptions.Version}
	}

	if len(l.AgentArns) > 0 {
		out.OnPremConfig = &nfsOnPremConfigOutput{AgentArns: l.AgentArns}
	}

	return out, nil
}

type updateLocationNfsInput struct {
	MountOptions *mountOptionsInput    `json:"MountOptions"`
	OnPremConfig *nfsOnPremConfigInput `json:"OnPremConfig"`
	LocationArn  string                `json:"LocationArn"`
	Subdirectory string                `json:"Subdirectory,omitempty"`
}

type updateLocationNfsOutput struct{}

func (h *Handler) handleUpdateLocationNfs(
	_ context.Context,
	in *updateLocationNfsInput,
) (*updateLocationNfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	var agentArns []string
	if in.OnPremConfig != nil {
		agentArns = in.OnPremConfig.AgentArns
	}

	if err := h.Backend.UpdateLocationNfs(in.LocationArn, in.Subdirectory, mo, agentArns); err != nil {
		return nil, err
	}

	return &updateLocationNfsOutput{}, nil
}

// --- ObjectStorage location ---

type createLocationObjectStorageInput struct {
	ServerHostname string     `json:"ServerHostname"`
	BucketName     string     `json:"BucketName"`
	Subdirectory   string     `json:"Subdirectory,omitempty"`
	AccessKey      string     `json:"AccessKey,omitempty"`
	SecretKey      string     `json:"SecretKey,omitempty"`
	ServerProtocol string     `json:"ServerProtocol,omitempty"`
	AgentArns      []string   `json:"AgentArns"`
	Tags           []tagInput `json:"Tags"`
	ServerPort     int32      `json:"ServerPort,omitempty"`
}

type createLocationObjectStorageOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationObjectStorage(
	_ context.Context,
	in *createLocationObjectStorageInput,
) (*createLocationObjectStorageOutput, error) {
	if in.ServerHostname == "" {
		return nil, fmt.Errorf("%w: ServerHostname is required", errInvalidRequest)
	}

	if in.BucketName == "" {
		return nil, fmt.Errorf("%w: BucketName is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationObjectStorage(
		in.ServerHostname, in.ServerProtocol, in.BucketName, in.Subdirectory,
		in.AccessKey, in.SecretKey, in.ServerPort, in.AgentArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationObjectStorageOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationObjectStorageInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationObjectStorageOutput struct {
	LocationArn    string   `json:"LocationArn"`
	LocationURI    string   `json:"LocationUri"`
	ServerHostname string   `json:"ServerHostname,omitempty"`
	BucketName     string   `json:"BucketName,omitempty"`
	Subdirectory   string   `json:"Subdirectory,omitempty"`
	AccessKey      string   `json:"AccessKey,omitempty"`
	ServerProtocol string   `json:"ServerProtocol,omitempty"`
	AgentArns      []string `json:"AgentArns,omitempty"`
	CreationTime   int64    `json:"CreationTime"`
	ServerPort     int32    `json:"ServerPort,omitempty"`
}

func (h *Handler) handleDescribeLocationObjectStorage(
	_ context.Context,
	in *describeLocationObjectStorageInput,
) (*describeLocationObjectStorageOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationObjectStorage(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationObjectStorageOutput{
		LocationArn:    l.LocationArn,
		LocationURI:    l.LocationURI,
		ServerHostname: l.ServerHostname,
		BucketName:     l.BucketName,
		Subdirectory:   l.Subdirectory,
		AccessKey:      l.AccessKey,
		ServerProtocol: l.ServerProtocol,
		ServerPort:     l.ServerPort,
		AgentArns:      l.AgentArns,
		CreationTime:   l.CreationTime.Unix(),
	}, nil
}

type updateLocationObjectStorageInput struct {
	LocationArn    string   `json:"LocationArn"`
	Subdirectory   string   `json:"Subdirectory,omitempty"`
	AccessKey      string   `json:"AccessKey,omitempty"`
	SecretKey      string   `json:"SecretKey,omitempty"`
	ServerProtocol string   `json:"ServerProtocol,omitempty"`
	AgentArns      []string `json:"AgentArns"`
	ServerPort     int32    `json:"ServerPort,omitempty"`
}

type updateLocationObjectStorageOutput struct{}

func (h *Handler) handleUpdateLocationObjectStorage(
	_ context.Context,
	in *updateLocationObjectStorageInput,
) (*updateLocationObjectStorageOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationObjectStorage(
		in.LocationArn, in.ServerProtocol, in.Subdirectory,
		in.AccessKey, in.SecretKey, in.ServerPort, in.AgentArns,
	); err != nil {
		return nil, err
	}

	return &updateLocationObjectStorageOutput{}, nil
}

// --- SMB location ---

type createLocationSmbInput struct {
	MountOptions   *mountOptionsInput `json:"MountOptions"`
	ServerHostname string             `json:"ServerHostname"`
	Subdirectory   string             `json:"Subdirectory,omitempty"`
	Domain         string             `json:"Domain,omitempty"`
	User           string             `json:"User"`
	Password       string             `json:"Password"`
	AgentArns      []string           `json:"AgentArns"`
	Tags           []tagInput         `json:"Tags"`
}

type createLocationSmbOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationSmb(
	_ context.Context,
	in *createLocationSmbInput,
) (*createLocationSmbOutput, error) {
	if in.ServerHostname == "" {
		return nil, fmt.Errorf("%w: ServerHostname is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	l, err := h.Backend.CreateLocationSmb(
		in.ServerHostname, in.Subdirectory, in.Domain, in.User, in.Password,
		mo, in.AgentArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationSmbOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationSmbInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationSmbOutput struct {
	MountOptions   *mountOptionsOutput `json:"MountOptions,omitempty"`
	LocationArn    string              `json:"LocationArn"`
	LocationURI    string              `json:"LocationUri"`
	ServerHostname string              `json:"ServerHostname,omitempty"`
	Subdirectory   string              `json:"Subdirectory,omitempty"`
	Domain         string              `json:"Domain,omitempty"`
	User           string              `json:"User,omitempty"`
	AgentArns      []string            `json:"AgentArns,omitempty"`
	CreationTime   int64               `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationSmb(
	_ context.Context,
	in *describeLocationSmbInput,
) (*describeLocationSmbOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationSmb(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationSmbOutput{
		LocationArn:    l.LocationArn,
		LocationURI:    l.LocationURI,
		ServerHostname: l.ServerHostname,
		Subdirectory:   l.Subdirectory,
		Domain:         l.Domain,
		User:           l.User,
		AgentArns:      l.AgentArns,
		CreationTime:   l.CreationTime.Unix(),
	}

	if l.MountOptions != nil {
		out.MountOptions = &mountOptionsOutput{Version: l.MountOptions.Version}
	}

	return out, nil
}

type updateLocationSmbInput struct {
	MountOptions *mountOptionsInput `json:"MountOptions"`
	LocationArn  string             `json:"LocationArn"`
	Subdirectory string             `json:"Subdirectory,omitempty"`
	Domain       string             `json:"Domain,omitempty"`
	User         string             `json:"User,omitempty"`
	Password     string             `json:"Password,omitempty"`
	AgentArns    []string           `json:"AgentArns"`
}

type updateLocationSmbOutput struct{}

func (h *Handler) handleUpdateLocationSmb(
	_ context.Context,
	in *updateLocationSmbInput,
) (*updateLocationSmbOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	if err := h.Backend.UpdateLocationSmb(
		in.LocationArn, in.Subdirectory, in.Domain, in.User, in.Password,
		mo, in.AgentArns,
	); err != nil {
		return nil, err
	}

	return &updateLocationSmbOutput{}, nil
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
