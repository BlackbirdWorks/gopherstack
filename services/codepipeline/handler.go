package codepipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codepipelineTargetPrefix = "CodePipeline_20150709."

	// transitionTypeInbound and transitionTypeOutbound are the valid values for StageTransitionType.
	transitionTypeInbound = "Inbound"

	// keyOwnerCustom is the owner value for custom action types.
	keyOwnerCustom = "Custom"
	// keyNonce is the JSON key for job nonce values.
	keyNonce = "nonce"
	// keyJobID is the JSON key for job IDs.
	keyJobID               = "id"
	transitionTypeOutbound = "Outbound"

	// maxResultsCap* constants define the per-operation pagination caps.
	maxResultsCapPipelineExecutions int32 = 100
	maxResultsCapWebhooks           int32 = 60
	maxResultsCapActionExecutions   int32 = 100
	maxResultsCapRuleExecutions     int32 = 100
	maxResultsCapActionTypes        int32 = 25
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// validArtifactStoreType returns true if t is a valid ArtifactStore type.
func validArtifactStoreType(t string) bool { return t == "S3" }

// cpParseNextToken converts an opaque NextToken string to a slice start index.
func cpParseNextToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// cpPaginate applies MaxResults/NextToken pagination to a slice.
// maxResultsCap is the per-operation maximum. A zero maxResults means "use cap".
func cpPaginate[T any](
	items []T,
	nextToken string,
	maxResults int32,
	maxResultsCap int32,
) ([]T, string, error) {
	limit := maxResultsCap

	if maxResults > 0 {
		if maxResults > maxResultsCap {
			return nil, "", fmt.Errorf(
				"%w: maxResults must be between 1 and %d",
				errInvalidRequest,
				maxResultsCap,
			)
		}

		limit = maxResults
	}

	start := cpParseNextToken(nextToken)

	if start >= len(items) {
		return items[:0], "", nil
	}

	end := start + int(limit)

	var outToken string

	if end < len(items) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(items)
	}

	return items[start:end], outToken, nil
}

// validPipelineType returns true if t is a valid PipelineType value.
func validPipelineType(t string) bool {
	return t == "" || t == PipelineTypeV1 || t == PipelineTypeV2
}

// validExecutionMode returns true if m is a valid ExecutionMode value.
func validExecutionMode(m string) bool {
	return m == "" || m == ExecutionModeQueued || m == ExecutionModeSuperseded ||
		m == ExecutionModeParallel
}

// validWebhookAuth returns true if a is a valid webhook Authentication value.
func validWebhookAuth(a string) bool {
	return a == "" || a == WebhookAuthGitHubHMAC || a == WebhookAuthIP ||
		a == WebhookAuthUnauthenticated
}

// Handler is the Echo HTTP handler for CodePipeline operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new CodePipeline handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.dispatchTable()

	return h
}

// Reset clears all handler and backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodePipeline" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcknowledgeJob",
		"AcknowledgeThirdPartyJob",
		"CreateCustomActionType",
		"CreatePipeline",
		"DeleteCustomActionType",
		"DeletePipeline",
		"DeleteWebhook",
		"DeregisterWebhookWithThirdParty",
		"DisableStageTransition",
		"EnableStageTransition",
		"GetActionType",
		"GetJobDetails",
		"GetPipeline",
		"ListPipelines",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"UpdatePipeline",
		"GetPipelineExecution",
		"GetPipelineState",
		"GetThirdPartyJobDetails",
		"ListActionExecutions",
		"ListActionTypes",
		"ListDeployActionExecutionTargets",
		"ListPipelineExecutions",
		"ListRuleExecutions",
		"ListRuleTypes",
		"ListWebhooks",
		"OverrideStageCondition",
		"PollForJobs",
		"PollForThirdPartyJobs",
		"PutActionRevision",
		"PutApprovalResult",
		"PutJobFailureResult",
		"PutJobSuccessResult",
		"PutThirdPartyJobFailureResult",
		"PutThirdPartyJobSuccessResult",
		"PutWebhook",
		"RegisterWebhookWithThirdParty",
		"RetryStageExecution",
		"RollbackStage",
		"StartPipelineExecution",
		"StopPipelineExecution",
		"UpdateActionType",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codepipeline" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches CodePipeline requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codepipelineTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodePipeline action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, codepipelineTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request (not used for CodePipeline).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for CodePipeline requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodePipeline", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AcknowledgeJob":                  service.WrapOp(h.handleAcknowledgeJob),
		"AcknowledgeThirdPartyJob":        service.WrapOp(h.handleAcknowledgeThirdPartyJob),
		"CreateCustomActionType":          service.WrapOp(h.handleCreateCustomActionType),
		"CreatePipeline":                  service.WrapOp(h.handleCreatePipeline),
		"DeleteCustomActionType":          service.WrapOp(h.handleDeleteCustomActionType),
		"DeletePipeline":                  service.WrapOp(h.handleDeletePipeline),
		"DeleteWebhook":                   service.WrapOp(h.handleDeleteWebhook),
		"DeregisterWebhookWithThirdParty": service.WrapOp(h.handleDeregisterWebhookWithThirdParty),
		"DisableStageTransition":          service.WrapOp(h.handleDisableStageTransition),
		"EnableStageTransition":           service.WrapOp(h.handleEnableStageTransition),
		"GetActionType":                   service.WrapOp(h.handleGetActionType),
		"GetJobDetails":                   service.WrapOp(h.handleGetJobDetails),
		"GetPipeline":                     service.WrapOp(h.handleGetPipeline),
		"ListPipelines":                   service.WrapOp(h.handleListPipelines),
		"ListTagsForResource":             service.WrapOp(h.handleListTagsForResource),
		"TagResource":                     service.WrapOp(h.handleTagResource),
		"UntagResource":                   service.WrapOp(h.handleUntagResource),
		"UpdatePipeline":                  service.WrapOp(h.handleUpdatePipeline),
		"GetPipelineExecution":            service.WrapOp(h.handleGetPipelineExecution),
		"GetPipelineState":                service.WrapOp(h.handleGetPipelineState),
		"GetThirdPartyJobDetails":         service.WrapOp(h.handleGetThirdPartyJobDetails),
		"ListActionExecutions":            service.WrapOp(h.handleListActionExecutions),
		"ListActionTypes":                 service.WrapOp(h.handleListActionTypes),
		"ListDeployActionExecutionTargets": service.WrapOp(
			h.handleListDeployActionExecutionTargets,
		),
		"ListPipelineExecutions":        service.WrapOp(h.handleListPipelineExecutions),
		"ListRuleExecutions":            service.WrapOp(h.handleListRuleExecutions),
		"ListRuleTypes":                 service.WrapOp(h.handleListRuleTypes),
		"ListWebhooks":                  service.WrapOp(h.handleListWebhooks),
		"OverrideStageCondition":        service.WrapOp(h.handleOverrideStageCondition),
		"PollForJobs":                   service.WrapOp(h.handlePollForJobs),
		"PollForThirdPartyJobs":         service.WrapOp(h.handlePollForThirdPartyJobs),
		"PutActionRevision":             service.WrapOp(h.handlePutActionRevision),
		"PutApprovalResult":             service.WrapOp(h.handlePutApprovalResult),
		"PutJobFailureResult":           service.WrapOp(h.handlePutJobFailureResult),
		"PutJobSuccessResult":           service.WrapOp(h.handlePutJobSuccessResult),
		"PutThirdPartyJobFailureResult": service.WrapOp(h.handlePutThirdPartyJobFailureResult),
		"PutThirdPartyJobSuccessResult": service.WrapOp(h.handlePutThirdPartyJobSuccessResult),
		"PutWebhook":                    service.WrapOp(h.handlePutWebhook),
		"RegisterWebhookWithThirdParty": service.WrapOp(h.handleRegisterWebhookWithThirdParty),
		"RetryStageExecution":           service.WrapOp(h.handleRetryStageExecution),
		"RollbackStage":                 service.WrapOp(h.handleRollbackStage),
		"StartPipelineExecution":        service.WrapOp(h.handleStartPipelineExecution),
		"StopPipelineExecution":         service.WrapOp(h.handleStopPipelineExecution),
		"UpdateActionType":              service.WrapOp(h.handleUpdateActionType),
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
	type errMapping struct {
		sentinel error
		errType  string
	}

	sentinels := []errMapping{
		{ErrPipelineNameInUse, "PipelineNameInUseException"},
		{ErrNotFound, "PipelineNotFoundException"},
		{ErrActionTypeNotFound, "ActionTypeNotFoundException"},
		{ErrJobNotFound, "JobNotFoundException"},
		{ErrWebhookNotFound, "WebhookNotFoundException"},
		{ErrAlreadyExists, "InvalidStructureException"},
		{ErrValidation, "ValidationException"},
		{ErrConflict, "ConflictException"},
		{ErrResourceInUse, "ResourceInUseException"},
		{ErrResourceNotFound, "ResourceNotFoundException"},
		{ErrStageNotFound, "StageNotFoundException"},
		{ErrInvalidStructure, "InvalidStructureException"},
		{errUnknownAction, "InvalidActionException"},
		{errInvalidRequest, "ValidationException"},
	}

	for _, m := range sentinels {
		if errors.Is(err, m.sentinel) {
			return errorBlob(c, http.StatusBadRequest, m.errType, err)
		}
	}

	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return errorBlob(c, http.StatusBadRequest, "ValidationException", err)
	}

	return errorBlob(c, http.StatusInternalServerError, "InternalFailure", err)
}

// errorBlob marshals a JSON error response and writes it to the echo context.
func errorBlob(c *echo.Context, status int, errType string, err error) error {
	payload, _ := json.Marshal(service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})

	return c.JSONBlob(status, payload)
}

// --- Pipeline operations ---

type createPipelineInput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Tags     []Tag                `json:"tags"`
}

type createPipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Tags     []Tag                `json:"tags"`
}

func (h *Handler) handleCreatePipeline(
	ctx context.Context,
	in *createPipelineInput,
) (*createPipelineOutput, error) {
	if in.Pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is required", errInvalidRequest)
	}

	if in.Pipeline.Name == "" {
		return nil, fmt.Errorf("%w: pipeline name is required", errInvalidRequest)
	}

	if in.Pipeline.RoleArn == "" {
		return nil, fmt.Errorf("%w: roleArn is required", ErrInvalidStructure)
	}

	if !validPipelineType(in.Pipeline.PipelineType) {
		return nil, fmt.Errorf(
			"%w: invalid pipelineType %q",
			ErrValidation,
			in.Pipeline.PipelineType,
		)
	}

	if !validExecutionMode(in.Pipeline.ExecutionMode) {
		return nil, fmt.Errorf(
			"%w: invalid executionMode %q",
			ErrValidation,
			in.Pipeline.ExecutionMode,
		)
	}

	if in.Pipeline.ArtifactStore.Type != "" &&
		!validArtifactStoreType(in.Pipeline.ArtifactStore.Type) {
		return nil, fmt.Errorf(
			"%w: invalid artifactStore type %q: must be S3",
			ErrValidation,
			in.Pipeline.ArtifactStore.Type,
		)
	}

	tagMap := tagsToMap(in.Tags)

	p, err := h.Backend.CreatePipeline(ctx, *in.Pipeline, tagMap)
	if err != nil {
		return nil, err
	}

	return &createPipelineOutput{
		Pipeline: &p.Declaration,
		Tags:     in.Tags,
	}, nil
}

type getPipelineInput struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type getPipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Metadata *PipelineMetadata    `json:"metadata"`
}

func (h *Handler) handleGetPipeline(
	ctx context.Context,
	in *getPipelineInput,
) (*getPipelineOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	p, err := h.Backend.GetPipeline(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	if in.Version != 0 && in.Version != p.Declaration.Version {
		return nil, fmt.Errorf("%w: pipeline %q version %d not found (current: %d)",
			ErrNotFound, in.Name, in.Version, p.Declaration.Version)
	}

	return &getPipelineOutput{
		Pipeline: &p.Declaration,
		Metadata: &p.Metadata,
	}, nil
}

type updatePipelineInput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
}

type updatePipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
}

func (h *Handler) handleUpdatePipeline(
	ctx context.Context,
	in *updatePipelineInput,
) (*updatePipelineOutput, error) {
	if in.Pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is required", errInvalidRequest)
	}

	if in.Pipeline.Name == "" {
		return nil, fmt.Errorf("%w: pipeline name is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdatePipeline(ctx, *in.Pipeline)
	if err != nil {
		return nil, err
	}

	return &updatePipelineOutput{Pipeline: &p.Declaration}, nil
}

type deletePipelineInput struct {
	Name string `json:"name"`
}

type deletePipelineOutput struct{}

func (h *Handler) handleDeletePipeline(
	ctx context.Context,
	in *deletePipelineInput,
) (*deletePipelineOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePipeline(ctx, in.Name); err != nil {
		return nil, err
	}

	return &deletePipelineOutput{}, nil
}

type listPipelinesInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listPipelinesOutput struct {
	NextToken string            `json:"nextToken,omitempty"`
	Pipelines []PipelineSummary `json:"pipelines"`
}

func (h *Handler) handleListPipelines(
	ctx context.Context,
	_ *listPipelinesInput,
) (*listPipelinesOutput, error) {
	summaries := h.Backend.ListPipelines(ctx)
	if summaries == nil {
		summaries = []PipelineSummary{}
	}

	return &listPipelinesOutput{Pipelines: summaries}, nil
}

// --- Tagging operations ---

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	if tags == nil {
		tags = []Tag{}
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

type tagResourceInput struct {
	ResourceArn string `json:"resourceArn"`
	Tags        []Tag  `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(ctx, in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

func tagsToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// validActionCategory returns true if cat is a valid AWS ActionCategory value.
func validActionCategory(cat string) bool {
	switch cat {
	case "Source", "Build", "Deploy", "Test", "Invoke", "Approval", "Compute":
		return true
	default:
		return false
	}
}

// validTransitionType returns true if t is a valid AWS StageTransitionType value.
func validTransitionType(t string) bool {
	return t == transitionTypeInbound || t == transitionTypeOutbound
}

// --- AcknowledgeJob ---

type acknowledgeJobInput struct {
	JobID string `json:"jobId"`
	Nonce string `json:"nonce"`
}

type acknowledgeJobOutput struct {
	Status string `json:"status"`
}

func (h *Handler) handleAcknowledgeJob(
	ctx context.Context,
	in *acknowledgeJobInput,
) (*acknowledgeJobOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	if in.Nonce == "" {
		return nil, fmt.Errorf("%w: nonce is required", errInvalidRequest)
	}

	status, err := h.Backend.AcknowledgeJob(ctx, in.JobID, in.Nonce)
	if err != nil {
		return nil, err
	}

	return &acknowledgeJobOutput{Status: status}, nil
}

// --- AcknowledgeThirdPartyJob ---

type acknowledgeThirdPartyJobInput struct {
	ClientToken string `json:"clientToken"`
	JobID       string `json:"jobId"`
	Nonce       string `json:"nonce"`
}

type acknowledgeThirdPartyJobOutput struct {
	Status string `json:"status"`
}

func (h *Handler) handleAcknowledgeThirdPartyJob(
	ctx context.Context,
	in *acknowledgeThirdPartyJobInput,
) (*acknowledgeThirdPartyJobOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	if in.Nonce == "" {
		return nil, fmt.Errorf("%w: nonce is required", errInvalidRequest)
	}

	if in.ClientToken == "" {
		return nil, fmt.Errorf("%w: clientToken is required", errInvalidRequest)
	}

	status, err := h.Backend.AcknowledgeThirdPartyJob(ctx, in.JobID, in.Nonce, in.ClientToken)
	if err != nil {
		return nil, err
	}

	return &acknowledgeThirdPartyJobOutput{Status: status}, nil
}

// --- CreateCustomActionType ---

type createCustomActionTypeInput struct {
	Settings                *ActionTypeSettings           `json:"settings,omitempty"`
	Category                string                        `json:"category"`
	Provider                string                        `json:"provider"`
	Version                 string                        `json:"version"`
	ConfigurationProperties []ActionConfigurationProperty `json:"configurationProperties,omitempty"`
	Tags                    []Tag                         `json:"tags,omitempty"`
	InputArtifactDetails    ArtifactDetails               `json:"inputArtifactDetails"`
	OutputArtifactDetails   ArtifactDetails               `json:"outputArtifactDetails"`
}

type customActionTypeResponse struct {
	Settings                      *ActionTypeSettings           `json:"settings,omitempty"`
	ID                            ActionTypeID                  `json:"id"`
	ActionConfigurationProperties []ActionConfigurationProperty `json:"actionConfigurationProperties"`
	InputArtifactDetails          ArtifactDetails               `json:"inputArtifactDetails"`
	OutputArtifactDetails         ArtifactDetails               `json:"outputArtifactDetails"`
}

type createCustomActionTypeOutput struct {
	Tags       []Tag                    `json:"tags,omitempty"`
	ActionType customActionTypeResponse `json:"actionType"`
}

func (h *Handler) handleCreateCustomActionType(
	ctx context.Context,
	in *createCustomActionTypeInput,
) (*createCustomActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
	}

	if !validActionCategory(in.Category) {
		return nil, fmt.Errorf("%w: invalid category %q", ErrValidation, in.Category)
	}

	if in.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", errInvalidRequest)
	}

	if in.Version == "" {
		return nil, fmt.Errorf("%w: version is required", errInvalidRequest)
	}

	cat := &CustomActionType{
		Category:                in.Category,
		Provider:                in.Provider,
		Version:                 in.Version,
		InputArtifactDetails:    in.InputArtifactDetails,
		OutputArtifactDetails:   in.OutputArtifactDetails,
		Settings:                in.Settings,
		ConfigurationProperties: in.ConfigurationProperties,
		Tags:                    tagsToMap(in.Tags),
	}

	created, err := h.Backend.CreateCustomActionType(ctx, cat)
	if err != nil {
		return nil, err
	}

	configProps := created.ConfigurationProperties
	if configProps == nil {
		configProps = []ActionConfigurationProperty{}
	}

	return &createCustomActionTypeOutput{
		ActionType: customActionTypeResponse{
			ID: ActionTypeID{
				Category: created.Category,
				Owner:    keyOwnerCustom,
				Provider: created.Provider,
				Version:  created.Version,
			},
			InputArtifactDetails:          created.InputArtifactDetails,
			OutputArtifactDetails:         created.OutputArtifactDetails,
			Settings:                      created.Settings,
			ActionConfigurationProperties: configProps,
		},
		Tags: in.Tags,
	}, nil
}

// --- DeleteCustomActionType ---

type deleteCustomActionTypeInput struct {
	Category string `json:"category"`
	Provider string `json:"provider"`
	Version  string `json:"version"`
}

type deleteCustomActionTypeOutput struct{}

func (h *Handler) handleDeleteCustomActionType(
	ctx context.Context,
	in *deleteCustomActionTypeInput,
) (*deleteCustomActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
	}

	if !validActionCategory(in.Category) {
		return nil, fmt.Errorf("%w: invalid category %q", ErrValidation, in.Category)
	}

	if in.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", errInvalidRequest)
	}

	if in.Version == "" {
		return nil, fmt.Errorf("%w: version is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteCustomActionType(ctx, in.Category, in.Provider, in.Version); err != nil {
		return nil, err
	}

	return &deleteCustomActionTypeOutput{}, nil
}

// --- GetActionType ---

type getActionTypeInput struct {
	Category string `json:"category"`
	Owner    string `json:"owner"`
	Provider string `json:"provider"`
	Version  string `json:"version"`
}

type getActionTypeOutput struct {
	ActionType customActionTypeResponse `json:"actionType"`
}

func (h *Handler) handleGetActionType(
	ctx context.Context,
	in *getActionTypeInput,
) (*getActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
	}

	if !validActionCategory(in.Category) {
		return nil, fmt.Errorf("%w: invalid category %q", ErrValidation, in.Category)
	}

	if in.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", errInvalidRequest)
	}

	if in.Version == "" {
		return nil, fmt.Errorf("%w: version is required", errInvalidRequest)
	}

	cat, err := h.Backend.GetActionType(ctx, in.Category, in.Owner, in.Provider, in.Version)
	if err != nil {
		return nil, err
	}

	catConfigProps := cat.ConfigurationProperties
	if catConfigProps == nil {
		catConfigProps = []ActionConfigurationProperty{}
	}

	return &getActionTypeOutput{
		ActionType: customActionTypeResponse{
			ID: ActionTypeID{
				Category: cat.Category,
				Owner:    keyOwnerCustom,
				Provider: cat.Provider,
				Version:  cat.Version,
			},
			InputArtifactDetails:          cat.InputArtifactDetails,
			OutputArtifactDetails:         cat.OutputArtifactDetails,
			Settings:                      cat.Settings,
			ActionConfigurationProperties: catConfigProps,
		},
	}, nil
}

// --- GetJobDetails ---

type jobDataResponse struct {
	ActionTypeID ActionTypeID `json:"actionTypeId"`
}

type jobDetailsResponse struct {
	Data      jobDataResponse `json:"data"`
	AccountID string          `json:"accountId"`
	ID        string          `json:"id"`
}

type getJobDetailsInput struct {
	JobID string `json:"jobId"`
}

type getJobDetailsOutput struct {
	JobDetails jobDetailsResponse `json:"jobDetails"`
}

func (h *Handler) handleGetJobDetails(
	ctx context.Context,
	in *getJobDetailsInput,
) (*getJobDetailsOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	job, err := h.Backend.GetJobDetails(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	return &getJobDetailsOutput{
		JobDetails: jobDetailsResponse{
			ID:        job.ID,
			AccountID: "",
			Data:      jobDataResponse{},
		},
	}, nil
}

// --- DeleteWebhook ---

type deleteWebhookInput struct {
	Name string `json:"name"`
}

type deleteWebhookOutput struct{}

func (h *Handler) handleDeleteWebhook(
	ctx context.Context,
	in *deleteWebhookInput,
) (*deleteWebhookOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebhook(ctx, in.Name); err != nil {
		return nil, err
	}

	return &deleteWebhookOutput{}, nil
}

// --- DeregisterWebhookWithThirdParty ---

type deregisterWebhookWithThirdPartyInput struct {
	WebhookName string `json:"webhookName"`
}

type deregisterWebhookWithThirdPartyOutput struct{}

func (h *Handler) handleDeregisterWebhookWithThirdParty(
	ctx context.Context,
	in *deregisterWebhookWithThirdPartyInput,
) (*deregisterWebhookWithThirdPartyOutput, error) {
	if err := h.Backend.DeregisterWebhookWithThirdParty(ctx, in.WebhookName); err != nil {
		return nil, err
	}

	return &deregisterWebhookWithThirdPartyOutput{}, nil
}

// --- DisableStageTransition ---

type disableStageTransitionInput struct {
	PipelineName   string `json:"pipelineName"`
	Reason         string `json:"reason"`
	StageName      string `json:"stageName"`
	TransitionType string `json:"transitionType"`
}

type disableStageTransitionOutput struct{}

func (h *Handler) handleDisableStageTransition(
	ctx context.Context,
	in *disableStageTransitionInput,
) (*disableStageTransitionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if in.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", errInvalidRequest)
	}

	if in.TransitionType == "" {
		return nil, fmt.Errorf("%w: transitionType is required", errInvalidRequest)
	}

	if !validTransitionType(in.TransitionType) {
		return nil, fmt.Errorf("%w: invalid transitionType %q, must be %s or %s",
			ErrValidation, in.TransitionType, transitionTypeInbound, transitionTypeOutbound)
	}

	if in.Reason == "" {
		return nil, fmt.Errorf("%w: reason is required", errInvalidRequest)
	}

	if err := h.Backend.DisableStageTransition(
		ctx, in.PipelineName, in.StageName, in.TransitionType, in.Reason,
	); err != nil {
		return nil, err
	}

	return &disableStageTransitionOutput{}, nil
}

// --- EnableStageTransition ---

type enableStageTransitionInput struct {
	PipelineName   string `json:"pipelineName"`
	StageName      string `json:"stageName"`
	TransitionType string `json:"transitionType"`
}

type enableStageTransitionOutput struct{}

func (h *Handler) handleEnableStageTransition(
	ctx context.Context,
	in *enableStageTransitionInput,
) (*enableStageTransitionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if in.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", errInvalidRequest)
	}

	if in.TransitionType == "" {
		return nil, fmt.Errorf("%w: transitionType is required", errInvalidRequest)
	}

	if !validTransitionType(in.TransitionType) {
		return nil, fmt.Errorf("%w: invalid transitionType %q, must be %s or %s",
			ErrValidation, in.TransitionType, transitionTypeInbound, transitionTypeOutbound)
	}

	if err := h.Backend.EnableStageTransition(ctx, in.PipelineName, in.StageName, in.TransitionType); err != nil {
		return nil, err
	}

	return &enableStageTransitionOutput{}, nil
}

// --- Pipeline execution handlers ---

type startPipelineExecutionInput struct {
	Name string `json:"name"`
}

type pipelineExecutionOutput struct {
	PipelineExecutionID string `json:"pipelineExecutionId"`
}

func (h *Handler) handleStartPipelineExecution(
	ctx context.Context,
	in *startPipelineExecutionInput,
) (*pipelineExecutionOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	exec, err := h.Backend.StartPipelineExecution(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	return &pipelineExecutionOutput{PipelineExecutionID: exec.PipelineExecutionID}, nil
}

type getPipelineExecutionInput struct {
	PipelineName        string `json:"pipelineName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
}

type getPipelineExecutionOutput struct {
	PipelineExecution map[string]any `json:"pipelineExecution"`
}

func (h *Handler) handleGetPipelineExecution(
	ctx context.Context,
	in *getPipelineExecutionInput,
) (*getPipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	exec, err := h.Backend.GetPipelineExecution(ctx, in.PipelineName, in.PipelineExecutionID)
	if err != nil {
		return nil, err
	}

	return &getPipelineExecutionOutput{
		PipelineExecution: map[string]any{
			"pipelineName":        exec.PipelineName,
			"pipelineExecutionId": exec.PipelineExecutionID,
			"status":              exec.Status,
			"pipelineVersion":     exec.PipelineVersion,
		},
	}, nil
}

type stopPipelineExecutionInput struct {
	PipelineName        string `json:"pipelineName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	Reason              string `json:"reason"`
	Abandon             bool   `json:"abandon"`
}

func (h *Handler) handleStopPipelineExecution(
	ctx context.Context,
	in *stopPipelineExecutionInput,
) (*pipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	exec, err := h.Backend.StopPipelineExecution(ctx, in.PipelineName, in.PipelineExecutionID, in.Reason)
	if err != nil {
		return nil, err
	}

	return &pipelineExecutionOutput{PipelineExecutionID: exec.PipelineExecutionID}, nil
}

type listPipelineExecutionsInput struct {
	PipelineName string `json:"pipelineName"`
	NextToken    string `json:"nextToken"`
	MaxResults   int32  `json:"maxResults"`
}

type listPipelineExecutionsOutput struct {
	NextToken                  string           `json:"nextToken,omitempty"`
	PipelineExecutionSummaries []map[string]any `json:"pipelineExecutionSummaries"`
}

func (h *Handler) handleListPipelineExecutions(
	ctx context.Context,
	in *listPipelineExecutionsInput,
) (*listPipelineExecutionsOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	execs, err := h.Backend.ListPipelineExecutions(ctx, in.PipelineName)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, len(execs))
	for i, e := range execs {
		items[i] = map[string]any{
			"pipelineExecutionId": e.PipelineExecutionID,
			"status":              e.Status,
			"pipelineVersion":     e.PipelineVersion,
			"trigger":             e.Trigger,
		}
	}

	page, nextToken, err := cpPaginate(
		items,
		in.NextToken,
		in.MaxResults,
		maxResultsCapPipelineExecutions,
	)
	if err != nil {
		return nil, err
	}

	return &listPipelineExecutionsOutput{
		NextToken:                  nextToken,
		PipelineExecutionSummaries: page,
	}, nil
}

// --- Pipeline state ---

type getPipelineStateInput struct {
	Name string `json:"name"`
}

type getPipelineStateOutput struct {
	PipelineName    string           `json:"pipelineName"`
	StageStates     []map[string]any `json:"stageStates"`
	PipelineVersion int              `json:"pipelineVersion"`
}

func (h *Handler) handleGetPipelineState(
	ctx context.Context,
	in *getPipelineStateInput,
) (*getPipelineStateOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	states, err := h.Backend.GetPipelineState(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, len(states))
	for i, s := range states {
		item := map[string]any{
			"stageName":    s.StageName,
			"actionStates": s.ActionStates,
		}
		if s.InboundTransitionState != nil {
			item["inboundTransitionState"] = map[string]any{
				"disabled": s.InboundTransitionState.Disabled,
				"reason":   s.InboundTransitionState.Reason,
			}
		}

		if s.OutboundTransitionState != nil {
			item["outboundTransitionState"] = map[string]any{
				"disabled": s.OutboundTransitionState.Disabled,
				"reason":   s.OutboundTransitionState.Reason,
			}
		}

		items[i] = item
	}

	return &getPipelineStateOutput{
		PipelineName: in.Name,
		StageStates:  items,
	}, nil
}

// --- Stage execution control ---

type retryStageExecutionInput struct {
	PipelineName        string `json:"pipelineName"`
	StageName           string `json:"stageName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	RetryMode           string `json:"retryMode"`
}

func (h *Handler) handleRetryStageExecution(
	ctx context.Context,
	in *retryStageExecutionInput,
) (*pipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	exec, err := h.Backend.RetryStageExecution(ctx, in.PipelineName, in.StageName, in.PipelineExecutionID)
	if err != nil {
		return nil, err
	}

	return &pipelineExecutionOutput{PipelineExecutionID: exec.PipelineExecutionID}, nil
}

type rollbackStageInput struct {
	PipelineName              string `json:"pipelineName"`
	StageName                 string `json:"stageName"`
	TargetPipelineExecutionID string `json:"targetPipelineExecutionId"`
}

func (h *Handler) handleRollbackStage(
	ctx context.Context,
	in *rollbackStageInput,
) (*pipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	exec, err := h.Backend.RollbackStage(ctx, in.PipelineName, in.StageName, in.TargetPipelineExecutionID)
	if err != nil {
		return nil, err
	}

	return &pipelineExecutionOutput{PipelineExecutionID: exec.PipelineExecutionID}, nil
}

type overrideStageConditionInput struct {
	PipelineName        string `json:"pipelineName"`
	StageName           string `json:"stageName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	ConditionType       string `json:"conditionType"`
}

type emptyOut struct{}

func (h *Handler) handleOverrideStageCondition(
	ctx context.Context,
	in *overrideStageConditionInput,
) (*emptyOut, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if err := h.Backend.OverrideStageCondition(ctx, in.PipelineName, in.StageName, in.PipelineExecutionID); err != nil {
		return nil, err
	}

	return &emptyOut{}, nil
}

// --- Webhook operations ---

type listWebhooksInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

// webhookDefinitionView is the AWS-spec shape for a webhook definition inside ListWebhooks.
type webhookDefinitionView struct {
	AuthenticationConfiguration WebhookAuthConfig `json:"authenticationConfiguration,omitzero"`
	Name                        string            `json:"name"`
	TargetPipeline              string            `json:"targetPipeline"`
	TargetAction                string            `json:"targetAction"`
	Authentication              string            `json:"authentication,omitempty"`
	Filters                     []WebhookFilter   `json:"filters"`
}

// webhookListEntry is the AWS-spec outer envelope returned per webhook in ListWebhooks.
type webhookListEntry struct {
	URL                      string                `json:"url,omitempty"`
	ARN                      string                `json:"arn,omitempty"`
	LastTriggered            string                `json:"lastTriggered,omitempty"`
	Definition               webhookDefinitionView `json:"definition"`
	RegisteredWithThirdParty bool                  `json:"registeredWithThirdParty"`
}

type listWebhooksOutput struct {
	NextToken string             `json:"NextToken,omitempty"`
	Webhooks  []webhookListEntry `json:"webhooks"`
}

func (h *Handler) handleListWebhooks(
	ctx context.Context,
	in *listWebhooksInput,
) (*listWebhooksOutput, error) {
	webhooks := h.Backend.ListWebhooks(ctx)
	entries := make([]webhookListEntry, len(webhooks))

	for i, wh := range webhooks {
		filters := wh.Filters
		if filters == nil {
			filters = []WebhookFilter{}
		}

		entries[i] = webhookListEntry{
			Definition: webhookDefinitionView{
				Name:                        wh.Name,
				TargetPipeline:              wh.TargetPipeline,
				TargetAction:                wh.TargetAction,
				Authentication:              wh.Authentication,
				Filters:                     filters,
				AuthenticationConfiguration: wh.AuthenticationConfiguration,
			},
			URL:                      wh.URL,
			ARN:                      wh.ARN,
			LastTriggered:            wh.LastTriggered,
			RegisteredWithThirdParty: wh.RegisteredWithThirdParty,
		}
	}

	page, nextToken, err := cpPaginate(entries, in.NextToken, in.MaxResults, maxResultsCapWebhooks)
	if err != nil {
		return nil, err
	}

	return &listWebhooksOutput{NextToken: nextToken, Webhooks: page}, nil
}

type putWebhookInput struct {
	Webhook struct {
		AuthenticationConfiguration WebhookAuthConfig `json:"authenticationConfiguration,omitzero"`
		Name                        string            `json:"name"`
		TargetPipeline              string            `json:"targetPipeline"`
		TargetAction                string            `json:"targetAction"`
		Authentication              string            `json:"authentication,omitempty"`
		Filters                     []WebhookFilter   `json:"filters,omitempty"`
	} `json:"webhook"`
	Tags []Tag `json:"tags"`
}

type putWebhookOutput struct {
	Webhook webhookListEntry `json:"webhook"`
}

func (h *Handler) handlePutWebhook(
	ctx context.Context,
	in *putWebhookInput,
) (*putWebhookOutput, error) {
	if in.Webhook.Name == "" {
		return nil, fmt.Errorf("%w: webhook name is required", errInvalidRequest)
	}

	if in.Webhook.Authentication != "" && !validWebhookAuth(in.Webhook.Authentication) {
		return nil, fmt.Errorf("%w: invalid authentication %q; must be %s, %s, or %s",
			ErrValidation, in.Webhook.Authentication,
			WebhookAuthGitHubHMAC, WebhookAuthIP, WebhookAuthUnauthenticated)
	}

	wh, err := h.Backend.PutWebhook(ctx, &Webhook{
		Name:                        in.Webhook.Name,
		TargetPipeline:              in.Webhook.TargetPipeline,
		TargetAction:                in.Webhook.TargetAction,
		Authentication:              in.Webhook.Authentication,
		Filters:                     in.Webhook.Filters,
		AuthenticationConfiguration: in.Webhook.AuthenticationConfiguration,
	})
	if err != nil {
		return nil, err
	}

	whFilters := wh.Filters
	if whFilters == nil {
		whFilters = []WebhookFilter{}
	}

	return &putWebhookOutput{
		Webhook: webhookListEntry{
			Definition: webhookDefinitionView{
				Name:                        wh.Name,
				TargetPipeline:              wh.TargetPipeline,
				TargetAction:                wh.TargetAction,
				Authentication:              wh.Authentication,
				Filters:                     whFilters,
				AuthenticationConfiguration: wh.AuthenticationConfiguration,
			},
			URL:                      wh.URL,
			ARN:                      wh.ARN,
			RegisteredWithThirdParty: wh.RegisteredWithThirdParty,
		},
	}, nil
}

type registerWebhookInput struct {
	WebhookName string `json:"webhookName"`
}

func (h *Handler) handleRegisterWebhookWithThirdParty(
	ctx context.Context,
	in *registerWebhookInput,
) (*emptyOut, error) {
	if err := h.Backend.RegisterWebhookWithThirdParty(ctx, in.WebhookName); err != nil {
		return nil, err
	}

	return &emptyOut{}, nil
}

// --- Job polling/result handlers ---

type pollForJobsInput struct {
	ActionTypeID struct {
		Category string `json:"category"`
		Owner    string `json:"owner"`
		Provider string `json:"provider"`
		Version  string `json:"version"`
	} `json:"actionTypeId"`
	MaxBatchSize int32 `json:"maxBatchSize"`
}

type pollForJobsOutput struct {
	Jobs []map[string]any `json:"jobs"`
}

func (h *Handler) handlePollForJobs(
	ctx context.Context,
	in *pollForJobsInput,
) (*pollForJobsOutput, error) {
	jobs, err := h.Backend.PollForJobs(
		ctx, in.ActionTypeID.Category, in.ActionTypeID.Owner,
		in.ActionTypeID.Provider, in.ActionTypeID.Version,
	)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, len(jobs))
	for i, j := range jobs {
		items[i] = map[string]any{keyJobID: j.ID, keyNonce: j.Nonce}
	}

	return &pollForJobsOutput{Jobs: items}, nil
}

type pollForThirdPartyJobsInput struct {
	ActionTypeID struct {
		Category string `json:"category"`
		Owner    string `json:"owner"`
		Provider string `json:"provider"`
		Version  string `json:"version"`
	} `json:"actionTypeId"`
	MaxBatchSize int32 `json:"maxBatchSize"`
}

type pollForThirdPartyJobsOutput struct {
	Jobs []map[string]any `json:"jobs"`
}

func (h *Handler) handlePollForThirdPartyJobs(
	ctx context.Context,
	in *pollForThirdPartyJobsInput,
) (*pollForThirdPartyJobsOutput, error) {
	jobs, err := h.Backend.PollForThirdPartyJobs(
		ctx, in.ActionTypeID.Category, in.ActionTypeID.Provider, in.ActionTypeID.Version,
	)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, len(jobs))
	for i, j := range jobs {
		items[i] = map[string]any{keyJobID: j.ID, keyNonce: j.Nonce}
	}

	return &pollForThirdPartyJobsOutput{Jobs: items}, nil
}

type getThirdPartyJobDetailsInput struct {
	JobID       string `json:"jobId"`
	ClientToken string `json:"clientToken"`
}

type getThirdPartyJobDetailsOutput struct {
	JobDetails map[string]any `json:"jobDetails"`
}

func (h *Handler) handleGetThirdPartyJobDetails(
	ctx context.Context,
	in *getThirdPartyJobDetailsInput,
) (*getThirdPartyJobDetailsOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	job, err := h.Backend.GetThirdPartyJobDetails(ctx, in.JobID, in.ClientToken)
	if err != nil {
		return nil, err
	}

	return &getThirdPartyJobDetailsOutput{
		JobDetails: map[string]any{keyJobID: job.ID, keyNonce: job.Nonce},
	}, nil
}

type putJobSuccessResultInput struct {
	JobID string `json:"jobId"`
}

func (h *Handler) handlePutJobSuccessResult(
	ctx context.Context,
	in *putJobSuccessResultInput,
) (*emptyOut, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	return &emptyOut{}, h.Backend.PutJobSuccessResult(ctx, in.JobID)
}

type putJobFailureResultInput struct {
	JobID          string `json:"jobId"`
	FailureDetails struct {
		Message string `json:"message"`
	} `json:"failureDetails"`
}

func (h *Handler) handlePutJobFailureResult(
	ctx context.Context,
	in *putJobFailureResultInput,
) (*emptyOut, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	return &emptyOut{}, h.Backend.PutJobFailureResult(ctx, in.JobID, in.FailureDetails.Message)
}

type putThirdPartyJobSuccessResultInput struct {
	JobID       string `json:"jobId"`
	ClientToken string `json:"clientToken"`
}

func (h *Handler) handlePutThirdPartyJobSuccessResult(
	ctx context.Context,
	in *putThirdPartyJobSuccessResultInput,
) (*emptyOut, error) {
	return &emptyOut{}, h.Backend.PutThirdPartyJobSuccessResult(ctx, in.JobID, in.ClientToken)
}

type putThirdPartyJobFailureResultInput struct {
	JobID          string `json:"jobId"`
	ClientToken    string `json:"clientToken"`
	FailureDetails struct {
		Message string `json:"message"`
	} `json:"failureDetails"`
}

func (h *Handler) handlePutThirdPartyJobFailureResult(
	ctx context.Context,
	in *putThirdPartyJobFailureResultInput,
) (*emptyOut, error) {
	return &emptyOut{}, h.Backend.PutThirdPartyJobFailureResult(
		ctx,
		in.JobID,
		in.ClientToken,
		in.FailureDetails.Message,
	)
}

// --- Action operations ---

type putActionRevisionInput struct {
	PipelineName   string `json:"pipelineName"`
	StageName      string `json:"stageName"`
	ActionName     string `json:"actionName"`
	ActionRevision struct {
		RevisionID       string `json:"revisionId"`
		RevisionChangeID string `json:"revisionChangeId"`
	} `json:"actionRevision"`
}

type putActionRevisionOutput struct {
	PipelineExecutionID string `json:"pipelineExecutionId"`
	NewRevision         bool   `json:"newRevision"`
}

func (h *Handler) handlePutActionRevision(
	ctx context.Context,
	in *putActionRevisionInput,
) (*putActionRevisionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if err := h.Backend.PutActionRevision(ctx, in.PipelineName, in.StageName, in.ActionName); err != nil {
		return nil, err
	}

	return &putActionRevisionOutput{NewRevision: true}, nil
}

type putApprovalResultInput struct {
	PipelineName   string `json:"pipelineName"`
	StageName      string `json:"stageName"`
	ActionName     string `json:"actionName"`
	ApprovalResult struct {
		Status  string `json:"status"`
		Summary string `json:"summary"`
	} `json:"approvalResult"`
}

type putApprovalResultOutput struct {
	ApprovedAt string `json:"approvedAt"`
}

func (h *Handler) handlePutApprovalResult(
	ctx context.Context,
	in *putApprovalResultInput,
) (*putApprovalResultOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if err := h.Backend.PutApprovalResult(
		ctx, in.PipelineName, in.StageName, in.ActionName,
		in.ApprovalResult.Status, in.ApprovalResult.Summary,
	); err != nil {
		return nil, err
	}

	return &putApprovalResultOutput{}, nil
}

type actionExecutionFilter struct {
	PipelineExecutionID string `json:"pipelineExecutionId"`
}

type listActionExecutionsInput struct {
	Filter       *actionExecutionFilter `json:"filter"`
	PipelineName string                 `json:"pipelineName"`
	NextToken    string                 `json:"nextToken"`
	MaxResults   int32                  `json:"maxResults"`
}

type listActionExecutionsOutput struct {
	NextToken              string           `json:"nextToken,omitempty"`
	ActionExecutionDetails []map[string]any `json:"actionExecutionDetails"`
}

func (h *Handler) handleListActionExecutions(
	ctx context.Context,
	in *listActionExecutionsInput,
) (*listActionExecutionsOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	var execFilter string
	if in.Filter != nil {
		execFilter = in.Filter.PipelineExecutionID
	}

	items, err := h.Backend.ListActionExecutions(ctx, in.PipelineName, execFilter)
	if err != nil {
		return nil, err
	}

	page, nextToken, pErr := cpPaginate(
		items,
		in.NextToken,
		in.MaxResults,
		maxResultsCapActionExecutions,
	)
	if pErr != nil {
		return nil, pErr
	}

	return &listActionExecutionsOutput{NextToken: nextToken, ActionExecutionDetails: page}, nil
}

type listActionTypesInput struct {
	ActionOwnerFilter string `json:"actionOwnerFilter"`
	RegionFilter      string `json:"regionFilter"`
	NextToken         string `json:"nextToken"`
}

type listActionTypesOutput struct {
	NextToken   string           `json:"nextToken,omitempty"`
	ActionTypes []map[string]any `json:"actionTypes"`
}

func (h *Handler) handleListActionTypes(
	ctx context.Context,
	in *listActionTypesInput,
) (*listActionTypesOutput, error) {
	types := h.Backend.ListActionTypes(ctx)
	items := make([]map[string]any, 0, len(types))

	for _, at := range types {
		owner := at.Owner
		if owner == "" {
			owner = keyOwnerCustom
		}

		if in.ActionOwnerFilter != "" && owner != in.ActionOwnerFilter {
			continue
		}

		item := map[string]any{
			"id": map[string]any{
				"category": at.Category,
				"owner":    owner,
				"provider": at.Provider,
				"version":  at.Version,
			},
			"inputArtifactDetails": map[string]any{
				"minimumCount": at.InputArtifactDetails.MinimumCount,
				"maximumCount": at.InputArtifactDetails.MaximumCount,
			},
			"outputArtifactDetails": map[string]any{
				"minimumCount": at.OutputArtifactDetails.MinimumCount,
				"maximumCount": at.OutputArtifactDetails.MaximumCount,
			},
		}

		if at.Settings != nil {
			item["settings"] = at.Settings
		}

		if len(at.ConfigurationProperties) > 0 {
			item["actionConfigurationProperties"] = at.ConfigurationProperties
		}

		items = append(items, item)
	}

	page, nextToken, err := cpPaginate(items, in.NextToken, 0, maxResultsCapActionTypes)
	if err != nil {
		return nil, err
	}

	return &listActionTypesOutput{NextToken: nextToken, ActionTypes: page}, nil
}

type updateActionTypeInputBody struct {
	Settings                *ActionTypeSettings           `json:"settings,omitempty"`
	ID                      ActionTypeID                  `json:"id"`
	ConfigurationProperties []ActionConfigurationProperty `json:"actionConfigurationProperties,omitempty"`
	InputArtifactDetails    ArtifactDetails               `json:"inputArtifactDetails"`
	OutputArtifactDetails   ArtifactDetails               `json:"outputArtifactDetails"`
}

type updateActionTypeInput struct {
	ActionType updateActionTypeInputBody `json:"actionType"`
}

func (h *Handler) handleUpdateActionType(
	ctx context.Context,
	in *updateActionTypeInput,
) (*emptyOut, error) {
	id := in.ActionType.ID

	if id.Category == "" {
		return nil, fmt.Errorf("%w: actionType.id.category is required", errInvalidRequest)
	}

	if id.Provider == "" {
		return nil, fmt.Errorf("%w: actionType.id.provider is required", errInvalidRequest)
	}

	if id.Version == "" {
		return nil, fmt.Errorf("%w: actionType.id.version is required", errInvalidRequest)
	}

	owner := id.Owner
	if owner == "" {
		owner = keyOwnerCustom
	}

	cat := &CustomActionType{
		Category:                id.Category,
		Owner:                   owner,
		Provider:                id.Provider,
		Version:                 id.Version,
		Settings:                in.ActionType.Settings,
		ConfigurationProperties: in.ActionType.ConfigurationProperties,
		InputArtifactDetails:    in.ActionType.InputArtifactDetails,
		OutputArtifactDetails:   in.ActionType.OutputArtifactDetails,
	}

	if err := h.Backend.UpdateActionType(ctx, cat); err != nil {
		return nil, err
	}

	return &emptyOut{}, nil
}

// --- Rule operations ---

type listRuleExecutionsInput struct {
	PipelineName string `json:"pipelineName"`
	NextToken    string `json:"nextToken"`
	MaxResults   int32  `json:"maxResults"`
}

type listRuleExecutionsOutput struct {
	NextToken            string           `json:"nextToken,omitempty"`
	RuleExecutionDetails []map[string]any `json:"ruleExecutionDetails"`
}

func (h *Handler) handleListRuleExecutions(
	ctx context.Context,
	in *listRuleExecutionsInput,
) (*listRuleExecutionsOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	items, err := h.Backend.ListRuleExecutions(ctx, in.PipelineName)
	if err != nil {
		return nil, err
	}

	page, nextToken, pErr := cpPaginate(
		items,
		in.NextToken,
		in.MaxResults,
		maxResultsCapRuleExecutions,
	)
	if pErr != nil {
		return nil, pErr
	}

	return &listRuleExecutionsOutput{NextToken: nextToken, RuleExecutionDetails: page}, nil
}

type listRuleTypesInput struct {
	RegionFilter string `json:"regionFilter"`
}

type listRuleTypesOutput struct {
	RuleTypes []map[string]any `json:"ruleTypes"`
}

func (h *Handler) handleListRuleTypes(
	_ context.Context,
	_ *listRuleTypesInput,
) (*listRuleTypesOutput, error) {
	return &listRuleTypesOutput{RuleTypes: h.Backend.ListRuleTypes()}, nil
}

type listDeployActionExecutionTargetsInput struct {
	PipelineName      string `json:"pipelineName"`
	ActionExecutionID string `json:"actionExecutionId"`
	NextToken         string `json:"nextToken"`
	MaxResults        int32  `json:"maxResults"`
}

type listDeployActionExecutionTargetsOutput struct {
	Targets []map[string]any `json:"targets"`
}

func (h *Handler) handleListDeployActionExecutionTargets(
	ctx context.Context,
	in *listDeployActionExecutionTargetsInput,
) (*listDeployActionExecutionTargetsOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	items, err := h.Backend.ListDeployActionExecutionTargets(ctx, in.PipelineName, in.ActionExecutionID)
	if err != nil {
		return nil, err
	}

	return &listDeployActionExecutionTargetsOutput{Targets: items}, nil
}
