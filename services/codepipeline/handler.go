package codepipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codepipelineTargetPrefix = "CodePipeline_20150709."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for CodePipeline operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodePipeline handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodePipeline", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
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
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
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
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "PipelineNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrActionTypeNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ActionTypeNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrJobNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "JobNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrWebhookNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "WebhookNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidStructureException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errUnknownAction):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidActionException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ValidationException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InternalFailure",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusInternalServerError, payload)
	}
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
	_ context.Context,
	in *createPipelineInput,
) (*createPipelineOutput, error) {
	if in.Pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is required", errInvalidRequest)
	}

	if in.Pipeline.Name == "" {
		return nil, fmt.Errorf("%w: pipeline name is required", errInvalidRequest)
	}

	tagMap := tagsToMap(in.Tags)

	p, err := h.Backend.CreatePipeline(*in.Pipeline, tagMap)
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
	_ context.Context,
	in *getPipelineInput,
) (*getPipelineOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	p, err := h.Backend.GetPipeline(in.Name)
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
	_ context.Context,
	in *updatePipelineInput,
) (*updatePipelineOutput, error) {
	if in.Pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is required", errInvalidRequest)
	}

	if in.Pipeline.Name == "" {
		return nil, fmt.Errorf("%w: pipeline name is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdatePipeline(*in.Pipeline)
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
	_ context.Context,
	in *deletePipelineInput,
) (*deletePipelineOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePipeline(in.Name); err != nil {
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
	_ context.Context,
	_ *listPipelinesInput,
) (*listPipelinesOutput, error) {
	summaries := h.Backend.ListPipelines()

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
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

type tagResourceInput struct {
	ResourceArn string `json:"resourceArn"`
	Tags        []Tag  `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
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
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
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

// --- AcknowledgeJob ---

type acknowledgeJobInput struct {
	JobID string `json:"jobId"`
	Nonce string `json:"nonce"`
}

type acknowledgeJobOutput struct {
	Status string `json:"status"`
}

func (h *Handler) handleAcknowledgeJob(
	_ context.Context,
	in *acknowledgeJobInput,
) (*acknowledgeJobOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	if in.Nonce == "" {
		return nil, fmt.Errorf("%w: nonce is required", errInvalidRequest)
	}

	status, err := h.Backend.AcknowledgeJob(in.JobID, in.Nonce)
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
	_ context.Context,
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

	status, err := h.Backend.AcknowledgeThirdPartyJob(in.JobID, in.Nonce, in.ClientToken)
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
	ActionConfigurationProperties []ActionConfigurationProperty `json:"actionConfigurationProperties,omitempty"`
	InputArtifactDetails          ArtifactDetails               `json:"inputArtifactDetails"`
	OutputArtifactDetails         ArtifactDetails               `json:"outputArtifactDetails"`
}

type createCustomActionTypeOutput struct {
	Tags       []Tag                    `json:"tags,omitempty"`
	ActionType customActionTypeResponse `json:"actionType"`
}

func (h *Handler) handleCreateCustomActionType(
	_ context.Context,
	in *createCustomActionTypeInput,
) (*createCustomActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
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

	created, err := h.Backend.CreateCustomActionType(cat)
	if err != nil {
		return nil, err
	}

	return &createCustomActionTypeOutput{
		ActionType: customActionTypeResponse{
			ID: ActionTypeID{
				Category: created.Category,
				Owner:    "Custom",
				Provider: created.Provider,
				Version:  created.Version,
			},
			InputArtifactDetails:          created.InputArtifactDetails,
			OutputArtifactDetails:         created.OutputArtifactDetails,
			Settings:                      created.Settings,
			ActionConfigurationProperties: created.ConfigurationProperties,
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
	_ context.Context,
	in *deleteCustomActionTypeInput,
) (*deleteCustomActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
	}

	if in.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", errInvalidRequest)
	}

	if in.Version == "" {
		return nil, fmt.Errorf("%w: version is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteCustomActionType(in.Category, in.Provider, in.Version); err != nil {
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
	_ context.Context,
	in *getActionTypeInput,
) (*getActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
	}

	if in.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", errInvalidRequest)
	}

	if in.Version == "" {
		return nil, fmt.Errorf("%w: version is required", errInvalidRequest)
	}

	cat, err := h.Backend.GetActionType(in.Category, in.Owner, in.Provider, in.Version)
	if err != nil {
		return nil, err
	}

	return &getActionTypeOutput{
		ActionType: customActionTypeResponse{
			ID: ActionTypeID{
				Category: cat.Category,
				Owner:    "Custom",
				Provider: cat.Provider,
				Version:  cat.Version,
			},
			InputArtifactDetails:          cat.InputArtifactDetails,
			OutputArtifactDetails:         cat.OutputArtifactDetails,
			Settings:                      cat.Settings,
			ActionConfigurationProperties: cat.ConfigurationProperties,
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
	_ context.Context,
	in *getJobDetailsInput,
) (*getJobDetailsOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	job, err := h.Backend.GetJobDetails(in.JobID)
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
	_ context.Context,
	in *deleteWebhookInput,
) (*deleteWebhookOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebhook(in.Name); err != nil {
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
	_ context.Context,
	in *deregisterWebhookWithThirdPartyInput,
) (*deregisterWebhookWithThirdPartyOutput, error) {
	if err := h.Backend.DeregisterWebhookWithThirdParty(in.WebhookName); err != nil {
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
	_ context.Context,
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

	if in.Reason == "" {
		return nil, fmt.Errorf("%w: reason is required", errInvalidRequest)
	}

	if err := h.Backend.DisableStageTransition(
		in.PipelineName, in.StageName, in.TransitionType, in.Reason,
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
	_ context.Context,
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

	if err := h.Backend.EnableStageTransition(in.PipelineName, in.StageName, in.TransitionType); err != nil {
		return nil, err
	}

	return &enableStageTransitionOutput{}, nil
}
