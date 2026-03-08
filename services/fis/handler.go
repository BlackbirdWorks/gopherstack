package fis

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// minSegmentsForID is the minimum number of path segments required for a resource ID.
	minSegmentsForID = 2
	// maxPathSegments limits how many segments pathSegments returns for tag paths.
	maxPathSegments = 3
)

const (
	// pathExperimentTemplates is the root path for experiment templates.
	pathExperimentTemplates = "experimentTemplates"
	// pathExperiments is the root path for experiments.
	pathExperiments = "experiments"
	// pathActions is the root path for FIS actions.
	pathActions = "actions"
	// pathTargetResourceTypes is the root path for target resource types.
	pathTargetResourceTypes = "targetResourceTypes"
	// pathTags is the root path for resource tags.
	pathTags = "tags"
)

// Handler is the Echo HTTP handler for the FIS REST API.
type Handler struct {
	Backend       StorageBackend
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new FIS handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// SetFaultStore injects the chaos FaultStore into the backend for inject-api-* actions.
func (h *Handler) SetFaultStore(store *chaos.FaultStore) {
	h.Backend.SetFaultStore(store)
}

// SetActionProviders registers external FIS action providers with the backend.
func (h *Handler) SetActionProviders(providers []service.FISActionProvider) {
	h.Backend.SetActionProviders(providers)
}

// Name returns the service name.
func (h *Handler) Name() string { return "FIS" }

// GetSupportedOperations returns the list of supported FIS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateExperimentTemplate",
		"GetExperimentTemplate",
		"UpdateExperimentTemplate",
		"DeleteExperimentTemplate",
		"ListExperimentTemplates",
		"StartExperiment",
		"GetExperiment",
		"StopExperiment",
		"ListExperiments",
		"GetAction",
		"ListActions",
		"GetTargetResourceType",
		"ListTargetResourceTypes",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "fis" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this FIS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches FIS REST API requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		for _, prefix := range []string{
			"/" + pathExperimentTemplates,
			"/" + pathExperiments,
			"/" + pathActions,
			"/" + pathTargetResourceTypes,
		} {
			if strings.HasPrefix(path, prefix) {
				return true
			}
		}

		// Match /tags/{arn} — requires exactly three segments.
		segs := pathSegments(path)
		if len(segs) >= minSegmentsForID && segs[0] == pathTags {
			return true
		}

		return false
	}
}

// MatchPriority returns the routing priority for the FIS handler.
// FIS uses path-based routing and is inserted between PriorityPathVersioned (85) and PriorityFormRDS (84).
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the FIS operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseFISPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource ID from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := pathSegments(c.Request().URL.Path)
	if len(segs) >= minSegmentsForID {
		return segs[1]
	}

	return ""
}

// Handler returns the Echo handler function for FIS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		op, id := parseFISPath(c.Request().Method, c.Request().URL.Path)
		if op == "" {
			return h.writeError(c, http.StatusNotFound, "not found", "")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "fis: failed to read request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "failed to read request body", "")
		}

		log.DebugContext(ctx, "fis request", "op", op, "id", id)

		return h.dispatch(ctx, c, op, id, body)
	}
}

// dispatch routes a parsed FIS operation to the appropriate backend call.
//
//nolint:cyclop // dispatch table has necessary branches for each operation
func (h *Handler) dispatch(ctx context.Context, c *echo.Context, op, id string, body []byte) error {
	switch op {
	case "CreateExperimentTemplate":
		return h.handleCreateExperimentTemplate(ctx, c, body)
	case "GetExperimentTemplate":
		return h.handleGetExperimentTemplate(c, id)
	case "UpdateExperimentTemplate":
		return h.handleUpdateExperimentTemplate(c, id, body)
	case "DeleteExperimentTemplate":
		return h.handleDeleteExperimentTemplate(c, id)
	case "ListExperimentTemplates":
		return h.handleListExperimentTemplates(c)
	case "StartExperiment":
		return h.handleStartExperiment(ctx, c, body)
	case "GetExperiment":
		return h.handleGetExperiment(c, id)
	case "StopExperiment":
		return h.handleStopExperiment(c, id)
	case "ListExperiments":
		return h.handleListExperiments(c)
	case "GetAction":
		return h.handleGetAction(c, id)
	case "ListActions":
		return h.handleListActions(c)
	case "GetTargetResourceType":
		rt, _ := url.PathUnescape(id)

		return h.handleGetTargetResourceType(c, rt)
	case "ListTargetResourceTypes":
		return h.handleListTargetResourceTypes(c)
	case "TagResource":
		return h.handleTagResource(c, id, body)
	case "UntagResource":
		return h.handleUntagResource(c, id, c.Request().URL.Query())
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, id)
	}

	return h.writeError(c, http.StatusNotFound, "unknown operation: "+op, "")
}

// ----------------------------------------
// ExperimentTemplate handlers
// ----------------------------------------

func (h *Handler) handleCreateExperimentTemplate(_ context.Context, c *echo.Context, body []byte) error {
	var input createExperimentTemplateRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), "")
	}

	tpl, err := h.Backend.CreateExperimentTemplate(&input, h.AccountID, h.DefaultRegion)
	if err != nil {
		return h.writeBackendError(c, err, "")
	}

	return c.JSON(http.StatusCreated, experimentTemplateResponseDTO{
		ExperimentTemplate: toTemplateDTO(tpl),
	})
}

func (h *Handler) handleGetExperimentTemplate(c *echo.Context, id string) error {
	tpl, err := h.Backend.GetExperimentTemplate(id)
	if err != nil {
		if errors.Is(err, ErrTemplateNotFound) {
			return h.writeError(c, http.StatusNotFound, err.Error(), id)
		}

		return h.writeError(c, http.StatusInternalServerError, err.Error(), id)
	}

	return c.JSON(http.StatusOK, experimentTemplateResponseDTO{
		ExperimentTemplate: toTemplateDTO(tpl),
	})
}

func (h *Handler) handleUpdateExperimentTemplate(c *echo.Context, id string, body []byte) error {
	var input updateExperimentTemplateRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), id)
	}

	tpl, err := h.Backend.UpdateExperimentTemplate(id, &input)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, experimentTemplateResponseDTO{
		ExperimentTemplate: toTemplateDTO(tpl),
	})
}

func (h *Handler) handleDeleteExperimentTemplate(c *echo.Context, id string) error {
	if err := h.Backend.DeleteExperimentTemplate(id); err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListExperimentTemplates(c *echo.Context) error {
	templates, err := h.Backend.ListExperimentTemplates()
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, err.Error(), "")
	}

	dtos := make([]experimentTemplateDTO, len(templates))
	for i, t := range templates {
		dtos[i] = toTemplateDTO(t)
	}

	return c.JSON(http.StatusOK, listExperimentTemplatesResponseDTO{
		ExperimentTemplates: dtos,
	})
}

// ----------------------------------------
// Experiment handlers
// ----------------------------------------

func (h *Handler) handleStartExperiment(ctx context.Context, c *echo.Context, body []byte) error {
	var input startExperimentRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), "")
	}

	exp, err := h.Backend.StartExperiment(ctx, &input, h.AccountID, h.DefaultRegion)
	if err != nil {
		if errors.Is(err, ErrTemplateNotFound) {
			return h.writeError(c, http.StatusNotFound, err.Error(), input.ExperimentTemplateID)
		}

		return h.writeError(c, http.StatusInternalServerError, err.Error(), "")
	}

	return c.JSON(http.StatusCreated, experimentResponseDTO{
		Experiment: toExperimentDTO(exp),
	})
}

func (h *Handler) handleGetExperiment(c *echo.Context, id string) error {
	exp, err := h.Backend.GetExperiment(id)
	if err != nil {
		if errors.Is(err, ErrExperimentNotFound) {
			return h.writeError(c, http.StatusNotFound, err.Error(), id)
		}

		return h.writeError(c, http.StatusInternalServerError, err.Error(), id)
	}

	return c.JSON(http.StatusOK, experimentResponseDTO{
		Experiment: toExperimentDTO(exp),
	})
}

func (h *Handler) handleStopExperiment(c *echo.Context, id string) error {
	exp, err := h.Backend.StopExperiment(id)
	if err != nil {
		if errors.Is(err, ErrExperimentNotFound) {
			return h.writeError(c, http.StatusNotFound, err.Error(), id)
		}

		if errors.Is(err, ErrExperimentNotRunning) {
			return h.writeError(c, http.StatusConflict, err.Error(), id)
		}

		return h.writeError(c, http.StatusInternalServerError, err.Error(), id)
	}

	return c.JSON(http.StatusOK, experimentResponseDTO{
		Experiment: toExperimentDTO(exp),
	})
}

func (h *Handler) handleListExperiments(c *echo.Context) error {
	experiments, err := h.Backend.ListExperiments()
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, err.Error(), "")
	}

	dtos := make([]experimentDTO, len(experiments))
	for i, e := range experiments {
		dtos[i] = toExperimentDTO(e)
	}

	return c.JSON(http.StatusOK, listExperimentsResponseDTO{
		Experiments: dtos,
	})
}

// ----------------------------------------
// Action discovery handlers
// ----------------------------------------

func (h *Handler) handleGetAction(c *echo.Context, id string) error {
	action, err := h.Backend.GetAction(id)
	if err != nil {
		if errors.Is(err, ErrActionNotFound) {
			return h.writeError(c, http.StatusNotFound, err.Error(), id)
		}

		return h.writeError(c, http.StatusInternalServerError, err.Error(), id)
	}

	return c.JSON(http.StatusOK, actionResponseDTO{
		Action: toActionDTO(action),
	})
}

func (h *Handler) handleListActions(c *echo.Context) error {
	actions := h.Backend.ListActions()
	dtos := make([]actionDTO, len(actions))

	for i := range actions {
		dtos[i] = toActionDTO(&actions[i])
	}

	return c.JSON(http.StatusOK, listActionsResponseDTO{Actions: dtos})
}

func (h *Handler) handleGetTargetResourceType(c *echo.Context, resourceType string) error {
	rt, err := h.Backend.GetTargetResourceType(resourceType)
	if err != nil {
		if errors.Is(err, ErrTargetResourceTypeNotFound) {
			return h.writeError(c, http.StatusNotFound, err.Error(), resourceType)
		}

		return h.writeError(c, http.StatusInternalServerError, err.Error(), resourceType)
	}

	return c.JSON(http.StatusOK, targetResourceTypeResponseDTO{
		TargetResourceType: toTargetResourceTypeDTO(rt),
	})
}

func (h *Handler) handleListTargetResourceTypes(c *echo.Context) error {
	types := h.Backend.ListTargetResourceTypes()
	dtos := make([]targetResourceTypeDTO, len(types))

	for i := range types {
		dtos[i] = toTargetResourceTypeDTO(&types[i])
	}

	return c.JSON(http.StatusOK, listTargetResourceTypesResponseDTO{TargetResourceTypes: dtos})
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleTagResource(c *echo.Context, arnStr string, body []byte) error {
	var input struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), arnStr)
	}

	if err := h.Backend.TagResource(arnStr, input.Tags); err != nil {
		return h.writeBackendError(c, err, arnStr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, arnStr string, query url.Values) error {
	keys := query["tagKeys"]
	if err := h.Backend.UntagResource(arnStr, keys); err != nil {
		return h.writeBackendError(c, err, arnStr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, arnStr string) error {
	tags, err := h.Backend.ListTagsForResource(arnStr)
	if err != nil {
		return h.writeBackendError(c, err, arnStr)
	}

	return c.JSON(http.StatusOK, tagsResponseDTO{Tags: tags})
}

// ----------------------------------------
// Error helpers
// ----------------------------------------

func (h *Handler) writeError(c *echo.Context, status int, message, resourceID string) error {
	resp := errorResponseDTO{Message: message, ResourceID: resourceID}

	return c.JSON(status, resp)
}

func (h *Handler) writeBackendError(c *echo.Context, err error, id string) error {
	switch {
	case errors.Is(err, ErrTemplateNotFound):
		return h.writeError(c, http.StatusNotFound, err.Error(), id)
	case errors.Is(err, ErrExperimentNotFound):
		return h.writeError(c, http.StatusNotFound, err.Error(), id)
	case errors.Is(err, ErrExperimentNotRunning):
		return h.writeError(c, http.StatusConflict, err.Error(), id)
	case errors.Is(err, ErrResourceNotFound):
		return h.writeError(c, http.StatusNotFound, err.Error(), id)
	default:
		return h.writeError(c, http.StatusInternalServerError, err.Error(), id)
	}
}

// ----------------------------------------
// Path parsing
// ----------------------------------------

// parseFISPath maps an HTTP method + URL path to a FIS operation name and optional resource ID.
// Returns ("", "") when no pattern matches.
//
//nolint:cyclop,gocognit,gocyclo,funlen // routing table per HTTP method + resource
func parseFISPath(method, path string) (string, string) {
	segs := pathSegments(path)
	if len(segs) == 0 {
		return "", ""
	}

	root := segs[0]
	hasID := len(segs) >= minSegmentsForID

	switch root {
	case pathExperimentTemplates:
		switch {
		case method == http.MethodPost && !hasID:
			return "CreateExperimentTemplate", ""
		case method == http.MethodGet && !hasID:
			return "ListExperimentTemplates", ""
		case method == http.MethodGet && hasID:
			return "GetExperimentTemplate", segs[1]
		case method == http.MethodPatch && hasID:
			return "UpdateExperimentTemplate", segs[1]
		case method == http.MethodDelete && hasID:
			return "DeleteExperimentTemplate", segs[1]
		}

	case pathExperiments:
		switch {
		case method == http.MethodPost && !hasID:
			return "StartExperiment", ""
		case method == http.MethodGet && !hasID:
			return "ListExperiments", ""
		case method == http.MethodGet && hasID:
			return "GetExperiment", segs[1]
		case method == http.MethodDelete && hasID:
			return "StopExperiment", segs[1]
		}

	case pathActions:
		switch {
		case method == http.MethodGet && !hasID:
			return "ListActions", ""
		case method == http.MethodGet && hasID:
			return "GetAction", segs[1]
		}

	case pathTargetResourceTypes:
		switch {
		case method == http.MethodGet && !hasID:
			return "ListTargetResourceTypes", ""
		case method == http.MethodGet && hasID:
			// Resource type may be URL-encoded (e.g. aws%3Aec2%3Ainstance); the caller decodes.
			return "GetTargetResourceType", segs[1]
		}

	case pathTags:
		if hasID {
			arnStr := strings.Join(segs[1:], "/")
			switch method {
			case http.MethodGet:
				return "ListTagsForResource", arnStr
			case http.MethodPost:
				return "TagResource", arnStr
			case http.MethodDelete:
				return "UntagResource", arnStr
			}
		}
	}

	return "", ""
}

// pathSegments splits a URL path into non-empty segments.
func pathSegments(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}

	return strings.SplitN(trimmed, "/", maxPathSegments)
}

// ----------------------------------------
// DTO conversion helpers
// ----------------------------------------

func toTemplateDTO(tpl *ExperimentTemplate) experimentTemplateDTO {
	targets := make(map[string]experimentTemplateTargetDTO, len(tpl.Targets))
	for name, t := range tpl.Targets {
		filters := make([]experimentTemplateTargetFilterDTO, len(t.Filters))
		for i, f := range t.Filters {
			filters[i] = experimentTemplateTargetFilterDTO(f)
		}

		targets[name] = experimentTemplateTargetDTO{
			ResourceType:  t.ResourceType,
			SelectionMode: t.SelectionMode,
			ResourceArns:  t.ResourceArns,
			ResourceTags:  t.ResourceTags,
			Filters:       filters,
			Parameters:    t.Parameters,
		}
	}

	actions := make(map[string]experimentTemplateActionDTO, len(tpl.Actions))
	for name, a := range tpl.Actions {
		actions[name] = experimentTemplateActionDTO(a)
	}

	stopConditions := make([]experimentTemplateStopConditionDTO, len(tpl.StopConditions))
	for i, sc := range tpl.StopConditions {
		stopConditions[i] = experimentTemplateStopConditionDTO(sc)
	}

	dto := experimentTemplateDTO{
		ID:             tpl.ID,
		Arn:            tpl.Arn,
		Description:    tpl.Description,
		RoleArn:        tpl.RoleArn,
		Tags:           tpl.Tags,
		Targets:        targets,
		Actions:        actions,
		StopConditions: stopConditions,
		CreationTime:   toUnix(tpl.CreationTime),
		LastUpdateTime: toUnix(tpl.LastUpdateTime),
	}

	if tpl.LogConfiguration != nil {
		lc := &experimentTemplateLogConfigurationDTO{
			LogSchemaVersion: tpl.LogConfiguration.LogSchemaVersion,
		}

		if tpl.LogConfiguration.CloudWatchLogsConfiguration != nil {
			lc.CloudWatchLogsConfiguration = &experimentTemplateCloudWatchLogsConfigurationDTO{
				LogGroupArn: tpl.LogConfiguration.CloudWatchLogsConfiguration.LogGroupArn,
			}
		}

		if tpl.LogConfiguration.S3Configuration != nil {
			lc.S3Configuration = &experimentTemplateS3ConfigurationDTO{
				BucketName: tpl.LogConfiguration.S3Configuration.BucketName,
				Prefix:     tpl.LogConfiguration.S3Configuration.Prefix,
			}
		}

		dto.LogConfiguration = lc
	}

	if tpl.ExperimentOptions != nil {
		dto.ExperimentOptions = &experimentTemplateExperimentOptionsDTO{
			AccountTargeting:          tpl.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: tpl.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	return dto
}

func toExperimentDTO(exp *Experiment) experimentDTO {
	targets := make(map[string]experimentTargetDTO, len(exp.Targets))
	for name, t := range exp.Targets {
		targets[name] = experimentTargetDTO(t)
	}

	actions := make(map[string]experimentActionDTO, len(exp.Actions))
	for name, a := range exp.Actions {
		dto := experimentActionDTO{
			ActionID:   a.ActionID,
			Parameters: a.Parameters,
			Targets:    a.Targets,
			Status: &experimentActionStatusDTO{
				Status: a.Status.Status,
				Reason: a.Status.Reason,
			},
			StartTime: toUnixPtr(a.StartTime),
			EndTime:   toUnixPtr(a.EndTime),
		}

		actions[name] = dto
	}

	stopConditions := make([]experimentStopConditionDTO, len(exp.StopConditions))
	for i, sc := range exp.StopConditions {
		stopConditions[i] = experimentStopConditionDTO(sc)
	}

	dto := experimentDTO{
		ID:                   exp.ID,
		Arn:                  exp.Arn,
		ExperimentTemplateID: exp.ExperimentTemplateID,
		RoleArn:              exp.RoleArn,
		Status:               experimentStatusDTO{Status: exp.Status.Status, Reason: exp.Status.Reason},
		Targets:              targets,
		Actions:              actions,
		StopConditions:       stopConditions,
		Tags:                 exp.Tags,
		StartTime:            toUnix(exp.StartTime),
		EndTime:              toUnixPtr(exp.EndTime),
	}

	if exp.LogConfiguration != nil {
		lc := &experimentLogConfigurationDTO{
			LogSchemaVersion: exp.LogConfiguration.LogSchemaVersion,
		}

		if exp.LogConfiguration.CloudWatchLogsConfiguration != nil {
			lc.CloudWatchLogsConfiguration = &experimentCloudWatchLogsConfigurationDTO{
				LogGroupArn: exp.LogConfiguration.CloudWatchLogsConfiguration.LogGroupArn,
			}
		}

		if exp.LogConfiguration.S3Configuration != nil {
			lc.S3Configuration = &experimentS3ConfigurationDTO{
				BucketName: exp.LogConfiguration.S3Configuration.BucketName,
				Prefix:     exp.LogConfiguration.S3Configuration.Prefix,
			}
		}

		dto.LogConfiguration = lc
	}

	if exp.ExperimentOptions != nil {
		dto.ExperimentOptions = &experimentExperimentOptionsDTO{
			AccountTargeting:          exp.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: exp.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	return dto
}

func toActionDTO(a *ActionSummary) actionDTO {
	params := make(map[string]actionParamDTO, len(a.Parameters))
	for k, v := range a.Parameters {
		params[k] = actionParamDTO(v)
	}

	targets := make(map[string]actionTargetDTO, len(a.Targets))
	for k, v := range a.Targets {
		targets[k] = actionTargetDTO(v)
	}

	return actionDTO{
		ID:          a.ID,
		Arn:         a.Arn,
		Description: a.Description,
		Parameters:  params,
		Targets:     targets,
		Tags:        a.Tags,
	}
}

func toTargetResourceTypeDTO(rt *TargetResourceTypeSummary) targetResourceTypeDTO {
	params := make(map[string]targetRTParamDTO, len(rt.Parameters))
	for k, v := range rt.Parameters {
		params[k] = targetRTParamDTO(v)
	}

	return targetResourceTypeDTO{
		ResourceType: rt.ResourceType,
		Description:  rt.Description,
		Parameters:   params,
	}
}
