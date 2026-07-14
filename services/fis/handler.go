package fis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opCreateExperimentTemplate                  = "CreateExperimentTemplate"
	opCreateTargetAccountConfiguration          = "CreateTargetAccountConfiguration"
	opDeleteExperimentTemplate                  = "DeleteExperimentTemplate"
	opDeleteTargetAccountConfiguration          = "DeleteTargetAccountConfiguration"
	opGetAction                                 = "GetAction"
	opGetExperiment                             = "GetExperiment"
	opGetExperimentTargetAccountConfiguration   = "GetExperimentTargetAccountConfiguration"
	opGetExperimentTemplate                     = "GetExperimentTemplate"
	opGetSafetyLever                            = "GetSafetyLever"
	opGetTargetAccountConfiguration             = "GetTargetAccountConfiguration"
	opGetTargetResourceType                     = "GetTargetResourceType"
	opListActions                               = "ListActions"
	opListExperimentResolvedTargets             = "ListExperimentResolvedTargets"
	opListExperimentTargetAccountConfigurations = "ListExperimentTargetAccountConfigurations"
	opListExperimentTemplates                   = "ListExperimentTemplates"
	opListExperiments                           = "ListExperiments"
	opListTagsForResource                       = "ListTagsForResource"
	opListTargetAccountConfigurations           = "ListTargetAccountConfigurations"
	opListTargetResourceTypes                   = "ListTargetResourceTypes"
	opStartExperiment                           = "StartExperiment"
	opStopExperiment                            = "StopExperiment"
	opTagResource                               = "TagResource"
	opUntagResource                             = "UntagResource"
	opUpdateExperimentTemplate                  = "UpdateExperimentTemplate"
	opUpdateSafetyLeverState                    = "UpdateSafetyLeverState"
	opUpdateTargetAccountConfiguration          = "UpdateTargetAccountConfiguration"
)

const (
	// minSegmentsForID is the minimum number of path segments required for a resource ID.
	minSegmentsForID = 2
	// maxPathSegments limits how many segments pathSegments returns.
	// Five segments is enough to handle the deepest FIS paths:
	// /experimentTemplates/{id}/targetAccountConfigurations/{accountId}.
	maxPathSegments = 5
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
	// pathSafetyLevers is the root path for safety levers.
	pathSafetyLevers = "safetyLevers"
	// pathTargetAccountConfigurations is the sub-path for target account configurations.
	pathTargetAccountConfigurations = "targetAccountConfigurations"
	// subPathResolvedTargets is the sub-path segment for resolved targets.
	subPathResolvedTargets = "resolvedTargets"
	// subPathStop is the sub-path segment for stopping an experiment (AWS canonical).
	subPathStop = "stop"
)

// Handler is the Echo HTTP handler for the FIS REST API.
type Handler struct {
	Backend       StorageBackend
	janitor       *Janitor
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new FIS handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// WithJanitor attaches a background janitor to the handler.
// If the backend is not an *InMemoryBackend, this is a no-op.
// The optional taskTimeout bounds each sweep; 0 means no per-task timeout.
func (h *Handler) WithJanitor(
	interval, experimentTTL time.Duration,
	taskTimeout ...time.Duration,
) *Handler {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(mem, interval, experimentTTL)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}

		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Shutdown cancels all running experiment goroutines to prevent resource leaks.
// It satisfies service.Shutdowner.
func (h *Handler) Shutdown(_ context.Context) {
	type stopper interface{ StopAllExperiments() }

	if s, ok := h.Backend.(stopper); ok {
		s.StopAllExperiments()
	}
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
		opCreateExperimentTemplate,
		opGetExperimentTemplate,
		opUpdateExperimentTemplate,
		opDeleteExperimentTemplate,
		opListExperimentTemplates,
		opStartExperiment,
		opGetExperiment,
		opStopExperiment,
		opListExperiments,
		opListExperimentResolvedTargets,
		opGetAction,
		opListActions,
		opGetTargetResourceType,
		opListTargetResourceTypes,
		opGetSafetyLever,
		opUpdateSafetyLeverState,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		opCreateTargetAccountConfiguration,
		opDeleteTargetAccountConfiguration,
		opGetExperimentTargetAccountConfiguration,
		opGetTargetAccountConfiguration,
		opListExperimentTargetAccountConfigurations,
		opListTargetAccountConfigurations,
		opUpdateTargetAccountConfiguration,
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
			"/" + pathSafetyLevers,
		} {
			if strings.HasPrefix(path, prefix) {
				return true
			}
		}

		// Match /tags/{arn} — only for FIS-owned resources (arn:aws:fis:...).
		segs := pathSegments(path)
		if len(segs) >= minSegmentsForID && segs[0] == pathTags {
			return strings.Contains(segs[1], ":fis:")
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

			return h.writeError(
				c,
				http.StatusInternalServerError,
				"failed to read request body",
				"",
			)
		}

		log.DebugContext(ctx, "fis request", "op", op, "id", id)

		return h.dispatch(ctx, c, op, id, body)
	}
}

// dispatch routes a parsed FIS operation to the appropriate backend call.
func (h *Handler) dispatch(ctx context.Context, c *echo.Context, op, id string, body []byte) error {
	if handled, err := h.dispatchTargetAccountOps(c, op, id, body); handled {
		return err
	}

	if handled, err := h.dispatchExperimentOps(ctx, c, op, id, body); handled {
		return err
	}

	if handled, err := h.dispatchActionTagOps(c, op, id, body); handled {
		return err
	}

	return h.writeError(c, http.StatusNotFound, "unknown operation: "+op, "")
}

// dispatchExperimentOps handles experiment template and experiment operations.
func (h *Handler) dispatchExperimentOps(
	ctx context.Context,
	c *echo.Context,
	op, id string,
	body []byte,
) (bool, error) {
	switch op {
	case opCreateExperimentTemplate:
		return true, h.handleCreateExperimentTemplate(ctx, c, body)
	case opGetExperimentTemplate:
		return true, h.handleGetExperimentTemplate(c, id)
	case opUpdateExperimentTemplate:
		return true, h.handleUpdateExperimentTemplate(c, id, body)
	case opDeleteExperimentTemplate:
		return true, h.handleDeleteExperimentTemplate(c, id)
	case opListExperimentTemplates:
		return true, h.handleListExperimentTemplates(c)
	case opStartExperiment:
		return true, h.handleStartExperiment(ctx, c, body)
	case opGetExperiment:
		return true, h.handleGetExperiment(c, id)
	case opStopExperiment:
		return true, h.handleStopExperiment(c, id)
	case opListExperiments:
		return true, h.handleListExperiments(c)
	case opListExperimentResolvedTargets:
		return true, h.handleListExperimentResolvedTargets(c, id)
	}

	return false, nil
}

// dispatchActionTagOps handles action, target resource type, tag, and safety lever operations.
func (h *Handler) dispatchActionTagOps(c *echo.Context, op, id string, body []byte) (bool, error) {
	switch op {
	case opGetAction:
		return true, h.handleGetAction(c, id)
	case opListActions:
		return true, h.handleListActions(c)
	case opGetTargetResourceType:
		rt, _ := url.PathUnescape(id)

		return true, h.handleGetTargetResourceType(c, rt)
	case opListTargetResourceTypes:
		return true, h.handleListTargetResourceTypes(c)
	case opTagResource:
		return true, h.handleTagResource(c, id, body)
	case opUntagResource:
		return true, h.handleUntagResource(c, id, c.Request().URL.Query())
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c, id)
	case opGetSafetyLever:
		return true, h.handleGetSafetyLever(c, id)
	case opUpdateSafetyLeverState:
		return true, h.handleUpdateSafetyLeverState(c, id, body)
	}

	return false, nil
}

// dispatchTargetAccountOps handles the target account configuration operations.
// Returns (true, err) when the operation was handled, (false, nil) otherwise.
func (h *Handler) dispatchTargetAccountOps(
	c *echo.Context,
	op, id string,
	body []byte,
) (bool, error) {
	switch op {
	case opCreateTargetAccountConfiguration:
		return true, h.handleCreateTargetAccountConfiguration(c, id, body)
	case opDeleteTargetAccountConfiguration:
		return true, h.handleDeleteTargetAccountConfiguration(c, id)
	case opGetTargetAccountConfiguration:
		return true, h.handleGetTargetAccountConfiguration(c, id)
	case opUpdateTargetAccountConfiguration:
		return true, h.handleUpdateTargetAccountConfiguration(c, id, body)
	case opListTargetAccountConfigurations:
		return true, h.handleListTargetAccountConfigurations(c, id)
	case opGetExperimentTargetAccountConfiguration:
		return true, h.handleGetExperimentTargetAccountConfiguration(c, id)
	case opListExperimentTargetAccountConfigurations:
		return true, h.handleListExperimentTargetAccountConfigurations(c, id)
	}

	return false, nil
}

// ----------------------------------------
// ExperimentTemplate handlers
// ----------------------------------------

func (h *Handler) handleCreateExperimentTemplate(
	_ context.Context,
	c *echo.Context,
	body []byte,
) error {
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
		return h.writeBackendError(c, err, id)
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

	ids := make([]string, len(templates))
	for i, t := range templates {
		ids[i] = t.ID
	}

	q := c.Request().URL.Query()
	maxResults, start := paginateWithToken(ids, q)

	end := min(start+maxResults, len(templates))

	var nextTok string

	if end < len(templates) {
		nextTok = encodePageToken(end)
	}

	page := templates[start:end]
	dtos := make([]experimentTemplateDTO, len(page))

	for i, t := range page {
		dtos[i] = toTemplateDTO(t)
	}

	return c.JSON(http.StatusOK, listExperimentTemplatesResponseDTO{
		ExperimentTemplates: dtos,
		NextToken:           nextTok,
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
		return h.writeBackendError(c, err, input.ExperimentTemplateID)
	}

	return c.JSON(http.StatusCreated, experimentResponseDTO{
		Experiment: toExperimentDTO(exp),
	})
}

func (h *Handler) handleGetExperiment(c *echo.Context, id string) error {
	exp, err := h.Backend.GetExperiment(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, experimentResponseDTO{
		Experiment: toExperimentDTO(exp),
	})
}

func (h *Handler) handleStopExperiment(c *echo.Context, id string) error {
	exp, err := h.Backend.StopExperiment(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
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

	q := c.Request().URL.Query()

	// Filter by experimentTemplateId.
	if tplFilter := q.Get("experimentTemplateId"); tplFilter != "" {
		filtered := experiments[:0]

		for _, e := range experiments {
			if e.ExperimentTemplateID == tplFilter {
				filtered = append(filtered, e)
			}
		}

		experiments = filtered
	}

	// Filter by status.
	if statusFilter := q.Get("status"); statusFilter != "" {
		filtered := experiments[:0]

		for _, e := range experiments {
			if e.Status.Status == statusFilter {
				filtered = append(filtered, e)
			}
		}

		experiments = filtered
	}

	// Apply cursor-based pagination.
	ids := make([]string, len(experiments))
	for i, e := range experiments {
		ids[i] = e.ID
	}

	maxResults, start := paginateWithToken(ids, q)

	end := min(start+maxResults, len(experiments))

	var nextTok string

	if end < len(experiments) {
		nextTok = encodePageToken(end)
	}

	page := experiments[start:end]
	dtos := make([]experimentDTO, len(page))

	for i, e := range page {
		dtos[i] = toExperimentDTO(e)
	}

	return c.JSON(http.StatusOK, listExperimentsResponseDTO{
		Experiments: dtos,
		NextToken:   nextTok,
	})
}

// ----------------------------------------
// Action discovery handlers
// ----------------------------------------

func (h *Handler) handleGetAction(c *echo.Context, id string) error {
	action, err := h.Backend.GetAction(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, actionResponseDTO{
		Action: toActionDTO(action),
	})
}

func (h *Handler) handleListActions(c *echo.Context) error {
	actions := h.Backend.ListActions()

	ids := make([]string, len(actions))
	for i, a := range actions {
		ids[i] = a.ID
	}

	q := c.Request().URL.Query()
	maxResults, start := paginateWithToken(ids, q)

	end := min(start+maxResults, len(actions))

	var nextTok string

	if end < len(actions) {
		nextTok = encodePageToken(end)
	}

	page := actions[start:end]
	dtos := make([]actionDTO, len(page))

	for i := range page {
		dtos[i] = toActionDTO(&page[i])
	}

	return c.JSON(http.StatusOK, listActionsResponseDTO{Actions: dtos, NextToken: nextTok})
}

func (h *Handler) handleGetTargetResourceType(c *echo.Context, resourceType string) error {
	rt, err := h.Backend.GetTargetResourceType(resourceType)
	if err != nil {
		return h.writeBackendError(c, err, resourceType)
	}

	return c.JSON(http.StatusOK, targetResourceTypeResponseDTO{
		TargetResourceType: toTargetResourceTypeDTO(rt),
	})
}

func (h *Handler) handleListTargetResourceTypes(c *echo.Context) error {
	types := h.Backend.ListTargetResourceTypes()

	ids := make([]string, len(types))
	for i, rt := range types {
		ids[i] = rt.ResourceType
	}

	q := c.Request().URL.Query()
	maxResults, start := paginateWithToken(ids, q)

	end := min(start+maxResults, len(types))

	var nextTok string

	if end < len(types) {
		nextTok = encodePageToken(end)
	}

	page := types[start:end]
	dtos := make([]targetResourceTypeDTO, len(page))

	for i := range page {
		dtos[i] = toTargetResourceTypeDTO(&page[i])
	}

	return c.JSON(
		http.StatusOK,
		listTargetResourceTypesResponseDTO{TargetResourceTypes: dtos, NextToken: nextTok},
	)
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
// Resolved targets handlers
// ----------------------------------------

func (h *Handler) handleListExperimentResolvedTargets(c *echo.Context, id string) error {
	resolved, err := h.Backend.ListExperimentResolvedTargets(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	dtos := make([]resolvedTargetDTO, len(resolved))
	for i, rt := range resolved {
		dtos[i] = resolvedTargetDTO(rt)
	}

	return c.JSON(http.StatusOK, listExperimentResolvedTargetsResponseDTO{
		ResolvedTargets: dtos,
	})
}

// ----------------------------------------
// Safety lever handlers
// ----------------------------------------

func (h *Handler) handleGetSafetyLever(c *echo.Context, id string) error {
	lever, err := h.Backend.GetSafetyLever(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, safetyLeverResponseDTO{SafetyLever: toSafetyLeverDTO(lever)})
}

func (h *Handler) handleUpdateSafetyLeverState(c *echo.Context, id string, body []byte) error {
	var input updateSafetyLeverStateRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), id)
	}

	lever, err := h.Backend.UpdateSafetyLeverState(id, &input)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, safetyLeverResponseDTO{SafetyLever: toSafetyLeverDTO(lever)})
}

// ----------------------------------------
// Target Account Configuration handlers
// ----------------------------------------

// splitCompositeID splits a composite "{resourceID}/{accountID}" identifier into its two parts.
func splitCompositeID(compositeID string) (string, string) {
	resourceID, accountID, _ := strings.Cut(compositeID, "/")

	return resourceID, accountID
}

func (h *Handler) handleCreateTargetAccountConfiguration(
	c *echo.Context,
	compositeID string,
	body []byte,
) error {
	templateID, accountID := splitCompositeID(compositeID)

	var input createTargetAccountConfigurationRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"invalid request body: "+err.Error(),
			compositeID,
		)
	}

	cfg, err := h.Backend.CreateTargetAccountConfiguration(
		templateID,
		accountID,
		input.RoleArn,
		input.Description,
	)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusCreated, targetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleDeleteTargetAccountConfiguration(
	c *echo.Context,
	compositeID string,
) error {
	templateID, accountID := splitCompositeID(compositeID)

	cfg, err := h.Backend.DeleteTargetAccountConfiguration(templateID, accountID)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusOK, targetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleGetTargetAccountConfiguration(c *echo.Context, compositeID string) error {
	templateID, accountID := splitCompositeID(compositeID)

	cfg, err := h.Backend.GetTargetAccountConfiguration(templateID, accountID)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusOK, targetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleUpdateTargetAccountConfiguration(
	c *echo.Context,
	compositeID string,
	body []byte,
) error {
	templateID, accountID := splitCompositeID(compositeID)

	var input updateTargetAccountConfigurationRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"invalid request body: "+err.Error(),
			compositeID,
		)
	}

	cfg, err := h.Backend.UpdateTargetAccountConfiguration(
		templateID,
		accountID,
		input.RoleArn,
		input.Description,
	)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusOK, targetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleListTargetAccountConfigurations(c *echo.Context, templateID string) error {
	cfgs, err := h.Backend.ListTargetAccountConfigurations(templateID)
	if err != nil {
		return h.writeBackendError(c, err, templateID)
	}

	dtos := make([]targetAccountConfigurationDTO, len(cfgs))
	for i, cfg := range cfgs {
		dtos[i] = toTargetAccountConfigDTO(cfg)
	}

	return c.JSON(http.StatusOK, listTargetAccountConfigurationsResponseDTO{
		TargetAccountConfigurations: dtos,
	})
}

func (h *Handler) handleGetExperimentTargetAccountConfiguration(
	c *echo.Context,
	compositeID string,
) error {
	experimentID, accountID := splitCompositeID(compositeID)

	cfg, err := h.Backend.GetExperimentTargetAccountConfiguration(experimentID, accountID)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusOK, experimentTargetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toExperimentTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleListExperimentTargetAccountConfigurations(
	c *echo.Context,
	experimentID string,
) error {
	cfgs, err := h.Backend.ListExperimentTargetAccountConfigurations(experimentID)
	if err != nil {
		return h.writeBackendError(c, err, experimentID)
	}

	dtos := make([]experimentTargetAccountConfigurationDTO, len(cfgs))
	for i, cfg := range cfgs {
		dtos[i] = toExperimentTargetAccountConfigDTO(cfg)
	}

	return c.JSON(http.StatusOK, listExperimentTargetAccountConfigurationsResponseDTO{
		TargetAccountConfigurations: dtos,
	})
}

// ----------------------------------------
// Error helpers
// ----------------------------------------

func (h *Handler) writeError(c *echo.Context, status int, message, resourceID string) error {
	return h.writeTypedError(c, status, "", message, resourceID)
}

func (h *Handler) writeTypedError(
	c *echo.Context,
	status int,
	errType, message, resourceID string,
) error {
	resp := errorResponseDTO{Type: errType, Message: message, ResourceID: resourceID}

	return c.JSON(status, resp)
}

// errorClass holds the AWS exception type name and HTTP status code that a sentinel
// backend error maps to.
type errorClass struct {
	exceptionType string
	httpStatus    int
}

// classifyError maps a backend sentinel error to its AWS exception type and HTTP
// status. The real FIS API model defines exactly four exception shapes service-wide
// (ValidationException, ResourceNotFoundException, ConflictException,
// ServiceQuotaExceededException) — there are no per-resource "XyzNotFoundException"
// or "TooManyTagsException" shapes, and every operation's generated deserializer in
// aws-sdk-go-v2/service/fis only recognizes these four __type strings. Emitting any
// other type name causes real SDK clients to fall back to a generic, untyped error
// even where the operation supports a typed one, breaking `errors.As` checks against
// the modeled exception types.
//
// Per the SDK's per-operation deserializers: every not-found condition (template,
// experiment, action, target resource type, safety lever, target account
// configuration, or a generically tagged resource) maps to ResourceNotFoundException;
// StopExperiment does not model ConflictException, so stopping a non-running
// experiment maps to ValidationException; and ServiceQuotaExceededException carries
// HTTP 402 (Payment Required), not 429.
func classifyError(err error) errorClass {
	switch {
	case errors.Is(err, ErrValidation), errors.Is(err, ErrTooManyTags), errors.Is(err, ErrExperimentNotRunning):
		return errorClass{exceptionType: "ValidationException", httpStatus: http.StatusBadRequest}
	case errors.Is(err, ErrTooManyExperiments):
		return errorClass{exceptionType: "ServiceQuotaExceededException", httpStatus: http.StatusPaymentRequired}
	case errors.Is(err, ErrSafetyLeverEngaged):
		return errorClass{exceptionType: "ConflictException", httpStatus: http.StatusConflict}
	case errors.Is(err, ErrTemplateNotFound),
		errors.Is(err, ErrExperimentNotFound),
		errors.Is(err, ErrActionNotFound),
		errors.Is(err, ErrTargetResourceTypeNotFound),
		errors.Is(err, ErrResourceNotFound),
		errors.Is(err, ErrSafetyLeverNotFound),
		errors.Is(err, ErrTargetAccountConfigNotFound):
		return errorClass{exceptionType: "ResourceNotFoundException", httpStatus: http.StatusNotFound}
	default:
		return errorClass{exceptionType: "InternalServerError", httpStatus: http.StatusInternalServerError}
	}
}

func (h *Handler) writeBackendError(c *echo.Context, err error, id string) error {
	ec := classifyError(err)

	return h.writeTypedError(c, ec.httpStatus, ec.exceptionType, err.Error(), id)
}

// ----------------------------------------
// Path parsing
// ----------------------------------------

// parseFISPath maps an HTTP method + URL path to a FIS operation name and optional resource ID.
// Returns ("", "") when no pattern matches.
func parseFISPath(method, path string) (string, string) {
	segs := pathSegments(path)
	if len(segs) == 0 {
		return "", ""
	}

	root := segs[0]
	hasID := len(segs) >= minSegmentsForID

	switch root {
	case pathExperimentTemplates:
		return parseFISExperimentTemplatePath(method, segs, hasID)
	case pathExperiments:
		return parseFISExperimentPath(method, segs, hasID)
	case pathActions:
		return parseFISActionPath(method, segs, hasID)
	case pathTargetResourceTypes:
		return parseFISTargetResourceTypePath(method, segs, hasID)
	case pathTags:
		return parseFISTagPath(method, segs, hasID)
	case pathSafetyLevers:
		return parseFISSafetyLeverPath(method, segs, hasID)
	}

	return "", ""
}

// parseFISExperimentTemplatePath routes experiment template paths.
func parseFISExperimentTemplatePath(method string, segs []string, hasID bool) (string, string) {
	if !hasID {
		if method == http.MethodPost {
			return opCreateExperimentTemplate, ""
		}
		if method == http.MethodGet {
			return opListExperimentTemplates, ""
		}

		return "", ""
	}

	if op, id := parseFISTemplateSubPath(method, segs); op != "" {
		return op, id
	}

	switch method {
	case http.MethodGet:
		return opGetExperimentTemplate, segs[1]
	case http.MethodPatch:
		return opUpdateExperimentTemplate, segs[1]
	case http.MethodDelete:
		return opDeleteExperimentTemplate, segs[1]
	}

	return "", ""
}

// parseFISTemplateSubPath routes sub-paths of experiment templates (target account configurations).
func parseFISTemplateSubPath(method string, segs []string) (string, string) {
	if len(segs) >= 4 && segs[2] == pathTargetAccountConfigurations {
		compositeID := segs[1] + "/" + segs[3]
		switch method {
		case http.MethodPost:
			return opCreateTargetAccountConfiguration, compositeID
		case http.MethodGet:
			return opGetTargetAccountConfiguration, compositeID
		case http.MethodPatch:
			return opUpdateTargetAccountConfiguration, compositeID
		case http.MethodDelete:
			return opDeleteTargetAccountConfiguration, compositeID
		}
	}

	if len(segs) >= 3 && segs[2] == pathTargetAccountConfigurations && method == http.MethodGet {
		return opListTargetAccountConfigurations, segs[1]
	}

	return "", ""
}

// parseFISExperimentPath routes experiment paths.
func parseFISExperimentPath(method string, segs []string, hasID bool) (string, string) {
	if !hasID {
		if method == http.MethodPost {
			return opStartExperiment, ""
		}
		if method == http.MethodGet {
			return opListExperiments, ""
		}

		return "", ""
	}

	if op, id := parseFISExperimentSubPath(method, segs); op != "" {
		return op, id
	}

	switch method {
	case http.MethodGet:
		return opGetExperiment, segs[1]
	case http.MethodDelete:
		return opStopExperiment, segs[1]
	}

	return "", ""
}

// parseFISExperimentSubPath routes sub-paths of experiments.
func parseFISExperimentSubPath(method string, segs []string) (string, string) {
	if method == http.MethodGet && len(segs) >= 3 && segs[2] == subPathResolvedTargets {
		return opListExperimentResolvedTargets, segs[1]
	}

	// POST /experiments/{id}/stop — AWS canonical StopExperiment route.
	if method == http.MethodPost && len(segs) >= 3 && segs[2] == subPathStop {
		return opStopExperiment, segs[1]
	}

	if len(segs) >= 4 && segs[2] == pathTargetAccountConfigurations && method == http.MethodGet {
		return opGetExperimentTargetAccountConfiguration, segs[1] + "/" + segs[3]
	}

	if len(segs) >= 3 && segs[2] == pathTargetAccountConfigurations && method == http.MethodGet {
		return opListExperimentTargetAccountConfigurations, segs[1]
	}

	return "", ""
}

// parseFISActionPath routes action paths.
func parseFISActionPath(method string, segs []string, hasID bool) (string, string) {
	switch {
	case method == http.MethodGet && !hasID:
		return opListActions, ""
	case method == http.MethodGet && hasID:
		return opGetAction, segs[1]
	}

	return "", ""
}

// parseFISTargetResourceTypePath routes target resource type paths.
func parseFISTargetResourceTypePath(method string, segs []string, hasID bool) (string, string) {
	switch {
	case method == http.MethodGet && !hasID:
		return opListTargetResourceTypes, ""
	case method == http.MethodGet && hasID:
		return opGetTargetResourceType, segs[1]
	}

	return "", ""
}

// parseFISTagPath routes tag management paths.
func parseFISTagPath(method string, segs []string, hasID bool) (string, string) {
	if !hasID {
		return "", ""
	}

	arnStr := strings.Join(segs[1:], "/")
	switch method {
	case http.MethodGet:
		return opListTagsForResource, arnStr
	case http.MethodPost:
		return opTagResource, arnStr
	case http.MethodDelete:
		return opUntagResource, arnStr
	}

	return "", ""
}

// parseFISSafetyLeverPath routes safety lever paths.
func parseFISSafetyLeverPath(method string, segs []string, hasID bool) (string, string) {
	if !hasID {
		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetSafetyLever, segs[1]
	case http.MethodPatch:
		return opUpdateSafetyLeverState, segs[1]
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
			lc.CloudWatchLogsConfiguration = &cwLogsConfigurationDTO{
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
			State: &experimentActionStatusDTO{
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

	statusDTO := experimentStatusDTO{
		Status: exp.Status.Status,
		Reason: exp.Status.Reason,
	}

	if exp.Status.Error != nil {
		statusDTO.Error = &experimentStatusErrorDTO{
			Code:      exp.Status.Error.Code,
			Location:  exp.Status.Error.Location,
			AccountID: exp.Status.Error.AccountID,
		}
	}

	dto := experimentDTO{
		ID:                               exp.ID,
		Arn:                              exp.Arn,
		ExperimentTemplateID:             exp.ExperimentTemplateID,
		RoleArn:                          exp.RoleArn,
		Status:                           statusDTO,
		State:                            statusDTO,
		Targets:                          targets,
		Actions:                          actions,
		StopConditions:                   stopConditions,
		Tags:                             exp.Tags,
		CreationTime:                     toUnix(exp.CreationTime),
		StartTime:                        toUnix(exp.StartTime),
		EndTime:                          toUnixPtr(exp.EndTime),
		TargetAccountConfigurationsCount: exp.TargetAccountConfigurationsCount,
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

func toSafetyLeverDTO(lever *SafetyLever) safetyLeverDTO {
	return safetyLeverDTO{
		ID:   lever.ID,
		Arn:  lever.Arn,
		Tags: lever.Tags,
		State: safetyLeverStateDTO{
			Status: lever.State.Status,
			Reason: lever.State.Reason,
		},
	}
}

func toTargetAccountConfigDTO(cfg *TargetAccountConfiguration) targetAccountConfigurationDTO {
	return targetAccountConfigurationDTO{
		AccountID:   cfg.AccountID,
		Description: cfg.Description,
		RoleArn:     cfg.RoleArn,
	}
}

func toExperimentTargetAccountConfigDTO(
	cfg *ExperimentTargetAccountConfiguration,
) experimentTargetAccountConfigurationDTO {
	return experimentTargetAccountConfigurationDTO{
		AccountID:   cfg.AccountID,
		Description: cfg.Description,
		RoleArn:     cfg.RoleArn,
	}
}

// ----------------------------------------
// Pagination helpers
// ----------------------------------------

// defaultMaxResults is the default page size for list operations.
const defaultMaxResults = 20

// absoluteMaxResults is the maximum allowed page size.
const absoluteMaxResults = 100

// encodePageToken encodes an integer offset as an opaque base64 token.
func encodePageToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodePageToken decodes a token produced by encodePageToken.
func decodePageToken(tok string) (int, error) {
	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(b))
}

// paginateWithToken decodes the offset-based nextToken and returns (pageSize, startOffset).
// The caller slices [startOffset : startOffset+pageSize].
func paginateWithToken(_ []string, q url.Values) (int, int) {
	mr := defaultMaxResults

	if v := q.Get("maxResults"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			mr = n
		}
	}

	mr = min(mr, absoluteMaxResults)

	start := 0

	if tok := q.Get("nextToken"); tok != "" {
		if n, err := decodePageToken(tok); err == nil && n > 0 {
			start = n
		}
	}

	return mr, start
}
