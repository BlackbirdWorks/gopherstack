package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathThingRegistrationTasks = "/thing-registration-tasks"
	pathManagedJobTemplates    = "/managed-job-templates"
	pathUpdateThingGroups      = "/thing-groups/updateThingGroupsForThing"

	suffixCancel  = "/cancel"
	suffixReports = "/reports"

	keyTaskID          = "taskId"
	keyTemplateArn     = "templateArn"
	keyTemplateVersion = "templateVersion"
	keyEnvironments    = "environments"
)

// ---------------------------------------------------------------------------
// Route resolvers for batch 4 ops
// ---------------------------------------------------------------------------

// resolveThingRegistrationOps resolves RegisterThing and bulk thing
// registration task routes.
func resolveThingRegistrationOps(path, method string) string {
	switch {
	case path == pathThings && method == http.MethodPost:
		return opRegisterThing
	case path == pathThingRegistrationTasks && method == http.MethodPost:
		return opStartThingRegistrationTask
	case path == pathThingRegistrationTasks && method == http.MethodGet:
		return opListThingRegistrationTasks
	case strings.HasPrefix(path, pathThingRegistrationTasks+"/") &&
		strings.HasSuffix(path, suffixCancel) && method == http.MethodPut:
		return opStopThingRegistrationTask
	case strings.HasPrefix(path, pathThingRegistrationTasks+"/") &&
		strings.HasSuffix(path, suffixReports) && method == http.MethodGet:
		return opListThingRegistrationTaskReports
	case strings.HasPrefix(path, pathThingRegistrationTasks+"/") && method == http.MethodGet:
		return opDescribeThingRegistrationTask
	}

	return unknownOperation
}

// resolveManagedJobTemplateOps resolves ManagedJobTemplate routes.
func resolveManagedJobTemplateOps(path, method string) string {
	switch {
	case path == pathManagedJobTemplates && method == http.MethodGet:
		return opListManagedJobTemplates
	case strings.HasPrefix(path, pathManagedJobTemplates+"/") && method == http.MethodGet:
		return opDescribeManagedJobTemplate
	}

	return unknownOperation
}

// resolveThingTypeUpdateOp resolves the PATCH /thing-types/{name} route.
func resolveThingTypeUpdateOp(path, method string) string {
	if strings.HasPrefix(path, "/thing-types/") && method == http.MethodPatch {
		return opUpdateThingType
	}

	return unknownOperation
}

// resolveThingGroupUpdateOp resolves the PUT
// /thing-groups/updateThingGroupsForThing route.
func resolveThingGroupUpdateOp(path, method string) string {
	if path == pathUpdateThingGroups && method == http.MethodPut {
		return opUpdateThingGroupsForThing
	}

	return unknownOperation
}

// resolveBatch4Ops resolves all batch 4 routes.
//
// Note: ListThingPrincipalsV2 (GET /things/{name}/principals-v2) is resolved
// directly inside thingOperation in handler.go, since the top-level
// resolveOperation switch dispatches any "/things/"-prefixed path there
// before this function ever runs.
func resolveBatch4Ops(path, method string) string {
	if op := resolveThingRegistrationOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveManagedJobTemplateOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveThingTypeUpdateOp(path, method); op != unknownOperation {
		return op
	}

	return resolveThingGroupUpdateOp(path, method)
}

// ---------------------------------------------------------------------------
// Dispatch for batch 4 ops
// ---------------------------------------------------------------------------

func (h *Handler) dispatchBatch4Ops(c *echo.Context, op string) (bool, error) {
	switch op {
	case opRegisterThing:
		return true, h.handleRegisterThing(c)
	case opStartThingRegistrationTask:
		return true, h.handleStartThingRegistrationTask(c)
	case opStopThingRegistrationTask:
		return true, h.handleStopThingRegistrationTask(c)
	case opListThingRegistrationTasks:
		return true, h.handleListThingRegistrationTasks(c)
	case opDescribeThingRegistrationTask:
		return true, h.handleDescribeThingRegistrationTask(c)
	case opListThingRegistrationTaskReports:
		return true, h.handleListThingRegistrationTaskReports(c)
	case opUpdateThingType:
		return true, h.handleUpdateThingType(c)
	case opUpdateThingGroupsForThing:
		return true, h.handleUpdateThingGroupsForThing(c)
	case opListThingPrincipalsV2:
		return true, h.handleListThingPrincipalsV2(c)
	case opDescribeManagedJobTemplate:
		return true, h.handleDescribeManagedJobTemplate(c)
	case opListManagedJobTemplates:
		return true, h.handleListManagedJobTemplates(c)
	}

	return false, nil
}

// ---------------------------------------------------------------------------
// Handler implementations
// ---------------------------------------------------------------------------

func (h *Handler) handleRegisterThing(c *echo.Context) error {
	var body struct {
		Parameters   map[string]string `json:"parameters"`
		TemplateBody string            `json:"templateBody"`
	}
	if err := readBody(c, &body); err != nil {
		return err
	}

	out, err := h.Backend.RegisterThing(&RegisterThingInput{
		TemplateBody: body.TemplateBody,
		Parameters:   body.Parameters,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"certificatePem": out.CertificatePem,
		"resourceArns":   out.ResourceArns,
	})
}

func (h *Handler) handleStartThingRegistrationTask(c *echo.Context) error {
	var body struct {
		TemplateBody    string `json:"templateBody"`
		InputFileBucket string `json:"inputFileBucket"`
		InputFileKey    string `json:"inputFileKey"`
		RoleArn         string `json:"roleArn"`
	}
	if err := readBody(c, &body); err != nil {
		return err
	}

	task, err := h.Backend.StartThingRegistrationTask(&StartThingRegistrationTaskInput{
		TemplateBody:    body.TemplateBody,
		InputFileBucket: body.InputFileBucket,
		InputFileKey:    body.InputFileKey,
		RoleArn:         body.RoleArn,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{keyTaskID: task.TaskID})
}

// thingRegistrationTaskID extracts the {taskId} path segment for the given suffix
// (or no suffix for a bare describe/task path).
func thingRegistrationTaskID(path, suffix string) string {
	after := strings.TrimPrefix(path, pathThingRegistrationTasks+"/")

	return strings.TrimSuffix(after, suffix)
}

func (h *Handler) handleStopThingRegistrationTask(c *echo.Context) error {
	taskID := thingRegistrationTaskID(c.Request().URL.Path, suffixCancel)

	if err := h.Backend.StopThingRegistrationTask(taskID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDescribeThingRegistrationTask(c *echo.Context) error {
	taskID := thingRegistrationTaskID(c.Request().URL.Path, "")

	task, err := h.Backend.DescribeThingRegistrationTask(taskID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTaskID:            task.TaskID,
		keyCreationDate:      task.CreationDate,
		keyLastModifiedDate:  task.LastModifiedDate,
		"templateBody":       task.TemplateBody,
		"inputFileBucket":    task.InputFileBucket,
		"inputFileKey":       task.InputFileKey,
		"roleArn":            task.RoleArn,
		keyStatus:            task.Status,
		"message":            task.Message,
		"successCount":       task.SuccessCount,
		"failureCount":       task.FailureCount,
		"percentageProgress": task.PercentageProgress,
	})
}

func (h *Handler) handleListThingRegistrationTasks(c *echo.Context) error {
	status := c.QueryParam(keyStatus)
	tasks := h.Backend.ListThingRegistrationTasks(status)

	ids := make([]string, len(tasks))
	for i, t := range tasks {
		ids[i] = t.TaskID
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(ids, pageSize, start)

	resp := map[string]any{"taskIds": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListThingRegistrationTaskReports(c *echo.Context) error {
	taskID := thingRegistrationTaskID(c.Request().URL.Path, suffixReports)
	reportType := c.QueryParam("reportType")

	links, err := h.Backend.ListThingRegistrationTaskReports(taskID, reportType)
	if err != nil {
		return h.handleError(c, err)
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(links, pageSize, start)

	resp := map[string]any{
		"resourceLinks": page,
		"reportType":    reportType,
	}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateThingType(c *echo.Context) error {
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")

	var body struct {
		ThingTypeProperties *struct {
			ThingTypeDescription string   `json:"thingTypeDescription"`
			SearchableAttributes []string `json:"searchableAttributes"`
		} `json:"thingTypeProperties"`
	}
	if err := readBody(c, &body); err != nil {
		return err
	}

	var desc string

	var searchable []string

	if body.ThingTypeProperties != nil {
		desc = body.ThingTypeProperties.ThingTypeDescription
		searchable = body.ThingTypeProperties.SearchableAttributes
	}

	if err := h.Backend.UpdateThingType(&UpdateThingTypeInput{
		ThingTypeName:        thingTypeName,
		Description:          desc,
		SearchableAttributes: searchable,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUpdateThingGroupsForThing(c *echo.Context) error {
	var body struct {
		ThingName             string   `json:"thingName"`
		ThingGroupsToAdd      []string `json:"thingGroupsToAdd"`
		ThingGroupsToRemove   []string `json:"thingGroupsToRemove"`
		OverrideDynamicGroups bool     `json:"overrideDynamicGroups"`
	}
	if err := readBody(c, &body); err != nil {
		return err
	}

	if err := h.Backend.UpdateThingGroupsForThing(&UpdateThingGroupsForThingInput{
		ThingName:             body.ThingName,
		ThingGroupsToAdd:      body.ThingGroupsToAdd,
		ThingGroupsToRemove:   body.ThingGroupsToRemove,
		OverrideDynamicGroups: body.OverrideDynamicGroups,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListThingPrincipalsV2(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/things/")
	thingName := strings.TrimSuffix(after, "/principals-v2")

	principals, err := h.Backend.ListThingPrincipalsV2(thingName)
	if err != nil {
		return h.handleError(c, err)
	}

	out := make([]map[string]any, len(principals))
	for i, p := range principals {
		out[i] = map[string]any{
			"principal":          p.Principal,
			"thingPrincipalType": p.ThingPrincipalType,
		}
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(out, pageSize, start)

	resp := map[string]any{"thingPrincipalObjects": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeManagedJobTemplate(c *echo.Context) error {
	templateName := strings.TrimPrefix(c.Request().URL.Path, pathManagedJobTemplates+"/")
	templateVersion := c.QueryParam(keyTemplateVersion)

	tmpl, err := h.Backend.DescribeManagedJobTemplate(templateName, templateVersion)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTemplateName:      tmpl.TemplateName,
		keyTemplateArn:       tmpl.TemplateArn,
		keyTemplateVersion:   tmpl.TemplateVersion,
		keyDescription:       tmpl.Description,
		"document":           tmpl.Document,
		keyEnvironments:      tmpl.Environments,
		"documentParameters": tmpl.DocumentParameters,
	})
}

func (h *Handler) handleListManagedJobTemplates(c *echo.Context) error {
	templateName := c.QueryParam(keyTemplateName)
	templates := h.Backend.ListManagedJobTemplates(templateName)

	out := make([]map[string]any, len(templates))
	for i, t := range templates {
		out[i] = map[string]any{
			keyTemplateName:    t.TemplateName,
			keyTemplateArn:     t.TemplateArn,
			keyTemplateVersion: t.TemplateVersion,
			keyDescription:     t.Description,
			keyEnvironments:    t.Environments,
		}
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(out, pageSize, start)

	resp := map[string]any{"managedJobTemplates": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
