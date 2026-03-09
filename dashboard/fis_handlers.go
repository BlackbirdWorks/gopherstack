package dashboard

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
)

// ----------------------------------------
// View models
// ----------------------------------------

// fisTemplateView is the view model for a single experiment template row.
type fisTemplateView struct {
	Tags        map[string]string
	ID          string
	Description string
	RoleArn     string
	CreatedAt   string
	Actions     int
	Targets     int
}

// fisExperimentView is the view model for a single experiment row.
type fisExperimentView struct {
	ID           string
	TemplateID   string
	State        string
	StateBadge   string
	StartedAt    string
	Duration     string
	ActionsTotal int
	ActionsDone  int
}

// fisActionView is the view model for a FIS action in the catalog.
type fisActionView struct {
	ID          string
	Description string
	TargetType  string
	Parameters  []fisActionParamView
}

// fisActionParamView holds a parameter name/description pair.
type fisActionParamView struct {
	Name        string
	Description string
	Required    bool
}

// fisTargetTypeView is the view model for a target resource type.
type fisTargetTypeView struct {
	ResourceType string
	Description  string
}

// fisIndexData is the template data for the FIS dashboard page.
type fisIndexData struct {
	PageData

	Templates   []fisTemplateView
	Experiments []fisExperimentView
	Actions     []fisActionView
	TargetTypes []fisTargetTypeView
}

// fisExperimentsFragData is the template data for the active experiments fragment.
type fisExperimentsFragData struct {
	Experiments []fisExperimentView
}

// ----------------------------------------
// Route setup
// ----------------------------------------

// setupFISRoutes registers all FIS dashboard routes.
func (h *DashboardHandler) setupFISRoutes() {
	h.SubRouter.GET("/dashboard/fis", h.fisIndex)
	h.SubRouter.GET("/dashboard/fis/experiments", h.fisExperimentsFragment)
	h.SubRouter.POST("/dashboard/fis/templates/create", h.fisCreateTemplate)
	h.SubRouter.POST("/dashboard/fis/templates/delete", h.fisDeleteTemplate)
	h.SubRouter.POST("/dashboard/fis/experiments/start", h.fisStartExperiment)
	h.SubRouter.POST("/dashboard/fis/experiments/stop", h.fisStopExperiment)
}

// ----------------------------------------
// Handlers
// ----------------------------------------

// fisIndex renders the main FIS dashboard page.
func (h *DashboardHandler) fisIndex(c *echo.Context) error {
	if h.FISOps == nil {
		h.renderTemplate(c.Response(), "fis/index.html", fisIndexData{
			PageData: PageData{
				Title:     "FIS",
				ActiveTab: "fis",
				Snippet:   fisSnippetData(),
			},
		})

		return nil
	}

	data := fisIndexData{
		PageData: PageData{
			Title:     "FIS",
			ActiveTab: "fis",
			Snippet:   fisSnippetData(),
		},
		Templates:   toFISTemplateViews(h.FISOps),
		Experiments: toFISExperimentViews(h.FISOps),
		Actions:     toFISActionViews(h.FISOps),
		TargetTypes: toFISTargetTypeViews(h.FISOps),
	}

	h.renderTemplate(c.Response(), "fis/index.html", data)

	return nil
}

// fisExperimentsFragment renders the active experiments fragment for HTMX polling.
func (h *DashboardHandler) fisExperimentsFragment(c *echo.Context) error {
	if h.FISOps == nil {
		h.renderFragment(c.Response(), "fis/experiments_fragment.html", fisExperimentsFragData{})

		return nil
	}

	data := fisExperimentsFragData{
		Experiments: toFISExperimentViews(h.FISOps),
	}

	h.renderFragment(c.Response(), "fis/experiments_fragment.html", data)

	return nil
}

// fisCreateTemplate handles POST /dashboard/fis/templates/create.
func (h *DashboardHandler) fisCreateTemplate(c *echo.Context) error {
	if h.FISOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	description := strings.TrimSpace(r.FormValue("description"))
	roleArn := strings.TrimSpace(r.FormValue("roleArn"))
	actionID := strings.TrimSpace(r.FormValue("actionId"))
	actionName := strings.TrimSpace(r.FormValue("actionName"))
	service := strings.TrimSpace(r.FormValue("service"))
	operations := strings.TrimSpace(r.FormValue("operations"))
	percentage := strings.TrimSpace(r.FormValue("percentage"))
	duration := strings.TrimSpace(r.FormValue("duration"))

	if description == "" || roleArn == "" || actionID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if actionName == "" {
		actionName = "action1"
	}

	params := map[string]string{}
	if service != "" {
		params["service"] = service
	}

	if operations != "" {
		params["operations"] = operations
	}

	if percentage != "" {
		params["percentage"] = percentage
	}

	if duration != "" {
		params["duration"] = duration
	} else {
		params["duration"] = "PT5M"
	}

	input := &fisCreateTemplateRequest{
		Description: description,
		RoleArn:     roleArn,
		Actions: map[string]fisActionDTO{
			actionName: {
				ActionID:   actionID,
				Parameters: params,
			},
		},
		StopConditions: []fisStopConditionDTO{
			{Source: "none"},
		},
		Tags: map[string]string{},
	}

	if _, err := h.FISOps.Backend.CreateExperimentTemplate(
		input,
		h.GlobalConfig.AccountID,
		h.GlobalConfig.Region,
	); err != nil {
		h.Logger.Error("failed to create FIS experiment template", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/fis")
}

// fisDeleteTemplate handles POST /dashboard/fis/templates/delete.
func (h *DashboardHandler) fisDeleteTemplate(c *echo.Context) error {
	if h.FISOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := strings.TrimSpace(r.FormValue("id"))
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.FISOps.Backend.DeleteExperimentTemplate(id); err != nil {
		h.Logger.Error("failed to delete FIS experiment template", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/fis")
}

// fisStartExperiment handles POST /dashboard/fis/experiments/start.
func (h *DashboardHandler) fisStartExperiment(c *echo.Context) error {
	if h.FISOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	templateID := strings.TrimSpace(r.FormValue("templateId"))
	if templateID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	input := &fisStartExperimentRequest{
		ExperimentTemplateID: templateID,
		Tags:                 map[string]string{},
	}

	if _, err := h.FISOps.Backend.StartExperiment(
		c.Request().Context(),
		input,
		h.GlobalConfig.AccountID,
		h.GlobalConfig.Region,
	); err != nil {
		h.Logger.Error("failed to start FIS experiment", "templateId", templateID, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/fis")
}

// fisStopExperiment handles POST /dashboard/fis/experiments/stop.
func (h *DashboardHandler) fisStopExperiment(c *echo.Context) error {
	if h.FISOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := strings.TrimSpace(r.FormValue("id"))
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.FISOps.Backend.StopExperiment(id); err != nil {
		h.Logger.Error("failed to stop FIS experiment", "id", id, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/fis")
}

// ----------------------------------------
// View conversion helpers
// ----------------------------------------

func toFISTemplateViews(h *fisbackend.Handler) []fisTemplateView {
	templates, err := h.Backend.ListExperimentTemplates()
	if err != nil {
		return nil
	}

	views := make([]fisTemplateView, 0, len(templates))

	for _, t := range templates {
		views = append(views, fisTemplateView{
			ID:          t.ID,
			Description: t.Description,
			RoleArn:     t.RoleArn,
			Actions:     len(t.Actions),
			Targets:     len(t.Targets),
			Tags:        t.Tags,
			CreatedAt:   t.CreationTime.Format("2006-01-02 15:04:05"),
		})
	}

	return views
}

func toFISExperimentViews(h *fisbackend.Handler) []fisExperimentView {
	experiments, err := h.Backend.ListExperiments()
	if err != nil {
		return nil
	}

	views := make([]fisExperimentView, 0, len(experiments))

	for _, exp := range experiments {
		state := exp.Status.Status
		done := countDoneActions(exp)
		total := len(exp.Actions)

		dur := ""
		if !exp.StartTime.IsZero() {
			end := time.Now()
			if exp.EndTime != nil {
				end = *exp.EndTime
			}

			dur = end.Sub(exp.StartTime).Round(time.Second).String()
		}

		views = append(views, fisExperimentView{
			ID:           exp.ID,
			TemplateID:   exp.ExperimentTemplateID,
			State:        strings.ToUpper(state),
			StateBadge:   fisStateBadgeClass(state),
			StartedAt:    exp.StartTime.Format("15:04:05"),
			Duration:     dur,
			ActionsTotal: total,
			ActionsDone:  done,
		})
	}

	return views
}

func countDoneActions(exp *fisbackend.Experiment) int {
	count := 0

	for _, a := range exp.Actions {
		s := a.Status.Status
		if s == "completed" || s == "stopped" || s == "failed" {
			count++
		}
	}

	return count
}

func fisStateBadgeClass(state string) string {
	switch state {
	case "pending":
		return "bg-slate-100 text-slate-700 dark:bg-slate-700 dark:text-slate-300"
	case "initiating":
		return "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400"
	case "running":
		return "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400"
	case "completed":
		return "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400"
	case "stopped", "stopping":
		return "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400"
	case "failed":
		return "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400"
	default:
		return "bg-slate-100 text-slate-700 dark:bg-slate-700 dark:text-slate-300"
	}
}

func toFISActionViews(h *fisbackend.Handler) []fisActionView {
	summaries := h.Backend.ListActions()

	views := make([]fisActionView, 0, len(summaries))

	for _, s := range summaries {
		params := make([]fisActionParamView, 0, len(s.Parameters))
		for name, p := range s.Parameters {
			params = append(params, fisActionParamView{
				Name:        name,
				Description: p.Description,
				Required:    p.Required,
			})
		}

		targetType := ""
		for _, t := range s.Targets {
			targetType = t.ResourceType

			break
		}

		views = append(views, fisActionView{
			ID:          s.ID,
			Description: s.Description,
			TargetType:  targetType,
			Parameters:  params,
		})
	}

	return views
}

func toFISTargetTypeViews(h *fisbackend.Handler) []fisTargetTypeView {
	types := h.Backend.ListTargetResourceTypes()

	views := make([]fisTargetTypeView, 0, len(types))

	for _, t := range types {
		views = append(views, fisTargetTypeView{
			ResourceType: t.ResourceType,
			Description:  t.Description,
		})
	}

	return views
}

// ----------------------------------------
// Snippet and request types
// ----------------------------------------

func fisSnippetData() *SnippetData {
	return &SnippetData{
		ID:    "fis-operations",
		Title: "Using FIS",
		Cli: `# Create an experiment template
aws fis create-experiment-template \
  --description "Stop EC2 instances" \
  --role-arn arn:aws:iam::000000000000:role/fis-role \
  --actions '{"StopInstances":{"actionId":"aws:fis:inject-api-internal-error",
    "parameters":{"service":"ec2","duration":"PT5M"}}}' \
  --stop-conditions '[{"source":"none"}]' \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for FIS
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := fis.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for FIS
import boto3

client = boto3.client('fis', endpoint_url='http://localhost:8000')`,
	}
}

// fisCreateTemplateRequest matches the FIS backend request type for template creation.
// We use the exported-compatible type via the fisbackend alias.
type fisCreateTemplateRequest = fisbackend.ExportedCreateTemplateRequest

// fisStartExperimentRequest matches the FIS backend request type for starting an experiment.
type fisStartExperimentRequest = fisbackend.ExportedStartExperimentRequest

// fisActionDTO is the JSON-compatible action entry for the create-template request.
type fisActionDTO = fisbackend.ExportedActionDTO

// fisStopConditionDTO is the JSON-compatible stop condition for create-template.
type fisStopConditionDTO = fisbackend.ExportedStopConditionDTO
