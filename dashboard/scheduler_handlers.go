package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	schedulerbackend "github.com/blackbirdworks/gopherstack/scheduler"
)

// schedulerView is the view model for a single EventBridge Scheduler schedule.
type schedulerView struct {
	Name       string
	ARN        string
	Expression string
	State      string
}

// schedulerIndexData is the template data for the Scheduler index page.
type schedulerIndexData struct {
	PageData

	Schedules []schedulerView
}

// schedulerIndex renders the Scheduler dashboard index.
func (h *DashboardHandler) schedulerIndex(c *echo.Context) error {
	w := c.Response()

	if h.SchedulerOps == nil {
		h.renderTemplate(w, "scheduler/index.html", schedulerIndexData{
			PageData: PageData{Title: "Scheduler Schedules", ActiveTab: "scheduler",
				Snippet: &SnippetData{
					ID:    "scheduler-operations",
					Title: "Using Scheduler",
					Cli:   "aws scheduler help --endpoint-url http://localhost:8000",
					Go:    "/* Write AWS SDK v2 Code for Scheduler */",
					Python: "# Write boto3 code for Scheduler\nimport boto3\n" +
						"client = boto3.client('scheduler', endpoint_url='http://localhost:8000')",
				}},
			Schedules: []schedulerView{},
		})

		return nil
	}

	schedules := h.SchedulerOps.Backend.ListSchedules()
	views := make([]schedulerView, 0, len(schedules))

	for _, s := range schedules {
		views = append(views, schedulerView{
			Name:       s.Name,
			ARN:        s.ARN,
			Expression: s.ScheduleExpression,
			State:      s.State,
		})
	}

	h.renderTemplate(w, "scheduler/index.html", schedulerIndexData{
		PageData: PageData{Title: "Scheduler Schedules", ActiveTab: "scheduler",
			Snippet: &SnippetData{
				ID:    "scheduler-operations",
				Title: "Using Scheduler",
				Cli:   "aws scheduler help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Scheduler */",
				Python: `# Write boto3 code for Scheduler
import boto3
client = boto3.client('scheduler', endpoint_url='http://localhost:8000')`,
			}},
		Schedules: views,
	})

	return nil
}

// schedulerCreate handles POST /dashboard/scheduler/create.
func (h *DashboardHandler) schedulerCreate(c *echo.Context) error {
	if h.SchedulerOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	expression := c.Request().FormValue("expression")
	targetARN := c.Request().FormValue("target_arn")
	roleARN := c.Request().FormValue("role_arn")

	if name == "" || expression == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.SchedulerOps.Backend.CreateSchedule(
		name,
		expression,
		schedulerbackend.Target{ARN: targetARN, RoleARN: roleARN},
		"ENABLED",
		schedulerbackend.FlexibleTimeWindow{Mode: "OFF"},
	)
	if err != nil {
		h.Logger.Error("failed to create schedule", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/scheduler")
}

// schedulerDelete handles POST /dashboard/scheduler/delete.
func (h *DashboardHandler) schedulerDelete(c *echo.Context) error {
	if h.SchedulerOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.SchedulerOps.Backend.DeleteSchedule(name); err != nil {
		h.Logger.Error("failed to delete schedule", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/scheduler")
}
