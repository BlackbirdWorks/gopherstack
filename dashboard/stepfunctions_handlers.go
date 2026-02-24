package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	sfnbackend "github.com/blackbirdworks/gopherstack/stepfunctions"
)

func (h *DashboardHandler) stepFunctionsIndex(c *echo.Context) error {
	if h.StepFunctionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	machines, _, _ := h.StepFunctionsOps.Backend.ListStateMachines("", 0)
	data := struct {
		PageData

		StateMachines []sfnbackend.StateMachine
	}{
		PageData:      PageData{Title: "Step Functions", ActiveTab: "stepfunctions"},
		StateMachines: machines,
	}

	h.renderTemplate(c.Response(), "stepfunctions/index.html", data)

	return nil
}

func (h *DashboardHandler) stepFunctionsStateMachineDetail(c *echo.Context) error {
	if h.StepFunctionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	arn := c.Request().URL.Query().Get("arn")

	sm, err := h.StepFunctionsOps.Backend.DescribeStateMachine(arn)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	executions, _, _ := h.StepFunctionsOps.Backend.ListExecutions(arn, "", "", 0)
	data := struct {
		PageData

		StateMachine *sfnbackend.StateMachine
		Executions   []sfnbackend.Execution
	}{
		PageData:     PageData{Title: "State Machine: " + sm.Name, ActiveTab: "stepfunctions"},
		StateMachine: sm,
		Executions:   executions,
	}

	h.renderTemplate(c.Response(), "stepfunctions/statemachine_detail.html", data)

	return nil
}
