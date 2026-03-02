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
		PageData:      PageData{Title: "Step Functions", ActiveTab: "stepfunctions",
		Snippet: &SnippetData{
			ID:    "stepfunctions-operations",
			Title: "Using Stepfunctions",
			Cli:   "aws stepfunctions help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Stepfunctions */",
			Python: "# Write boto3 code for Stepfunctions\nimport boto3\nclient = boto3.client('stepfunctions', endpoint_url='http://localhost:8000')",
		},},
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
		PageData:     PageData{Title: "State Machine: " + sm.Name, ActiveTab: "stepfunctions",
		Snippet: &SnippetData{
			ID:    "stepfunctions-operations",
			Title: "Using Stepfunctions",
			Cli:   "aws stepfunctions help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Stepfunctions */",
			Python: `# Write boto3 code for Stepfunctions
import boto3
client = boto3.client('stepfunctions', endpoint_url='http://localhost:8000')`,
		},},
		StateMachine: sm,
		Executions:   executions,
	}

	h.renderTemplate(c.Response(), "stepfunctions/statemachine_detail.html", data)

	return nil
}

func (h *DashboardHandler) stepFunctionsExecutionDetail(c *echo.Context) error {
	if h.StepFunctionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	execArn := c.Request().URL.Query().Get("arn")

	exec, err := h.StepFunctionsOps.Backend.DescribeExecution(execArn)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	events, _, err := h.StepFunctionsOps.Backend.GetExecutionHistory(execArn, "", 0, false)
	if err != nil {
		h.Logger.Warn("failed to get execution history", "arn", execArn, "err", err)
	}

	data := struct {
		PageData

		Execution *sfnbackend.Execution
		Events    []sfnbackend.HistoryEvent
	}{
		PageData:  PageData{Title: "Execution: " + exec.Name, ActiveTab: "stepfunctions",
		Snippet: &SnippetData{
			ID:    "stepfunctions-operations",
			Title: "Using Stepfunctions",
			Cli:   "aws stepfunctions help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Stepfunctions */",
			Python: `# Write boto3 code for Stepfunctions
import boto3
client = boto3.client('stepfunctions', endpoint_url='http://localhost:8000')`,
		},},
		Execution: exec,
		Events:    events,
	}

	h.renderTemplate(c.Response(), "stepfunctions/execution_detail.html", data)

	return nil
}
