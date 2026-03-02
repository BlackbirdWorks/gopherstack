package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
)

func (h *DashboardHandler) eventBridgeIndex(c *echo.Context) error {
	if h.EventBridgeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	buses, _, _ := h.EventBridgeOps.Backend.ListEventBuses("", "")
	data := struct {
		PageData

		Buses []ebbackend.EventBus
	}{
		PageData: PageData{Title: "EventBridge", ActiveTab: "eventbridge",
			Snippet: &SnippetData{
				ID:    "eventbridge-operations",
				Title: "Using Eventbridge",
				Cli:   "aws eventbridge help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Eventbridge */",
				Python: "# Write boto3 code for Eventbridge\nimport boto3\n" +
					"client = boto3.client('events', endpoint_url='http://localhost:8000')",
			}},
		Buses: buses,
	}

	h.renderTemplate(c.Response(), "eventbridge/index.html", data)

	return nil
}

func (h *DashboardHandler) eventBridgeRules(c *echo.Context) error {
	if h.EventBridgeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	busName := c.Request().URL.Query().Get("bus")
	if busName == "" {
		busName = "default"
	}

	rules, _, _ := h.EventBridgeOps.Backend.ListRules(busName, "", "")
	data := struct {
		PageData

		BusName string
		Rules   []ebbackend.Rule
	}{
		PageData: PageData{Title: "EventBridge Rules", ActiveTab: "eventbridge",
			Snippet: &SnippetData{
				ID:    "eventbridge-operations",
				Title: "Using Eventbridge",
				Cli:   "aws eventbridge help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Eventbridge */",
				Python: `# Write boto3 code for Eventbridge
import boto3
client = boto3.client('eventbridge', endpoint_url='http://localhost:8000')`,
			}},
		BusName: busName,
		Rules:   rules,
	}

	h.renderTemplate(c.Response(), "eventbridge/rules.html", data)

	return nil
}

func (h *DashboardHandler) eventBridgeEventLog(c *echo.Context) error {
	if h.EventBridgeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	log := h.EventBridgeOps.Backend.GetEventLog()
	data := struct {
		PageData

		Events []ebbackend.EventLogEntry
	}{
		PageData: PageData{Title: "EventBridge Event Log", ActiveTab: "eventbridge",
			Snippet: &SnippetData{
				ID:    "eventbridge-operations",
				Title: "Using Eventbridge",
				Cli:   "aws eventbridge help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Eventbridge */",
				Python: `# Write boto3 code for Eventbridge
import boto3
client = boto3.client('eventbridge', endpoint_url='http://localhost:8000')`,
			}},
		Events: log,
	}

	h.renderTemplate(c.Response(), "eventbridge/event_log.html", data)

	return nil
}
