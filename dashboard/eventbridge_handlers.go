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
		PageData: PageData{Title: "EventBridge", ActiveTab: "eventbridge"},
		Buses:    buses,
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
		PageData: PageData{Title: "EventBridge Rules", ActiveTab: "eventbridge"},
		BusName:  busName,
		Rules:    rules,
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
		PageData: PageData{Title: "EventBridge Event Log", ActiveTab: "eventbridge"},
		Events:   log,
	}

	h.renderTemplate(c.Response(), "eventbridge/event_log.html", data)

	return nil
}
