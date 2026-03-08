package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

// chaosRuleDisplay is a view model for displaying a fault rule in the dashboard.
type chaosRuleDisplay struct {
	Service     string
	Region      string
	Operation   string
	Probability string
	ErrorCode   string
	StatusCode  int
}

// chaosActivityDisplay is a view model for displaying an activity event in the dashboard.
type chaosActivityDisplay struct {
	Timestamp    string
	Service      string
	Operation    string
	Region       string
	FaultApplied string
	Probability  string
	Triggered    bool
}

// chaosIndexData is the template data for the Chaos Engineering dashboard page.
type chaosIndexData struct {
	PageData

	Rules     []chaosRuleDisplay
	Activity  []chaosActivityDisplay
	LatencyMs int
	JitterMs  int
}

// chaosRulesFragData is the template data for the fault rules fragment.
type chaosRulesFragData struct {
	Rules []chaosRuleDisplay
}

// chaosActivityFragData is the template data for the activity log fragment.
type chaosActivityFragData struct {
	Activity []chaosActivityDisplay
}

// setupChaosRoutes registers all Chaos Engineering dashboard routes.
func (h *DashboardHandler) setupChaosRoutes() {
	h.SubRouter.GET("/dashboard/chaos", h.chaosIndex)
	h.SubRouter.GET("/dashboard/chaos/rules", h.chaosRulesFragment)
	h.SubRouter.GET("/dashboard/chaos/activity", h.chaosActivityFragment)
	h.SubRouter.POST("/dashboard/chaos/faults", h.chaosAddFault)
	h.SubRouter.DELETE("/dashboard/chaos/faults", h.chaosDeleteFault)
	h.SubRouter.POST("/dashboard/chaos/faults/clear", h.chaosClearFaults)
	h.SubRouter.POST("/dashboard/chaos/effects", h.chaosSetEffects)
	h.SubRouter.POST("/dashboard/chaos/effects/reset", h.chaosResetEffects)
}

// chaosIndex renders the main Chaos Engineering dashboard page.
func (h *DashboardHandler) chaosIndex(c *echo.Context) error {
	if h.FaultStore == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	effects := h.FaultStore.GetEffects()
	data := chaosIndexData{
		PageData: PageData{
			Title:     "Chaos Engineering",
			ActiveTab: "chaos",
			Snippet: &SnippetData{
				ID:    "chaos-operations",
				Title: "Using Chaos API",
				Cli: `# Add a fault rule
curl -X PATCH http://localhost:8000/_gopherstack/chaos/faults \
  -H 'Content-Type: application/json' \
  -d '[{"service":"s3","probability":1.0,"error":{"statusCode":503,"code":"ServiceUnavailable"}}]'`,
				Go: `// Use the Gopherstack Chaos REST API directly
resp, err := http.Post("http://localhost:8000/_gopherstack/chaos/faults", ...)`,
				Python: `# Gopherstack Chaos API
import requests
requests.patch("http://localhost:8000/_gopherstack/chaos/faults",
    json=[{"service": "s3", "probability": 1.0,
           "error": {"statusCode": 503, "code": "ServiceUnavailable"}}])`,
			},
		},
		Rules:     toRuleDisplayList(h.FaultStore.GetRules()),
		Activity:  toActivityDisplayList(h.FaultStore.GetActivity()),
		LatencyMs: effects.Latency,
		JitterMs:  effects.Jitter,
	}

	h.renderTemplate(c.Response(), "chaos/index.html", data)

	return nil
}

// chaosRulesFragment renders the fault rules table fragment for HTMX polling.
func (h *DashboardHandler) chaosRulesFragment(c *echo.Context) error {
	if h.FaultStore == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	data := chaosRulesFragData{
		Rules: toRuleDisplayList(h.FaultStore.GetRules()),
	}

	h.renderFragment(c.Response(), "chaos/rules_fragment.html", data)

	return nil
}

// chaosActivityFragment renders the activity log fragment for HTMX polling.
func (h *DashboardHandler) chaosActivityFragment(c *echo.Context) error {
	if h.FaultStore == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	data := chaosActivityFragData{
		Activity: toActivityDisplayList(h.FaultStore.GetActivity()),
	}

	h.renderFragment(c.Response(), "chaos/activity_fragment.html", data)

	return nil
}

// chaosAddFault adds a new fault rule from a form POST.
func (h *DashboardHandler) chaosAddFault(c *echo.Context) error {
	if h.FaultStore == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	r := c.Request()

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "invalid form data")
	}

	rule := chaos.FaultRule{
		Service:   r.FormValue("service"),
		Region:    r.FormValue("region"),
		Operation: r.FormValue("operation"),
	}

	prob := parseFloat(r.FormValue("probability"), 1.0)
	rule.Probability = prob / 100.0 //nolint:mnd // convert percentage to fraction

	errorCode := r.FormValue("errorCode")
	statusCode := parseInt(r.FormValue("statusCode"), http.StatusServiceUnavailable)

	if errorCode != "" {
		rule.Error = &chaos.FaultError{
			Code:       errorCode,
			StatusCode: statusCode,
		}
	}

	h.FaultStore.AppendRules([]chaos.FaultRule{rule})

	// Return refreshed rules fragment.
	data := chaosRulesFragData{
		Rules: toRuleDisplayList(h.FaultStore.GetRules()),
	}

	h.renderFragment(c.Response(), "chaos/rules_fragment.html", data)

	return nil
}

// chaosDeleteFault deletes the fault rule matching the JSON body.
func (h *DashboardHandler) chaosDeleteFault(c *echo.Context) error {
	if h.FaultStore == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	var rule chaos.FaultRule
	if err := json.NewDecoder(c.Request().Body).Decode(&rule); err != nil {
		return c.String(http.StatusBadRequest, "invalid JSON: "+err.Error())
	}

	h.FaultStore.DeleteRules([]chaos.FaultRule{rule})

	data := chaosRulesFragData{
		Rules: toRuleDisplayList(h.FaultStore.GetRules()),
	}

	h.renderFragment(c.Response(), "chaos/rules_fragment.html", data)

	return nil
}

// chaosClearFaults removes all fault rules.
func (h *DashboardHandler) chaosClearFaults(c *echo.Context) error {
	if h.FaultStore == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	h.FaultStore.SetRules([]chaos.FaultRule{})

	data := chaosRulesFragData{}

	h.renderFragment(c.Response(), "chaos/rules_fragment.html", data)

	return nil
}

// chaosSetEffects updates the network effects from a form POST.
func (h *DashboardHandler) chaosSetEffects(c *echo.Context) error {
	if h.FaultStore == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	r := c.Request()

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "invalid form data")
	}

	latency := parseInt(r.FormValue("latency"), 0)
	jitter := parseInt(r.FormValue("jitter"), 0)

	h.FaultStore.SetEffects(chaos.NetworkEffects{
		Latency: latency,
		Jitter:  jitter,
	})

	return c.HTML(http.StatusOK, renderEffectsIndicator(latency, jitter))
}

// chaosResetEffects resets network effects to zero.
func (h *DashboardHandler) chaosResetEffects(c *echo.Context) error {
	if h.FaultStore == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	h.FaultStore.SetEffects(chaos.NetworkEffects{})

	return c.HTML(http.StatusOK, renderEffectsIndicator(0, 0))
}

// renderEffectsIndicator returns a small HTML snippet showing the current latency setting.
func renderEffectsIndicator(latency, jitter int) string {
	if latency == 0 && jitter == 0 {
		noDelayCls := `inline-flex items-center gap-1 px-2 py-1 rounded-full ` +
			`text-xs font-medium bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400`
		dotCls := `w-1.5 h-1.5 rounded-full bg-slate-400`

		return fmt.Sprintf(`<span class="%s"><span class="%s"></span>No delay active</span>`,
			noDelayCls, dotCls)
	}

	label := fmt.Sprintf("%dms latency", latency)
	if jitter > 0 {
		label += fmt.Sprintf(" ±%dms jitter", jitter)
	}

	activeCls := `inline-flex items-center gap-1 px-2 py-1 rounded-full ` +
		`text-xs font-medium bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400`
	dotCls := `w-1.5 h-1.5 rounded-full bg-amber-500 animate-pulse`

	return fmt.Sprintf(`<span class="%s"><span class="%s"></span>%s</span>`,
		activeCls, dotCls, label)
}

// toRuleDisplayList converts fault rules to display models.
func toRuleDisplayList(rules []chaos.FaultRule) []chaosRuleDisplay {
	result := make([]chaosRuleDisplay, 0, len(rules))

	for _, r := range rules {
		d := chaosRuleDisplay{
			Service:   orStar(r.Service),
			Region:    orStar(r.Region),
			Operation: orStar(r.Operation),
		}

		if r.Probability <= 0 || r.Probability >= 1.0 {
			d.Probability = "100%"
		} else {
			d.Probability = fmt.Sprintf("%.0f%%", r.Probability*100) //nolint:mnd // percentage display
		}

		if r.Error != nil {
			d.ErrorCode = r.Error.Code
			d.StatusCode = r.Error.StatusCode
		} else {
			d.ErrorCode = "ServiceUnavailable"
			d.StatusCode = http.StatusServiceUnavailable
		}

		result = append(result, d)
	}

	return result
}

// toActivityDisplayList converts activity events to display models.
func toActivityDisplayList(events []chaos.ActivityEvent) []chaosActivityDisplay {
	result := make([]chaosActivityDisplay, 0, len(events))

	for _, e := range events {
		d := chaosActivityDisplay{
			Timestamp:    e.Timestamp.Format("15:04:05"),
			Service:      orDash(e.Service),
			Operation:    orDash(e.Operation),
			Region:       orDash(e.Region),
			FaultApplied: e.FaultApplied,
			Triggered:    e.Triggered,
		}

		if e.Probability <= 0 || e.Probability >= 1.0 {
			d.Probability = "100%"
		} else {
			d.Probability = fmt.Sprintf("%.0f%%", e.Probability*100) //nolint:mnd // percentage display
		}

		result = append(result, d)
	}

	return result
}

// orStar returns "*" when s is empty (meaning "match any").
func orStar(s string) string {
	if s == "" {
		return "*"
	}

	return s
}

// orDash returns "-" when s is empty.
func orDash(s string) string {
	if s == "" {
		return "-"
	}

	return s
}

// parseFloat parses a float64 from a string, returning fallback on error.
func parseFloat(s string, fallback float64) float64 {
	var v float64
	if _, err := fmt.Sscanf(s, "%f", &v); err != nil {
		return fallback
	}

	return v
}

// parseInt parses an int from a string, returning fallback on error.
func parseInt(s string, fallback int) int {
	var v int
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		return fallback
	}

	return v
}
