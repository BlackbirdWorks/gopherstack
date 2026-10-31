package dashboard

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ociEventsPrefix     = ociProxyPrefix + "/events"
	ociEventsAPIPrefix  = "/20181201"
	ociEventsDeliveries = "/_gopherstack/oci/events/deliveries"
	ociEventsMaxActions = 10
	ociEventsItemSegs   = 2
	ociEventsEnabledKey = "isEnabled"
)

// registerOCIEventsRoutes serves the Events page by dispatching in-process to the emulator's handler without
// request signing. Reachable: rule list/get/create/delete, an enable toggle and the delivery log. Every write
// forwards a body rebuilt from a fixed set of fields.
func (h *DashboardHandler) registerOCIEventsRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	if ops := h.config.OCIEventsOps; ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI Events", next) }

	h.SubRouter.Add(http.MethodGet, ociEventsPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	h.SubRouter.Add(http.MethodGet, ociEventsPrefix+"/deliveries", guard(func(c *echo.Context) error {
		return ociDispatch(c, inner, ociEventsDeliveries, nil)
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociEventsPrefix+"/api")

		parts, ok := ociSplitPath(raw)
		if !ok || !ociEventsAllowed(r.Method, parts) {
			return ociInvalid(c, "unsupported Events proxy request")
		}

		body, ok := ociEventsBody(c, r.Method)
		if !ok {
			return ociInvalid(c, "invalid Events proxy request body")
		}

		return ociDispatch(c, inner, ociEventsAPIPrefix+raw, body)
	})

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(m, ociEventsPrefix+"/api/*", serve)
	}
}

func ociEventsAllowed(method string, parts []string) bool {
	if parts[0] != ociKeyRules {
		return false
	}

	switch len(parts) {
	case 1:
		return method == http.MethodGet || method == http.MethodPost
	case ociEventsItemSegs:
		return slices.Contains([]string{http.MethodGet, http.MethodPut, http.MethodDelete}, method)
	}

	return false
}

// ociEventsBody builds the forwarded body of a write; nil keeps the (empty) original.
func ociEventsBody(c *echo.Context, method string) ([]byte, bool) {
	switch method {
	case http.MethodPost:
		return ociEventsCreateBody(c)
	case http.MethodPut:
		return ociEventsToggleBody(c)
	}

	return nil, true
}

type ociEventsActionIn struct {
	IsEnabled   *bool  `json:"isEnabled"`
	ActionType  string `json:"actionType"`
	Description string `json:"description"`
	TopicID     string `json:"topicId"`
	StreamID    string `json:"streamId"`
	FunctionID  string `json:"functionId"`
}

func (a ociEventsActionIn) rebuild() (map[string]any, bool) {
	if a.IsEnabled == nil {
		return nil, false
	}

	out := map[string]any{"actionType": a.ActionType, ociEventsEnabledKey: *a.IsEnabled}
	if a.Description != "" {
		out["description"] = a.Description
	}

	switch a.ActionType {
	case "ONS":
		out["topicId"] = a.TopicID
	case "OSS":
		out["streamId"] = a.StreamID
	case "FAAS":
		out["functionId"] = a.FunctionID
	default:
		return nil, false
	}

	return out, true
}

func ociEventsCreateBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		IsEnabled     *bool  `json:"isEnabled"`
		CompartmentID string `json:"compartmentId"`
		DisplayName   string `json:"displayName"`
		Description   string `json:"description"`
		Condition     string `json:"condition"`
		Actions       struct {
			Actions []ociEventsActionIn `json:"actions"`
		} `json:"actions"`
	}

	if !ociDecodeBody(c, &in) || in.CompartmentID == "" || in.DisplayName == "" || in.IsEnabled == nil ||
		len(in.Actions.Actions) > ociEventsMaxActions {
		return nil, false
	}

	acts := make([]map[string]any, 0, len(in.Actions.Actions))

	for _, a := range in.Actions.Actions {
		m, ok := a.rebuild()
		if !ok {
			return nil, false
		}

		acts = append(acts, m)
	}

	out := map[string]any{
		ociKeyCompartment: in.CompartmentID, "displayName": in.DisplayName, "condition": in.Condition,
		ociEventsEnabledKey: *in.IsEnabled, "actions": map[string]any{"actions": acts},
	}
	if in.Description != "" {
		out["description"] = in.Description
	}

	b, err := json.Marshal(out)

	return b, err == nil
}

func ociEventsToggleBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		IsEnabled *bool `json:"isEnabled"`
	}

	if !ociDecodeBody(c, &in) || in.IsEnabled == nil {
		return nil, false
	}

	b, err := json.Marshal(map[string]any{ociEventsEnabledKey: *in.IsEnabled})

	return b, err == nil
}
