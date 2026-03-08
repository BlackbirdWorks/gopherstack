package chaos

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// serviceTarget represents a single service's chaos-injectable surface.
type serviceTarget struct {
	Name       string   `json:"name"`
	Operations []string `json:"operations"`
	Regions    []string `json:"regions"`
}

// targetsResponse is the JSON payload returned by the GET /targets endpoint.
type targetsResponse struct {
	Services []serviceTarget `json:"services"`
}

// RegisterRoutes mounts the chaos REST API under the /_gopherstack/chaos prefix.
//
//   - GET    /_gopherstack/chaos/faults    — return current fault rules
//   - POST   /_gopherstack/chaos/faults    — replace entire fault configuration
//   - PATCH  /_gopherstack/chaos/faults    — append rules to existing configuration
//   - DELETE /_gopherstack/chaos/faults    — remove matching rules
//   - GET    /_gopherstack/chaos/effects   — return current network effect settings
//   - POST   /_gopherstack/chaos/effects   — update network effect configuration
//   - GET    /_gopherstack/chaos/targets   — return auto-discovered injectable targets
//   - GET    /_gopherstack/chaos/activity  — return recent fault injection events
func RegisterRoutes(group *echo.Group, store *FaultStore, registry *service.Registry) {
	h := &apiHandler{store: store, registry: registry}

	group.GET("/faults", h.getFaults)
	group.POST("/faults", h.postFaults)
	group.PATCH("/faults", h.patchFaults)
	group.DELETE("/faults", h.deleteFaults)
	group.GET("/effects", h.getEffects)
	group.POST("/effects", h.postEffects)
	group.GET("/targets", h.getTargets)
	group.GET("/activity", h.getActivity)
}

// apiHandler handles all chaos API endpoints.
type apiHandler struct {
	store    *FaultStore
	registry *service.Registry
}

// getFaults returns the current fault rules as JSON.
func (h *apiHandler) getFaults(c *echo.Context) error {
	rules := h.store.GetRules()
	if rules == nil {
		rules = []FaultRule{}
	}

	return c.JSON(http.StatusOK, rules)
}

// postFaults replaces the entire fault configuration.
func (h *apiHandler) postFaults(c *echo.Context) error {
	var rules []FaultRule
	if err := json.NewDecoder(c.Request().Body).Decode(&rules); err != nil {
		return c.String(http.StatusBadRequest, "invalid JSON: "+err.Error())
	}

	h.store.SetRules(rules)

	return c.JSON(http.StatusOK, rules)
}

// patchFaults appends rules to the existing fault configuration.
func (h *apiHandler) patchFaults(c *echo.Context) error {
	var rules []FaultRule
	if err := json.NewDecoder(c.Request().Body).Decode(&rules); err != nil {
		return c.String(http.StatusBadRequest, "invalid JSON: "+err.Error())
	}

	h.store.AppendRules(rules)

	return c.JSON(http.StatusOK, h.store.GetRules())
}

// deleteFaults removes rules matching the provided list.
func (h *apiHandler) deleteFaults(c *echo.Context) error {
	var rules []FaultRule
	if err := json.NewDecoder(c.Request().Body).Decode(&rules); err != nil {
		return c.String(http.StatusBadRequest, "invalid JSON: "+err.Error())
	}

	h.store.DeleteRules(rules)

	return c.JSON(http.StatusOK, h.store.GetRules())
}

// getEffects returns the current network effect settings.
func (h *apiHandler) getEffects(c *echo.Context) error {
	return c.JSON(http.StatusOK, h.store.GetEffects())
}

// postEffects updates the network effect configuration.
func (h *apiHandler) postEffects(c *echo.Context) error {
	var effects NetworkEffects
	if err := json.NewDecoder(c.Request().Body).Decode(&effects); err != nil {
		return c.String(http.StatusBadRequest, "invalid JSON: "+err.Error())
	}

	h.store.SetEffects(effects)

	return c.JSON(http.StatusOK, effects)
}

// getTargets returns all chaos-injectable targets discovered from the registry.
// Entries that share the same ChaosServiceName are merged so that the response
// contains one entry per logical AWS service, with all operations and regions
// combined. This prevents duplicate entries when multiple handlers share the
// same SigV4 signing service name (e.g. S3 and S3 Control both sign as "s3").
func (h *apiHandler) getTargets(c *echo.Context) error {
	// Use an ordered key list so the response is deterministic.
	var order []string

	type mergedTarget struct {
		opSeen     map[string]bool
		regionSeen map[string]bool
		name       string
		ops        []string
		regions    []string
	}

	merged := make(map[string]*mergedTarget)

	for _, entry := range h.registry.GetAll() {
		cp, ok := entry.Registerable.(service.ChaosProvider)
		if !ok {
			continue
		}

		name := cp.ChaosServiceName()

		t, exists := merged[name]
		if !exists {
			t = &mergedTarget{
				name:       name,
				opSeen:     make(map[string]bool),
				regionSeen: make(map[string]bool),
			}
			merged[name] = t
			order = append(order, name)
		}

		for _, op := range cp.ChaosOperations() {
			if !t.opSeen[op] {
				t.opSeen[op] = true
				t.ops = append(t.ops, op)
			}
		}

		for _, r := range cp.ChaosRegions() {
			if !t.regionSeen[r] {
				t.regionSeen[r] = true
				t.regions = append(t.regions, r)
			}
		}
	}

	targets := make([]serviceTarget, 0, len(order))
	for _, name := range order {
		mt := merged[name]
		targets = append(targets, serviceTarget{
			Name:       mt.name,
			Operations: mt.ops,
			Regions:    mt.regions,
		})
	}

	return c.JSON(http.StatusOK, targetsResponse{Services: targets})
}

// getActivity returns the recent fault injection activity log.
func (h *apiHandler) getActivity(c *echo.Context) error {
	events := h.store.GetActivity()
	if events == nil {
		events = []ActivityEvent{}
	}

	return c.JSON(http.StatusOK, events)
}
