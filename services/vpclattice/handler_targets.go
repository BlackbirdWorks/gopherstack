package vpclattice

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// ------- Target handlers -------

func (h *Handler) handleRegisterTargets(c *echo.Context, tgID string, body map[string]any) error {
	targets := extractTargets(body)

	failures, err := h.Backend.RegisterTargets(tgID, targets)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySuccessful:   successfulTargetsJSON(targets, failures),
		keyUnsuccessful: failureListJSON(failures),
	})
}

func (h *Handler) handleDeregisterTargets(c *echo.Context, tgID string, body map[string]any) error {
	targets := extractTargets(body)

	failures, err := h.Backend.DeregisterTargets(tgID, targets)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySuccessful:   successfulTargetsJSON(targets, failures),
		keyUnsuccessful: failureListJSON(failures),
	})
}

func (h *Handler) handleListTargets(c *echo.Context, tgID string, body map[string]any) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")

	// parse target filters from body
	var filters []Target
	if rawTargets, ok := body["targets"].([]any); ok {
		for _, rt := range rawTargets {
			if fm, ok2 := rt.(map[string]any); ok2 {
				var f Target
				f.ID, _ = fm["id"].(string)
				if p, ok3 := fm["port"].(float64); ok3 {
					f.Port = int32(p)
				}
				filters = append(filters, f)
			}
		}
	}

	items, next, err := h.Backend.ListTargets(ctx, tgID, filters, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, t := range items {
		summaries = append(summaries, targetSummaryToJSON(t))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// failureListJSON converts target failures to their JSON representation.
func failureListJSON(failures []*TargetFailure) []any {
	failureList := make([]any, 0, len(failures))
	for _, f := range failures {
		failureList = append(failureList, targetFailureToJSON(f))
	}

	return failureList
}

// successfulTargetsJSON returns the requested targets that are not present in
// failures, in the {id, port} shape AWS uses for RegisterTargets/
// DeregisterTargets "successful" list entries.
func successfulTargetsJSON(targets []*Target, failures []*TargetFailure) []any {
	failed := make(map[string]bool, len(failures))
	for _, f := range failures {
		failed[targetKey(f.ID, f.Port)] = true
	}

	successList := make([]any, 0, len(targets))

	for _, t := range targets {
		if failed[targetKey(t.ID, t.Port)] {
			continue
		}

		successList = append(successList, map[string]any{"id": t.ID, keyPort: t.Port})
	}

	return successList
}

func targetKey(id string, port int32) string {
	return id + "|" + strconv.Itoa(int(port))
}

// ------- Target JSON serialization -------

func targetSummaryToJSON(t *TargetSummary) map[string]any {
	return map[string]any{
		"id":         t.ID,
		keyPort:      t.Port,
		keyStatus:    t.Status,
		"reasonCode": t.ReasonCode,
	}
}

func targetFailureToJSON(f *TargetFailure) map[string]any {
	return map[string]any{
		"id":             f.ID,
		keyPort:          f.Port,
		"failureCode":    f.Code,
		"failureMessage": f.Message,
	}
}

// ------- Target body extraction helpers -------

func extractTargets(body map[string]any) []*Target {
	var targets []*Target

	if raw, ok := body["targets"].([]any); ok {
		for _, tRaw := range raw {
			if tMap, ok2 := tRaw.(map[string]any); ok2 {
				t := &Target{}
				t.ID, _ = tMap["id"].(string)
				t.Port = bodyInt32(tMap, keyPort)
				targets = append(targets, t)
			}
		}
	}

	return targets
}
