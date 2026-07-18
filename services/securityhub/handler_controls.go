package securityhub

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

func classifySecurityControlsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == "/securityControl/definition":
		return opGetSecurityControlDefinition, ""
	case method == http.MethodGet && path == "/securityControls/definitions":
		return opListSecurityControlDefinitions, ""
	case method == http.MethodPost && path == "/securityControls/batchGet":
		return opBatchGetSecurityControls, ""
	case method == http.MethodPatch && path == "/securityControl/update":
		return opUpdateSecurityControl, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleGetSecurityControlDefinition(c *echo.Context) error {
	secCtlID := c.QueryParam("SecurityControlId")

	def, err := h.Backend.GetSecurityControlDefinition(secCtlID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Security control not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SecurityControlDefinition": securityControlDefinitionToMap(def),
	})
}

func (h *Handler) handleListSecurityControlDefinitions(c *echo.Context) error {
	standardsArn := c.QueryParam("StandardsArn")
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	defs, nextOut := h.Backend.ListSecurityControlDefinitions(standardsArn, nextToken, maxResults)
	items := make([]map[string]any, len(defs))

	for i, d := range defs {
		items[i] = securityControlDefinitionToMap(d)
	}

	resp := map[string]any{"SecurityControlDefinitions": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func securityControlDefinitionToMap(d *SecurityControlDefinition) map[string]any {
	return map[string]any{
		keySecurityControlID:        d.SecurityControlID,
		keyTitle:                    d.Title,
		keyDescription:              d.Description,
		keyRemediationURL:           d.RemediationURL,
		keySeverityRating:           d.SeverityRating,
		"CurrentRegionAvailability": d.CurrentRegionAvailability,
		"CustomizableProperties":    d.CustomizableProperties,
		"ParameterDefinitions":      d.ParameterDefinitions,
	}
}

func (h *Handler) handleBatchGetSecurityControls(c *echo.Context, body map[string]any) error {
	rawIDs, _ := body["SecurityControlIds"].([]any)

	var ids []string

	for _, id := range rawIDs {
		if s, ok := id.(string); ok {
			ids = append(ids, s)
		}
	}

	controls, unprocessed := h.Backend.BatchGetSecurityControls(ids)
	items := make([]map[string]any, len(controls))

	for i, ctrl := range controls {
		items[i] = map[string]any{
			keySecurityControlID:    ctrl.SecurityControlID,
			"SecurityControlArn":    ctrl.SecurityControlArn,
			keyTitle:                ctrl.Title,
			keyDescription:          ctrl.Description,
			keyRemediationURL:       ctrl.RemediationURL,
			keySeverityRating:       ctrl.SeverityRating,
			"SecurityControlStatus": ctrl.SecurityControlStatus,
			"UpdateStatus":          ctrl.UpdateStatus,
			"Parameters":            ctrl.Parameters,
			"LastUpdateReason":      ctrl.LastUpdateReason,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SecurityControls": items,
		"UnprocessedIds":   unprocessed,
	})
}

func (h *Handler) handleUpdateSecurityControl(c *echo.Context, body map[string]any) error {
	secCtlID, _ := body["SecurityControlId"].(string)
	parameters, _ := body["Parameters"].(map[string]any)
	lastUpdateReason, _ := body["LastUpdateReason"].(string)

	if err := h.Backend.UpdateSecurityControl(secCtlID, parameters, lastUpdateReason); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// controlsOpHandlers returns the Security Controls operation dispatch table
// for handleREST.
func (h *Handler) controlsOpHandlers(c *echo.Context, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opGetSecurityControlDefinition:   func() error { return h.handleGetSecurityControlDefinition(c) },
		opListSecurityControlDefinitions: func() error { return h.handleListSecurityControlDefinitions(c) },
		opBatchGetSecurityControls:       func() error { return h.handleBatchGetSecurityControls(c, body) },
		opUpdateSecurityControl:          func() error { return h.handleUpdateSecurityControl(c, body) },
	}
}
