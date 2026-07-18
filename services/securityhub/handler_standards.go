package securityhub

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyStandardsPath(method, path string) (string, string) {
	if strings.HasPrefix(path, pathAssociations) {
		return classifyAssociationsPath(method, path)
	}

	switch {
	case method == http.MethodPost && path == "/standards/register":
		return opBatchEnableStandards, ""
	case method == http.MethodPost && path == "/standards/deregister":
		return opBatchDisableStandards, ""
	case method == http.MethodPost && path == "/standards/get":
		return opGetEnabledStandards, ""
	case method == http.MethodGet && path == "/standards":
		return opDescribeStandards, ""
	case strings.HasPrefix(path, "/standards/controls/") && method == http.MethodGet:
		return opDescribeStdControls, strings.TrimPrefix(path, "/standards/controls/")
	case strings.HasPrefix(path, "/standards/control/") && method == http.MethodPatch:
		return opUpdateStdControl, strings.TrimPrefix(path, "/standards/control/")
	}

	return opUnknown, ""
}

func classifyAssociationsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == pathAssociations:
		return opListStdCtlAssocs, ""
	case method == http.MethodPost && path == pathAssociations+"/batchGet":
		return opBatchGetStdCtlAssocs, ""
	case method == http.MethodPatch && path == pathAssociations:
		return opBatchUpdateStdCtlAssocs, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleBatchEnableStandards(c *echo.Context, body map[string]any) error {
	rawRequests, _ := body["StandardsSubscriptionRequests"].([]any)

	var requests []map[string]any

	for _, r := range rawRequests {
		if m, ok := r.(map[string]any); ok {
			requests = append(requests, m)
		}
	}

	subscriptions, _ := h.Backend.BatchEnableStandards(requests)
	items := standardsSubscriptionsToMaps(subscriptions)

	return c.JSON(http.StatusOK, map[string]any{
		keyStandardsSubscriptions: items,
	})
}

func (h *Handler) handleBatchDisableStandards(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["StandardsSubscriptionArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	subscriptions, _ := h.Backend.BatchDisableStandards(arns)
	items := standardsSubscriptionsToMaps(subscriptions)

	return c.JSON(http.StatusOK, map[string]any{
		keyStandardsSubscriptions: items,
	})
}

func (h *Handler) handleGetEnabledStandards(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["StandardsSubscriptionArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	subscriptions, nextOut := h.Backend.GetEnabledStandards(arns, nextToken, maxResults)
	items := standardsSubscriptionsToMaps(subscriptions)

	resp := map[string]any{keyStandardsSubscriptions: items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func standardsSubscriptionsToMaps(subs []*StandardsSubscription) []map[string]any {
	items := make([]map[string]any, len(subs))

	for i, s := range subs {
		items[i] = map[string]any{
			"StandardsSubscriptionArn": s.StandardsSubscriptionArn,
			keyStandardsArn:            s.StandardsArn,
			"StandardsInput":           s.StandardsInput,
			"StandardsStatus":          s.StandardsStatus,
			"StatusReason":             s.StatusReason,
		}
	}

	return items
}

func (h *Handler) handleDescribeStandards(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	standards, nextOut := h.Backend.DescribeStandards(nextToken, maxResults)
	items := make([]map[string]any, len(standards))

	for i, s := range standards {
		items[i] = map[string]any{
			keyStandardsArn:    s.StandardsArn,
			keyName:            s.Name,
			keyDescription:     s.Description,
			"EnabledByDefault": s.EnabledByDefault,
		}
	}

	resp := map[string]any{"Standards": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeStandardsControls(c *echo.Context, subscriptionArn string) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	controls, nextOut := h.Backend.DescribeStandardsControls(subscriptionArn, nextToken, maxResults)
	items := make([]map[string]any, len(controls))

	for i, ctrl := range controls {
		items[i] = map[string]any{
			"StandardsControlArn":    ctrl.StandardsControlArn,
			"ControlStatus":          ctrl.ControlStatus,
			"DisabledReason":         ctrl.DisabledReason,
			"ControlStatusUpdatedAt": ctrl.ControlStatusUpdatedAt,
			"ControlId":              ctrl.ControlID,
			keyTitle:                 ctrl.Title,
			keyDescription:           ctrl.Description,
			keyRemediationURL:        ctrl.RemediationURL,
			keySeverityRating:        ctrl.SeverityRating,
			"RelatedRequirements":    ctrl.RelatedRequirements,
		}
	}

	resp := map[string]any{"Controls": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateStandardsControl(c *echo.Context, controlArn string, body map[string]any) error {
	controlStatus, _ := body["ControlStatus"].(string)
	disabledReason, _ := body["DisabledReason"].(string)

	const statusDisabled = "DISABLED"
	if controlStatus != "" && controlStatus != statusEnabled && controlStatus != statusDisabled {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: "ControlStatus must be ENABLED or DISABLED",
		})
	}

	if controlStatus == statusDisabled && disabledReason == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: "DisabledReason is required when disabling a control",
		})
	}

	if err := h.Backend.UpdateStandardsControl(controlArn, controlStatus, disabledReason); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListStandardsControlAssociations(c *echo.Context) error {
	secCtlID := c.QueryParam("SecurityControlId")
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	assocs, nextOut := h.Backend.ListStandardsControlAssociations(secCtlID, nextToken, maxResults)
	items := standardsControlAssociationsToMaps(assocs)

	resp := map[string]any{"StandardsControlAssociationSummaries": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchGetStdCtlAssociations(c *echo.Context, body map[string]any) error {
	rawRequests, _ := body["StandardsControlAssociationIds"].([]any)

	var requests []map[string]any

	for _, r := range rawRequests {
		if m, ok := r.(map[string]any); ok {
			requests = append(requests, m)
		}
	}

	assocs, unprocessed := h.Backend.BatchGetStandardsControlAssociations(requests)
	items := standardsControlAssociationsToMaps(assocs)

	return c.JSON(http.StatusOK, map[string]any{
		"StandardsControlAssociationDetails": items,
		"UnprocessedAssociations":            unprocessed,
	})
}

func (h *Handler) handleBatchUpdateStdCtlAssociations(c *echo.Context, body map[string]any) error {
	rawUpdates, _ := body["StandardsControlAssociationUpdates"].([]any)

	var updates []map[string]any

	for _, u := range rawUpdates {
		if m, ok := u.(map[string]any); ok {
			updates = append(updates, m)
		}
	}

	unprocessed, err := h.Backend.BatchUpdateStandardsControlAssociations(updates)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"UnprocessedAssociationUpdates": unprocessed,
	})
}

func standardsControlAssociationsToMaps(assocs []*StandardsControlAssociation) []map[string]any {
	items := make([]map[string]any, len(assocs))

	for i, a := range assocs {
		items[i] = map[string]any{
			keySecurityControlID:          a.SecurityControlID,
			keyStandardsArn:               a.StandardsArn,
			"AssociationStatus":           a.AssociationStatus,
			keyUpdatedAt:                  a.UpdatedAt,
			"RelatedRequirements":         a.RelatedRequirements,
			"StandardsControlTitle":       a.StandardsControlTitle,
			"StandardsControlDescription": a.StandardsControlDescription,
			"StandardsControlArns":        a.StandardsControlArns,
			"UpdatedReason":               a.UpdatedReason,
		}
	}

	return items
}

// standardsOpHandlers returns the Standards operation dispatch table for
// handleREST.
func (h *Handler) standardsOpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opBatchEnableStandards:    func() error { return h.handleBatchEnableStandards(c, body) },
		opBatchDisableStandards:   func() error { return h.handleBatchDisableStandards(c, body) },
		opGetEnabledStandards:     func() error { return h.handleGetEnabledStandards(c, body) },
		opDescribeStandards:       func() error { return h.handleDescribeStandards(c) },
		opDescribeStdControls:     func() error { return h.handleDescribeStandardsControls(c, resource) },
		opUpdateStdControl:        func() error { return h.handleUpdateStandardsControl(c, resource, body) },
		opListStdCtlAssocs:        func() error { return h.handleListStandardsControlAssociations(c) },
		opBatchGetStdCtlAssocs:    func() error { return h.handleBatchGetStdCtlAssociations(c, body) },
		opBatchUpdateStdCtlAssocs: func() error { return h.handleBatchUpdateStdCtlAssociations(c, body) },
	}
}
