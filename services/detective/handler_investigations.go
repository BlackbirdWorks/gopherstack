package detective

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleStartInvestigation(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn       string `json:"GraphArn"`
		EntityArn      string `json:"EntityArn"`
		ScopeStartTime string `json:"ScopeStartTime"`
		ScopeEndTime   string `json:"ScopeEndTime"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.EntityArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "EntityArn is required"))
	}

	if req.ScopeStartTime == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "ScopeStartTime is required"))
	}

	if req.ScopeEndTime == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "ScopeEndTime is required"))
	}

	scopeStart, parseErr := parseTime(req.ScopeStartTime)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid ScopeStartTime"))
	}

	scopeEnd, parseErr := parseTime(req.ScopeEndTime)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid ScopeEndTime"))
	}

	id, startErr := h.Backend.StartInvestigation(req.GraphArn, req.EntityArn, scopeStart, scopeEnd)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"InvestigationId": id, //nolint:goconst // existing issue.
	})
}

func (h *Handler) handleGetInvestigation(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn        string `json:"GraphArn"`
		InvestigationId string `json:"InvestigationId"` //nolint:revive,staticcheck // existing issue.
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.InvestigationId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "InvestigationId is required"))
	}

	inv, getErr := h.Backend.GetInvestigation(req.GraphArn, req.InvestigationId)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCreatedTime:    inv.CreatedTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		"EntityArn":       inv.EntityARN,
		"EntityType":      inv.EntityType,
		keyGraphArn:       inv.GraphARN,
		"InvestigationId": inv.InvestigationID,
		"ScopeEndTime":    inv.ScopeEndTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		"ScopeStartTime":  inv.ScopeStartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		"Severity":        inv.Severity,
		"State":           inv.State,
		keyStatusField:    inv.Status,
	})
}

func (h *Handler) handleListInvestigations(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string `json:"GraphArn"`
		NextToken  string `json:"NextToken"`
		MaxResults int32  `json:"MaxResults"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	investigations, nextToken, listErr := h.Backend.ListInvestigations(req.GraphArn, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	details := make([]map[string]any, 0, len(investigations))
	for _, inv := range investigations {
		details = append(details, map[string]any{
			keyCreatedTime:    inv.CreatedTime.UTC().Format("2006-01-02T15:04:05.000Z"),
			"EntityArn":       inv.EntityARN,
			"EntityType":      inv.EntityType,
			"InvestigationId": inv.InvestigationID,
			"Severity":        inv.Severity,
			"State":           inv.State,
			keyStatusField:    inv.Status,
		})
	}

	resp := map[string]any{
		"InvestigationDetails": details,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateInvestigationState(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn        string `json:"GraphArn"`
		InvestigationId string `json:"InvestigationId"` //nolint:revive,staticcheck // existing issue.
		State           string `json:"State"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.InvestigationId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "InvestigationId is required"))
	}

	if req.State == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "State is required"))
	}

	if updateErr := h.Backend.UpdateInvestigationState(req.GraphArn, req.InvestigationId, req.State); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListIndicators(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn        string `json:"GraphArn"`
		InvestigationId string `json:"InvestigationId"` //nolint:revive,staticcheck // existing issue.
		IndicatorType   string `json:"IndicatorType"`
		NextToken       string `json:"NextToken"`
		MaxResults      int32  `json:"MaxResults"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.InvestigationId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "InvestigationId is required"))
	}

	indicators, nextToken, listErr := h.Backend.ListIndicators(
		req.GraphArn, req.InvestigationId, req.IndicatorType, req.MaxResults, req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	indicatorList := make([]map[string]any, 0, len(indicators))
	for _, ind := range indicators {
		indicatorList = append(indicatorList, map[string]any{
			"IndicatorType":   ind.IndicatorType,
			"IndicatorDetail": indicatorDetailToJSON(ind.Detail),
		})
	}

	resp := map[string]any{
		keyGraphArn:       req.GraphArn,
		"InvestigationId": req.InvestigationId,
		"Indicators":      indicatorList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// indicatorDetailToJSON encodes the type-specific sub-detail of an
// IndicatorDetail. Only the single sub-detail matching the Indicator's
// IndicatorType is ever populated (a union, like the real SDK shape), so
// exactly one key appears in the result.
func indicatorDetailToJSON(d IndicatorDetail) map[string]any {
	result := make(map[string]any, 1)

	switch {
	case d.FlaggedIPAddress != nil:
		result["FlaggedIpAddressDetail"] = map[string]any{
			keyIPAddress: d.FlaggedIPAddress.IPAddress,
			keyReason:    d.FlaggedIPAddress.Reason,
		}
	case d.ImpossibleTravel != nil:
		result["ImpossibleTravelDetail"] = map[string]any{
			"StartingIpAddress": d.ImpossibleTravel.StartingIPAddress,
			"StartingLocation":  d.ImpossibleTravel.StartingLocation,
			"EndingIpAddress":   d.ImpossibleTravel.EndingIPAddress,
			"EndingLocation":    d.ImpossibleTravel.EndingLocation,
			"HourlyTimeDelta":   d.ImpossibleTravel.HourlyTimeDelta,
		}
	case d.NewASO != nil:
		result["NewAsoDetail"] = map[string]any{
			"Aso":                    d.NewASO.ASO,
			keyIsNewForEntireAccount: d.NewASO.IsNewForEntireAccount,
		}
	case d.NewGeolocation != nil:
		result["NewGeolocationDetail"] = map[string]any{
			keyIPAddress:             d.NewGeolocation.IPAddress,
			"Location":               d.NewGeolocation.Location,
			keyIsNewForEntireAccount: d.NewGeolocation.IsNewForEntireAccount,
		}
	case d.NewUserAgent != nil:
		result["NewUserAgentDetail"] = map[string]any{
			"UserAgent":              d.NewUserAgent.UserAgent,
			keyIsNewForEntireAccount: d.NewUserAgent.IsNewForEntireAccount,
		}
	case d.RelatedFinding != nil:
		result["RelatedFindingDetail"] = map[string]any{
			"Arn":        d.RelatedFinding.Arn,
			"Type":       d.RelatedFinding.Type,
			keyIPAddress: d.RelatedFinding.IPAddress,
		}
	case d.RelatedFindingGroup != nil:
		result["RelatedFindingGroupDetail"] = map[string]any{
			"Id": d.RelatedFindingGroup.ID,
		}
	case d.TTPsObserved != nil:
		result["TTPsObservedDetail"] = map[string]any{
			"Tactic":          d.TTPsObserved.Tactic,
			"Technique":       d.TTPsObserved.Technique,
			"Procedure":       d.TTPsObserved.Procedure,
			"APIName":         d.TTPsObserved.APIName,
			keyIPAddress:      d.TTPsObserved.IPAddress,
			"APISuccessCount": d.TTPsObserved.APISuccessCount,
			"APIFailureCount": d.TTPsObserved.APIFailureCount,
		}
	}

	return result
}
