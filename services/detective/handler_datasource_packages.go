package detective

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleBatchGetGraphMemberDatasources(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string   `json:"GraphArn"`
		AccountIds []string `json:"AccountIds"` //nolint:revive // existing issue.
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	results, unprocessed, getErr := h.Backend.BatchGetGraphMemberDatasources(req.GraphArn, req.AccountIds)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	memberDatasources := make([]map[string]any, 0, len(results))
	for _, r := range results {
		memberDatasources = append(memberDatasources, map[string]any{
			keyAccountID:                     r.AccountID,
			keyGraphArn:                      r.GraphARN,
			keyDatasourcePackageIngestStates: r.DatasourcePackageIngestStates,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MemberDatasources":    memberDatasources,
		keyUnprocessedAccounts: unprocessedToJSON(unprocessed),
	})
}

func (h *Handler) handleBatchGetMembershipDatasources(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArns []string `json:"GraphArns"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	results, unprocessed, getErr := h.Backend.BatchGetMembershipDatasources(req.GraphArns)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	membershipDatasources := make([]map[string]any, 0, len(results))
	for _, r := range results {
		membershipDatasources = append(membershipDatasources, map[string]any{
			keyAccountID:                     r.AccountID,
			keyGraphArn:                      r.GraphARN,
			keyDatasourcePackageIngestStates: r.DatasourcePackageIngestStates,
		})
	}

	unprocessedGraphs := make([]map[string]any, 0, len(unprocessed))
	for _, g := range unprocessed {
		unprocessedGraphs = append(unprocessedGraphs, map[string]any{
			keyGraphArn: g.GraphArn,
			keyReason:   g.Reason,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MembershipDatasources": membershipDatasources,
		"UnprocessedGraphs":     unprocessedGraphs,
	})
}

func (h *Handler) handleListDatasourcePackages(c *echo.Context) error {
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

	packages, nextToken, listErr := h.Backend.ListDatasourcePackages(req.GraphArn, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	pkgDetails := make(map[string]any, len(packages))
	for k, v := range packages {
		pkgDetails[k] = map[string]any{
			"DatasourcePackageIngestState": v.IngestState,
		}
	}

	resp := map[string]any{
		"DatasourcePackages": pkgDetails,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateDatasourcePackages(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn           string   `json:"GraphArn"`
		DatasourcePackages []string `json:"DatasourcePackages"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if updateErr := h.Backend.UpdateDatasourcePackages(req.GraphArn, req.DatasourcePackages); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
