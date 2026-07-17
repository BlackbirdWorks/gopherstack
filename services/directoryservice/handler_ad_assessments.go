package directoryservice

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleStartADAssessment(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	assessmentID, startErr := h.Backend.StartADAssessment(h.contextWithRegion(c), req.DirectoryID)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"AssessmentId": assessmentID}) //nolint:goconst // existing issue.
}

func (h *Handler) handleDeleteADAssessment(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: "AssessmentId",
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.DeleteADAssessment(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDescribeADAssessment(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID  string `json:"DirectoryId"`
		AssessmentID string `json:"AssessmentId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.AssessmentID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId and AssessmentId are required"))
	}

	a, descErr := h.Backend.DescribeADAssessment(h.contextWithRegion(c), req.DirectoryID, req.AssessmentID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ADAssessment": map[string]any{
			"AssessmentId":   a.AssessmentID,
			keyDirectoryID:   a.DirectoryID,
			keyStatus:        a.Status,
			"AssessmentType": a.AssessType,
			keyRegion:        a.Region,
			keyStartTime:     awstime.Epoch(a.StartTime),
		},
	})
}

func (h *Handler) handleListADAssessments(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		PageSize    int32  `json:"PageSize"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	assessments, nextToken, listErr := h.Backend.ListADAssessments(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.PageSize,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	assessList := make([]map[string]any, 0, len(assessments))
	for _, a := range assessments {
		assessList = append(assessList, map[string]any{
			"AssessmentId":   a.AssessmentID,
			keyDirectoryID:   a.DirectoryID,
			keyStatus:        a.Status,
			"AssessmentType": a.AssessType,
			keyRegion:        a.Region,
			keyStartTime:     awstime.Epoch(a.StartTime),
		})
	}

	resp := map[string]any{"ADAssessments": assessList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
