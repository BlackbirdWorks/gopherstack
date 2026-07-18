package amplify

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// JSON response keys used by the branch handlers.
const keyBranch = "branch"

// handleBranches handles POST/GET /apps/{appId}/branches.
func (h *Handler) handleBranches(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createBranch(ctx, c, appID)
	case http.MethodGet:
		return h.listBranches(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleBranchName handles GET/POST/DELETE /apps/{appId}/branches/{branchName}.
func (h *Handler) handleBranchName(ctx context.Context, c *echo.Context, appID, branchName string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getBranch(ctx, c, appID, branchName)
	case http.MethodDelete:
		return h.deleteBranch(ctx, c, appID, branchName)
	case http.MethodPost:
		return h.updateBranch(ctx, c, appID, branchName)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// createBranch handles POST /apps/{appId}/branches.
func (h *Handler) createBranch(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		Tags            map[string]string `json:"tags"`
		BranchName      string            `json:"branchName"`
		Description     string            `json:"description"`
		Stage           string            `json:"stage"`
		EnableAutoBuild bool              `json:"enableAutoBuild"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	if input.BranchName == "" {
		return amplifyErrorJSON(c, http.StatusBadRequest, "branchName is required")
	}

	branch, createErr := h.Backend.CreateBranch(
		appID,
		input.BranchName,
		input.Description,
		input.Stage,
		input.EnableAutoBuild,
		input.Tags,
	)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateBranch", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyBranch: toBranchView(branch)})
}

// getBranch handles GET /apps/{appId}/branches/{branchName}.
func (h *Handler) getBranch(ctx context.Context, c *echo.Context, appID, branchName string) error {
	branch, err := h.Backend.GetBranch(appID, branchName)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetBranch", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyBranch: toBranchView(branch)})
}

// listBranches handles GET /apps/{appId}/branches.
func (h *Handler) listBranches(ctx context.Context, c *echo.Context, appID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxResults = n
		}
	}

	branches, outToken, err := h.Backend.ListBranches(appID, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListBranches", err)
	}

	resp := map[string]any{"branches": toBranchViews(branches)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// deleteBranch handles DELETE /apps/{appId}/branches/{branchName}.
func (h *Handler) deleteBranch(ctx context.Context, c *echo.Context, appID, branchName string) error {
	if err := h.Backend.DeleteBranch(appID, branchName); err != nil {
		return h.handleBackendError(ctx, c, "DeleteBranch", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateBranch handles POST /apps/{appId}/branches/{branchName}.
func (h *Handler) updateBranch(ctx context.Context, c *echo.Context, appID, branchName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		Description     string `json:"description"`
		Stage           string `json:"stage"`
		EnableAutoBuild bool   `json:"enableAutoBuild"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	branch, updateErr := h.Backend.UpdateBranch(
		appID, branchName, input.Description, input.Stage, input.EnableAutoBuild,
	)
	if updateErr != nil {
		return h.handleBackendError(ctx, c, "UpdateBranch", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyBranch: toBranchView(branch)})
}

// parseBranchesOperation maps the method for /apps/{appId}/branches to its operation name.
func parseBranchesOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateBranch"
	case http.MethodGet:
		return "ListBranches"
	default:
		return opUnknown
	}
}

// parseBranchOperation maps the method for /apps/{appId}/branches/{branchName}
// to its operation name.
func parseBranchOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetBranch"
	case http.MethodDelete:
		return "DeleteBranch"
	case http.MethodPost:
		return "UpdateBranch"
	default:
		return opUnknown
	}
}

// branchView is the JSON representation of a Branch with timestamps as Unix
// epoch float64 values, as required by the AWS SDK v2 deserialiser.
type branchView struct {
	Tags            map[string]string `json:"tags,omitempty"`
	AppID           string            `json:"appId"`
	BranchARN       string            `json:"branchArn"`
	BranchName      string            `json:"branchName"`
	Description     string            `json:"description,omitempty"`
	Stage           Stage             `json:"stage,omitempty"`
	CreateTime      float64           `json:"createTime"`
	UpdateTime      float64           `json:"updateTime"`
	EnableAutoBuild bool              `json:"enableAutoBuild"`
}

func toBranchView(b *Branch) branchView {
	var tagMap map[string]string
	if b.Tags != nil {
		tagMap = b.Tags.Clone()
	}

	return branchView{
		Tags:            tagMap,
		CreateTime:      float64(b.CreateTime.Unix()),
		UpdateTime:      float64(b.UpdateTime.Unix()),
		AppID:           b.AppID,
		BranchARN:       b.BranchARN,
		BranchName:      b.BranchName,
		Description:     b.Description,
		Stage:           b.Stage,
		EnableAutoBuild: b.EnableAutoBuild,
	}
}

func toBranchViews(branches []*Branch) []branchView {
	views := make([]branchView, len(branches))
	for i, b := range branches {
		views[i] = toBranchView(b)
	}

	return views
}
