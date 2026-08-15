package bedrock

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// extractPromptRouterOperation mirrors routeStubPromptRouterOps's dispatch
// order exactly, so ExtractOperation agrees with the real dispatch contract
// for the PromptRouter/ImportedModel families -- previously absent from
// ExtractOperation's extractor list entirely (found by gopherstack-n1mb's
// route table; Handler() itself already dispatched these correctly).
func extractPromptRouterOperation(path, method string) (string, bool) {
	switch {
	case path == promptRoutersPrefix && method == http.MethodPost:
		return "CreatePromptRouter", true
	case path == promptRoutersPrefix && method == http.MethodGet:
		return "ListPromptRouters", true
	case strings.HasPrefix(path, promptRoutersPrefix+"/") && method == http.MethodGet:
		return "GetPromptRouter", true
	case strings.HasPrefix(path, promptRoutersPrefix+"/") && method == http.MethodDelete:
		return "DeletePromptRouter", true
	case path == importedModelsPrefix && method == http.MethodGet:
		return "ListImportedModels", true
	case strings.HasPrefix(path, importedModelsPrefix+"/") && method == http.MethodGet:
		return "GetImportedModel", true
	case strings.HasPrefix(path, importedModelsPrefix+"/") && method == http.MethodDelete:
		return "DeleteImportedModel", true
	}

	return "", false
}

// routeStubPromptRouterOps handles prompt router and imported model operations.
func (h *Handler) routeStubPromptRouterOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == promptRoutersPrefix && method == http.MethodPost:
		return true, h.handleCreatePromptRouter(c)
	case path == promptRoutersPrefix && method == http.MethodGet:
		return true, h.handleListPromptRouters(c)
	case strings.HasPrefix(path, promptRoutersPrefix+"/") && method == http.MethodGet:
		routerARN, _ := url.PathUnescape(strings.TrimPrefix(path, promptRoutersPrefix+"/"))

		return true, h.handleGetPromptRouter(c, routerARN)
	case strings.HasPrefix(path, promptRoutersPrefix+"/") && method == http.MethodDelete:
		routerARN, _ := url.PathUnescape(strings.TrimPrefix(path, promptRoutersPrefix+"/"))

		return true, h.handleDeletePromptRouter(c, routerARN)
	case path == importedModelsPrefix && method == http.MethodGet:
		return true, h.handleListImportedModels(c)
	case strings.HasPrefix(path, importedModelsPrefix+"/") && method == http.MethodGet:
		modelARN, _ := url.PathUnescape(strings.TrimPrefix(path, importedModelsPrefix+"/"))

		return true, h.handleGetImportedModel(c, modelARN)
	case strings.HasPrefix(path, importedModelsPrefix+"/") && method == http.MethodDelete:
		modelARN, _ := url.PathUnescape(strings.TrimPrefix(path, importedModelsPrefix+"/"))

		return true, h.handleDeleteImportedModel(c, modelARN)
	}

	return false, nil
}

type promptRouterTargetModelWire struct {
	ModelArn string `json:"modelArn"`
}

type routingCriteriaWire struct {
	ResponseQualityDifference float64 `json:"responseQualityDifference"`
}

type createPromptRouterInput struct {
	FallbackModel    *promptRouterTargetModelWire  `json:"fallbackModel"`
	RoutingCriteria  *routingCriteriaWire          `json:"routingCriteria"`
	PromptRouterName string                        `json:"promptRouterName"`
	Description      string                        `json:"description,omitempty"`
	Models           []promptRouterTargetModelWire `json:"models"`
	Tags             []Tag                         `json:"tags,omitempty"`
}

func (h *Handler) handleCreatePromptRouter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[createPromptRouterInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	var fallbackModelArn string
	if in.FallbackModel != nil {
		fallbackModelArn = in.FallbackModel.ModelArn
	}

	modelArns := make([]string, 0, len(in.Models))
	for _, m := range in.Models {
		modelArns = append(modelArns, m.ModelArn)
	}

	var responseQualityDiff float64
	if in.RoutingCriteria != nil {
		responseQualityDiff = in.RoutingCriteria.ResponseQualityDifference
	}

	router, opErr := h.Backend.CreatePromptRouter(
		in.PromptRouterName, in.Description, fallbackModelArn, modelArns, responseQualityDiff, in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyPromptRouterArn: router.PromptRouterArn})
}

func promptRouterToWire(r *PromptRouter) map[string]any {
	models := make([]promptRouterTargetModelWire, 0, len(r.ModelArns))
	for _, modelArn := range r.ModelArns {
		models = append(models, promptRouterTargetModelWire{ModelArn: modelArn})
	}

	return map[string]any{
		keyPromptRouterArn: r.PromptRouterArn,
		"promptRouterName": r.PromptRouterName,
		keyStatus:          r.Status,
		"type":             r.Type,
		"description":      r.Description,
		"fallbackModel":    promptRouterTargetModelWire{ModelArn: r.FallbackModelArn},
		"models":           models,
		"routingCriteria":  routingCriteriaWire{ResponseQualityDifference: r.RoutingResponseQualityDiff},
		keyCreatedAt:       r.CreatedAt.Format(time.RFC3339),
		keyUpdatedAt:       r.UpdatedAt.Format(time.RFC3339),
	}
}

func (h *Handler) handleGetPromptRouter(c *echo.Context, routerARN string) error {
	router, err := h.Backend.GetPromptRouter(routerARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, promptRouterToWire(router))
}

func (h *Handler) handleListPromptRouters(c *echo.Context) error {
	q := c.Request().URL.Query()
	routers, nextToken := h.Backend.ListPromptRouters(q.Get("type"), q.Get("nextToken"))

	summaries := make([]map[string]any, 0, len(routers))
	for _, r := range routers {
		summaries = append(summaries, promptRouterToWire(r))
	}

	resp := map[string]any{"promptRouterSummaries": summaries}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeletePromptRouter(c *echo.Context, routerARN string) error {
	if err := h.Backend.DeletePromptRouter(routerARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
