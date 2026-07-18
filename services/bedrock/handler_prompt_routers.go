package bedrock

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

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

type createPromptRouterInput struct {
	PromptRouterName string `json:"promptRouterName"`
	Tags             []Tag  `json:"tags,omitempty"`
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

	router, opErr := h.Backend.CreatePromptRouter(in.PromptRouterName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyPromptRouterArn: router.PromptRouterArn})
}

func (h *Handler) handleGetPromptRouter(c *echo.Context, routerARN string) error {
	router, err := h.Backend.GetPromptRouter(routerARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPromptRouterArn: router.PromptRouterArn,
		"promptRouterName": router.PromptRouterName,
		keyStatus:          router.Status,
		keyCreatedAt:       router.CreatedAt.Format(time.RFC3339),
		keyUpdatedAt:       router.UpdatedAt.Format(time.RFC3339),
	})
}

func (h *Handler) handleListPromptRouters(c *echo.Context) error {
	routers := h.Backend.ListPromptRouters()
	summaries := make([]map[string]any, 0, len(routers))

	for _, r := range routers {
		summaries = append(summaries, map[string]any{
			keyPromptRouterArn: r.PromptRouterArn,
			"promptRouterName": r.PromptRouterName,
			keyStatus:          r.Status,
			keyCreatedAt:       r.CreatedAt.Format(time.RFC3339),
			keyUpdatedAt:       r.UpdatedAt.Format(time.RFC3339),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"promptRouters": summaries})
}

func (h *Handler) handleDeletePromptRouter(c *echo.Context, routerARN string) error {
	if err := h.Backend.DeletePromptRouter(routerARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
