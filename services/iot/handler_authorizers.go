package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleSetDefaultAuthorizer(c *echo.Context) error {
	var req struct {
		AuthorizerName string `json:"authorizerName"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.SetDefaultAuthorizer(req.AuthorizerName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"authorizerName": req.AuthorizerName})
}

func (h *Handler) handleClearDefaultAuthorizer(c *echo.Context) error {
	if err := h.Backend.ClearDefaultAuthorizer(); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDescribeDefaultAuthorizer(c *echo.Context) error {
	name, err := h.Backend.DescribeDefaultAuthorizer()
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"authorizerDescription": map[string]any{"authorizerName": name}})
}

func resolveAuthorizerOps(path, method string) string {
	switch {
	case path == "/authorizers" && method == http.MethodGet:
		return opListAuthorizers
	// POST /authorizer/{authorizerName}/test → TestInvokeAuthorizer (checked before
	// the generic POST case below, which is CreateAuthorizer).
	case strings.HasPrefix(path, "/authorizer/") &&
		strings.HasSuffix(path, "/test") && method == http.MethodPost:
		return opTestInvokeAuthorizer
	case strings.HasPrefix(path, "/authorizer/") && method == http.MethodPost:
		return opCreateAuthorizer
	case strings.HasPrefix(path, "/authorizer/") && method == http.MethodGet:
		return opDescribeAuthorizer
	case strings.HasPrefix(path, "/authorizer/") && method == http.MethodPut:
		return opUpdateAuthorizer
	case strings.HasPrefix(path, "/authorizer/") && method == http.MethodDelete:
		return opDeleteAuthorizer
	}

	return unknownOperation
}

func (h *Handler) handleCreateAuthorizer(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/authorizer/")
	var input CreateAuthorizerInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.AuthorizerName = name
	a, err := h.Backend.CreateAuthorizer(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAuthorizerName: a.AuthorizerName,
		keyAuthorizerARN:  a.AuthorizerARN,
	})
}

func (h *Handler) handleDescribeAuthorizer(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/authorizer/")
	a, err := h.Backend.DescribeAuthorizer(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"authorizerDescription": a})
}

func (h *Handler) handleListAuthorizers(c *echo.Context) error {
	authorizers := h.Backend.ListAuthorizers()
	summaries := make([]map[string]any, len(authorizers))
	for i, a := range authorizers {
		summaries[i] = map[string]any{
			keyAuthorizerName: a.AuthorizerName,
			keyAuthorizerARN:  a.AuthorizerARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"authorizers": summaries})
}

func (h *Handler) handleUpdateAuthorizer(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/authorizer/")
	var req struct {
		AuthorizerFunctionARN string `json:"authorizerFunctionArn"`
		Status                string `json:"status"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	a, err := h.Backend.UpdateAuthorizer(name, req.AuthorizerFunctionARN, req.Status)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAuthorizerName: a.AuthorizerName,
		keyAuthorizerARN:  a.AuthorizerARN,
	})
}

func (h *Handler) handleDeleteAuthorizer(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/authorizer/")
	if err := h.Backend.DeleteAuthorizer(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) dispatchAuthorizerOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateAuthorizer:
		return true, h.handleCreateAuthorizer(c)
	case opDescribeAuthorizer:
		return true, h.handleDescribeAuthorizer(c)
	case opListAuthorizers:
		return true, h.handleListAuthorizers(c)
	case opUpdateAuthorizer:
		return true, h.handleUpdateAuthorizer(c)
	case opDeleteAuthorizer:
		return true, h.handleDeleteAuthorizer(c)
	case opSetDefaultAuthorizer:
		return true, h.handleSetDefaultAuthorizer(c)
	case opClearDefaultAuthorizer:
		return true, h.handleClearDefaultAuthorizer(c)
	case opDescribeDefaultAuthorizer:
		return true, h.handleDescribeDefaultAuthorizer(c)
	}

	return false, nil
}

// resolveDefaultAuthorizerPathOps resolves the default-authorizer endpoints.
func resolveDefaultAuthorizerPathOps(path, method string) string {
	switch {
	case path == pathDefaultAuthorizer && method == http.MethodPost:
		return opSetDefaultAuthorizer
	case path == pathDefaultAuthorizer && method == http.MethodDelete:
		return opClearDefaultAuthorizer
	case path == pathDefaultAuthorizer && method == http.MethodGet:
		return opDescribeDefaultAuthorizer
	}

	return unknownOperation
}
