package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateLFTagExpression(_ context.Context, c *echo.Context, body []byte) error {
	var in createLFTagExpressionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.Name) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	if len(in.Expression) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Expression is required")
	}

	if err := h.Backend.CreateLFTagExpression(in.Name, in.Description, in.CatalogID, in.Expression); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createLFTagExpressionOutput{})
}

func (h *Handler) handleDeleteLFTagExpression(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteLFTagExpressionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.Name) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	if err := h.Backend.DeleteLFTagExpression(in.Name, in.CatalogID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteLFTagExpressionOutput{})
}

func (h *Handler) handleListLFTagExpressions(_ context.Context, c *echo.Context, body []byte) error {
	var in listLFTagExpressionsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	exprs, nextToken := h.Backend.ListLFTagExpressions(in.CatalogID, in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, listLFTagExpressionsOutput{
		LFTagExpressions: exprs,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleGetLFTagExpression(_ context.Context, c *echo.Context, body []byte) error {
	var in getLFTagExpressionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	expr, err := h.Backend.GetLFTagExpression(in.Name, in.CatalogID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getLFTagExpressionOutput{
		Name:        expr.Name,
		Description: expr.Description,
		CatalogID:   expr.CatalogID,
		Expression:  expr.Expression,
	})
}

func (h *Handler) handleUpdateLFTagExpression(_ context.Context, c *echo.Context, body []byte) error {
	var in updateLFTagExpressionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.Name) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}
	if err := h.Backend.UpdateLFTagExpression(in.Name, in.CatalogID, in.Description, in.Expression); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateLFTagExpressionOutput{})
}
