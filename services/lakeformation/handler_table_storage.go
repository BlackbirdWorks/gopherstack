package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetTableObjects(_ context.Context, c *echo.Context, body []byte) error {
	var in getTableObjectsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	objects, nextToken := h.Backend.GetTableObjects(
		in.CatalogID, in.DatabaseName, in.TableName, in.TransactionID,
		in.MaxResults, in.NextToken,
	)

	return c.JSON(http.StatusOK, getTableObjectsOutput{Objects: objects, NextToken: nextToken})
}

func (h *Handler) handleListTableStorageOptimizers(_ context.Context, c *echo.Context, body []byte) error {
	var in listTableStorageOptimizersInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	opts := h.Backend.ListTableStorageOptimizers(in.CatalogID, in.DatabaseName, in.TableName, in.StorageOptimizerType)

	return c.JSON(http.StatusOK, listTableStorageOptimizersOutput{StorageOptimizerList: opts})
}

func (h *Handler) handleUpdateTableObjects(_ context.Context, c *echo.Context, body []byte) error {
	var in updateTableObjectsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	if err := h.Backend.UpdateTableObjects(
		in.CatalogID, in.DatabaseName, in.TableName, in.TransactionID, in.WriteOperations,
	); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateTableObjectsOutput{})
}

func (h *Handler) handleUpdateTableStorageOptimizer(_ context.Context, c *echo.Context, body []byte) error {
	var in updateTableStorageOptimizerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	result := h.Backend.UpdateTableStorageOptimizer(
		in.CatalogID, in.DatabaseName, in.TableName, in.StorageOptimizerConfig,
	)

	return c.JSON(http.StatusOK, updateTableStorageOptimizerOutput{Result: result})
}
