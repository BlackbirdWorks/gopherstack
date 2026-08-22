package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in createDataCellsFilterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.TableData == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableData is required")
	}

	if strings.TrimSpace(in.TableData.Name) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableData.Name is required")
	}

	if err := h.Backend.CreateDataCellsFilter(in.TableData); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createDataCellsFilterOutput{})
}

func (h *Handler) handleDeleteDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteDataCellsFilterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.DeleteDataCellsFilter(in.TableCatalogID, in.DatabaseName, in.TableName, in.Name); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteDataCellsFilterOutput{})
}

func (h *Handler) handleListDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in listDataCellsFilterInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	// ListDataCellsFilterInput (lakeformation@v1.50.4
	// api_op_ListDataCellsFilter.go) marks no member required, including
	// Table -- ListDataCellsFilter itself documents tableCatalogID/
	// databaseName/tableName as optional filters (see
	// (*InMemoryBackend).ListDataCellsFilter) -- gopherstack-4ly2.
	var tableCatalogID, databaseName, tableName string
	if in.Table != nil {
		tableCatalogID = in.Table.CatalogID
		databaseName = in.Table.DatabaseName
		tableName = in.Table.Name
	}

	filters, nextToken := h.Backend.ListDataCellsFilter(
		tableCatalogID,
		databaseName,
		tableName,
		in.MaxResults,
		in.NextToken,
	)

	return c.JSON(http.StatusOK, listDataCellsFilterOutput{
		DataCellsFilters: filters,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleGetDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in getDataCellsFilterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	f, err := h.Backend.GetDataCellsFilter(in.TableCatalogID, in.DatabaseName, in.TableName, in.Name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getDataCellsFilterOutput{DataCellsFilter: f})
}

func (h *Handler) handleUpdateDataCellsFilter(_ context.Context, c *echo.Context, body []byte) error {
	var in updateDataCellsFilterInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if in.TableData == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableData is required")
	}
	if err := h.Backend.UpdateDataCellsFilter(in.TableData); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateDataCellsFilterOutput{})
}
