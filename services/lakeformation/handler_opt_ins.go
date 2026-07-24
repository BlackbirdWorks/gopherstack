package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateLakeFormationOptIn(_ context.Context, c *echo.Context, body []byte) error {
	var in createLakeFormationOptInInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.Principal == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Principal is required")
	}

	if in.Resource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	if err := h.Backend.CreateLakeFormationOptIn(in.Principal, in.Resource, in.Condition); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createLakeFormationOptInOutput{})
}

func (h *Handler) handleDeleteLakeFormationOptIn(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteLakeFormationOptInInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.Principal == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Principal is required")
	}

	if in.Resource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	if err := h.Backend.DeleteLakeFormationOptIn(in.Principal, in.Resource, in.Condition); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteLakeFormationOptInOutput{})
}

func (h *Handler) handleListLakeFormationOptIns(_ context.Context, c *echo.Context, body []byte) error {
	var in listLakeFormationOptInsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	principalIdentifier := ""
	if in.Principal != nil {
		principalIdentifier = in.Principal.DataLakePrincipalIdentifier
	}

	optIns, nextToken := h.Backend.ListLakeFormationOptIns(
		principalIdentifier,
		in.Resource,
		in.MaxResults,
		in.NextToken,
	)

	return c.JSON(http.StatusOK, listLakeFormationOptInsOutput{
		LakeFormationOptInsInfoList: toLFOptInWireList(optIns),
		NextToken:                   nextToken,
	})
}
