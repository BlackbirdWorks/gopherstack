package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateLakeFormationIdentityCenterConfiguration(
	_ context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in createLakeFormationIdentityCenterConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	catalogID := in.CatalogID
	if catalogID == "" {
		catalogID = h.AccountID
	}

	appArn, err := h.Backend.CreateLakeFormationIdentityCenterConfiguration(
		catalogID,
		in.InstanceArn,
		in.ExternalFiltering,
		in.ShareRecipients,
		in.ServiceIntegrations,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createLakeFormationIdentityCenterConfigurationOutput{ApplicationArn: appArn})
}

func (h *Handler) handleDeleteLakeFormationIdentityCenterConfiguration(
	_ context.Context, c *echo.Context, body []byte,
) error {
	var in deleteLakeFormationIdentityCenterConfigurationInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	catalogID := in.CatalogID
	if catalogID == "" {
		catalogID = h.AccountID
	}
	if err := h.Backend.DeleteLakeFormationIdentityCenterConfiguration(catalogID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteLakeFormationIdentityCenterConfigurationOutput{})
}

func (h *Handler) handleDescribeLakeFormationIdentityCenterConfiguration(
	_ context.Context, c *echo.Context, body []byte,
) error {
	var in describeLakeFormationIdentityCenterConfigurationInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	catalogID := in.CatalogID
	if catalogID == "" {
		catalogID = h.AccountID
	}
	cfg, err := h.Backend.DescribeLakeFormationIdentityCenterConfiguration(catalogID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeLakeFormationIdentityCenterConfigurationOutput{
		CatalogID:           cfg.CatalogID,
		InstanceArn:         cfg.InstanceArn,
		ApplicationArn:      cfg.ApplicationArn,
		ExternalFiltering:   cfg.ExternalFiltering,
		ShareRecipients:     cfg.ShareRecipients,
		ServiceIntegrations: cfg.ServiceIntegrations,
	})
}

func (h *Handler) handleUpdateLakeFormationIdentityCenterConfiguration(
	_ context.Context, c *echo.Context, body []byte,
) error {
	var in updateLakeFormationIdentityCenterConfigurationInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	catalogID := in.CatalogID
	if catalogID == "" {
		catalogID = h.AccountID
	}
	if err := h.Backend.UpdateLakeFormationIdentityCenterConfiguration(
		catalogID, in.ExternalFiltering, in.ApplicationStatus, in.ShareRecipients, in.ServiceIntegrations,
	); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateLakeFormationIdentityCenterConfigurationOutput{})
}
