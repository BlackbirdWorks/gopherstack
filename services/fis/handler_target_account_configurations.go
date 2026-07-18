package fis

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// Target Account Configuration handlers
// ----------------------------------------

// splitCompositeID splits a composite "{resourceID}/{accountID}" identifier into its two parts.
func splitCompositeID(compositeID string) (string, string) {
	resourceID, accountID, _ := strings.Cut(compositeID, "/")

	return resourceID, accountID
}

func (h *Handler) handleCreateTargetAccountConfiguration(
	c *echo.Context,
	compositeID string,
	body []byte,
) error {
	templateID, accountID := splitCompositeID(compositeID)

	var input createTargetAccountConfigurationRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"invalid request body: "+err.Error(),
			compositeID,
		)
	}

	cfg, err := h.Backend.CreateTargetAccountConfiguration(
		templateID,
		accountID,
		input.RoleArn,
		input.Description,
	)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusCreated, targetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleDeleteTargetAccountConfiguration(
	c *echo.Context,
	compositeID string,
) error {
	templateID, accountID := splitCompositeID(compositeID)

	cfg, err := h.Backend.DeleteTargetAccountConfiguration(templateID, accountID)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusOK, targetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleGetTargetAccountConfiguration(c *echo.Context, compositeID string) error {
	templateID, accountID := splitCompositeID(compositeID)

	cfg, err := h.Backend.GetTargetAccountConfiguration(templateID, accountID)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusOK, targetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleUpdateTargetAccountConfiguration(
	c *echo.Context,
	compositeID string,
	body []byte,
) error {
	templateID, accountID := splitCompositeID(compositeID)

	var input updateTargetAccountConfigurationRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"invalid request body: "+err.Error(),
			compositeID,
		)
	}

	cfg, err := h.Backend.UpdateTargetAccountConfiguration(
		templateID,
		accountID,
		input.RoleArn,
		input.Description,
	)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusOK, targetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleListTargetAccountConfigurations(c *echo.Context, templateID string) error {
	cfgs, err := h.Backend.ListTargetAccountConfigurations(templateID)
	if err != nil {
		return h.writeBackendError(c, err, templateID)
	}

	dtos := make([]targetAccountConfigurationDTO, len(cfgs))
	for i, cfg := range cfgs {
		dtos[i] = toTargetAccountConfigDTO(cfg)
	}

	return c.JSON(http.StatusOK, listTargetAccountConfigurationsResponseDTO{
		TargetAccountConfigurations: dtos,
	})
}

func (h *Handler) handleGetExperimentTargetAccountConfiguration(
	c *echo.Context,
	compositeID string,
) error {
	experimentID, accountID := splitCompositeID(compositeID)

	cfg, err := h.Backend.GetExperimentTargetAccountConfiguration(experimentID, accountID)
	if err != nil {
		return h.writeBackendError(c, err, compositeID)
	}

	return c.JSON(http.StatusOK, experimentTargetAccountConfigurationResponseDTO{
		TargetAccountConfiguration: toExperimentTargetAccountConfigDTO(cfg),
	})
}

func (h *Handler) handleListExperimentTargetAccountConfigurations(
	c *echo.Context,
	experimentID string,
) error {
	cfgs, err := h.Backend.ListExperimentTargetAccountConfigurations(experimentID)
	if err != nil {
		return h.writeBackendError(c, err, experimentID)
	}

	dtos := make([]experimentTargetAccountConfigurationDTO, len(cfgs))
	for i, cfg := range cfgs {
		dtos[i] = toExperimentTargetAccountConfigDTO(cfg)
	}

	return c.JSON(http.StatusOK, listExperimentTargetAccountConfigurationsResponseDTO{
		TargetAccountConfigurations: dtos,
	})
}

// ----------------------------------------
// DTO conversion helpers
// ----------------------------------------

func toTargetAccountConfigDTO(cfg *TargetAccountConfiguration) targetAccountConfigurationDTO {
	return targetAccountConfigurationDTO{
		AccountID:   cfg.AccountID,
		Description: cfg.Description,
		RoleArn:     cfg.RoleArn,
	}
}

func toExperimentTargetAccountConfigDTO(
	cfg *ExperimentTargetAccountConfiguration,
) experimentTargetAccountConfigurationDTO {
	return experimentTargetAccountConfigurationDTO{
		AccountID:   cfg.AccountID,
		Description: cfg.Description,
		RoleArn:     cfg.RoleArn,
	}
}
