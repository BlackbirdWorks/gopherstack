package fis

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// Action discovery handlers
// ----------------------------------------

func (h *Handler) handleGetAction(c *echo.Context, id string) error {
	action, err := h.Backend.GetAction(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, actionResponseDTO{
		Action: toActionDTO(action),
	})
}

func (h *Handler) handleListActions(c *echo.Context) error {
	actions := h.Backend.ListActions()

	ids := make([]string, len(actions))
	for i, a := range actions {
		ids[i] = a.ID
	}

	page, nextTok := paginatePage(actions, ids, c.Request().URL.Query())
	dtos := make([]actionSummaryDTO, len(page))

	for i := range page {
		dtos[i] = toActionSummaryDTO(&page[i])
	}

	return c.JSON(http.StatusOK, listActionsResponseDTO{Actions: dtos, NextToken: nextTok})
}

func (h *Handler) handleGetTargetResourceType(c *echo.Context, resourceType string) error {
	rt, err := h.Backend.GetTargetResourceType(resourceType)
	if err != nil {
		return h.writeBackendError(c, err, resourceType)
	}

	return c.JSON(http.StatusOK, targetResourceTypeResponseDTO{
		TargetResourceType: toTargetResourceTypeDTO(rt),
	})
}

func (h *Handler) handleListTargetResourceTypes(c *echo.Context) error {
	types := h.Backend.ListTargetResourceTypes()

	ids := make([]string, len(types))
	for i, rt := range types {
		ids[i] = rt.ResourceType
	}

	page, nextTok := paginatePage(types, ids, c.Request().URL.Query())
	dtos := make([]targetResourceTypeSummaryDTO, len(page))

	for i := range page {
		dtos[i] = targetResourceTypeSummaryDTO{
			ResourceType: page[i].ResourceType,
			Description:  page[i].Description,
		}
	}

	return c.JSON(
		http.StatusOK,
		listTargetResourceTypesResponseDTO{TargetResourceTypes: dtos, NextToken: nextTok},
	)
}

// ----------------------------------------
// DTO conversion helpers
// ----------------------------------------

func toActionDTO(a *ActionSummary) actionDTO {
	params := make(map[string]actionParamDTO, len(a.Parameters))
	for k, v := range a.Parameters {
		params[k] = actionParamDTO(v)
	}

	return actionDTO{
		ID:          a.ID,
		Arn:         a.Arn,
		Description: a.Description,
		Parameters:  params,
		Targets:     toActionTargetDTOs(a.Targets),
		Tags:        a.Tags,
	}
}

func toActionSummaryDTO(a *ActionSummary) actionSummaryDTO {
	return actionSummaryDTO{
		ID:          a.ID,
		Arn:         a.Arn,
		Description: a.Description,
		Targets:     toActionTargetDTOs(a.Targets),
		Tags:        a.Tags,
	}
}

func toActionTargetDTOs(targets map[string]ActionTarget) map[string]actionTargetDTO {
	dtos := make(map[string]actionTargetDTO, len(targets))
	for k, v := range targets {
		dtos[k] = actionTargetDTO(v)
	}

	return dtos
}

func toTargetResourceTypeDTO(rt *TargetResourceTypeSummary) targetResourceTypeDTO {
	params := make(map[string]targetRTParamDTO, len(rt.Parameters))
	for k, v := range rt.Parameters {
		params[k] = targetRTParamDTO(v)
	}

	return targetResourceTypeDTO{
		ResourceType: rt.ResourceType,
		Description:  rt.Description,
		Parameters:   params,
	}
}
