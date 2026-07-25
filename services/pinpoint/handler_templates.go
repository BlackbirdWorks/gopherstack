package pinpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// extractTemplateOperation resolves the operation name for paths under /v1/templates/.
func (h *Handler) extractTemplateOperation(method, path string) string {
	suffix := strings.TrimPrefix(path, "/v1/templates/")
	parts := strings.SplitN(suffix, "/", dispatchSplitThree)

	if len(parts) < templateSubPathParts {
		return unknownOperation
	}

	templateType := parts[1]
	subPath := ""

	if len(parts) == dispatchSplitThree {
		subPath = parts[2]
	}

	switch subPath {
	case "versions":
		return "ListTemplateVersions"
	case "active-version":
		return "UpdateTemplateActiveVersion"
	case "":
		switch method {
		case http.MethodPost:
			return h.createTemplateOpName(templateType)
		case http.MethodGet:
			return h.getTemplateOpName(templateType)
		case http.MethodPut:
			return h.updateTemplateOpName(templateType)
		case http.MethodDelete:
			return h.deleteTemplateOpName(templateType)
		}
	}

	return unknownOperation
}

func (h *Handler) createTemplateOpName(t string) string {
	switch t {
	case templateTypeEmail:
		return "CreateEmailTemplate"
	case templateTypeInApp:
		return "CreateInAppTemplate"
	case templateTypePush:
		return "CreatePushTemplate"
	case templateTypeSMS:
		return "CreateSmsTemplate"
	case templateTypeVoice:
		return "CreateVoiceTemplate"
	}

	return unknownOperation
}

func (h *Handler) getTemplateOpName(t string) string {
	switch t {
	case templateTypeEmail:
		return "GetEmailTemplate"
	case templateTypeInApp:
		return "GetInAppTemplate"
	case templateTypePush:
		return "GetPushTemplate"
	case templateTypeSMS:
		return "GetSmsTemplate"
	case templateTypeVoice:
		return "GetVoiceTemplate"
	}

	return unknownOperation
}

func (h *Handler) updateTemplateOpName(t string) string {
	switch t {
	case templateTypeEmail:
		return "UpdateEmailTemplate"
	case templateTypeInApp:
		return "UpdateInAppTemplate"
	case templateTypePush:
		return "UpdatePushTemplate"
	case templateTypeSMS:
		return "UpdateSmsTemplate"
	case templateTypeVoice:
		return "UpdateVoiceTemplate"
	}

	return unknownOperation
}

func (h *Handler) deleteTemplateOpName(t string) string {
	switch t {
	case templateTypeEmail:
		return "DeleteEmailTemplate"
	case templateTypeInApp:
		return "DeleteInAppTemplate"
	case templateTypePush:
		return "DeletePushTemplate"
	case templateTypeSMS:
		return "DeleteSmsTemplate"
	case templateTypeVoice:
		return "DeleteVoiceTemplate"
	}

	return unknownOperation
}

// dispatchTemplates routes requests under /v1/templates.
func (h *Handler) dispatchTemplates(c *echo.Context, path string) error {
	// /v1/templates (list all)
	if path == "/v1/templates" || path == "/v1/templates/" {
		return h.handleListTemplates(c)
	}

	suffix := strings.TrimPrefix(path, "/v1/templates/")
	// suffix format: {templateName}/{type} or {templateName}/{type}/versions or {templateName}/{type}/active-version
	parts := strings.SplitN(suffix, "/", dispatchSplitThree)

	if len(parts) < templateSubPathParts {
		return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
	}

	templateName, templateType := parts[0], parts[1]
	subPath := ""

	if len(parts) == dispatchSplitThree {
		subPath = parts[2]
	}

	switch subPath {
	case "versions":
		return h.handleListTemplateVersions(c, templateName, templateType)
	case "active-version":
		return h.handleUpdateTemplateActiveVersion(c, templateName, templateType)
	case "":
		switch c.Request().Method {
		case http.MethodPost:
			return h.handleCreateTemplate(c, templateName, templateType)
		case http.MethodGet:
			return h.handleGetTemplate(c, templateName, templateType)
		case http.MethodPut:
			return h.handleUpdateTemplate(c, templateName, templateType)
		case http.MethodDelete:
			return h.handleDeleteTemplateByType(c, templateName, templateType)
		}
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

// handleCreateTemplate handles creation of any template type (email, inapp, push, sms).
func (h *Handler) handleCreateTemplate(c *echo.Context, templateName, templateType string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	templateARN, creationErr := h.createTemplateByType(body, region, templateName, templateType)
	if creationErr != nil {
		switch {
		case errors.Is(creationErr, errInvalidRequestBody):
			return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
		case errors.Is(creationErr, ErrAlreadyExists):
			return writeErrorResponse(c, http.StatusConflict, "ConflictException", creationErr.Error())
		default:
			return writeErrorResponse(
				c,
				http.StatusInternalServerError,
				"InternalServerErrorException",
				creationErr.Error(),
			)
		}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, createTemplateMessageBody{
		ARN:     templateARN,
		Message: "Created",
	})

	return nil
}

// createTemplateByType creates a template based on templateType and returns its ARN.
func (h *Handler) createTemplateByType(body []byte, region, templateName, templateType string) (string, error) {
	switch templateType {
	case templateTypeEmail:
		return h.createEmailTemplateARN(body, region, templateName)
	case templateTypeInApp:
		return h.createInAppTemplateARN(body, region, templateName)
	case templateTypePush:
		return h.createPushTemplateARN(body, region, templateName)
	case templateTypeSMS:
		return h.createSMSTemplateARN(body, region, templateName)
	case templateTypeVoice:
		return h.createVoiceTemplateARN(body, region, templateName)
	}

	return "", fmt.Errorf("%w: %s", errUnsupportedTemplateType, templateType)
}

func (h *Handler) createEmailTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createEmailTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreateEmailTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
}

func (h *Handler) createInAppTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createInAppTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreateInAppTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
}

func (h *Handler) createPushTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createPushTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreatePushTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
}

func (h *Handler) createSMSTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createSmsTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreateSmsTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
}

func (h *Handler) createVoiceTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createVoiceTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreateVoiceTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
}

// handleGetTemplate handles GET for any template type.
func (h *Handler) handleGetTemplate(c *echo.Context, templateName, templateType string) error {
	switch templateType {
	case templateTypeEmail:
		t, err := h.Backend.GetEmailTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, t)

		return nil
	case templateTypeInApp:
		t, err := h.Backend.GetInAppTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, t)

		return nil
	case templateTypePush:
		t, err := h.Backend.GetPushTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, t)

		return nil
	case templateTypeSMS:
		t, err := h.Backend.GetSmsTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, t)

		return nil
	case templateTypeVoice:
		t, err := h.Backend.GetVoiceTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, t)

		return nil
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "unknown template type")
}

// handleUpdateTemplate handles PUT for any template type.
func (h *Handler) handleUpdateTemplate(c *echo.Context, templateName, templateType string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	if updateErr := h.applyTemplateUpdate(c, body, templateName, templateType); updateErr != nil {
		return updateErr
	}

	httputils.WriteJSON(
		c.Request().Context(),
		c.Response(),
		http.StatusAccepted,
		messageBodyResponse{Message: acceptedMessage},
	)

	return nil
}

// applyTemplateUpdate applies the update for the given template type.
func (h *Handler) applyTemplateUpdate(c *echo.Context, body []byte, templateName, templateType string) error {
	switch templateType {
	case templateTypeEmail:
		return h.updateEmailTemplateFromBody(c, body, templateName)
	case templateTypeInApp:
		return h.updateInAppTemplateFromBody(c, body, templateName)
	case templateTypePush:
		return h.updatePushTemplateFromBody(c, body, templateName)
	case templateTypeSMS:
		return h.updateSMSTemplateFromBody(c, body, templateName)
	case templateTypeVoice:
		return h.updateVoiceTemplateFromBody(c, body, templateName)
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "unknown template type")
}

// updateEmailTemplateFromBody parses and applies an email template update.
func (h *Handler) updateEmailTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createEmailTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdateEmailTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// updateInAppTemplateFromBody parses and applies an in-app template update.
func (h *Handler) updateInAppTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createInAppTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdateInAppTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// updatePushTemplateFromBody parses and applies a push template update.
func (h *Handler) updatePushTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createPushTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdatePushTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// updateSMSTemplateFromBody parses and applies an SMS template update.
func (h *Handler) updateSMSTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createSmsTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdateSmsTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// updateVoiceTemplateFromBody parses and applies a voice template update.
func (h *Handler) updateVoiceTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createVoiceTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdateVoiceTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// handleDeleteTemplateByType handles DELETE for any template type.
func (h *Handler) handleDeleteTemplateByType(c *echo.Context, templateName, templateType string) error {
	switch templateType {
	case templateTypeEmail:
		if _, err := h.Backend.DeleteEmailTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	case templateTypeInApp:
		if _, err := h.Backend.DeleteInAppTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	case templateTypePush:
		if _, err := h.Backend.DeletePushTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	case templateTypeSMS:
		if _, err := h.Backend.DeleteSmsTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	case templateTypeVoice:
		if _, err := h.Backend.DeleteVoiceTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	default:
		return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "unknown template type")
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, messageBodyResponse{Message: "Deleted"})

	return nil
}

// handleListTemplates handles GET /v1/templates.
func (h *Handler) handleListTemplates(c *echo.Context) error {
	items, err := h.Backend.ListTemplates()
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	prefix := c.QueryParam("prefix")
	templateType := strings.ToUpper(c.QueryParam("template-type"))

	resp := templatesListResponse{Item: make([]templateListItem, 0, len(items))}

	for _, item := range items {
		if prefix != "" && !strings.HasPrefix(item.TemplateName, prefix) {
			continue
		}

		if templateType != "" && !strings.EqualFold(item.TemplateType, templateType) {
			continue
		}

		resp.Item = append(resp.Item, *item)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleListTemplateVersions handles GET /v1/templates/{templateName}/{type}/versions.
func (h *Handler) handleListTemplateVersions(c *echo.Context, templateName, templateType string) error {
	items, err := h.Backend.ListTemplateVersions(templateName, templateType)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	resp := templateVersionsListResponse{Item: make([]templateVersionItem, 0, len(items))}

	for _, item := range items {
		resp.Item = append(resp.Item, *item)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleUpdateTemplateActiveVersion handles PUT /v1/templates/{templateName}/{type}/active-version.
func (h *Handler) handleUpdateTemplateActiveVersion(c *echo.Context, templateName, templateType string) error {
	_, _ = httputils.ReadBody(c.Request())

	if err := h.Backend.UpdateTemplateActiveVersion(templateName, templateType); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	httputils.WriteJSON(
		c.Request().Context(),
		c.Response(),
		http.StatusAccepted,
		messageBodyResponse{Message: acceptedMessage},
	)

	return nil
}

// ──────────────────────────────────────────────────
// Endpoint handlers
// ──────────────────────────────────────────────────
