package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

type createEmailTemplateInput struct {
	TemplateName    string               `json:"TemplateName"`
	TemplateContent EmailTemplateContent `json:"TemplateContent"`
}

func (h *Handler) handleCreateEmailTemplate(c *echo.Context) (any, error) {
	var in createEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateEmailTemplate(in.TemplateName, &in.TemplateContent); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// email template handlers

func (h *Handler) handleGetEmailTemplate(name string) (any, error) {
	t, err := h.Backend.GetEmailTemplate(name)
	if err != nil {
		return nil, err
	}

	return toEmailTemplateOutput(t), nil
}

func (h *Handler) handleDeleteEmailTemplate(name string) (any, error) {
	if err := h.Backend.DeleteEmailTemplate(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateEmailTemplateInput struct {
	TemplateContent EmailTemplateContent `json:"TemplateContent"`
}

func (h *Handler) handleUpdateEmailTemplate(c *echo.Context, name string) (any, error) {
	var in updateEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateEmailTemplate(name, &in.TemplateContent); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListEmailTemplates(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListEmailTemplates(nextToken, 0)

	items := make([]emailTemplateMetadataOutput, 0, len(pg.Data))
	for _, t := range pg.Data {
		items = append(items, toEmailTemplateMetadataOutput(t))
	}

	return map[string]any{
		"TemplatesMetadata": items,
		keyNextToken:        pg.Next,
	}, nil
}

type testRenderEmailTemplateInput struct {
	TemplateData string `json:"TemplateData"`
}

func (h *Handler) handleTestRenderEmailTemplate(c *echo.Context, name string) (any, error) {
	var in testRenderEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	rendered, err := h.Backend.TestRenderEmailTemplate(name, in.TemplateData)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RenderedTemplate": rendered}, nil
}
