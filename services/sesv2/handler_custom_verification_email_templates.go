package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

type createCustomVerificationEmailTemplateInput struct {
	TemplateName          string `json:"TemplateName"`
	FromEmailAddress      string `json:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject"`
	TemplateContent       string `json:"TemplateContent"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL"`
}

func (h *Handler) handleCreateCustomVerificationEmailTemplate(c *echo.Context) (any, error) {
	var in createCustomVerificationEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	tmpl := &CustomVerificationEmailTemplate{
		TemplateName:          in.TemplateName,
		FromEmailAddress:      in.FromEmailAddress,
		TemplateSubject:       in.TemplateSubject,
		TemplateContent:       in.TemplateContent,
		SuccessRedirectionURL: in.SuccessRedirectionURL,
		FailureRedirectionURL: in.FailureRedirectionURL,
	}

	if _, err := h.Backend.CreateCustomVerificationEmailTemplate(tmpl); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// custom verification template handlers

func (h *Handler) handleGetCustomVerificationEmailTemplate(name string) (any, error) {
	t, err := h.Backend.GetCustomVerificationEmailTemplate(name)
	if err != nil {
		return nil, err
	}

	return toCustomVerificationEmailTemplateOutput(t), nil
}

func (h *Handler) handleDeleteCustomVerificationEmailTemplate(name string) (any, error) {
	if err := h.Backend.DeleteCustomVerificationEmailTemplate(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateCustomVerificationEmailTemplateInput struct {
	FromEmailAddress      string `json:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject"`
	TemplateContent       string `json:"TemplateContent"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL"`
}

func (h *Handler) handleUpdateCustomVerificationEmailTemplate(
	c *echo.Context,
	name string,
) (any, error) {
	var in updateCustomVerificationEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	tmpl := &CustomVerificationEmailTemplate{
		TemplateName:          name,
		FromEmailAddress:      in.FromEmailAddress,
		TemplateSubject:       in.TemplateSubject,
		TemplateContent:       in.TemplateContent,
		SuccessRedirectionURL: in.SuccessRedirectionURL,
		FailureRedirectionURL: in.FailureRedirectionURL,
	}

	if err := h.Backend.UpdateCustomVerificationEmailTemplate(tmpl); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListCustomVerificationEmailTemplates(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListCustomVerificationEmailTemplates(nextToken, 0)

	items := make([]customVerificationEmailTemplateMetadataOutput, 0, len(pg.Data))
	for _, t := range pg.Data {
		items = append(items, toCustomVerificationEmailTemplateMetadataOutput(t))
	}

	return map[string]any{
		"CustomVerificationEmailTemplates": items,
		keyNextToken:                       pg.Next,
	}, nil
}
