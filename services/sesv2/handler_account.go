package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

// account handlers

func (h *Handler) handleGetAccount() (any, error) {
	acct, err := h.Backend.GetAccount()
	if err != nil {
		return nil, err
	}

	return toAccountOutput(acct), nil
}

func (h *Handler) handleGetBlacklistReports() (any, error) {
	reports, err := h.Backend.GetBlacklistReports()
	if err != nil {
		return nil, err
	}

	return map[string]any{"BlacklistReport": reports}, nil
}

func (h *Handler) handlePutAccountDedicatedIPWarmupAttributes(c *echo.Context) (any, error) {
	var in struct {
		AutoWarmupEnabled bool `json:"AutoWarmupEnabled"`
	}

	_ = json.NewDecoder(c.Request().Body).Decode(&in)

	if err := h.Backend.PutAccountDedicatedIPWarmupAttributes(in.AutoWarmupEnabled); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type putAccountDetailsInput struct {
	MailType        string `json:"MailType"`
	WebsiteURL      string `json:"WebsiteURL"`
	ContactLanguage string `json:"ContactLanguage"`
	UseCaseName     string `json:"UseCaseName"`
}

func (h *Handler) handlePutAccountDetails(c *echo.Context) (any, error) {
	var in putAccountDetailsInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutAccountDetails(&AccountDetails{
		MailType:        in.MailType,
		WebsiteURL:      in.WebsiteURL,
		ContactLanguage: in.ContactLanguage,
		UseCaseName:     in.UseCaseName,
	}); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type putAccountSendingInput struct {
	SendingEnabled bool `json:"SendingEnabled"`
}

func (h *Handler) handlePutAccountSendingAttributes(c *echo.Context) (any, error) {
	var in putAccountSendingInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutAccountSendingAttributes(in.SendingEnabled); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// handlePutAccountPricingAttributes serves PUT /v2/email/account/pricing-attributes.
func (h *Handler) handlePutAccountPricingAttributes(c *echo.Context) (any, error) {
	var in struct {
		Plan string `json:"Plan"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutAccountPricingAttributes(in.Plan); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handlePutAccountSuppressionAttributes(c *echo.Context) (any, error) {
	var in struct {
		SuppressedReasons []string `json:"SuppressedReasons"`
	}

	_ = json.NewDecoder(c.Request().Body).Decode(&in)

	if err := h.Backend.PutAccountSuppressionAttributes(in.SuppressedReasons); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
