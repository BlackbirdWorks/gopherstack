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

// handleGetBlacklistReports reads BlacklistItemNames (required,
// api_op_GetBlacklistReports.go), an httpQuery-bound repeated param
// (serializers.go:2778-2782: encoder.AddQuery("BlacklistItemNames")). This
// backend has no real DNS blacklist (RBL) data source to check against, so
// -- like lightsail's disclosed Get*MetricData stubs -- it honestly reports
// every requested IP as not listed, rather than either fabricating listings
// or (as before this fix) silently discarding which IPs were even asked
// about and returning an empty map with no keys at all.
func (h *Handler) handleGetBlacklistReports(c *echo.Context) (any, error) {
	ips := c.Request().URL.Query()["BlacklistItemNames"]
	if len(ips) == 0 {
		return nil, fmt.Errorf("%w: BlacklistItemNames is required", ErrInvalidInput)
	}

	reports, err := h.Backend.GetBlacklistReports(ips)
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

// UseCaseDescription is the real, deprecated PutAccountDetailsInput member
// (aws-sdk-go-v2/service/sesv2@v1.66.4 api_op_PutAccountDetails.go:60-63);
// "UseCaseName" is not a real member and was silently dropping this field.
type putAccountDetailsInput struct {
	MailType           string `json:"MailType"`
	WebsiteURL         string `json:"WebsiteURL"`
	ContactLanguage    string `json:"ContactLanguage"`
	UseCaseDescription string `json:"UseCaseDescription"`
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
		UseCaseName:     in.UseCaseDescription,
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
