package marketplacemetering

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// seedPathPrefix is the emulator-only surface that declares what a seller's catalog would hold, since the real
// Marketplace seller portal is not emulated.
const seedPathPrefix = "/_gopherstack/marketplace/"

const (
	seedProducts     = "products"
	seedSubscription = "subscriptions"
	seedTokens       = "registration-tokens"
	seedLicenses     = "licenses"
)

type seedSubscriptionInput struct {
	ProductCode string `json:"productCode"`
	AccountID   string `json:"accountId"`
}

type seedTokenOutput struct {
	RegistrationToken string `json:"registrationToken"`
}

func seedResource(r *http.Request) string {
	if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, seedPathPrefix) {
		return ""
	}

	switch res := strings.TrimPrefix(r.URL.Path, seedPathPrefix); res {
	case seedProducts, seedSubscription, seedTokens, seedLicenses:
		return res
	}

	return ""
}

func seedOperation(resource string) string {
	switch resource {
	case seedProducts:
		return "SeedProduct"
	case seedSubscription:
		return "SeedSubscription"
	case seedTokens:
		return "SeedRegistrationToken"
	default:
		return "SeedLicense"
	}
}

func (h *Handler) serveSeed(c *echo.Context, resource string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, fail(ErrValidation, "could not read the request body"))
	}

	out, err := h.seed(resource, body)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) seed(resource string, body []byte) (any, error) {
	switch resource {
	case seedProducts:
		var spec ProductSpec
		if err := decode(body, &spec); err != nil {
			return nil, err
		}

		if err := h.Backend.RegisterProduct(spec); err != nil {
			return nil, err
		}

		return spec, nil
	case seedLicenses:
		var spec LicenseSpec
		if err := decode(body, &spec); err != nil {
			return nil, err
		}

		return h.Backend.RegisterLicense(spec)
	}

	var in seedSubscriptionInput
	if err := decode(body, &in); err != nil {
		return nil, err
	}

	if resource == seedTokens {
		tok, err := h.Backend.IssueRegistrationToken(in.ProductCode, in.AccountID)
		if err != nil {
			return nil, err
		}

		return seedTokenOutput{RegistrationToken: tok}, nil
	}

	return h.Backend.Subscribe(in.ProductCode, in.AccountID)
}
