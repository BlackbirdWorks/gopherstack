package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

// messageSecurityOptions mirrors types.MessageSecurityOptions: SigningScheme is
// a union serialized as a one-key object (DefaultScheme or SmimeScheme).
type messageSecurityOptions struct {
	SigningScheme *signingScheme `json:"SigningScheme,omitempty"`
}

type signingScheme struct {
	DefaultScheme *struct{}           `json:"DefaultScheme,omitempty"`
	SmimeScheme   *smimeSigningScheme `json:"SmimeScheme,omitempty"`
}

type smimeSigningScheme struct {
	SignatureFormat string `json:"SignatureFormat"`
}

func (o *messageSecurityOptions) toBackend() *MessageSecurity {
	if o == nil || o.SigningScheme == nil {
		return nil
	}

	s := o.SigningScheme

	switch {
	case s.DefaultScheme != nil && s.SmimeScheme == nil:
		return &MessageSecurity{}
	case s.SmimeScheme != nil && s.DefaultScheme == nil:
		return &MessageSecurity{Smime: true, SmimeSignatureFormat: s.SmimeScheme.SignatureFormat}
	}

	return nil
}

func messageSecurityFromBackend(m *MessageSecurity) *messageSecurityOptions {
	if m.Smime {
		return &messageSecurityOptions{SigningScheme: &signingScheme{
			SmimeScheme: &smimeSigningScheme{SignatureFormat: m.SmimeSignatureFormat},
		}}
	}

	return &messageSecurityOptions{SigningScheme: &signingScheme{DefaultScheme: &struct{}{}}}
}

func (h *Handler) handleUpdateConfigurationSet(c *echo.Context) (any, error) {
	var in struct {
		MessageSecurityOptions *messageSecurityOptions `json:"MessageSecurityOptions"`
		ConfigurationSetName   string                  `json:"ConfigurationSetName"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	if in.ConfigurationSetName == "" {
		return nil, fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidInput)
	}

	if err := h.Backend.UpdateConfigurationSet(
		in.ConfigurationSetName,
		in.MessageSecurityOptions.toBackend(),
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
