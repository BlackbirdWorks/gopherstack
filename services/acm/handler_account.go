package acm

import (
	"context"
	"encoding/json"
)

type expiryEventsConfiguration struct {
	DaysBeforeExpiry *int32 `json:"DaysBeforeExpiry,omitempty"`
}

type getAccountConfigurationOutput struct {
	ExpiryEvents *expiryEventsConfiguration `json:"ExpiryEvents,omitempty"`
}

type putAccountConfigurationInput struct {
	ExpiryEvents     *expiryEventsConfiguration `json:"ExpiryEvents,omitempty"`
	IdempotencyToken string                     `json:"IdempotencyToken"`
}

type putAccountConfigurationOutput struct{}

// jsonGetAccountConfiguration handles the GetAccountConfiguration operation.
func (h *Handler) jsonGetAccountConfiguration(ctx context.Context, _ []byte) (any, error) {
	cfg := h.Backend.GetAccountConfiguration(ctx)
	days := cfg.DaysBeforeExpiry

	return &getAccountConfigurationOutput{
		ExpiryEvents: &expiryEventsConfiguration{DaysBeforeExpiry: &days},
	}, nil
}

func (h *Handler) jsonPutAccountConfiguration(ctx context.Context, body []byte) (any, error) {
	var input putAccountConfigurationInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	var days *int32
	if input.ExpiryEvents != nil {
		days = input.ExpiryEvents.DaysBeforeExpiry
	}

	if err := h.Backend.PutAccountConfiguration(ctx, input.IdempotencyToken, days); err != nil {
		return nil, err
	}

	return &putAccountConfigurationOutput{}, nil
}
