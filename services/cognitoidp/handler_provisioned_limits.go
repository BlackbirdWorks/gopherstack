package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// toLimitWireType converts a backend ProvisionedLimit into its wire shape.
func toLimitWireType(pl *ProvisionedLimit) limitWireType {
	return limitWireType{
		LimitDefinition: limitDefinitionWireType{
			LimitClass: pl.LimitClass,
			Attributes: map[string]string{"Category": pl.Category},
		},
		FreeLimitValue:        pl.FreeLimitValue,
		ProvisionedLimitValue: pl.ProvisionedLimitValue,
	}
}

func (h *Handler) handleGetProvisionedLimit(
	_ context.Context,
	in *getProvisionedLimitInput,
) (*getProvisionedLimitOutput, error) {
	pl, err := h.Backend.GetProvisionedLimit(in.LimitDefinition.LimitClass, in.LimitDefinition.Attributes)
	if err != nil {
		return nil, err
	}

	return &getProvisionedLimitOutput{Limit: toLimitWireType(pl)}, nil
}

func (h *Handler) handleUpdateProvisionedLimit(
	_ context.Context,
	in *updateProvisionedLimitInput,
) (*updateProvisionedLimitOutput, error) {
	pl, err := h.Backend.UpdateProvisionedLimit(
		in.LimitDefinition.LimitClass, in.LimitDefinition.Attributes, in.RequestedLimitValue,
	)
	if err != nil {
		return nil, err
	}

	return &updateProvisionedLimitOutput{Limit: toLimitWireType(pl)}, nil
}

func (h *Handler) provisionedLimitsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetProvisionedLimit":    service.WrapOp(h.handleGetProvisionedLimit),
		"UpdateProvisionedLimit": service.WrapOp(h.handleUpdateProvisionedLimit),
	}
}
