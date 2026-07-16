package eventbridge

import (
	"context"
	"encoding/json"
)

func (h *Handler) registryActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateRegistry": func(ctx context.Context, b []byte) (any, error) {
			var input CreateRegistryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateRegistry(ctx, input)
		},
		"DeleteRegistry": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteRegistry(ctx, input.RegistryName)
		},
		"DescribeRegistry": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeRegistry(ctx, input.RegistryName)
		},
		"ListRegistries": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			regs, next, err := h.Backend.ListRegistries(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken  string           `json:"NextToken,omitempty"`
				Registries []SchemaRegistry `json:"Registries"`
			}{Registries: regs, NextToken: next}, nil
		},
		"UpdateRegistry": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateRegistryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateRegistry(ctx, input)
		},
	}
}
