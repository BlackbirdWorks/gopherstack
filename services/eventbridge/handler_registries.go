package eventbridge

import (
	"context"
	"encoding/json"
)

func (h *Handler) registryActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateRegistry: func(ctx context.Context, b []byte) (any, error) {
			var input CreateRegistryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateRegistry(ctx, input)
		},
		opDeleteRegistry: func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteRegistry(ctx, input.RegistryName)
		},
		opDescribeRegistry: func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeRegistry(ctx, input.RegistryName)
		},
		opListRegistries: func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
				Limit      int32  `json:"Limit"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			regs, next, err := h.Backend.ListRegistries(ctx, input.NamePrefix, input.NextToken, int(input.Limit))
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken  string           `json:"NextToken,omitempty"`
				Registries []SchemaRegistry `json:"Registries"`
			}{Registries: regs, NextToken: next}, nil
		},
		opUpdateRegistry: func(ctx context.Context, b []byte) (any, error) {
			var input UpdateRegistryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateRegistry(ctx, input)
		},
	}
}
