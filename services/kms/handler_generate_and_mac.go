package kms

import (
	"context"
	"encoding/json"
)

// buildGenerateAndMacActions returns dispatch entries for data key pair, MAC, and random operations.
func (h *Handler) buildGenerateAndMacActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		opGenerateDataKeyPair: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateDataKeyPairInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyPair(ctx, &input)
		},
		opGenerateDataKeyPairWithoutPlaintext: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateDataKeyPairWithoutPlaintextInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyPairWithoutPlaintext(ctx, &input)
		},
		opGenerateMac: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateMacInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateMac(ctx, &input)
		},
		"GenerateRandom": func(ctx context.Context, b []byte) (any, error) {
			var input GenerateRandomInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateRandom(ctx, &input)
		},
		opVerifyMac: func(ctx context.Context, b []byte) (any, error) {
			var input VerifyMacInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.VerifyMac(ctx, &input)
		},
	}
}
