package kms

import (
	"context"
	"encoding/json"
)

// buildCryptoActions returns dispatch entries for encrypt, decrypt, sign, verify, and data-key operations.
func (h *Handler) buildCryptoActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		opEncrypt: func(ctx context.Context, b []byte) (any, error) {
			var input EncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Encrypt(ctx, &input)
		},
		opDecrypt: func(ctx context.Context, b []byte) (any, error) {
			var input DecryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Decrypt(ctx, &input)
		},
		opGenerateDataKey: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateDataKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKey(ctx, &input)
		},
		opGenerateDataKeyWithoutPlaintext: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateDataKeyWithoutPlaintextInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyWithoutPlaintext(ctx, &input)
		},
		"ReEncrypt": func(ctx context.Context, b []byte) (any, error) {
			var input ReEncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ReEncrypt(ctx, &input)
		},
		opSign: func(ctx context.Context, b []byte) (any, error) {
			var input SignInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Sign(ctx, &input)
		},
		opVerify: func(ctx context.Context, b []byte) (any, error) {
			var input VerifyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Verify(ctx, &input)
		},
		opGetPublicKey: func(ctx context.Context, b []byte) (any, error) {
			var input GetPublicKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetPublicKey(ctx, &input)
		},
	}
}
