package kms

import (
	"context"
	"encoding/json"
)

// buildCustomKeyStoreActions returns dispatch entries for custom key store and ECDH operations.
func (h *Handler) buildCustomKeyStoreActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input CreateCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateCustomKeyStore(ctx, &input)
		},
		"DeleteCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteCustomKeyStore(ctx, &input)
		},
		"DescribeCustomKeyStores": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeCustomKeyStoresInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeCustomKeyStores(ctx, &input)
		},
		"ConnectCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input ConnectCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.ConnectCustomKeyStore(ctx, &input)
		},
		"DisconnectCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input DisconnectCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisconnectCustomKeyStore(ctx, &input)
		},
		opDeriveSharedSecret: func(ctx context.Context, b []byte) (any, error) {
			var input DeriveSharedSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeriveSharedSecret(ctx, &input)
		},
	}
}
