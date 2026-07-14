package kms

import (
	"context"
	"encoding/json"
)

// replicateKeyAction handles ReplicateKey dispatch, including tag validation and
// copying the source key's tags (overlaid with any request-supplied tags) to the
// new replica. Tags are validated BEFORE replicating for the same reason as
// createKeyAction: a malformed tag must reject the whole request rather than
// leaving a real, untagged replica key behind.
func (h *Handler) replicateKeyAction(ctx context.Context, b []byte) (any, error) {
	var input ReplicateKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	// Capture source key ID for tag copying before we replicate.
	var sourceKeyID string
	if desc, descErr := h.Backend.DescribeKey(ctx, &DescribeKeyInput{KeyID: input.KeyID}); descErr == nil {
		sourceKeyID = desc.KeyMetadata.KeyID
	}

	out, err := h.Backend.ReplicateKey(ctx, &input)
	if err != nil {
		return nil, err
	}

	h.copyTagsToReplica(sourceKeyID, out.ReplicaKeyMetadata.KeyID, input.Tags)

	return out, nil
}

func (h *Handler) buildReplicationAndMaintenanceActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"GetParametersForImport": func(ctx context.Context, b []byte) (any, error) {
			var input GetParametersForImportInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParametersForImport(ctx, &input)
		},
		"ListKeyPolicies": func(ctx context.Context, b []byte) (any, error) {
			var input ListKeyPoliciesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListKeyPolicies(ctx, &input)
		},
		"ListKeyRotations": func(ctx context.Context, b []byte) (any, error) {
			var input ListKeyRotationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListKeyRotations(ctx, &input)
		},
		"ReplicateKey": h.replicateKeyAction,
		"RotateKeyOnDemand": func(ctx context.Context, b []byte) (any, error) {
			var input RotateKeyOnDemandInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RotateKeyOnDemand(ctx, &input)
		},
		"UpdateCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateCustomKeyStore(ctx, &input)
		},
		"UpdateKeyDescription": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateKeyDescriptionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateKeyDescription(ctx, &input)
		},
		"UpdatePrimaryRegion": func(ctx context.Context, b []byte) (any, error) {
			var input UpdatePrimaryRegionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdatePrimaryRegion(ctx, &input)
		},
	}
}
