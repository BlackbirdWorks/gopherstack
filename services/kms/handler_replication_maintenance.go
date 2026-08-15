package kms

import (
	"context"
	"encoding/json"
	"sort"
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

	replicaKeyID := out.ReplicaKeyMetadata.KeyID

	h.copyTagsToReplica(sourceKeyID, replicaKeyID, input.Tags)

	if replicaTags := h.getTags(replicaKeyID); len(replicaTags) > 0 {
		out.ReplicaTags = tagsFromMap(replicaTags)
	}

	if policyOut, policyErr := h.Backend.GetKeyPolicy(
		ctx, &GetKeyPolicyInput{KeyID: replicaKeyID},
	); policyErr == nil {
		out.ReplicaPolicy = policyOut.Policy
	}

	return out, nil
}

// tagsFromMap converts a tag key/value map to the []Tag wire shape, sorted by
// key for deterministic output.
func tagsFromMap(kv map[string]string) []Tag {
	out := make([]Tag, 0, len(kv))
	for k, v := range kv {
		out = append(out, Tag{TagKey: k, TagValue: v})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].TagKey < out[j].TagKey })

	return out
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
