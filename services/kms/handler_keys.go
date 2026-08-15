package kms

import (
	"context"
	"encoding/json"
)

// buildKeyLifecycleActions returns dispatch entries for key creation, description, listing and deletion.
func (h *Handler) buildKeyLifecycleActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateKey": h.createKeyAction,
		opDescribeKey: unmarshalAction(
			func(ctx context.Context, i *DescribeKeyInput) (any, error) { return h.Backend.DescribeKey(ctx, i) },
		),
		"ListKeys": unmarshalAction(func(ctx context.Context, i *ListKeysInput) (any, error) {
			return h.Backend.ListKeys(ctx, i)
		}),
		"DisableKey": unmarshalAction(
			func(ctx context.Context, i *DisableKeyInput) (any, error) {
				return struct{}{}, h.Backend.DisableKey(ctx, i)
			},
		),
		"EnableKey": unmarshalAction(
			func(ctx context.Context, i *EnableKeyInput) (any, error) {
				return struct{}{}, h.Backend.EnableKey(ctx, i)
			},
		),
		"ScheduleKeyDeletion": unmarshalAction(
			func(ctx context.Context, i *ScheduleKeyDeletionInput) (any, error) {
				return h.Backend.ScheduleKeyDeletion(ctx, i)
			},
		),
		"CancelKeyDeletion": unmarshalAction(
			func(ctx context.Context, i *CancelKeyDeletionInput) (any, error) {
				return h.Backend.CancelKeyDeletion(ctx, i)
			},
		),
		"ImportKeyMaterial": unmarshalAction(
			func(ctx context.Context, i *ImportKeyMaterialInput) (any, error) {
				if err := h.Backend.ImportKeyMaterial(ctx, i); err != nil {
					return nil, err
				}

				return ImportKeyMaterialOutput{KeyID: i.KeyID}, nil
			},
		),
		"DeleteImportedKeyMaterial": unmarshalAction(
			func(ctx context.Context, i *DeleteImportedKeyMaterialInput) (any, error) {
				if err := h.Backend.DeleteImportedKeyMaterial(ctx, i); err != nil {
					return nil, err
				}

				return DeleteImportedKeyMaterialOutput{KeyID: i.KeyID}, nil
			},
		),
	}
}

// createKeyAction handles CreateKey dispatch, including tag validation.
// Tags are validated BEFORE the key is created: AWS validates the entire CreateKey
// request atomically, and creating the key first would leak an orphaned, untagged
// key (with no reachable KeyId, since the response is discarded on error) into the
// backend whenever the caller supplied a malformed tag.
func (h *Handler) createKeyAction(ctx context.Context, b []byte) (any, error) {
	var input CreateKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	out, err := h.Backend.CreateKey(ctx, &input)
	if err != nil {
		return nil, err
	}

	if tagErr := h.applyInputTags(out.KeyMetadata.KeyID, input.Tags); tagErr != nil {
		return nil, tagErr
	}

	return out, nil
}
