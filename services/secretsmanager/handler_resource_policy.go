package secretsmanager

import "context"

func (h *Handler) smResourcePolicyActions() map[string]smActionFn {
	return map[string]smActionFn{
		opGetResourcePolicy: decodeAction(
			func(ctx context.Context, input *GetResourcePolicyInput) (any, error) {
				return h.Backend.GetResourcePolicy(ctx, input)
			}),
		"PutResourcePolicy": decodeAction(
			func(ctx context.Context, input *PutResourcePolicyInput) (any, error) {
				return h.Backend.PutResourcePolicy(ctx, input)
			}),
		"DeleteResourcePolicy": decodeAction(
			func(ctx context.Context, input *DeleteResourcePolicyInput) (any, error) {
				return h.Backend.DeleteResourcePolicy(ctx, input)
			}),
		opValidateResourcePolicy: decodeAction(
			func(ctx context.Context, input *ValidateResourcePolicyInput) (any, error) {
				return h.Backend.ValidateResourcePolicy(ctx, input)
			}),
	}
}
