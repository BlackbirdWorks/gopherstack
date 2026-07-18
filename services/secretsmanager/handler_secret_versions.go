package secretsmanager

import "context"

func (h *Handler) smSecretVersionsActions() map[string]smActionFn {
	return map[string]smActionFn{
		"ListSecretVersionIds": decodeAction(
			func(ctx context.Context, input *ListSecretVersionIDsInput) (any, error) {
				return h.Backend.ListSecretVersionIDs(ctx, input)
			}),
		"UpdateSecretVersionStage": decodeAction(
			func(ctx context.Context, input *UpdateSecretVersionStageInput) (any, error) {
				return h.Backend.UpdateSecretVersionStage(ctx, input)
			}),
		"BatchGetSecretValue": decodeAction(
			func(ctx context.Context, input *BatchGetSecretValueInput) (any, error) {
				return h.Backend.BatchGetSecretValue(ctx, input)
			}),
	}
}
