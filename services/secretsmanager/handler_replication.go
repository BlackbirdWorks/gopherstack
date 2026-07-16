package secretsmanager

import "context"

func (h *Handler) smReplicationActions() map[string]smActionFn {
	return map[string]smActionFn{
		"ReplicateSecretToRegions": decodeAction(
			func(ctx context.Context, input *ReplicateSecretToRegionsInput) (any, error) {
				return h.Backend.ReplicateSecretToRegions(ctx, input)
			}),
		"RemoveRegionsFromReplication": decodeAction(
			func(ctx context.Context, input *RemoveRegionsFromReplicationInput) (any, error) {
				return h.Backend.RemoveRegionsFromReplication(ctx, input)
			}),
		"StopReplicationToReplica": decodeAction(
			func(ctx context.Context, input *StopReplicationToReplicaInput) (any, error) {
				return h.Backend.StopReplicationToReplica(ctx, input)
			}),
	}
}
