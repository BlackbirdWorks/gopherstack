package stepfunctions

import "context"

const inventoryPageSize = 1000

// InventoryResources lists every state machine, alias and activity of the request Region plus every tagged ARN
// of the tag store.
func (h *Handler) InventoryResources(ctx context.Context) []TaggedEntry {
	arns := h.resourceARNs(ctx)
	out := h.TaggedResources()

	seen := make(map[string]struct{}, len(out))
	for _, e := range out {
		seen[e.ARN] = struct{}{}
	}

	for _, a := range arns {
		if _, ok := seen[a]; !ok {
			seen[a] = struct{}{}
			out = append(out, TaggedEntry{ARN: a})
		}
	}

	return out
}

func (h *Handler) resourceARNs(ctx context.Context) []string {
	var out []string

	machines := drainPages(func(token string) ([]StateMachine, string, error) {
		return h.Backend.ListStateMachines(ctx, token, inventoryPageSize)
	})

	for i := range machines {
		out = append(out, machines[i].StateMachineArn)

		aliases := drainPages(func(token string) ([]StateMachineAlias, string, error) {
			return h.Backend.ListStateMachineAliases(machines[i].StateMachineArn, token, inventoryPageSize)
		})

		for j := range aliases {
			out = append(out, aliases[j].StateMachineAliasArn)
		}
	}

	activities := drainPages(func(token string) ([]Activity, string, error) {
		return h.Backend.ListActivities(ctx, token, inventoryPageSize)
	})

	for i := range activities {
		out = append(out, activities[i].ActivityArn)
	}

	return out
}

func drainPages[T any](list func(token string) ([]T, string, error)) []T {
	var out []T

	token := ""

	for {
		items, next, err := list(token)
		if err != nil {
			return out
		}

		out = append(out, items...)

		if next == "" {
			return out
		}

		token = next
	}
}
