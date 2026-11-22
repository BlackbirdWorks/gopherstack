package cloudwatchlogs

import (
	"context"
	"strings"
)

const inventoryPageSize = 50

// InventoryResources lists every log group of the request Region plus every ARN-keyed tagged resource.
func (h *Handler) InventoryResources(ctx context.Context) []TaggedEntry {
	out := h.TaggedResources()

	seen := make(map[string]struct{}, len(out))
	for _, e := range out {
		seen[e.ARN] = struct{}{}
	}

	token := ""

	for {
		groups, next, err := h.Backend.DescribeLogGroups(ctx, "", token, "", inventoryPageSize)
		if err != nil {
			return out
		}

		for i := range groups {
			groupARN := strings.TrimSuffix(groups[i].Arn, ":*")
			if _, ok := seen[groupARN]; !ok {
				seen[groupARN] = struct{}{}
				out = append(out, TaggedEntry{ARN: groupARN})
			}
		}

		if next == "" {
			return out
		}

		token = next
	}
}
