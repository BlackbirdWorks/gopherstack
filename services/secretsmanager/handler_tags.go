package secretsmanager

import (
	"context"
	"encoding/json"
)

func (h *Handler) smTagActions() map[string]smActionFn {
	return map[string]smActionFn{
		"TagResource": func(ctx context.Context, _ string, b []byte) (any, error) {
			var input TagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.TagResource(ctx, &input)
		},
		"UntagResource": func(ctx context.Context, _ string, b []byte) (any, error) {
			var input UntagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UntagResource(ctx, &input)
		},
	}
}
