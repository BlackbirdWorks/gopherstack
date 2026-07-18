package secretsmanager

import (
	"context"
	"encoding/json"
)

func (h *Handler) smRandomPasswordActions() map[string]smActionFn {
	return map[string]smActionFn{
		"GetRandomPassword": func(_ context.Context, _ string, b []byte) (any, error) {
			var input GetRandomPasswordInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetRandomPassword(&input)
		},
	}
}
