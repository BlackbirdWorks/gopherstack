package secretsmanager

import (
	"context"
	"encoding/json/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (h *Handler) smRandomPasswordActions() map[string]smActionFn {
	return map[string]smActionFn{
		"GetRandomPassword": func(_ context.Context, _ string, b []byte) (any, error) {
			var input GetRandomPasswordInput
			if err := json.Unmarshal(b, &input, jsonx.V1()); err != nil {
				return nil, err
			}

			return h.Backend.GetRandomPassword(&input)
		},
	}
}
