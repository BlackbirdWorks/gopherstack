package apigateway

import (
	"encoding/json"
	"net/http"
)

const opUpdateUsage = "UpdateUsage"

// usageActions returns the action map for the usage-plan usage data operations
// (GetUsage / UpdateUsage).
func (h *Handler) usageActions() map[string]actionFn {
	return map[string]actionFn{
		opGetUsage: func(b []byte) (int, any, error) {
			var input GetUsageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GetUsage(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opUpdateUsage: func(b []byte) (int, any, error) {
			var raw map[string]string
			if err := json.Unmarshal(b, &raw); err != nil {
				return 0, nil, err
			}

			usagePlanID := raw[keyUsagePlanID]
			keyID := raw[keyKeyID]
			delete(raw, keyUsagePlanID)
			delete(raw, keyKeyID)

			usage, err := h.Backend.UpdateUsage(usagePlanID, keyID, raw)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, usage, nil
		},
	}
}
