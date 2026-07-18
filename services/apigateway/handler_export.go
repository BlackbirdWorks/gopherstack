package apigateway

import (
	"encoding/json"
	"net/http"
)

const opGetExport = "GetExport"

type getExportInput struct {
	RestAPIID  string `json:"restApiId"`
	StageName  string `json:"stageName"`
	ExportType string `json:"exportType"`
}

// exportActions returns the action map for the OpenAPI export operation.
func (h *Handler) exportActions() map[string]actionFn {
	return map[string]actionFn{
		opGetExport: func(b []byte) (int, any, error) {
			var input getExportInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			export, err := h.Backend.GetExport(input.RestAPIID, input.StageName, input.ExportType)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, export, nil
		},
	}
}
