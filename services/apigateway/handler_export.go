package apigateway

import (
	"encoding/json"
	"fmt"
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

			encoded, err := json.Marshal(export)
			if err != nil {
				return 0, nil, err
			}

			// AWS's API docs (API_GetExport.html) document ContentDisposition
			// as a real response header but do not specify its value's format
			// (unlike GetSdk's ContentDisposition, which AWS also leaves
			// unspecified but this emulator already synthesizes in sdk.go);
			// this filename follows the same synthesized convention.
			disposition := fmt.Sprintf(
				`attachment; filename="%s-%s-%s.json"`, input.RestAPIID, input.StageName, input.ExportType,
			)

			return http.StatusOK, &rawBinaryResponse{
				contentType:        contentTypeJSON,
				contentDisposition: disposition,
				body:               encoded,
			}, nil
		},
	}
}
